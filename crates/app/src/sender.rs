//! Sender 侧逻辑
//!
//! 从 walkdir 接收 entry，通过 transport 发送给 Receiver 执行写入。
//! 单进程模式下发送 `CopyEntry` 命令（Receiver 自行读写）。
//! 双进程模式下发送 `FileData` 数据流（Phase 3）。

// 标准库
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// 外部 crate
use dashmap::DashMap;
use data_mover::qos::QosManager;
use data_mover::{EntryEnum, StorageEntryMessage, StorageEnum};
use tracing::{error, info};
use transport::message::SenderMsg;
use transport::traits::SenderTransport;

// 内部模块
use crate::tar_pack;

/// Sender worker 配置
pub struct SenderWorkerConfig {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    pub large_file_log_threshold: u64,
}

/// 单个 Sender worker task（S1）
///
/// 从共享的 `walkdir_iter` 拉取 entry，通过 transport 发送给 Receiver。
/// 单进程模式：发送 `CopyEntry` 命令。
/// 双进程模式：读取源文件数据后发送 `FileData` 流（Phase 3）。
#[allow(clippy::too_many_arguments)]
pub async fn sender_worker(
    worker_id: usize, walkdir_iter: data_mover::AsyncReceiver<StorageEntryMessage>, src_storage: Arc<StorageEnum>,
    dest_storage: Arc<StorageEnum>, transport: Arc<dyn SenderTransport>, config: &SenderWorkerConfig,
    qos_manager: Option<QosManager>, bytes_tracker: Option<Arc<AtomicU64>>,
    object_buffers: Arc<DashMap<String, Vec<Arc<EntryEnum>>>>, active_entry_counter: Arc<AtomicUsize>,
    entry_counter: Arc<AtomicUsize>, size_counter: Arc<AtomicU64>,
) {
    info!("[Sender Worker {}] Started", worker_id);

    while let Some(msg) = walkdir_iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => {
                active_entry_counter.fetch_add(1, Ordering::Relaxed);
                entry_counter.fetch_add(1, Ordering::Relaxed);
                size_counter.fetch_add(entry.get_size(), Ordering::Relaxed);

                if entry.get_size() > config.large_file_log_threshold {
                    info!(
                        "[Sender Worker {}] Processing: {:?}, size={}",
                        worker_id,
                        entry.get_relative_path(),
                        entry.get_size()
                    );
                }

                if entry.get_version_id().is_some() {
                    // S3 多版本对象：缓冲收集后一次性处理
                    handle_versioned_entry(
                        worker_id,
                        &entry,
                        &transport,
                        &src_storage,
                        &dest_storage,
                        &object_buffers,
                        config,
                        qos_manager.clone(),
                        bytes_tracker.clone(),
                    )
                    .await;
                } else {
                    // 非版本化对象：直接发送给 Receiver
                    if let Err(e) = transport.send(SenderMsg::CopyEntry { entry: entry.clone() }).await {
                        error!(
                            "[Sender Worker {}] Failed to send CopyEntry for {:?}: {}",
                            worker_id,
                            entry.get_relative_path(),
                            e
                        );
                    }
                }

                active_entry_counter.fetch_sub(1, Ordering::Relaxed);
            }

            StorageEntryMessage::Packaged(entry) => {
                active_entry_counter.fetch_add(1, Ordering::Relaxed);
                entry_counter.fetch_add(1, Ordering::Relaxed);

                info!(
                    "[Sender Worker {}] Packing directory {:?}",
                    worker_id,
                    entry.get_relative_path()
                );

                // tar 打包在 Sender 侧完成（需要 src+dest storage）
                match tar_pack::pack_directory(
                    &src_storage,
                    &dest_storage,
                    &entry,
                    config.is_source_reserved,
                    qos_manager.clone(),
                    bytes_tracker.clone(),
                )
                .await
                {
                    Ok((tar_entry, manifest)) => {
                        if let Err(e) = transport
                            .send(SenderMsg::TarPacked {
                                tar_entry: Arc::new(tar_entry),
                                manifest_entries: manifest,
                            })
                            .await
                        {
                            error!("[Sender Worker {}] Failed to send TarPacked: {}", worker_id, e);
                        }
                    }
                    Err(e) => {
                        error!(
                            "[Sender Worker {}] Pack failed for {:?}: {}",
                            worker_id,
                            entry.get_relative_path(),
                            e
                        );
                        let _ = transport
                            .send(SenderMsg::EntryError {
                                path: entry.get_relative_path().to_path_buf(),
                                reason: format!("{e}"),
                                // 单进程模式：tar 打包无 ndx 概念（不走双进程 ndx 状态机）
                                ndx: None,
                            })
                            .await;
                    }
                }

                active_entry_counter.fetch_sub(1, Ordering::Relaxed);
            }

            StorageEntryMessage::Error { path, reason, .. } => {
                error!(
                    "[Sender Worker {}] Walkdir error for {}: {}",
                    worker_id,
                    path.display(),
                    reason
                );
            }

            _ => {}
        }
    }

    info!("[Sender Worker {}] Completed", worker_id);
}

/// 处理 S3 多版本对象：缓冲所有版本后统一发送
#[allow(clippy::too_many_arguments)]
async fn handle_versioned_entry(
    worker_id: usize, entry: &Arc<EntryEnum>, transport: &Arc<dyn SenderTransport>, _src_storage: &Arc<StorageEnum>,
    dest_storage: &Arc<StorageEnum>, object_buffers: &Arc<DashMap<String, Vec<Arc<EntryEnum>>>>,
    _config: &SenderWorkerConfig, _qos_manager: Option<QosManager>, _bytes_tracker: Option<Arc<AtomicU64>>,
) {
    let object_key = entry.get_relative_path().to_string_lossy().to_string();

    // 缓冲版本
    let mut collected_versions = None;
    {
        let mut versions = object_buffers.entry(object_key.clone()).or_default();
        versions.push(entry.clone());

        if let Some(version_count) = entry.get_version_count()
            && versions.len() >= version_count as usize
        {
            collected_versions = Some(versions.clone());
            // 清理缓冲区
            drop(versions);
            object_buffers.remove(&object_key);
        }
    }

    // 所有版本收集完毕，逐版本发送
    if let Some(mut versions) = collected_versions {
        // 按 mtime 升序排列（旧→新）
        versions.sort_by_key(|e| e.get_mtime());

        let is_dest_versioned = matches!(dest_storage.as_ref(), StorageEnum::S3(_));

        if is_dest_versioned {
            // 目标支持版本化：发送所有版本
            for ver in &versions {
                if let Err(e) = transport.send(SenderMsg::CopyEntry { entry: ver.clone() }).await {
                    error!(
                        "[Sender Worker {}] Failed to send versioned CopyEntry for {}: {}",
                        worker_id, object_key, e
                    );
                }
            }
        } else {
            // 目标不支持版本化：仅发送最新版本
            if let Some(latest) = versions.last()
                && let Err(e) = transport.send(SenderMsg::CopyEntry { entry: latest.clone() }).await
            {
                error!(
                    "[Sender Worker {}] Failed to send latest version CopyEntry for {}: {}",
                    worker_id, object_key, e
                );
            }
        }
    }
}
