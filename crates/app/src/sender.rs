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
use storage_adapter::qos::QosManager;
use storage_adapter::{AsyncReceiver, EntryEnum, StorageEntryMessage, StorageEnum};
use tracing::{error, info};
use transport::message::SenderMsg;
use transport::traits::SenderTransport;

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result};
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
    worker_id: usize, walkdir_iter: AsyncReceiver<StorageEntryMessage>, src_storage: Arc<StorageEnum>,
    dest_storage: Arc<StorageEnum>, transport: Arc<dyn SenderTransport>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>, config: &SenderWorkerConfig,
    qos_manager: Option<QosManager>, bytes_tracker: Option<Arc<AtomicU64>>,
    object_buffers: Arc<DashMap<String, Vec<Arc<EntryEnum>>>>, active_entry_counter: Arc<AtomicUsize>,
    entry_counter: Arc<AtomicUsize>, size_counter: Arc<AtomicU64>,
) -> Result<()> {
    info!("[Sender Worker {}] Started", worker_id);
    let mut failure = None;

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

                let result = if entry.get_version_id().is_some() {
                    // S3 多版本对象：缓冲收集后一次性处理
                    handle_versioned_entry(worker_id, &entry, &transport, &dest_storage, &object_buffers).await
                } else {
                    send_entry(worker_id, &transport, &entry).await
                };
                if let Err(error) = result {
                    failure.get_or_insert(error);
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

                if let Err(error) = send_packaged_entry(
                    worker_id,
                    &entry,
                    &src_storage,
                    &dest_storage,
                    &transport,
                    config,
                    qos_manager.clone(),
                    bytes_tracker.clone(),
                )
                .await
                {
                    failure.get_or_insert(error);
                }

                active_entry_counter.fetch_sub(1, Ordering::Relaxed);
            }

            StorageEntryMessage::Error {
                ref path, ref reason, ..
            } => {
                error!(
                    "[Sender Worker {}] Walkdir error for {}: {}",
                    worker_id,
                    path.display(),
                    reason
                );
                // 全量模式下与增量分支（orchestrator.rs 的 scan worker）对齐：把 walkdir
                // 扫描错误广播出去，计入 ErrorStats.scan、进 ERROR STATISTICS 报表与
                // progress error 计数（issue #57，此前只打日志，报表恒不体现）。
                broadcaster.broadcast(msg.clone()).await;
                failure.get_or_insert_with(|| AppError::ScanError(reason.clone()));
            }

            _ => {}
        }
    }

    info!("[Sender Worker {}] Completed", worker_id);
    failure.map_or(Ok(()), Err)
}

async fn send_entry(worker_id: usize, transport: &Arc<dyn SenderTransport>, entry: &Arc<EntryEnum>) -> Result<()> {
    if let Err(error) = transport.send(SenderMsg::CopyEntry { entry: entry.clone() }).await {
        error!(
            "[Sender Worker {}] Failed to send CopyEntry for {:?}: {}",
            worker_id,
            entry.get_relative_path(),
            error
        );
        return Err(error.into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_packaged_entry(
    worker_id: usize, entry: &Arc<EntryEnum>, src_storage: &Arc<StorageEnum>, dest_storage: &Arc<StorageEnum>,
    transport: &Arc<dyn SenderTransport>, config: &SenderWorkerConfig, qos_manager: Option<QosManager>,
    bytes_tracker: Option<Arc<AtomicU64>>,
) -> Result<()> {
    match tar_pack::pack_directory(
        src_storage,
        dest_storage,
        entry,
        config.is_source_reserved,
        qos_manager,
        bytes_tracker,
    )
    .await
    {
        Ok((tar_entry, manifest_entries)) => {
            transport
                .send(SenderMsg::TarPacked {
                    tar_entry: Arc::new(tar_entry),
                    manifest_entries,
                })
                .await?;
            Ok(())
        }
        Err(error) => {
            error!(
                "[Sender Worker {}] Pack failed for {:?}: {}",
                worker_id,
                entry.get_relative_path(),
                error
            );
            if let Err(notification_error) = transport
                .send(SenderMsg::EntryError {
                    path: entry.get_relative_path().to_path_buf(),
                    reason: error.to_string(),
                    ndx: None,
                })
                .await
            {
                error!(
                    "[Sender Worker {}] Failed to send pack error: {}",
                    worker_id, notification_error
                );
            }
            // 通知失败不能覆盖实际打包的原始错误。
            Err(error)
        }
    }
}

/// 处理 S3 多版本对象：缓冲所有版本后统一发送
async fn handle_versioned_entry(
    worker_id: usize, entry: &Arc<EntryEnum>, transport: &Arc<dyn SenderTransport>, dest_storage: &Arc<StorageEnum>,
    object_buffers: &Arc<DashMap<String, Vec<Arc<EntryEnum>>>>,
) -> Result<()> {
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

        let mut failure = None;
        if dest_storage.is_bucket_versioned().await? {
            // 目标支持版本化：旧到新发送所有版本，失败后仍排空已收齐的版本组。
            for version in &versions {
                if let Err(error) = send_entry(worker_id, transport, version).await {
                    failure.get_or_insert(error);
                }
            }
        } else if let Some(latest) = versions.last() {
            // 目标不支持版本化：仅发送最新版本。
            send_entry(worker_id, transport, latest).await?;
        }
        return failure.map_or(Ok(()), Err);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::error::Result;
    use std::io::{Error as IoError, ErrorKind};
    use std::path::PathBuf;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use storage_adapter::error::StorageError;
    use storage_adapter::{AsyncReceiver, ErrorEvent, S3Entry};
    use transport::error::{Result as TransportResult, TransportError};
    use transport::message::ReceiverMsg;

    use crate::storage_factory::{StorageRole, create_storage_for_role};
    use tempfile::tempdir;
    use transport::in_process::create_in_process_pair;

    use super::*;

    async fn create_storage(path: &str, block_size: Option<u64>, _ensure_dir: bool) -> Result<StorageEnum> {
        create_storage_for_role(path, block_size, false, StorageRole::Source).await
    }

    /// 全量模式 walkdir 扫描错误应与增量分支（`orchestrator.rs` 的 scan worker）对齐：
    /// 广播给 `broadcaster` 订阅者，计入 `ErrorStats.scan`、进 ERROR STATISTICS 报表
    /// （此前只打日志，报表恒不体现该错误，见 issue #57）。
    #[tokio::test]
    async fn sender_worker_broadcasts_walkdir_error() {
        let (tx, rx) = async_channel::unbounded::<StorageEntryMessage>();
        let walkdir_iter = AsyncReceiver::new(rx);
        tx.send(StorageEntryMessage::Error {
            event: ErrorEvent::Scan,
            path: PathBuf::from("bad/path.txt"),
            entry: None,
            reason: "walkdir stat failed".to_string(),
        })
        .await
        .unwrap();
        drop(tx);

        let (sender_transport, _receiver_transport) = create_in_process_pair();
        let transport: Arc<dyn SenderTransport> = Arc::new(sender_transport);

        let tmp = tempdir().unwrap();
        let storage = Arc::new(create_storage(tmp.path().to_str().unwrap(), None, false).await.unwrap());

        let config = SenderWorkerConfig {
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            large_file_log_threshold: u64::MAX,
        };

        let mut broadcaster = BroadcastForwarder::new(16);
        let mut rx_bc = broadcaster.subscribe();

        let result = sender_worker(
            0,
            walkdir_iter,
            storage.clone(),
            storage,
            transport,
            &broadcaster,
            &config,
            None,
            None,
            Arc::new(DashMap::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
        .await;
        assert!(matches!(result, Err(AppError::ScanError(_))));

        match rx_bc.try_recv() {
            Ok(StorageEntryMessage::Error {
                path, reason, event, ..
            }) => {
                assert_eq!(path, PathBuf::from("bad/path.txt"));
                assert_eq!(reason, "walkdir stat failed");
                assert_eq!(event, ErrorEvent::Scan);
            }
            other => panic!("expected broadcast StorageEntryMessage::Error, got {other:?}"),
        }
    }

    #[derive(Default)]
    struct RecordingTransport {
        sent: Mutex<Vec<SenderMsg>>,
        failures: usize,
    }

    #[async_trait]
    impl SenderTransport for RecordingTransport {
        async fn send(&self, msg: SenderMsg) -> TransportResult<()> {
            let mut sent = self.sent.lock().unwrap();
            let index = sent.len();
            sent.push(msg);
            if index < self.failures {
                if index == 0 {
                    return Err(StorageError::IoError(IoError::new(ErrorKind::TimedOut, "first send")).into());
                }
                return Err(TransportError::ChannelClosed);
            }
            Ok(())
        }

        async fn recv(&self) -> Option<ReceiverMsg> {
            None
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    fn entry(path: &str, mtime: i64, version: Option<&str>, count: Option<u32>) -> Arc<EntryEnum> {
        Arc::new(EntryEnum::S3(S3Entry {
            name: path.into(),
            relative_path: path.into(),
            extension: None,
            is_dir: false,
            size: 7,
            mtime,
            tags: None,
            version_id: version.map(str::to_owned),
            is_latest: false,
            is_delete_marker: false,
            version_count: count,
        }))
    }

    async fn run_messages(
        messages: Vec<StorageEntryMessage>, transport: Arc<dyn SenderTransport>, storage: Arc<StorageEnum>,
        active: Arc<AtomicUsize>, count: Arc<AtomicUsize>,
    ) -> Result<()> {
        let (tx, rx) = async_channel::bounded(messages.len().max(1));
        for message in messages {
            tx.send(message).await.unwrap();
        }
        drop(tx);
        let config = SenderWorkerConfig {
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            large_file_log_threshold: u64::MAX,
        };
        sender_worker(
            0,
            AsyncReceiver::new(rx),
            storage.clone(),
            storage,
            transport,
            &BroadcastForwarder::new(16),
            &config,
            None,
            None,
            Arc::new(DashMap::new()),
            active,
            count,
            Arc::new(AtomicU64::new(0)),
        )
        .await
    }

    #[tokio::test]
    async fn sender_retains_first_typed_send_error_and_drains_later_entries() {
        let tmp = tempdir().unwrap();
        let storage = Arc::new(create_storage(tmp.path().to_str().unwrap(), None, false).await.unwrap());
        let transport = Arc::new(RecordingTransport {
            failures: 2,
            ..RecordingTransport::default()
        });
        let active = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let result = run_messages(
            ["first", "second", "third"]
                .into_iter()
                .map(|path| StorageEntryMessage::Scanned(entry(path, 1, None, None)))
                .collect(),
            transport.clone(),
            storage,
            active.clone(),
            count.clone(),
        )
        .await;
        assert!(
            matches!(result, Err(AppError::TransportError(TransportError::StorageError(
            StorageError::IoError(error)
        ))) if error.kind() == ErrorKind::TimedOut)
        );
        assert_eq!(transport.sent.lock().unwrap().len(), 3);
        assert_eq!(count.load(Ordering::Relaxed), 3);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn latest_version_send_failure_is_returned_after_complete_sorted_group() {
        let tmp = tempdir().unwrap();
        let storage = Arc::new(create_storage(tmp.path().to_str().unwrap(), None, false).await.unwrap());
        let recorded = Arc::new(RecordingTransport {
            failures: 1,
            ..RecordingTransport::default()
        });
        let transport: Arc<dyn SenderTransport> = recorded.clone();
        let buffers = Arc::new(DashMap::new());
        for (mtime, version) in [(30, "latest"), (10, "oldest")] {
            handle_versioned_entry(
                0,
                &entry("object", mtime, Some(version), Some(3)),
                &transport,
                &storage,
                &buffers,
            )
            .await
            .unwrap();
            assert!(recorded.sent.lock().unwrap().is_empty());
        }
        let result = handle_versioned_entry(
            0,
            &entry("object", 20, Some("middle"), Some(3)),
            &transport,
            &storage,
            &buffers,
        )
        .await;
        assert!(matches!(result, Err(AppError::TransportError(_))));
        assert!(buffers.is_empty());
        let sent = recorded.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(matches!(&sent[0], SenderMsg::CopyEntry { entry } if entry.get_version_id() == Some("latest")));
    }

    #[tokio::test]
    async fn pack_failure_keeps_storage_cause_when_error_notification_also_fails() {
        let tmp = tempdir().unwrap();
        let storage = Arc::new(create_storage(tmp.path().to_str().unwrap(), None, false).await.unwrap());
        let transport = Arc::new(RecordingTransport {
            failures: 2,
            ..RecordingTransport::default()
        });
        let active = Arc::new(AtomicUsize::new(0));
        let result = run_messages(
            vec![
                StorageEntryMessage::Packaged(entry("missing-directory", 1, None, None)),
                StorageEntryMessage::Scanned(entry("later", 2, None, None)),
            ],
            transport.clone(),
            storage,
            active.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
        .await;
        assert!(matches!(result, Err(AppError::StorageError(_))));
        let sent = transport.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(matches!(&sent[0], SenderMsg::EntryError { .. }));
        assert!(matches!(&sent[1], SenderMsg::CopyEntry { .. }));
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }
}
