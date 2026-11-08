//! 增量复制 worker 的任务执行与存储连接；编排入口负责扫描、阶段屏障和最终收尾。
use super::{create_storage_pair_with_retry, mount_concurrency_for_pair, wait_for_mount_init};
use crate::{
    broadcast::BroadcastForwarder,
    config::SyncJobConfig,
    copy_history::{HistoryCopyContext, HistoryCopyPolicy, HistoryCopyWorker},
    error::{AppError, Result},
    file_copy::CopyOptions,
    file_copy::selection::SkipTracker,
    sync::{ResumeOpts, process_entry, process_metadata_only_entry},
    tar_pack,
    task_stream::AsyncReceiver,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use task_model::{
    ChangeKind, ErrorEvent, ObservedEntryExt, StorageEntryMessage, TargetRetention, TargetRetentionReason,
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tracing::{Instrument, error, info, info_span};

pub(super) async fn spawn_handlers(
    c: &SyncJobConfig, handler_inputs: Vec<AsyncReceiver<StorageEntryMessage>>,
    broadcaster: BroadcastForwarder<StorageEntryMessage>, options: CopyOptions, block_size: Option<usize>,
    history_policy: HistoryCopyPolicy,
) -> Vec<JoinHandle<Result<()>>> {
    let copy_concurrency = handler_inputs.len();
    let is_source_reserved = c.source_cleanup.retains_source();
    // ── 10.5. 创建 mount 限流信号量 + 完成计数器 ──
    // 容量按 storage 类型选择（详见 sync 路径同名注释）。
    let mount_semaphore = Arc::new(Semaphore::new(mount_concurrency_for_pair(&c.src_path, &c.dest_path)));
    let alive_handlers = Arc::new(AtomicUsize::new(0));
    let mount_done_handlers = Arc::new(AtomicUsize::new(0));

    // ── 11. 启动 N 个 handler worker（MPMC 竞争消费，每个 worker 独立 StoragePair） ──
    let mut handler_handles = Vec::new();
    let src_path_str = c.src_path.clone();
    let dest_path_str = c.dest_path.clone();

    for (handler_id, rx) in handler_inputs.into_iter().enumerate() {
        let src_path_c = src_path_str.clone();
        let dest_path_c = dest_path_str.clone();
        let bc = broadcaster.clone();
        let qos = options.qos.clone();
        let bt = options.bytes_counter.clone();
        let transfer_options = options.clone();
        let enable_integrity_check = options.enable_integrity_check;
        let enable_acl = c.enable_acl;
        let resume_opts = ResumeOpts {
            job_id: c.job_id.clone(),
            job_dir: c.job_dir.clone(),
            transfer_mode: options.transfer_mode,
            cancel: options.cancel.clone(),
        };
        let mount_sem = mount_semaphore.clone();
        let alive_counter = alive_handlers.clone();
        let done_counter = mount_done_handlers.clone();

        let span = info_span!("inc_handler_worker", handler_id);
        let handle = tokio::spawn(
            async move {
                // 每个 worker 创建独立 StoragePair（带限流重试，防止 portmapper 过载）
                let storage_pair = match create_storage_pair_with_retry(
                    &mount_sem,
                    &src_path_c,
                    &dest_path_c,
                    block_size,
                    &format!("Handler {handler_id}"),
                )
                .await
                {
                    Ok(pair) => pair,
                    Err(e) => {
                        error!(
                            "Handler worker {}: Failed to create StoragePair after retries: {}",
                            handler_id, e
                        );
                        done_counter.fetch_add(1, Ordering::Release);
                        return Err(e);
                    }
                };
                alive_counter.fetch_add(1, Ordering::Relaxed);
                done_counter.fetch_add(1, Ordering::Release);
                let src = storage_pair.get_src_storage().clone();
                let dest = storage_pair.get_dest_storage().clone();

                let mut failure: Option<AppError> = None;
                let mut skipped = SkipTracker::default();
                let mut history = HistoryCopyWorker::new(
                    HistoryCopyContext {
                        source: src.clone(),
                        destination: dest.clone(),
                        broadcaster: bc.clone(),
                        job_id: resume_opts.job_id.clone(),
                        options: transfer_options,
                    },
                    history_policy,
                );
                while let Some(message) = rx.next().await {
                    if history.process(&message).await? {
                        continue;
                    }
                    let selected = match &message {
                        StorageEntryMessage::New(item) | StorageEntryMessage::Changed { entry: item, .. } => {
                            Some(&item.entry)
                        }
                        _ => None,
                    };
                    if let Some(entry) = selected
                        && skipped.skip(entry, &dest, &bc).await?
                    {
                        continue;
                    }
                    match message {
                        StorageEntryMessage::New(ref entry) => {
                            match process_entry(
                                &entry.entry,
                                src.clone(),
                                dest.clone(),
                                enable_integrity_check,
                                enable_acl,
                                is_source_reserved,
                                qos.clone(),
                                bt.clone(),
                                &bc,
                                resume_opts.clone(),
                            )
                            .await
                            {
                                Ok(()) => bc.broadcast(message.clone()).await?,
                                Err(e) => {
                                    error!(
                                        "Handler {}: Failed to process new entry {:?}: {}",
                                        handler_id,
                                        entry.get_relative_path(),
                                        e
                                    );
                                    bc.broadcast(StorageEntryMessage::Error {
                                        event: ErrorEvent::Copy,
                                        path: entry.get_relative_path().to_path_buf(),
                                        entry: Some(Arc::clone(&entry.entry)),
                                        reason: format!("{e}"),
                                    })
                                    .await?;
                                    failure.get_or_insert(e);
                                }
                            }
                        }
                        StorageEntryMessage::Changed { ref entry, kind } => {
                            if kind == ChangeKind::DirectoryMarkerRemoved {
                                bc.broadcast(StorageEntryMessage::TargetPreserved {
                                    retention: Box::new(TargetRetention::entry(
                                        entry.entry.path(),
                                        TargetRetentionReason::SourceDeletionUnconfirmed,
                                    )),
                                    reason: "目录对象缺失尚无独立的源缺失与目标归属证明，保留目标标记对象及确认事实"
                                        .into(),
                                })
                                .await?;
                                continue;
                            }
                            // MetadataOnly：chmod/chown 变更，跳过 copy_file 只同步属性
                            let result = if kind == ChangeKind::MetadataOnly {
                                process_metadata_only_entry(
                                    &entry.entry,
                                    src.clone(),
                                    dest.clone(),
                                    enable_acl,
                                    is_source_reserved,
                                    &bc,
                                )
                                .await
                            } else {
                                process_entry(
                                    &entry.entry,
                                    src.clone(),
                                    dest.clone(),
                                    enable_integrity_check,
                                    enable_acl,
                                    is_source_reserved,
                                    qos.clone(),
                                    bt.clone(),
                                    &bc,
                                    resume_opts.clone(),
                                )
                                .await
                            };
                            match result {
                                Ok(()) => bc.broadcast(message.clone()).await?,
                                Err(e) => {
                                    error!(
                                        "Handler {}: Failed to process changed entry {:?} (kind={}): {}",
                                        handler_id,
                                        entry.get_relative_path(),
                                        kind,
                                        e
                                    );
                                    bc.broadcast(StorageEntryMessage::Error {
                                        event: ErrorEvent::Copy,
                                        path: entry.get_relative_path().to_path_buf(),
                                        entry: Some(Arc::clone(&entry.entry)),
                                        reason: format!("{e}"),
                                    })
                                    .await?;
                                    failure.get_or_insert(e);
                                }
                            }
                        }
                        StorageEntryMessage::Deleted(_) | StorageEntryMessage::Renamed(_) => {
                            // 扫描比较只交付 New/Changed；目标变更必须经过 Phase B 的存在性确认。
                            return Err(AppError::ConfigError(
                                "目标删除或重命名只能在源端扫描完成后的统一清理阶段执行".into(),
                            ));
                        }
                        StorageEntryMessage::Packaged(ref entry) => {
                            info!("Packing directory {:?}", entry.get_relative_path());

                            match tar_pack::pack_directory(
                                &src,
                                &dest,
                                entry,
                                is_source_reserved,
                                qos.clone(),
                                bt.clone(),
                            )
                            .await
                            {
                                Ok((tar_entry, manifest)) => {
                                    let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                                    bc.broadcast(StorageEntryMessage::New(Arc::new(tar_entry).into()))
                                        .await?;
                                    bc.broadcast(StorageEntryMessage::TarManifest {
                                        tar_path,
                                        entries: manifest,
                                    })
                                    .await?;
                                }
                                Err(e) => {
                                    error!(
                                        "Handler {}: Pack failed for {:?}: {}",
                                        handler_id,
                                        entry.get_relative_path(),
                                        e
                                    );
                                    bc.broadcast(StorageEntryMessage::Error {
                                        event: ErrorEvent::Pack,
                                        path: entry.get_relative_path().to_path_buf(),
                                        entry: Some(entry.clone()),
                                        reason: format!("{e}"),
                                    })
                                    .await?;
                                    failure.get_or_insert(e);
                                }
                            }
                        }
                        _ => {
                            // Scanned、Copyed、IntegrityChecked 不会经由 detect_broadcaster 到达此处
                        }
                    }
                }

                info!("Handler worker {} completed", handler_id);
                let history_result = history.finish().await;
                match failure {
                    Some(error) => Err(error),
                    None => history_result,
                }
            }
            .instrument(span),
        );
        handler_handles.push(handle);
    }
    // drop work_rx 本体（只有 clone 在 worker 手里）

    // ── 11.5. 等待所有 handler worker 完成 mount 初始化，输出存活汇总 ──
    wait_for_mount_init(&[(&mount_done_handlers, &alive_handlers, "Handler")], copy_concurrency).await;

    handler_handles
}
