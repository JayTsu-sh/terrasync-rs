//! Sender 侧逻辑
//!
//! 从遍历流接收条目，为本地全量复制 worker 分派工作。
//! 单进程模式下发送 `CopyEntry` 命令（Receiver 自行读写）。
//! 双进程模式下发送 `FileData` 数据流（Phase 3）。

// 标准库
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use task_model::ObservedEntryExt;

// 外部 crate
use crate::storage_factory::StorageContext;
use crate::task_stream::AsyncReceiver;
use data_mover::model::ObservedEntry;
use data_mover::qos::QosManager;
use task_model::StorageEntryMessage;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::copy_history::{HistoryCompletion, HistoryCopyContext, HistoryCopyPolicy, HistoryCopyWorker};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::CopyOptions;
use crate::file_copy::selection::skip_unrepresentable;
use crate::receiver::ReceiverConfig;
use crate::tar_pack;
use task_model::observation::EntryProgress;

#[cfg(test)]
#[path = "sender_cancellation_tests.rs"]
mod cancellation_tests;

pub(crate) mod copy_queue;
use copy_queue::{CopyCommand, CopyCommandSink};

/// Sender worker 配置
pub struct SenderWorkerConfig {
    pub target_history: Option<bool>,
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    pub large_file_log_threshold: u64,
}

/// 单个 Sender worker task（S1）
///
/// 从共享的遍历流读取条目，把普通条目和打包结果放入本地工作队列。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sender_worker(
    worker_id: usize, walkdir_iter: AsyncReceiver<StorageEntryMessage>, src_storage: Arc<StorageContext>,
    dest_storage: Arc<StorageContext>, transport: Arc<dyn CopyCommandSink>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>, config: &SenderWorkerConfig,
    qos_manager: Option<QosManager>, bytes_tracker: Option<Arc<AtomicU64>>, copy_config: &ReceiverConfig,
    active_entry_counter: Arc<AtomicUsize>, entry_counter: Arc<AtomicUsize>, size_counter: Arc<AtomicU64>,
) -> Result<()> {
    info!("[Sender Worker {}] Started", worker_id);
    let mut failure = None;
    let mut history = HistoryCopyWorker::new(
        HistoryCopyContext {
            source: src_storage.clone(),
            destination: dest_storage.clone(),
            broadcaster: broadcaster.clone(),
            job_id: copy_config.job_id.clone(),
            options: CopyOptions {
                transfer_mode: copy_config.transfer_mode,
                qos: qos_manager.clone(),
                enable_integrity_check: copy_config.enable_integrity_check,
                bytes_counter: bytes_tracker.clone(),
                cancel: copy_config.cancel.clone(),
            },
        },
        HistoryCopyPolicy {
            active_entry_limit: 1,
            target_history: config.target_history,
            completion: HistoryCompletion::Full,
            recovering: false,
        },
    );

    loop {
        let next = match copy_config.cancel.as_ref() {
            Some(cancel) => tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(StorageError::Cancelled.into()),
                message = walkdir_iter.next() => message,
            },
            None => walkdir_iter.next().await,
        };
        let Some(msg) = next else { break };
        if broadcaster.is_closed() {
            break;
        }
        match msg {
            StorageEntryMessage::Diagnostic { .. } => broadcaster.broadcast(msg).await?,
            StorageEntryMessage::Scanned(entry) => {
                if entry.history().is_some() {
                    active_entry_counter.fetch_add(1, Ordering::Relaxed);
                    if entry.history().is_some_and(|history| history.segment() == 0) {
                        entry_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    size_counter.fetch_add(
                        EntryProgress::copy_inventory(&entry, !config.target_history.unwrap_or(true)).logical_bytes,
                        Ordering::Relaxed,
                    );
                    let result = history.copy_segment(&entry).await;
                    active_entry_counter.fetch_sub(1, Ordering::Relaxed);
                    result?;
                    continue;
                }
                if skip_unrepresentable(&entry, &dest_storage, broadcaster).await? {
                    continue;
                }
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

                let result = send_entry(worker_id, &transport, &entry, copy_config.cancel.as_ref()).await;
                active_entry_counter.fetch_sub(1, Ordering::Relaxed);
                if let Err(error) = result {
                    if error.stops_event_pipeline() {
                        return Err(error);
                    }
                    failure.get_or_insert(error);
                }
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
                broadcaster.broadcast(msg.clone()).await?;
                failure.get_or_insert_with(|| AppError::ScanError(reason.clone()));
            }

            _ => {}
        }
    }

    info!("[Sender Worker {}] Completed", worker_id);
    let history_result = history.finish().await;
    failure.map_or(history_result, Err)
}

async fn send_entry(
    worker_id: usize, transport: &Arc<dyn CopyCommandSink>, entry: &Arc<ObservedEntry>,
    cancel: Option<&CancellationToken>,
) -> Result<()> {
    let sending = transport.send(CopyCommand::CopyEntry { entry: entry.clone() });
    let result = match cancel {
        Some(cancel) => tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(StorageError::Cancelled.into()),
            result = sending => result,
        },
        None => sending.await,
    };
    if let Err(error) = result {
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
    worker_id: usize, entry: &Arc<ObservedEntry>, src_storage: &Arc<StorageContext>,
    dest_storage: &Arc<StorageContext>, transport: &Arc<dyn CopyCommandSink>, config: &SenderWorkerConfig,
    qos_manager: Option<QosManager>, bytes_tracker: Option<Arc<AtomicU64>>,
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
                .send(CopyCommand::TarPacked {
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
            if let Err(notification_error) = transport.send(CopyCommand::PackFailed).await {
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::error::Result;
    use std::io::{Error as IoError, ErrorKind};
    use std::path::PathBuf;
    use std::sync::Mutex;

    use crate::task_stream::AsyncReceiver;
    use async_trait::async_trait;
    use task_model::ErrorEvent;

    use crate::storage_factory::{StorageRole, create_storage_for_role};
    use tempfile::tempdir;

    use super::copy_queue::copy_queue;
    use super::*;

    async fn create_storage(path: &str, block_size: Option<u64>, _ensure_dir: bool) -> Result<StorageContext> {
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

        let (sender_transport, _receiver_transport) = copy_queue();
        let transport: Arc<dyn CopyCommandSink> = Arc::new(sender_transport);

        let tmp = tempdir().unwrap();
        let storage = Arc::new(create_storage(tmp.path().to_str().unwrap(), None, false).await.unwrap());

        let config = SenderWorkerConfig {
            target_history: None,
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
            &ReceiverConfig {
                job_id: "test".into(),
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                job_dir: String::new(),
                transfer_mode: Default::default(),
                cancel: None,
            },
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
        sent: Mutex<Vec<CopyCommand>>,
        failures: usize,
    }

    #[async_trait]
    impl CopyCommandSink for RecordingTransport {
        async fn send(&self, msg: CopyCommand) -> Result<()> {
            let mut sent = self.sent.lock().unwrap();
            let index = sent.len();
            sent.push(msg);
            if index < self.failures {
                if index == 0 {
                    return Err(IoError::new(ErrorKind::TimedOut, "first send").into());
                }
                return Err(AppError::CopyQueueClosed);
            }
            Ok(())
        }
    }

    fn entry(path: &str, mtime: i64, version: Option<&str>, _count: Option<u32>) -> Arc<ObservedEntry> {
        Arc::new({
            let fixture_path: std::path::PathBuf = path.into();
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::S3,
                fixture_path,
                data_mover::model::EntryKind::File,
                7,
                [0, mtime, 0],
                None,
                None,
                None,
                version.map(|version| (Some(version.to_owned()), false, false)),
            )
        })
    }

    async fn run_messages(
        messages: Vec<StorageEntryMessage>, transport: Arc<dyn CopyCommandSink>, storage: Arc<StorageContext>,
        active: Arc<AtomicUsize>, count: Arc<AtomicUsize>,
    ) -> Result<()> {
        let (tx, rx) = async_channel::bounded(messages.len().max(1));
        for message in messages {
            tx.send(message).await.unwrap();
        }
        drop(tx);
        let config = SenderWorkerConfig {
            target_history: None,
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
            &ReceiverConfig {
                job_id: "test".into(),
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                job_dir: String::new(),
                transfer_mode: Default::default(),
                cancel: None,
            },
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
        assert!(matches!(result, Err(AppError::IoError(error)) if error.kind() == ErrorKind::TimedOut));
        assert_eq!(transport.sent.lock().unwrap().len(), 3);
        assert_eq!(count.load(Ordering::Relaxed), 3);
        assert_eq!(active.load(Ordering::Relaxed), 0);
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
        assert!(matches!(&sent[0], CopyCommand::PackFailed));
        assert!(matches!(&sent[1], CopyCommand::CopyEntry { .. }));
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }
}
