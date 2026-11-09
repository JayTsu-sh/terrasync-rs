//! 单进程 Receiver 的就绪事实广播与失败收束；不发送双进程协议回执。
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use data_mover::qos::QosManager;
use task_model::{ObservedEntryExt, StorageEntryMessage};
use tracing::{error, info};
use transport::{in_process::InProcessReceiverTransport, message::SenderMsg, traits::ReceiverTransport};

use super::{ReceiverConfig, process_entry_on_receiver};
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, Result, StorageError},
    file_copy::VersionFailureContext,
    source_cleanup::{ReadyFileCleanup, can_stream_files},
    storage_factory::StorageContext,
};

pub(crate) struct LocalCopyReceiver {
    pub(crate) id: usize,
    pub(crate) transport: Arc<InProcessReceiverTransport>,
    pub(crate) source: Arc<StorageContext>,
    pub(crate) destination: Arc<StorageContext>,
    pub(crate) config: Arc<ReceiverConfig>,
    pub(crate) qos: Option<QosManager>,
    pub(crate) bytes: Option<Arc<AtomicU64>>,
    pub(crate) broadcaster: BroadcastForwarder<StorageEntryMessage>,
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) cleanup: Option<Arc<ReadyFileCleanup>>,
}

impl LocalCopyReceiver {
    pub(crate) async fn run(self) -> Result<()> {
        let Self {
            id: recv_id,
            transport: rt,
            source: src,
            destination: dest,
            config: cfg,
            qos,
            bytes: bt,
            broadcaster: bc,
            active: task_counter,
            cleanup,
        } = self;
        let cleanup = cleanup.filter(|_| can_stream_files(&src, &dest));
        // 从 transport 拉消息处理（MPMC，N 个 worker 竞争消费）
        let mut failure = None;
        loop {
            let next = match cfg.cancel.as_ref() {
                Some(cancel) => tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        // 关闭生产端并停止取队列；未开始的条目不虚构逐项复制失败。
                        rt.close().await?;
                        return Err(failure.unwrap_or_else(|| StorageError::Cancelled.into()));
                    }
                    message = rt.recv() => message,
                },
                None => rt.recv().await,
            };
            let Some(msg) = next else { break };

            match msg {
                SenderMsg::CopyEntry { entry } => {
                    let _guard = AtomicCounterGuard::increment(&task_counter);
                    // 在进入清理 await 前释放已完成复制 future，避免同一 match 临时值跨越两个阶段。
                    let copied = process_entry_on_receiver(&entry, &src, &dest, &cfg, qos.clone(), bt.clone()).await;
                    match copied {
                        Ok(published) => {
                            bc.broadcast(StorageEntryMessage::New(entry.clone().into())).await?;
                            if let Some(cleanup) = &cleanup {
                                // 原生 SDK 的大 future 不嵌入整个接收循环；每个 worker 至多持有一项。
                                Box::pin(cleanup.remove(&src, &dest, &entry, published)).await?;
                            }
                        }
                        Err(e) => {
                            error!(
                                "Receiver {}: CopyEntry failed {:?}: {}",
                                recv_id,
                                entry.get_relative_path(),
                                e
                            );
                            let context = e.storage_error().map_or_else(
                                || VersionFailureContext {
                                    phase: "copy_entry".into(),
                                    backend: dest.storage().kind(),
                                    metadata_only: false,
                                },
                                |error| {
                                    VersionFailureContext::for_error(error, src.storage().kind(), dest.storage().kind())
                                },
                            );
                            let data_failed = !context.metadata_only;
                            bc.broadcast(StorageEntryMessage::CopyVersionFailed {
                                entry: entry.clone().into(),
                                phase: context.phase,
                                backend: context.backend,
                                reason: e.to_string(),
                                first_in_entry: true,
                            })
                            .await?;
                            failure.get_or_insert(AppError::ReportedCopyFailure {
                                source: Box::new(e),
                                data_failed,
                            });
                        }
                    }
                }
                SenderMsg::TarPacked {
                    tar_entry,
                    manifest_entries,
                } => {
                    let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                    bc.broadcast(StorageEntryMessage::New(tar_entry.into())).await?;
                    bc.broadcast(StorageEntryMessage::TarManifest {
                        tar_path,
                        entries: manifest_entries,
                    })
                    .await?;
                }
                _ => {}
            }
        }
        // recv() 返回 None 时自然退出（sender_transport 被 drop 后 channel 关闭）
        info!("Receiver {}: Completed", recv_id);
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// RAII guard：构造时 +1，Drop 时 -1，确保计数器在 panic/early-return 时也能正确回收。
struct AtomicCounterGuard<'a>(&'a AtomicUsize);

impl<'a> AtomicCounterGuard<'a> {
    fn increment(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for AtomicCounterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "in_process_cancellation_tests.rs"]
mod cancellation_tests;
