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
    error::{AppError, Result},
    file_copy::VersionFailureContext,
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
        } = self;
        // 从 transport 拉消息处理（MPMC，N 个 worker 竞争消费）
        let mut failure = None;
        while let Some(msg) = rt.recv().await {
            match msg {
                SenderMsg::CopyEntry { entry } => {
                    let _guard = AtomicCounterGuard::increment(&task_counter);
                    match process_entry_on_receiver(&entry, &src, &dest, &cfg, qos.clone(), bt.clone()).await {
                        Ok(()) => {
                            bc.broadcast(StorageEntryMessage::New(entry.into())).await?;
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
