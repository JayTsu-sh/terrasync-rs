//! 单进程 Receiver 的就绪事实广播与失败收束；不发送双进程协议回执。
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use async_channel::Receiver;
use data_mover::{model::ObservedEntry, qos::QosManager};
use task_model::{ObservedEntryExt, StorageEntryMessage};
use tracing::{error, info};

use super::{ReceiverConfig, process_entry_on_receiver};
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, Result, StorageError},
    file_copy::{CopiedPublication, VersionFailureContext},
    sender::copy_queue::CopyCommand,
    source_cleanup::{ReadyFileCleanup, can_stream_files},
    storage_factory::StorageContext,
};

pub(crate) struct LocalCopyReceiver {
    pub(crate) commands: Receiver<CopyCommand>,
    pub(crate) executor: LocalCopyExecutor,
}

/// 单项工作与结果保留所有权，终态广播只在执行结束后发生。
pub(crate) struct LocalCopyWork {
    pub(crate) entry: Arc<ObservedEntry>,
}

pub(crate) struct LocalCopyCompletion {
    pub(crate) work: LocalCopyWork,
    pub(crate) result: Result<Option<CopiedPublication>>,
}

#[derive(Clone)]
pub(crate) struct LocalCopyExecutor {
    pub(crate) id: usize,
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
        let Self { commands, mut executor } = self;
        executor.cleanup = executor
            .cleanup
            .filter(|_| can_stream_files(&executor.source, &executor.destination));
        // 从本地工作队列领取任务（MPMC，N 个 worker 竞争消费）
        let mut failure = None;
        loop {
            let next = match executor.config.cancel.as_ref() {
                Some(cancel) => tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        // 关闭生产端并停止取队列；未开始的条目不虚构逐项复制失败。
                        commands.close();
                        return Err(failure.unwrap_or_else(|| StorageError::Cancelled.into()));
                    }
                    message = commands.recv() => message,
                },
                None => commands.recv().await,
            };
            let Ok(msg) = next else { break };

            match msg {
                CopyCommand::CopyEntry { entry } => {
                    let _guard = AtomicCounterGuard::increment(&executor.active);
                    let completion = executor.execute(LocalCopyWork { entry }).await;
                    if let Err(error) = executor.publish(completion).await {
                        if !error.is_reported_copy_failure() {
                            return Err(error);
                        }
                        failure.get_or_insert(error);
                    }
                }
                CopyCommand::TarPacked {
                    tar_entry,
                    manifest_entries,
                } => {
                    let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                    executor
                        .broadcaster
                        .broadcast(StorageEntryMessage::New(tar_entry.into()))
                        .await?;
                    executor
                        .broadcaster
                        .broadcast(StorageEntryMessage::TarManifest {
                            tar_path,
                            entries: manifest_entries,
                        })
                        .await?;
                }
                CopyCommand::PackFailed => {}
            }
        }
        // 生产端全部退出后排空队列，然后结束
        info!("Receiver {}: Completed", executor.id);
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl LocalCopyExecutor {
    pub(crate) async fn execute(&self, work: LocalCopyWork) -> LocalCopyCompletion {
        let result = process_entry_on_receiver(
            &work.entry,
            &self.source,
            &self.destination,
            &self.config,
            self.qos.clone(),
            self.bytes.clone(),
        )
        .await;
        LocalCopyCompletion { work, result }
    }

    pub(crate) async fn publish(&self, completion: LocalCopyCompletion) -> Result<()> {
        let LocalCopyCompletion {
            work: LocalCopyWork { entry },
            result,
        } = completion;
        match result {
            Ok(published) => {
                self.broadcaster
                    .broadcast(StorageEntryMessage::New(entry.clone().into()))
                    .await?;
                if let Some(cleanup) = &self.cleanup {
                    Box::pin(cleanup.remove(&self.source, &self.destination, &entry, published)).await?;
                }
                Ok(())
            }
            Err(error) => {
                error!(
                    "Receiver {}: CopyEntry failed {:?}: {}",
                    self.id,
                    entry.get_relative_path(),
                    error
                );
                let context = error.storage_error().map_or_else(
                    || VersionFailureContext {
                        phase: "copy_entry".into(),
                        backend: self.destination.storage().kind(),
                        metadata_only: false,
                    },
                    |error| {
                        VersionFailureContext::for_error(
                            error,
                            self.source.storage().kind(),
                            self.destination.storage().kind(),
                        )
                    },
                );
                let data_failed = !context.metadata_only;
                self.broadcaster
                    .broadcast(StorageEntryMessage::CopyVersionFailed {
                        entry: entry.into(),
                        phase: context.phase,
                        backend: context.backend,
                        reason: error.to_string(),
                        first_in_entry: true,
                    })
                    .await?;
                Err(AppError::ReportedCopyFailure {
                    source: Box::new(error),
                    data_failed,
                })
            }
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
