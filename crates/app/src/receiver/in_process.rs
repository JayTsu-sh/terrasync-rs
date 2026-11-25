//! 单进程 Receiver 的就绪事实广播与失败收束；不发送双进程协议回执。
use crate::copy_bytes::CopyBytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_channel::Receiver;
use data_mover::{
    model::{BackendKind, EntryIdentityKey, EntryKind, IdentityStrength, ObservedEntry},
    qos::QosManager,
    storage::SourceDescriptor,
};
use task_model::observation::ObservedItem;
use task_model::{ObservedEntryExt, StorageEntryMessage};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::{ReceiverConfig, process_entry_on_receiver};
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, Result, StorageError},
    file_copy::{
        CopiedPublication, VersionFailureContext,
        metadata::{captured_metadata_plan, set_item_metadata},
        namespace as copy_namespace,
    },
    sender::copy_queue::CopyCommand,
    source_cleanup::{ReadyFileCleanup, can_stream_files},
    storage_factory::StorageContext,
};

pub(crate) struct LocalCopyReceiver {
    pub(crate) commands: Receiver<CopyCommand>,
    pub(crate) executor: LocalCopyExecutor,
}

/// 单项工作与结果保留所有权，终态广播只在执行结束后发生。
#[derive(Clone)]
pub(crate) struct LocalCopyWork {
    pub(crate) entry: Arc<ObservedEntry>,
    pub(crate) control: Option<([u8; 32], LocalCopyPhase)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalCopyPhase {
    Copy,
    Prepare,
    Finalize { identity: EntryIdentityKey },
}

// 工作结果只在有界的接收队列内持有；避免给普通文件结果新增逐项堆分配。
#[allow(clippy::large_enum_variant)]
pub(crate) enum LocalCopyOutcome {
    Copied(Option<CopiedPublication>),
    Prepared(EntryIdentityKey),
}

/// 阶段就绪证据由执行点建立；它不代表整个 entry 的数据和元数据迁移成功。
#[derive(Default)]
pub(crate) struct LocalCopyPhaseProof(bool);

impl LocalCopyPhaseProof {
    fn established() -> Self {
        Self(true)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.0
    }
}

pub(crate) struct LocalCopyCompletion {
    pub(crate) work: LocalCopyWork,
    pub(crate) result: Result<LocalCopyOutcome>,
    pub(crate) phase_proof: LocalCopyPhaseProof,
}

#[derive(Clone)]
pub(crate) struct LocalCopyExecutor {
    pub(crate) id: usize,
    pub(crate) source: Arc<StorageContext>,
    pub(crate) destination: Arc<StorageContext>,
    pub(crate) config: Arc<ReceiverConfig>,
    pub(crate) qos: Option<QosManager>,
    pub(crate) bytes: Option<Arc<CopyBytes>>,
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
                    let completion = executor.execute(LocalCopyWork { entry, control: None }).await;
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
        let _guard = work
            .control
            .as_ref()
            .map(|_| AtomicCounterGuard::increment(&self.active));
        let mut phase_proof = LocalCopyPhaseProof::default();
        let result = match work.control.map(|(_, phase)| phase) {
            Some(LocalCopyPhase::Finalize { identity }) => {
                let (result, proof) = self.finalize_directory(&work.entry, identity).await;
                phase_proof = proof;
                result
            }
            Some(LocalCopyPhase::Prepare) => match self.prepare_directory(&work.entry).await {
                Ok(descriptor) => {
                    phase_proof = LocalCopyPhaseProof::established();
                    Ok(LocalCopyOutcome::Prepared(descriptor.source_identity.identity_key()))
                }
                Err(error) => Err(error),
            },
            phase => {
                let copied = process_entry_on_receiver(
                    &work.entry,
                    &self.source,
                    &self.destination,
                    &self.config,
                    self.qos.clone(),
                    self.bytes.clone(),
                )
                .await;
                match (copied, phase) {
                    (Ok(published), phase) => {
                        if matches!(phase, Some(LocalCopyPhase::Copy)) {
                            // 包含软链接的真实命名空间成功；已有的源失败仍必须成为entry错误。
                            phase_proof = LocalCopyPhaseProof::established();
                            if work.entry.metadata().failures().next().is_some() {
                                let item = ObservedItem::from(work.entry.clone());
                                match captured_metadata_plan(self.destination.storage(), item.view()) {
                                    Err(source @ StorageError::SourceMetadataAcquisitionFailed { .. }) => {
                                        Err(source.into())
                                    }
                                    Err(source) => {
                                        phase_proof = LocalCopyPhaseProof::default();
                                        Err(source.into())
                                    }
                                    Ok(_) => Ok(LocalCopyOutcome::Copied(published)),
                                }
                            } else {
                                Ok(LocalCopyOutcome::Copied(published))
                            }
                        } else {
                            Ok(LocalCopyOutcome::Copied(published))
                        }
                    }
                    (Err(error), Some(LocalCopyPhase::Copy)) => {
                        if published_with_source_metadata_failure(&error, &work.entry) {
                            phase_proof = LocalCopyPhaseProof::established();
                        }
                        Err(error)
                    }
                    (Err(error), _) => Err(error),
                }
            }
        };
        if self.config.cancel.as_ref().is_some_and(CancellationToken::is_cancelled) {
            phase_proof = LocalCopyPhaseProof::default();
        }
        LocalCopyCompletion {
            work,
            result,
            phase_proof,
        }
    }

    async fn prepare_directory(&self, entry: &Arc<ObservedEntry>) -> Result<SourceDescriptor> {
        let target = self.destination.storage();
        // 本地逐级确认父目录无需给每一级再发 mkdir。NFS 保留原有祖先替换语义。
        if cfg!(unix)
            && target.kind() == BackendKind::Local
            && entry.kind() == EntryKind::Directory
            && !entry.path().as_str().is_empty()
        {
            let descriptor =
                copy_namespace::prepare_local_directory(target, entry.path(), self.config.cancel.as_ref()).await?;
            return Self::validate_directory_descriptor(entry, descriptor);
        }
        process_entry_on_receiver(
            entry,
            &self.source,
            &self.destination,
            &self.config,
            self.qos.clone(),
            self.bytes.clone(),
        )
        .await?;
        self.directory_descriptor(entry).await
    }

    async fn directory_descriptor(&self, entry: &ObservedEntry) -> Result<SourceDescriptor> {
        let namespace = self.destination.storage().namespace().map_err(StorageError::from)?;
        let descriptor =
            copy_namespace::stat_fresh(namespace.as_ref(), entry.path(), self.config.cancel.as_ref()).await?;
        Self::validate_directory_descriptor(entry, descriptor)
    }

    fn validate_directory_descriptor(entry: &ObservedEntry, descriptor: SourceDescriptor) -> Result<SourceDescriptor> {
        if descriptor.kind != EntryKind::Directory
            || descriptor.source_identity.strength() != IdentityStrength::StableWithinBackend
        {
            return Err(StorageError::TargetPublicationChanged {
                path: entry.path().clone(),
                source: None,
            }
            .into());
        }
        Ok(descriptor)
    }

    async fn finalize_directory(
        &self, entry: &Arc<ObservedEntry>, identity: EntryIdentityKey,
    ) -> (Result<LocalCopyOutcome>, LocalCopyPhaseProof) {
        let mut proof = LocalCopyPhaseProof::default();
        let result = async {
            let item = ObservedItem::from(entry.clone());
            if entry.metadata().failures().next().is_some() {
                let before = self.confirm_directory(entry, identity).await;
                // 只在源事实已失败时提前编译；正常路径不重复编译或额外回源。
                match captured_metadata_plan(self.destination.storage(), item.view()) {
                    Err(source @ StorageError::SourceMetadataAcquisitionFailed { .. }) => {
                        // 未执行任何目标属性写；仍须复核prepare对应的真实目标身份。
                        let confirmed = match before {
                            Ok(_) => self.confirm_directory(entry, identity).await,
                            Err(error) => Err(error),
                        };
                        if let Err(identity_error) = confirmed {
                            return Err(AppError::CopyAndSourceMetadataFailed {
                                source: Box::new(identity_error),
                                metadata: Box::new(source),
                            });
                        }
                        proof = LocalCopyPhaseProof::established();
                        return Err(source.into());
                    }
                    Err(source) => return Err(source.into()),
                    Ok(_) => {}
                }
            }
            set_item_metadata(
                self.destination.storage(),
                item.view(),
                None,
                Some(identity),
                self.config.cancel.clone(),
            )
            .await?;
            // 写入和最终当前路径核验均绑定到 Prepare 的目标身份。
            proof = LocalCopyPhaseProof::established();
            Ok(LocalCopyOutcome::Copied(None))
        }
        .await;
        (result, proof)
    }

    async fn confirm_directory(&self, entry: &ObservedEntry, identity: EntryIdentityKey) -> Result<SourceDescriptor> {
        let descriptor = self.directory_descriptor(entry).await?;
        if descriptor.source_identity.identity_key() != identity {
            return Err(StorageError::TargetPublicationChanged {
                path: entry.path().clone(),
                source: None,
            }
            .into());
        }
        Ok(descriptor)
    }

    pub(crate) async fn publish(&self, completion: LocalCopyCompletion) -> Result<()> {
        let LocalCopyCompletion {
            work: LocalCopyWork { entry, control },
            result,
            phase_proof: _,
        } = completion;
        match result {
            Ok(LocalCopyOutcome::Prepared(_)) => Ok(()),
            Ok(LocalCopyOutcome::Copied(published)) => {
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
                let mut context = error.storage_error().map_or_else(
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
                if control.is_some_and(|(_, phase)| matches!(phase, LocalCopyPhase::Prepare)) && !context.metadata_only
                {
                    context.phase = "directory_prepare".into();
                }
                if control.is_some_and(|(_, phase)| matches!(phase, LocalCopyPhase::Finalize { .. })) {
                    context.phase = "directory_metadata_finalize".into();
                    context.metadata_only = true;
                }
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

/// 只接受本次真实发布成功、且直接原因仅为源事实获取失败的组合。
/// 目标校验失败也会携带`PublishedDataMetadata`，不能据此一概认可阶段。
fn published_with_source_metadata_failure(error: &AppError, entry: &ObservedEntry) -> bool {
    matches!(error.storage_error(), Some(StorageError::PublishedDataMetadata { data, source })
        if &data.final_destination == entry.path()
            && matches!(source.as_ref(), StorageError::SourceMetadataAcquisitionFailed { path, version: None, .. } if path == entry.path()))
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
