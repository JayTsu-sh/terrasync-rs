//! 每代一个根目录观察；不进入普通 entry 队列，不参与根删除或逐项计数。
use std::path::Path;
use std::sync::{Arc, OnceLock};

use data_mover::model::{BackendKind, EntryKind, ObservationPlan, ObservedEntry, SourceIdentity, StoragePath};
use data_mover::storage::{NamespaceReadOutcome, NamespaceReadPolicy, SourceDescriptor, read_namespace_entry};
use task_model::observation::ObservedItem;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result, StorageError};
use crate::file_copy::metadata::{captured_metadata_plan, set_item_metadata};
use crate::scan::observe_path_with_read_policy;
use crate::storage_factory::StorageContext;

struct RootFailure {
    backend: BackendKind,
    phase: &'static str,
    source: Arc<StorageError>,
}

impl RootFailure {
    fn error(&self) -> AppError {
        AppError::CopyRootMetadataFailure {
            backend: self.backend,
            phase: self.phase,
            source: self.source.clone(),
        }
    }
}

enum RootPreparation {
    Omitted,
    Ready(SourceIdentity),
    Failed(RootFailure),
}

pub(crate) struct CopyRootMetadata {
    source: Option<Arc<StorageContext>>,
    destination: Option<Arc<StorageContext>>,
    observation: ObservationPlan,
    policy: NamespaceReadPolicy,
    cancel: CancellationToken,
    require_persistence: bool,
    captured: OnceLock<Arc<ObservedEntry>>,
    prepared: OnceLock<RootPreparation>,
    finalized: OnceLock<()>,
}

impl CopyRootMetadata {
    pub(crate) fn new(
        source: Arc<StorageContext>, destination: Arc<StorageContext>, active: bool, observation: ObservationPlan,
        policy: NamespaceReadPolicy, cancel: CancellationToken, require_persistence: bool,
    ) -> Self {
        Self {
            source: active.then_some(source),
            destination: active.then_some(destination),
            observation,
            policy,
            cancel,
            require_persistence,
            captured: OnceLock::new(),
            prepared: OnceLock::new(),
            finalized: OnceLock::new(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.source.is_some()
    }

    /// 获取失败留到内容复制后报告；不把元数据拒绝变成可读数据的准入失败。
    pub(crate) async fn capture(&self) -> Result<()> {
        let prepared = self.prepare().await;
        self.prepared
            .set(prepared)
            .map_err(|_| AppError::ConfigError("root metadata captured more than once".into()))?;
        // 命名空间读取已耗尽自身恢复或被拒绝，不再启动内容扫描重置同一次故障窗口。
        // 可独立交付数据的属性失败由 SourceMetadataAcquisitionFailed 保留到收尾。
        if let Some(RootPreparation::Failed(failure)) = self.prepared.get()
            && matches!(failure.source.as_ref(), StorageError::NamespaceRead(_))
        {
            return Err(failure.error());
        }
        if self.cancel.is_cancelled() {
            return match self.prepared.get() {
                Some(RootPreparation::Failed(failure)) => Err(failure.error()),
                _ => Err(StorageError::Cancelled.into()),
            };
        }
        Ok(())
    }

    async fn prepare(&self) -> RootPreparation {
        let (Some(source), Some(target)) = (&self.source, &self.destination) else {
            return RootPreparation::Omitted;
        };
        let mut backend = source.storage().kind();
        let mut phase = "root_source_metadata_capture";
        let outcome = async {
            let entry = observe_path_with_read_policy(
                source.storage(),
                Path::new(""),
                Some(self.observation),
                self.policy,
                &self.cancel,
            )
            .await?;
            if entry.kind() != EntryKind::Directory {
                return Ok(None);
            }
            let entry = Arc::new(entry);
            let _ = self.captured.set(entry.clone());
            let item = ObservedItem::from(entry);
            if let Err(error) = captured_metadata_plan(target.storage(), item.view()) {
                if !matches!(&error, StorageError::SourceMetadataAcquisitionFailed { .. }) {
                    backend = target.storage().kind();
                    phase = "root_metadata_plan";
                }
                return Err(error);
            }
            backend = target.storage().kind();
            phase = "root_target_identity_capture";
            let descriptor = self.read_target(target).await?;
            if descriptor.kind != EntryKind::Directory {
                return Err(StorageError::TargetPublicationChanged {
                    path: StoragePath::root(),
                });
            }
            Ok(Some(descriptor.source_identity))
        }
        .await;
        match outcome {
            Ok(Some(identity)) => RootPreparation::Ready(identity),
            Ok(None) => RootPreparation::Omitted,
            Err(source_error) => RootPreparation::Failed(RootFailure {
                backend,
                phase,
                source: Arc::new(source_error),
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<Option<Vec<u8>>> {
        self.captured
            .get()
            .map(|entry| {
                entry
                    .try_encode_snapshot()
                    .map(|snapshot| snapshot.as_bytes().to_vec())
                    .map_err(|source| AppError::CopyRootMetadataFailure {
                        backend: entry.source_identity().backend().kind(),
                        phase: "root_snapshot_encode",
                        source: Arc::new(source.into()),
                    })
            })
            .transpose()
    }

    pub(crate) fn retain_capture_failure(&self, result: Result<()>) -> Result<()> {
        match (result, self.prepared.get()) {
            (Err(source), Some(RootPreparation::Failed(failure)))
                if !source.contains_copy_root_failure(&failure.source) =>
            {
                Err(AppError::MigrationAndRootMetadataFailed {
                    source: Box::new(source),
                    metadata: Box::new(failure.error()),
                })
            }
            (result, _) => result,
        }
    }

    async fn read_target(&self, target: &StorageContext) -> std::result::Result<SourceDescriptor, StorageError> {
        match read_namespace_entry(target.storage(), &StoragePath::root(), None, self.policy, &self.cancel).await? {
            NamespaceReadOutcome::Descriptor {
                descriptor: Some(descriptor),
                ..
            } => Ok(descriptor),
            _ => Err(StorageError::TargetPublicationChanged {
                path: StoragePath::root(),
            }),
        }
    }

    /// 调用方在内容阶段结束后重开目标根；对子目录和根收尾核对相同端点身份。
    pub(crate) async fn verify_target_binding(&self, target: &StorageContext) -> Result<()> {
        let Some(RootPreparation::Ready(identity)) = self.prepared.get() else {
            return Ok(());
        };
        self.confirm_target(target, identity).await.map(|_| ())
    }

    async fn confirm_target(&self, target: &StorageContext, identity: &SourceIdentity) -> Result<SourceDescriptor> {
        let outcome = async {
            let namespace = target.storage().namespace()?;
            let descriptor = self.read_target(target).await?;
            if &descriptor.source_identity != identity || descriptor.kind != EntryKind::Directory {
                return Err(StorageError::TargetPublicationChanged {
                    path: StoragePath::root(),
                });
            }
            if self.require_persistence {
                namespace.persist_observed(&descriptor, self.cancel.clone()).await?;
            }
            Ok(descriptor)
        }
        .await;
        outcome.map_err(|source| AppError::CopyRootMetadataFailure {
            backend: target.storage().kind(),
            phase: "root_target_binding",
            source: Arc::new(source),
        })
    }

    /// 全部子项的命名空间操作之后执行；根元数据就绪前不授权源清理。
    pub(crate) async fn finish(&self, target: &StorageContext) -> Result<()> {
        let identity = match self.prepared.get() {
            Some(RootPreparation::Omitted) => return Ok(()),
            Some(RootPreparation::Failed(failure)) => return Err(failure.error()),
            Some(RootPreparation::Ready(identity)) => identity,
            None => return Err(AppError::ConfigError("root metadata not captured".into())),
        };
        if self.finalized.get().is_some() {
            return Ok(());
        }
        let entry = self
            .captured
            .get()
            .ok_or_else(|| AppError::ConfigError("root observation missing".into()))?;
        self.confirm_target(target, identity).await?;
        let outcome = async {
            let namespace = target.storage().namespace()?;
            let item = ObservedItem::from(entry.clone());
            let after = set_item_metadata(target.storage(), item.view(), None, Some(self.cancel.clone())).await?;
            if &after.source_identity != identity {
                return Err(StorageError::TargetPublicationChanged {
                    path: StoragePath::root(),
                });
            }
            if self.require_persistence {
                namespace.persist_observed(&after, self.cancel.clone()).await?;
            }
            Ok(())
        }
        .await;
        outcome.map_err(|source| AppError::CopyRootMetadataFailure {
            backend: target.storage().kind(),
            phase: "root_metadata_finalize",
            source: Arc::new(source),
        })?;
        let _ = self.finalized.set(());
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "copy_root_tests.rs"]
mod tests;
