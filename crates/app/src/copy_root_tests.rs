//! 实际本地目录验证根观察、目标替换与取消；不依赖全局配置。
use std::{
    fs::{File, FileTimes, Permissions},
    os::unix::fs::PermissionsExt,
    time::{Duration, UNIX_EPOCH},
};

use super::*;
use crate::{
    migration_plan::MigrationPlan,
    storage_factory::{StorageRole, create_storage_for_role},
};
use async_trait::async_trait;
use data_mover::{
    model::{FailureClass, MetadataObservations},
    storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure},
};
use db::DatabaseError;

async fn roots(source: &Path, target: &Path, cancel: CancellationToken) -> Result<CopyRootMetadata> {
    let source = Arc::new(create_storage_for_role(&source.to_string_lossy(), None, false, StorageRole::Source).await?);
    let target =
        Arc::new(create_storage_for_role(&target.to_string_lossy(), None, false, StorageRole::Destination).await?);
    let plan = MigrationPlan::compile(&source, &target, false).await?;
    Ok(CopyRootMetadata::new(
        source,
        target,
        plan.root_metadata,
        plan.observation,
        NamespaceReadPolicy {
            operation_timeout: Duration::from_millis(500),
            recovery_window: Duration::from_millis(100),
            retry_interval: Duration::from_millis(10),
        },
        cancel,
        true,
    ))
}

#[tokio::test]
async fn captures_root_before_children_change_it_and_finishes_after_child_updates() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let expected = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
    std::fs::set_permissions(source.path(), Permissions::from_mode(0o750))?;
    File::open(source.path())?.set_times(FileTimes::new().set_modified(expected))?;
    let root = roots(source.path(), target.path(), CancellationToken::new()).await?;
    root.capture().await?;
    std::fs::write(source.path().join("new"), b"source")?;
    std::fs::write(target.path().join("copied"), b"target")?;
    let destination =
        create_storage_for_role(&target.path().to_string_lossy(), None, false, StorageRole::Destination).await?;
    root.finish(&destination).await?;
    assert_eq!(std::fs::metadata(target.path())?.modified()?, expected);
    assert_eq!(std::fs::metadata(target.path())?.permissions().mode() & 0o7777, 0o750);
    let snapshot = root
        .snapshot()?
        .ok_or_else(|| AppError::ConfigError("root snapshot missing".into()))?;
    let entry = root
        .captured
        .get()
        .ok_or_else(|| AppError::ConfigError("root observation missing".into()))?;
    assert_eq!(
        ObservedEntry::decode_snapshot(&snapshot, entry.source_identity().backend())
            .map_err(DatabaseError::from)?
            .metadata(),
        entry.metadata()
    );
    assert_eq!(std::fs::read(target.path().join("copied"))?, b"target");
    Ok(())
}

#[tokio::test]
async fn replaced_target_root_is_refused_before_any_directory_finalization() -> Result<()> {
    let source = tempfile::tempdir()?;
    let parent = tempfile::tempdir()?;
    let target = parent.path().join("target");
    let held = parent.path().join("held");
    std::fs::create_dir(&target)?;
    std::fs::set_permissions(&target, Permissions::from_mode(0o777))?;
    let root = roots(source.path(), &target, CancellationToken::new()).await?;
    root.capture().await?;
    std::fs::rename(&target, &held)?;
    std::fs::create_dir(&target)?;
    std::fs::set_permissions(&target, Permissions::from_mode(0o711))?;
    let destination = create_storage_for_role(&target.to_string_lossy(), None, false, StorageRole::Destination).await?;
    assert!(matches!(
        root.verify_target_binding(&destination).await,
        Err(AppError::CopyRootMetadataFailure {
            phase: "root_target_binding",
            ..
        })
    ));
    assert!(root.finish(&destination).await.is_err());
    assert_eq!(std::fs::metadata(&held)?.permissions().mode() & 0o7777, 0o777);
    assert_eq!(std::fs::metadata(&target)?.permissions().mode() & 0o7777, 0o711);
    assert!(source.path().is_dir());
    Ok(())
}

struct UnboundRootMetadata(Arc<dyn Metadata>);

#[async_trait]
impl Metadata for UnboundRootMetadata {
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        self.0.observe(path, plan).await
    }

    async fn apply(
        &self, path: &StoragePath, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.0.apply(path, mutation, cancel).await
    }
}

fn omit_directory_binding(destination: &mut Arc<StorageContext>) -> Result<()> {
    let destination = Arc::make_mut(destination);
    destination.roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .namespace(destination.roles.namespace()?)
    .read_source(destination.roles.read_source()?)
    .staged_destination(destination.roles.staged_destination()?)
    .metadata(Arc::new(UnboundRootMetadata(destination.roles.metadata()?)))
    .build()
    .unwrap_or_else(|error| panic!("construct unbound metadata role: {error}"));
    Ok(())
}

#[tokio::test]
async fn ordinary_root_finalization_succeeds_without_directory_binding() -> Result<()> {
    let source = tempfile::tempdir()?;
    let expected = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
    std::fs::set_permissions(source.path(), Permissions::from_mode(0o750))?;
    File::open(source.path())?.set_times(FileTimes::new().set_modified(expected))?;
    for require_persistence in [false, true] {
        let target = tempfile::tempdir()?;
        std::fs::set_permissions(target.path(), Permissions::from_mode(0o711))?;
        let mut root = roots(source.path(), target.path(), CancellationToken::new()).await?;
        root.require_persistence = require_persistence;
        omit_directory_binding(root.destination.as_mut().ok_or(AppError::ScanWorkerStopped)?)?;
        let destination = root.destination.clone().ok_or(AppError::ScanWorkerStopped)?;
        root.capture().await?;
        assert!(root.directory_bindings().is_none());
        std::fs::write(target.path().join("copied"), b"target")?;
        root.finish(&destination).await?;
        let actual = std::fs::metadata(target.path())?;
        assert_eq!(actual.permissions().mode() & 0o7777, 0o750);
        assert_eq!(actual.modified()?, expected);
        assert_eq!(std::fs::read(target.path().join("copied"))?, b"target");
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_root_finalization_checks_identity_before_an_unbound_write() -> Result<()> {
    let source = tempfile::tempdir()?;
    let parent = tempfile::tempdir()?;
    let target = parent.path().join("target");
    let held = parent.path().join("held");
    std::fs::create_dir(&target)?;
    std::fs::set_permissions(source.path(), Permissions::from_mode(0o750))?;
    std::fs::set_permissions(&target, Permissions::from_mode(0o777))?;
    let mut root = roots(source.path(), &target, CancellationToken::new()).await?;
    root.require_persistence = false;
    omit_directory_binding(root.destination.as_mut().ok_or(AppError::ScanWorkerStopped)?)?;
    root.capture().await?;
    std::fs::rename(&target, &held)?;
    std::fs::create_dir(&target)?;
    std::fs::set_permissions(&target, Permissions::from_mode(0o711))?;
    let mut destination =
        Arc::new(create_storage_for_role(&target.to_string_lossy(), None, false, StorageRole::Destination).await?);
    omit_directory_binding(&mut destination)?;
    let failure = root
        .finish(&destination)
        .await
        .err()
        .ok_or(AppError::ScanWorkerStopped)?;
    assert!(matches!(
        failure,
        AppError::CopyRootMetadataFailure {
            phase: "root_target_binding",
            ..
        }
    ));
    assert!(matches!(
        failure.storage_error(),
        Some(StorageError::TargetPublicationChanged { .. })
    ));
    assert_eq!(std::fs::metadata(&held)?.permissions().mode() & 0o7777, 0o777);
    assert_eq!(std::fs::metadata(&target)?.permissions().mode() & 0o7777, 0o711);
    Ok(())
}

#[tokio::test]
async fn root_finalization_keeps_its_binding_guarantee_after_reopening_the_target() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::set_permissions(source.path(), Permissions::from_mode(0o750))?;
    std::fs::set_permissions(target.path(), Permissions::from_mode(0o711))?;
    let mut root = roots(source.path(), target.path(), CancellationToken::new()).await?;
    root.require_persistence = false;
    root.capture().await?;
    assert!(root.directory_bindings().is_some());
    let mut destination = Arc::new(
        create_storage_for_role(&target.path().to_string_lossy(), None, false, StorageRole::Destination).await?,
    );
    omit_directory_binding(&mut destination)?;
    let failure = root
        .finish(&destination)
        .await
        .err()
        .ok_or(AppError::ScanWorkerStopped)?;
    assert!(
        matches!(failure.storage_error(), Some(StorageError::MetadataApplication(error))
        if matches!(error.storage_error(), Some(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Unsupported))
    );
    assert_eq!(std::fs::metadata(target.path())?.permissions().mode() & 0o7777, 0o711);
    Ok(())
}

#[tokio::test]
async fn cancelled_root_capture_stops_admission_and_keeps_the_typed_failure() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let root = roots(source.path(), target.path(), cancel).await?;
    assert!(root.capture().await.is_err());
    assert!(root.snapshot()?.is_none());
    let destination =
        create_storage_for_role(&target.path().to_string_lossy(), None, false, StorageRole::Destination).await?;
    let failure = root
        .finish(&destination)
        .await
        .err()
        .ok_or_else(|| AppError::ConfigError("cancel was ignored".into()))?;
    assert!(failure.storage_error().is_some_and(StorageError::is_cancelled));
    assert!(matches!(
        failure,
        AppError::CopyRootMetadataFailure {
            phase: "root_source_metadata_capture",
            ..
        }
    ));
    let combined = root.retain_capture_failure(Err(AppError::ConfigError("independent pipeline failure".into())));
    assert!(matches!(combined, Err(AppError::MigrationAndRootMetadataFailed { .. })));
    Ok(())
}

#[tokio::test]
async fn retaining_a_direct_capture_failure_keeps_the_same_observation_once() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let root = roots(source.path(), target.path(), cancel).await?;
    let error = root.capture().await.err().ok_or(AppError::ScanWorkerStopped)?;
    assert!(matches!(
        root.retain_capture_failure(Err(error)),
        Err(AppError::CopyRootMetadataFailure { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn retaining_capture_failure_finds_the_same_observation_in_a_second_branch() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let root = roots(source.path(), target.path(), cancel).await?;
    let error = root.capture().await.err().ok_or(AppError::ScanWorkerStopped)?;
    let error = AppError::MigrationAndSourceCleanupFailed {
        source: Box::new(AppError::ConfigError("consumer failed independently".into())),
        cleanup: Box::new(AppError::CopyExecutionStopped {
            source: Arc::new(error),
        }),
    };
    assert!(matches!(
        root.retain_capture_failure(Err(error)),
        Err(AppError::MigrationAndSourceCleanupFailed { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn retaining_capture_failure_preserves_a_distinct_root_failure() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let root = roots(source.path(), target.path(), cancel).await?;
    let original = root.capture().await.err().ok_or(AppError::ScanWorkerStopped)?;
    let different = AppError::CopyRootMetadataFailure {
        backend: BackendKind::Local,
        phase: "root_source_metadata_capture",
        source: Arc::new(StorageError::Cancelled),
    };
    let error = root.retain_capture_failure(Err(different));
    let Err(AppError::MigrationAndRootMetadataFailed { source, metadata }) = error else {
        panic!("a distinct root failure must not hide the capture failure: {error:?}");
    };
    let (
        AppError::CopyRootMetadataFailure { source: first, .. },
        AppError::CopyRootMetadataFailure { source: second, .. },
        AppError::CopyRootMetadataFailure { source: captured, .. },
    ) = (*source, *metadata, original)
    else {
        panic!("both root causes must remain typed");
    };
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&second, &captured));
    Ok(())
}

#[tokio::test]
async fn weak_target_identity_refuses_controlled_directory_admission() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let mut root = roots(source.path(), target.path(), CancellationToken::new()).await?;
    root.capture().await?;
    assert!(root.directory_bindings().is_some());
    let prepared = root.prepared.take();
    let Some(RootPreparation::Ready(mut prepared_target)) = prepared else {
        panic!("real local target must be prepared");
    };
    assert!(prepared_target.supports_bound_metadata);
    prepared_target.identity = SourceIdentity::new(
        prepared_target.identity.backend().clone(),
        IdentityStrength::PathScoped,
        b"root-path",
    )
    .map_err(StorageError::from)?;
    assert!(root.prepared.set(RootPreparation::Ready(prepared_target)).is_ok());
    assert!(
        root.directory_bindings().is_none(),
        "HDFS-like path identity must retain ordinary scan path"
    );
    Ok(())
}

#[tokio::test]
async fn weak_source_identity_is_allowed_with_stable_target_in_retain_source_mode() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let mut root = roots(source.path(), target.path(), CancellationToken::new()).await?;
    root.require_persistence = false;
    root.capture().await?;
    let captured = root.captured.take().ok_or(AppError::ScanWorkerStopped)?;
    let weak = SourceIdentity::new(
        captured.source_identity().backend().clone(),
        IdentityStrength::PathScoped,
        b"source-root-path",
    )
    .map_err(StorageError::from)?;
    let entry =
        ObservedEntry::new(StoragePath::root(), EntryKind::Directory, None, None, weak).map_err(StorageError::from)?;
    assert!(root.captured.set(Arc::new(entry)).is_ok());
    // 保留源任务只要求目标可识别替换；不把HDFS等源的路径身份误当删源许可。
    assert!(root.directory_bindings().is_some());
    Ok(())
}
