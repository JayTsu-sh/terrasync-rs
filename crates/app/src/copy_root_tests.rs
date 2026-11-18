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
