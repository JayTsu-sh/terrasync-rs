//! 未确认目标持久性时，数据和属性即使可读且一致，也不能触发源删除。
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use data_mover::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use data_mover::storage::{
    ListRequest, ListingStream, Namespace, SourceDescriptor, StorageBuilder, StorageRoleFailure,
};
use tokio_util::sync::CancellationToken;

use super::{SourceCleanupCheck, remove_entry};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file, metadata::set_entry_metadata};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

struct RefusedPersistence {
    inner: Arc<dyn Namespace>,
    class: FailureClass,
}

#[async_trait]
impl Namespace for RefusedPersistence {
    async fn stat(&self, path: &StoragePath) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    async fn persist_observed(
        &self, expected: &SourceDescriptor, _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                expected.path.clone(),
                Operation::Publish,
                self.class,
                Transience::Permanent,
                "injected target persistence refusal",
            )
            .unwrap(),
        ))
    }
}

fn refuse(target: &mut StorageContext, class: FailureClass) -> Result<()> {
    target.roles = StorageBuilder::new(target.roles.identity().clone(), target.roles.capabilities().clone())
        .read_source(target.roles.read_source()?)
        .metadata(target.roles.metadata()?)
        .staged_destination(target.roles.staged_destination()?)
        .namespace(Arc::new(RefusedPersistence {
            inner: target.roles.namespace()?,
            class,
        }))
        .build()
        .unwrap();
    Ok(())
}

fn check() -> SourceCleanupCheck {
    SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new())
}

#[tokio::test]
async fn persistence_errors_keep_both_files_and_directories_and_preserve_the_typed_cause() -> Result<()> {
    for directory in [false, true] {
        for class in [
            FailureClass::Protocol,
            FailureClass::Unsupported,
            FailureClass::Cancelled,
        ] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            let name = if directory { "directory" } else { "file" };
            if directory {
                std::fs::create_dir(source_root.path().join(name))?;
                std::fs::create_dir(target_root.path().join(name))?;
            } else {
                std::fs::write(source_root.path().join(name), b"payload")?;
            }
            let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let mut target =
                create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let entry = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
            if directory {
                set_entry_metadata(target.storage(), &entry).await?;
            } else {
                copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
            }
            refuse(&mut target, class)?;
            let error = remove_entry(&source, &target, &entry, &check(), None)
                .await
                .unwrap_err();
            assert_eq!(
                error.storage_error().is_some_and(StorageError::is_cancelled),
                class == FailureClass::Cancelled
            );
            assert!(matches!(error,
                AppError::StorageError(StorageError::TargetPersistenceUnconfirmed { path, source: StorageRoleFailure::Entry(failure) })
                    if path.as_str() == name && failure.class() == class
                        && failure.diagnostic().contains("injected target persistence refusal")));
            assert!(source_root.path().join(name).exists());
            assert!(target_root.path().join(name).exists());
            if !directory {
                assert_eq!(std::fs::read(source_root.path().join(name))?, b"payload");
                assert_eq!(std::fs::read(target_root.path().join(name))?, b"payload");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn later_metadata_only_updates_are_persisted_before_source_removal() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    std::fs::set_permissions(source_root.path().join("file"), std::fs::Permissions::from_mode(0o640))?;
    let entry = observe_path(source.storage(), Path::new("file")).await?;
    set_entry_metadata(target.storage(), &entry).await?;
    remove_entry(&source, &target, &entry, &check(), None).await?;
    assert!(!source_root.path().join("file").exists());
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
    assert_eq!(
        std::fs::metadata(target_root.path().join("file"))?.permissions().mode() & 0o777,
        0o640
    );
    Ok(())
}
