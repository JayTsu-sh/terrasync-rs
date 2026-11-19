#![allow(clippy::unwrap_used)]

use crate::error::AppError;
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::{observe_path, observe_path_with_plan};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use data_mover::model::{
    EntryOperationFailure, FailureClass, MetadataFamily, MetadataObservations, ObservationMode, ObservationPlan,
    Operation, StoragePath, Transience,
};
use data_mover::storage::{
    ListRequest, ListingStream, Metadata, MetadataMutation, Namespace, SourceDescriptor, StorageBuilder,
    StorageRoleFailure,
};
use tokio_util::sync::CancellationToken;

use crate::error::StorageError;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

struct FailingMetadata(StorageRoleFailure);

#[async_trait]
impl Metadata for FailingMetadata {
    async fn observe(&self, _: &StoragePath, _: ObservationPlan) -> Result<MetadataObservations, StorageRoleFailure> {
        Err(self.0.clone())
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Err(self.0.clone())
    }
}

#[tokio::test]
async fn optional_source_metadata_failure_retains_inline_facts_and_delivers_readable_data() {
    let root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"readable").unwrap();
    let source = context(root.path(), FailureClass::PermissionDenied, false).await;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default())
        .await
        .unwrap();
    let entry = observe_path_with_plan(
        source.storage(),
        Path::new("file"),
        ObservationPlan::default()
            .with_timestamps(ObservationMode::InlineOnly)
            .with_ownership_mode(ObservationMode::InlineOnly)
            .with_tags(ObservationMode::BestEffort),
    )
    .await
    .unwrap();
    assert!(entry.metadata().timestamps().value().is_some());
    assert!(entry.metadata().ownership_mode().value().is_some());
    let failures: Vec<_> = entry.metadata().failures().collect();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].family, MetadataFamily::Tags);
    assert_eq!(failures[0].class, FailureClass::PermissionDenied);
    let result = copy_file(&source, &target, &Arc::new(entry), CopyOptions::default(), None).await;
    assert!(matches!(
        result,
        Err(AppError::StorageError(StorageError::PublishedDataMetadata { data, source }))
            if data.transferred_bytes == b"readable".len() as u64
                && matches!(*source, StorageError::SourceMetadataAcquisitionFailed { .. })
    ));
    assert_eq!(std::fs::read(target_root.path().join("file")).unwrap(), b"readable");
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"readable");
}

struct FailingLink {
    inner: Arc<dyn Namespace>,
    failure: StorageRoleFailure,
}

#[async_trait]
impl Namespace for FailingLink {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    async fn read_link(&self, _: &StoragePath) -> Result<data_mover::model::SymlinkTarget, StorageRoleFailure> {
        Err(self.failure.clone())
    }
}

async fn context(root: &Path, class: FailureClass, link: bool) -> StorageContext {
    let original = create_storage(&root.to_string_lossy(), CreateStorageOptions::default())
        .await
        .unwrap();
    let failure = StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::new("file").unwrap(),
            Operation::Metadata,
            class,
            Transience::Permanent,
            "after successful stat",
        )
        .unwrap(),
    );
    let namespace = original.roles.namespace().unwrap();
    let metadata = original.roles.metadata().unwrap();
    let roles = StorageBuilder::new(original.roles.identity().clone(), original.roles.capabilities().clone())
        .read_source(original.roles.read_source().unwrap())
        .staged_destination(original.roles.staged_destination().unwrap())
        .namespace(if link {
            Arc::new(FailingLink {
                inner: namespace,
                failure: failure.clone(),
            })
        } else {
            namespace
        })
        .metadata(if link {
            metadata
        } else {
            Arc::new(FailingMetadata(failure))
        })
        .build()
        .unwrap();
    StorageContext {
        roles,
        block_size: original.block_size,
        reconnect_config: None,
    }
}

#[tokio::test]
async fn metadata_failure_after_stat_keeps_not_found_and_cancelled_classification() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"exists at stat").unwrap();
    for class in [FailureClass::NotFound, FailureClass::Cancelled] {
        let error = observe_path(context(root.path(), class, false).await.storage(), Path::new("file"))
            .await
            .unwrap_err();
        assert!(matches!(&error, StorageError::Role(StorageRoleFailure::Entry(cause)) if cause.class() == class));
        assert_eq!(error.is_not_found(), class == FailureClass::NotFound);
        assert_eq!(error.is_cancelled(), class == FailureClass::Cancelled);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn link_failure_after_stat_keeps_not_found_and_cancelled_classification() {
    let root = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("missing", root.path().join("file")).unwrap();
    for class in [FailureClass::NotFound, FailureClass::Cancelled] {
        let error = observe_path(context(root.path(), class, true).await.storage(), Path::new("file"))
            .await
            .unwrap_err();
        assert!(matches!(&error, StorageError::Role(StorageRoleFailure::Entry(cause)) if cause.class() == class));
        assert_eq!(error.is_not_found(), class == FailureClass::NotFound);
        assert_eq!(error.is_cancelled(), class == FailureClass::Cancelled);
    }
}
