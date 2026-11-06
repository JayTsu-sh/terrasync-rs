#![allow(clippy::unwrap_used)]

use crate::scan::observe_path;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use data_mover::model::{
    EntryOperationFailure, FailureClass, MetadataObservations, ObservationPlan, Operation, StoragePath, Transience,
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

struct FailingLink {
    inner: Arc<dyn Namespace>,
    failure: StorageRoleFailure,
}

#[async_trait]
impl Namespace for FailingLink {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path).await
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
