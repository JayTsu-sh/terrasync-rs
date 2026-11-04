#![allow(clippy::unwrap_used)]
use super::*;
use async_trait::async_trait;
use data_mover::{
    model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataObservations, ObservationPlan,
        SourceIdentity, StoragePath,
    },
    storage::{
        BackendCapabilities, BackendConfig as NativeBackendConfig, CapabilityAvailability, ListRequest, ListingStream,
        Metadata, MetadataMutation, Namespace, S3BackendConfig, SourceDescriptor, StorageBuilder, StorageRoleFailure,
        UnsupportedReason,
    },
    traversal::RemoteScanFailureKind,
};
use std::{result::Result as StdResult, sync::Arc, time::Duration};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct StalledNamespace;

#[async_trait]
impl Namespace for StalledNamespace {
    async fn stat(&self, path: &StoragePath) -> StdResult<SourceDescriptor, StorageRoleFailure> {
        Ok(SourceDescriptor::new(
            path.clone(),
            EntryKind::Directory,
            None,
            SourceIdentity::new(
                BackendIdentity::new(BackendKind::S3, "recovery-options").unwrap(),
                IdentityStrength::PathScoped,
                b"root",
            )
            .unwrap(),
        ))
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_producer(request.path, |sink| async move { sink.closed().await })
    }
}

#[async_trait]
impl Metadata for StalledNamespace {
    async fn observe(
        &self, _: &StoragePath, _: ObservationPlan,
    ) -> StdResult<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> StdResult<(), StorageRoleFailure> {
        unreachable!("this traversal only observes")
    }
}

fn storage() -> ConnectedStorage {
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused in traversal test").unwrap());
    let roles = StorageBuilder::new(
        BackendIdentity::new(BackendKind::S3, "recovery-options").unwrap(),
        BackendCapabilities::new(
            unavailable.clone(),
            unavailable,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(Arc::new(StalledNamespace))
    .metadata(Arc::new(StalledNamespace))
    .build()
    .unwrap();
    ConnectedStorage {
        roles,
        block_size: 4096,
        reconnect_config: None,
    }
}

fn reconnect_config() -> Arc<NativeBackendConfig> {
    Arc::new(NativeBackendConfig::S3(S3BackendConfig {
        url: "invalid-test-endpoint".into(),
        block_size: None,
        single_put_threshold: None,
    }))
}

#[tokio::test]
async fn ordinary_traversal_deadline_applies_without_recovery_configuration() {
    for can_reconnect in [false, true] {
        let mut storage = storage();
        storage.reconnect_config = can_reconnect.then(reconnect_config);
        let options = WalkOptions {
            operation_timeout: Some(Duration::from_millis(10)),
            ..WalkOptions::default()
        };
        let mut execution = storage
            .start_traversal(TraversalRequest::new(StoragePath::root()), &options)
            .unwrap();
        assert!(
            execution.recovery.is_none(),
            "reconnect capability must not select a recovery budget"
        );
        timeout(Duration::from_secs(1), async {
            while execution.session.next_item().await.is_some() {}
        })
        .await
        .unwrap();
        assert!(
            execution.session.finish().await.is_err(),
            "a timed out listing cannot complete successfully"
        );
        assert_eq!(
            execution.failure.failure().unwrap().kind,
            RemoteScanFailureKind::OperationTimedOut
        );
    }
}

#[tokio::test]
async fn a_caller_can_select_a_recovery_window_without_inheriting_the_scan_budget() {
    let mut storage = storage();
    storage.reconnect_config = Some(reconnect_config());
    let options = WalkOptions {
        operation_timeout: Some(Duration::from_millis(10)),
        recovery_window: Some(Duration::from_millis(60)),
        ..WalkOptions::default()
    };
    let mut execution = storage
        .start_traversal(TraversalRequest::new(StoragePath::root()), &options)
        .unwrap();
    assert!(execution.recovery.is_some());
    timeout(Duration::from_secs(1), async {
        while execution.session.next_item().await.is_some() {}
    })
    .await
    .unwrap();
    assert!(execution.session.finish().await.is_err());
    assert_eq!(
        execution.failure.failure().unwrap().kind,
        RemoteScanFailureKind::OperationTimedOut
    );
}
