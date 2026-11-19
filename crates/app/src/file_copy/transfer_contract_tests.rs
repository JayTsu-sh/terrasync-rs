//! 真实 Local 数据与拒绝重复元数据调用的 role，验证任务层确实使用捕获计划。
#![allow(clippy::unwrap_used)]
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use data_mover::metadata::ApplicationFailureKind;
#[cfg(unix)]
use data_mover::model::{EntryKind, ObservedEntry};
use data_mover::model::{
    EntryOperationFailure, FailureClass, MetadataCapabilities, MetadataObservations, ObservationPlan, Operation,
    StoragePath, Transience,
};
use data_mover::storage::{
    ByteStream, CheckpointObservation, CopiedMetadataTarget, DestinationPrepareRequest, Metadata, MetadataBatchFailure,
    MetadataMutation, PrepareRequest, PreparedStage, PublicationEvidence, PublicationFailure, PublishRequest,
    StagedDestination, StorageBuilder, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};
use data_mover::transfer::TransferPhase;
use task_model::TransferMode;
use tokio_util::sync::CancellationToken;

use super::{CopyOptions, VersionFailureContext, transfer};
use crate::error::StorageError;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn refused(message: &str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Metadata,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            message,
        )
        .unwrap(),
    )
}

struct MetadataReadOnly {
    inner: Arc<dyn Metadata>,
    refuse_observation: bool,
    unexpected: Arc<AtomicU64>,
}

#[async_trait]
impl Metadata for MetadataReadOnly {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        if self.refuse_observation {
            self.unexpected.fetch_add(1, Ordering::Relaxed);
            return Err(refused("unexpected source metadata reread"));
        }
        self.inner.observe(path, plan).await
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.unexpected.fetch_add(1, Ordering::Relaxed);
        Err(refused("unexpected post-publication metadata write"))
    }
}

#[derive(Clone, Copy)]
enum MetadataFault {
    Refused,
    BatchFailed,
}

struct MetadataBatchProbe {
    inner: Arc<dyn StagedDestination>,
    fault: Option<MetadataFault>,
    fail_verify: bool,
    calls: AtomicU64,
    cleanup_calls: AtomicU64,
}

#[async_trait]
impl StagedDestination for MetadataBatchProbe {
    fn supports_direct(&self) -> bool {
        self.inner.supports_direct()
    }
    fn resumable_at_destination(&self) -> bool {
        self.inner.resumable_at_destination()
    }
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        self.inner.copied_metadata_target()
    }
    fn may_set_owner(&self, uid: u32, gid: u32) -> bool {
        self.inner.may_set_owner(uid, gid)
    }
    async fn prepare_direct(
        &self, request: PrepareRequest, cancel: CancellationToken,
    ) -> std::result::Result<PreparedStage, StorageRoleFailure> {
        self.inner.prepare_direct(request, cancel).await
    }
    async fn prepare_at_destination(
        &self, request: DestinationPrepareRequest,
    ) -> std::result::Result<PreparedStage, StorageRoleFailure> {
        self.inner.prepare_at_destination(request).await
    }
    async fn write(
        &self, stage: &PreparedStage, input: ByteStream,
    ) -> std::result::Result<WriteEvidence, StorageRoleFailure> {
        self.inner.write(stage, input).await
    }
    async fn observe_checkpoint(
        &self, stage: &PreparedStage,
    ) -> std::result::Result<CheckpointObservation, StorageRoleFailure> {
        self.inner.observe_checkpoint(stage).await
    }
    async fn verify(
        &self, stage: &PreparedStage, request: VerifyRequest,
    ) -> std::result::Result<VerificationEvidence, StorageRoleFailure> {
        let evidence = self.inner.verify(stage, request).await?;
        if self.fail_verify {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    stage.final_destination().path().clone(),
                    Operation::Verify,
                    FailureClass::Protocol,
                    Transience::Permanent,
                    "injected final data verification failure",
                )
                .unwrap(),
            ));
        }
        Ok(evidence)
    }
    async fn remove_failed_direct(&self, stage: PreparedStage) -> std::result::Result<(), StorageRoleFailure> {
        self.cleanup_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.remove_failed_direct(stage).await
    }
    async fn apply_metadata_batch(
        &self, stage: &PreparedStage, mutations: Vec<MetadataMutation>, cancel: CancellationToken,
    ) -> std::result::Result<(), MetadataBatchFailure> {
        let previous = self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(fault) = self.fault
            && previous == 0
        {
            assert!(!mutations.is_empty());
            return Err(match fault {
                MetadataFault::Refused => MetadataBatchFailure {
                    failed_index: 0,
                    completed: 0,
                    error: Some(refused("injected staged metadata refusal")),
                },
                MetadataFault::BatchFailed => MetadataBatchFailure::whole_batch(
                    mutations.len(),
                    0,
                    Some(refused("injected staged metadata batch failure")),
                ),
            });
        }
        self.inner.apply_metadata_batch(stage, mutations, cancel).await
    }
    async fn publish(
        &self, stage: &PreparedStage, request: PublishRequest,
    ) -> std::result::Result<PublicationEvidence, PublicationFailure> {
        assert!(self.calls.load(Ordering::Relaxed) > 0, "元数据必须在发布前处理");
        self.inner.publish(stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> std::result::Result<(), StorageRoleFailure> {
        self.inner.discard(stage).await
    }
}

fn guard_metadata(
    context: &mut StorageContext, refuse_observation: bool, staged: Option<Arc<dyn StagedDestination>>,
) -> TestResult<Arc<AtomicU64>> {
    let original = &context.roles;
    let unexpected = Arc::new(AtomicU64::new(0));
    let staged = match staged {
        Some(staged) => staged,
        None => original.staged_destination()?,
    };
    context.roles = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .staged_destination(staged)
        .namespace(original.namespace()?)
        .metadata(Arc::new(MetadataReadOnly {
            inner: original.metadata()?,
            refuse_observation,
            unexpected: unexpected.clone(),
        }))
        .build()?;
    Ok(unexpected)
}

#[tokio::test]
async fn captured_metadata_is_batched_once_without_source_reread_or_later_target_write() -> TestResult {
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        for integrity in [false, true] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), b"payload")?;
            let mut source =
                create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
            let mut target =
                create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
            let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
            let source_calls = guard_metadata(&mut source, true, None)?;
            let probe = Arc::new(MetadataBatchProbe {
                inner: target.roles.staged_destination()?,
                fault: None,
                fail_verify: false,
                cleanup_calls: AtomicU64::new(0),
                calls: AtomicU64::new(0),
            });
            let target_calls = guard_metadata(&mut target, false, Some(probe.clone()))?;
            let bytes = Arc::new(AtomicU64::new(0));
            let outcome = transfer::copy_current(
                &source,
                &target,
                &entry,
                &CopyOptions {
                    transfer_mode: mode,
                    enable_integrity_check: integrity,
                    bytes_counter: Some(bytes.clone()),
                    ..Default::default()
                },
                None,
            )
            .await?;
            assert_eq!(outcome.blake3.is_some(), integrity);
            assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                (
                    source_calls.load(Ordering::Relaxed),
                    target_calls.load(Ordering::Relaxed)
                ),
                (0, 0)
            );
            assert_eq!(bytes.load(Ordering::Relaxed), 7);
            assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
            assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
        }
    }
    Ok(())
}

#[tokio::test]
async fn refused_metadata_keeps_published_data_and_counts_its_bytes_in_both_modes() -> TestResult {
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        for integrity in [false, true] {
            assert_metadata_failure_data(mode, integrity, MetadataFault::Refused).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn metadata_batch_failure_only_counts_direct_data_written_at_the_final_path() -> TestResult {
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        for integrity in [false, true] {
            assert_metadata_failure_data(mode, integrity, MetadataFault::BatchFailed).await?;
        }
    }
    Ok(())
}

async fn assert_metadata_failure_data(mode: TransferMode, integrity: bool, fault: MetadataFault) -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    std::fs::write(target_root.path().join("file"), b"old")?;
    let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    let probe = Arc::new(MetadataBatchProbe {
        inner: target.roles.staged_destination()?,
        fault: Some(fault),
        fail_verify: false,
        cleanup_calls: AtomicU64::new(0),
        calls: AtomicU64::new(0),
    });
    let target_calls = guard_metadata(&mut target, false, Some(probe.clone()))?;
    let bytes = Arc::new(AtomicU64::new(0));
    let error = transfer::copy_current(
        &source,
        &target,
        &entry,
        &CopyOptions {
            transfer_mode: mode,
            enable_integrity_check: integrity,
            bytes_counter: Some(bytes.clone()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap_err();
    let StorageError::Transfer(failure) = &error else {
        panic!("expected typed metadata transfer failure: {error:?}");
    };
    assert_eq!(failure.phase(), TransferPhase::Metadata);
    let (kind, message) = match fault {
        MetadataFault::Refused => (ApplicationFailureKind::Refused, "injected staged metadata refusal"),
        MetadataFault::BatchFailed => (
            ApplicationFailureKind::BatchFailed,
            "injected staged metadata batch failure",
        ),
    };
    assert_eq!(failure.metadata_failure().ok_or("metadata cause missing")?.kind(), kind);
    let data_ready = mode == TransferMode::Direct || matches!(fault, MetadataFault::Refused);
    assert_eq!(
        failure.published_data().map(|data| data.blake3.is_some()),
        data_ready.then_some(integrity)
    );
    assert_eq!(failure.final_destination_changed(), data_ready);
    let context = VersionFailureContext::for_error(&error, source.roles.kind(), target.roles.kind());
    assert_eq!(context.metadata_only, data_ready);
    assert_eq!(
        context.phase,
        if data_ready {
            "target_metadata_apply"
        } else {
            "Metadata"
        }
    );
    assert!(error.to_string().contains(message));
    assert_eq!(target_calls.load(Ordering::Relaxed), 0);
    assert_eq!(bytes.load(Ordering::Relaxed), if data_ready { 7 } else { 0 });
    let expected_target: &[u8] = if data_ready { b"payload" } else { b"old" };
    assert_eq!(std::fs::read(target_root.path().join("file"))?, expected_target);
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_plan_without_mode_keeps_private_creation_and_existing_direct_permissions() -> TestResult {
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        for existing in [false, true] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), b"payload")?;
            if existing {
                std::fs::write(target_root.path().join("file"), b"old")?;
                std::fs::set_permissions(target_root.path().join("file"), std::fs::Permissions::from_mode(0o640))?;
            }
            let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
            let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
            let observed = observe_path(source.storage(), Path::new("file")).await?;
            let entry = Arc::new(ObservedEntry::new(
                observed.path().clone(),
                EntryKind::File,
                observed.size(),
                observed.modified(),
                observed.source_identity().clone(),
            )?);
            transfer::copy_current(
                &source,
                &target,
                &entry,
                &CopyOptions {
                    transfer_mode: mode,
                    ..Default::default()
                },
                None,
            )
            .await?;
            let expected = if existing && mode == TransferMode::Direct {
                0o640
            } else {
                0o600
            };
            assert_eq!(
                std::fs::metadata(target_root.path().join("file"))?.permissions().mode() & 0o777,
                expected
            );
            assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
            assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_data_failure_removes_owned_target_while_source_and_typed_reason_survive() -> TestResult {
    for existing in [false, true] {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"source payload")?;
        if existing {
            std::fs::write(target_root.path().join("file"), b"old payload")?;
        }
        let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
        let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
        let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
        let role = Arc::new(MetadataBatchProbe {
            inner: target.roles.staged_destination()?,
            fault: None,
            fail_verify: true,
            calls: AtomicU64::new(0),
            cleanup_calls: AtomicU64::new(0),
        });
        guard_metadata(&mut target, false, Some(role.clone()))?;
        let failure = transfer::copy_current(
            &source,
            &target,
            &entry,
            &CopyOptions {
                enable_integrity_check: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(&failure, StorageError::Transfer(error)
            if error.phase() == TransferPhase::Verify && error.cleanup_failure().is_none()));
        assert!(failure.to_string().contains("injected final data verification failure"));
        assert_eq!(role.cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(!target_root.path().join("file").exists());
        assert_eq!(std::fs::read(source_root.path().join("file"))?, b"source payload");
    }
    Ok(())
}

#[tokio::test]
async fn copied_publication_cannot_authorize_another_source_snapshot() -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"verified source")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    for original in [false, true] {
        let published = transfer::copy_entry(&source, &target, &entry, CopyOptions::default())
            .await?
            .ok_or("ordinary file did not return its verified publication")?;
        let snapshot = entry.as_ref().clone();
        let expected = if original { entry.as_ref() } else { &snapshot };
        let checked = published.into_target(expected);
        if original {
            let checked = checked?;
            assert_eq!(checked.path, *entry.path());
            assert!(
                checked.version.version_id().is_none(),
                "durability confirmation must check Current"
            );
        } else {
            assert!(matches!(checked, Err(StorageError::TargetPublicationChanged { .. })));
        }
        assert_eq!(std::fs::read(source_root.path().join("file"))?, b"verified source");
        assert_eq!(std::fs::read(target_root.path().join("file"))?, b"verified source");
    }
    Ok(())
}
