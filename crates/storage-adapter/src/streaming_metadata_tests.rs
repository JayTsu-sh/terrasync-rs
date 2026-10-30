#![allow(clippy::unwrap_used)]

use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use data_mover::storage::{
    BackendConfig, CheckpointObservation, CopiedMetadataTarget, DestinationPrepareRequest, LocalBackendConfig,
    MetadataMutation, PreparedStage, PublicationEvidence, PublicationFailure, PublishRequest, StagedDestination,
    StorageBuilder, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence, connect_backend,
};
use data_mover::transfer::TransferPhase;
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use tempfile::TempDir;

/// 使用真实本地 stage，仅拦截元数据，验证上游引擎的应用和发布顺序。
struct MetadataProbe {
    inner: Arc<dyn StagedDestination>,
    final_path: PathBuf,
    fail: bool,
    called: AtomicBool,
    cleanup_failure: AtomicBool,
    tags: Mutex<Vec<ObjectTag>>,
}

#[async_trait]
impl StagedDestination for MetadataProbe {
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        let mut target = self.inner.copied_metadata_target().unwrap();
        target.tags = CopiedValueTarget::Supported;
        Some(target)
    }
    fn may_set_owner(&self, uid: u32, gid: u32) -> bool {
        self.inner.may_set_owner(uid, gid)
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
        self.inner.verify(stage, request).await
    }
    async fn apply_metadata(
        &self, stage: &PreparedStage, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        assert_eq!(std::fs::read(&self.final_path).unwrap(), b"old");
        self.called.store(true, Ordering::SeqCst);
        if self.fail {
            return Err(role_failure("injected metadata failure"));
        }
        if let MetadataMutation::Tags(tags) = mutation {
            *self.tags.lock().unwrap() = tags;
            Ok(())
        } else {
            self.inner.apply_metadata(stage, mutation, cancel).await
        }
    }
    async fn publish(
        &self, stage: &PreparedStage, request: PublishRequest,
    ) -> std::result::Result<PublicationEvidence, PublicationFailure> {
        assert!(self.called.load(Ordering::SeqCst));
        self.inner.publish(stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> std::result::Result<(), StorageRoleFailure> {
        self.inner.discard(stage).await?;
        if self.cleanup_failure.load(Ordering::SeqCst) {
            return Err(role_failure("injected cleanup failure"));
        }
        Ok(())
    }
}

async fn destination(fail: bool) -> (TempDir, StorageEnum, Arc<MetadataProbe>) {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"old").unwrap();
    let local = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.path().to_path_buf(),
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
    }))
    .await
    .unwrap();
    let probe = Arc::new(MetadataProbe {
        inner: local.staged_destination().unwrap(),
        final_path: root.path().join("file"),
        fail,
        called: AtomicBool::new(false),
        cleanup_failure: AtomicBool::new(false),
        tags: Mutex::default(),
    });
    let roles = StorageBuilder::new(local.identity().clone(), local.capabilities().clone())
        .read_source(local.read_source().unwrap())
        .staged_destination(probe.clone())
        .namespace(local.namespace().unwrap())
        .metadata(local.metadata().unwrap())
        .build()
        .unwrap();
    (
        root,
        StorageEnum::Local(ConnectedStorage { roles, block_size: 4 }),
        probe,
    )
}

async fn stream(destination: &StorageEnum, entry: &EntryEnum) -> Result<()> {
    let handle = StorageEnum::prepare_stream(destination, entry).await?;
    let (tx, rx) = mpsc::channel(2);
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::from_static(b"new"),
    })
    .await
    .unwrap();
    drop(tx);
    StorageEnum::write_chunk_stream(destination, entry, rx, &handle, None, Arc::new(|_, _| {})).await?;
    assert_eq!(handle.received_size().await, 3);
    StorageEnum::commit_chunk_stream(destination, entry, 3, handle).await
}

#[tokio::test]
async fn stream_metadata_failure_keeps_old_destination() {
    let (root, destination, probe) = destination(true).await;
    let mut entry = destination.get_metadata(Path::new("file")).await.unwrap();
    if let EntryEnum::NAS(entry) = &mut entry {
        entry.mode = Some(0o640);
    }
    let error = stream(&destination, &entry).await.unwrap_err();
    assert!(probe.called.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
    match error {
        StorageError::StreamTransfer(failure) => {
            assert_eq!(failure.phase, TransferPhase::Metadata);
            assert!(!failure.final_destination_changed);
            assert!(failure.cleanup_failure.is_none());
            assert!(failure.metadata_report.is_some());
            assert!(!failure.metadata_failures.is_empty());
        }
        error => panic!("expected typed transfer failure: {error:?}"),
    }
    let paths: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(paths, vec![std::ffi::OsString::from("file")]);
}

#[tokio::test]
async fn stream_s3_tags_are_applied_before_publication() {
    let (root, destination, probe) = destination(false).await;
    let entry = EntryEnum::S3(S3Entry {
        name: "file".into(),
        relative_path: "file".into(),
        extension: None,
        size: 3,
        mtime: 1_234_000_000,
        tags: Some(vec![Tag {
            key: "team".into(),
            value: "archive".into(),
        }]),
        version_id: None,
        is_latest: true,
        is_delete_marker: false,
        version_count: None,
        is_dir: false,
    });
    stream(&destination, &entry).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"new");
    let tags = probe.tags.lock().unwrap();
    assert_eq!(tags[0].key(), "team");
    assert_eq!(tags[0].value(), "archive");
}

#[cfg(unix)]
#[tokio::test]
async fn stream_applies_mode_owner_and_timestamps_on_stage() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let (root, destination, _) = destination(false).await;
    let mut entry = destination.get_metadata(Path::new("file")).await.unwrap();
    if let EntryEnum::NAS(entry) = &mut entry {
        entry.mode = Some(0o640);
        entry.atime = 2_000_000_000;
        entry.mtime = 3_000_000_000;
    }
    stream(&destination, &entry).await.unwrap();
    let metadata = std::fs::metadata(root.path().join("file")).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o640);
    assert_eq!(Some(metadata.uid()), entry.get_uid());
    assert_eq!(Some(metadata.gid()), entry.get_gid());
    assert_eq!(metadata.mtime(), 3);
    assert_eq!(metadata.atime(), 2);
}

#[tokio::test]
async fn qos_cache_shares_one_budget_for_the_same_config_under_concurrent_access() {
    let manager = QosManager::try_new(None, 1.0, None).unwrap();
    let _first = source_qos(&manager, 128).unwrap();
    let groups = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let manager = &manager;
                scope.spawn(move || source_qos(manager, 256).unwrap())
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    for group in groups {
        assert_eq!(
            group
                .transfer_budget()
                .admit_request(1024, &CancellationToken::new())
                .await
                .unwrap(),
            128
        );
    }
    let independent = QosManager::try_new(None, 1.0, None).unwrap();
    assert_eq!(
        source_qos(&independent, 256)
            .unwrap()
            .transfer_budget()
            .admit_request(1024, &CancellationToken::new())
            .await
            .unwrap(),
        256
    );
}

#[tokio::test]
async fn stream_keeps_primary_metadata_error_when_cleanup_also_fails() {
    let (root, destination, probe) = destination(true).await;
    probe.cleanup_failure.store(true, Ordering::SeqCst);
    let entry = destination.get_metadata(Path::new("file")).await.unwrap();
    let error = stream(&destination, &entry).await.unwrap_err();
    match error {
        StorageError::StreamTransfer(diagnostic) => {
            assert_eq!(diagnostic.phase, TransferPhase::Metadata);
            assert!(diagnostic.role.is_some());
            assert!(!diagnostic.metadata_failures.is_empty());
            assert!(diagnostic.cleanup_failure.is_some());
        }
        error => panic!("expected primary error and cleanup evidence: {error:?}"),
    }
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
}
