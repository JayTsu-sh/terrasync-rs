#![allow(clippy::unwrap_used)]

use super::*;
use crate::file_commit::stream as commit_stream;
use crate::file_copy::CopyOptions;
use crate::file_copy::metadata as copy_metadata;
use crate::file_copy::transfer as copy_transfer;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};
use async_trait::async_trait;
use blake3::Hasher;
use bytes::Bytes;
use data_mover::qos::QosManager;
use data_mover::storage::{
    BackendConfig, ByteStream, CheckpointObservation, CopiedMetadataTarget, CopiedValueTarget,
    DestinationPrepareRequest, LocalBackendConfig, Metadata, MetadataMutation, PreparedStage, PublicationEvidence,
    PublicationFailure, PublishRequest, StagedDestination, StorageBuilder, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence, connect_backend,
};
use data_mover::transfer::TransferPhase;
use std::error::Error;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::task::Poll;
use std::time::Duration;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedItem;
use tempfile::TempDir;
use tokio::sync::Notify;

fn role_failure(message: &str) -> storage::StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Write,
            FailureClass::Protocol,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("固定诊断")),
    )
}

/// 使用真实本地 stage，仅拦截元数据，验证上游引擎的应用和发布顺序。
struct MetadataProbe {
    inner: Arc<dyn StagedDestination>,
    final_path: PathBuf,
    fail: bool,
    called: AtomicBool,
    cleanup_failure: AtomicBool,
    resumable: AtomicBool,
    tags: Mutex<Vec<ObjectTag>>,
}

#[async_trait]
impl StagedDestination for MetadataProbe {
    fn resumable_at_destination(&self) -> bool {
        self.resumable.load(Ordering::SeqCst)
    }

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

async fn destination(fail: bool) -> (TempDir, StorageContext, Arc<MetadataProbe>) {
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
        resumable: AtomicBool::new(true),
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
        StorageContext {
            roles,
            block_size: 4,
            reconnect_config: None,
        },
        probe,
    )
}

async fn stream(destination: &StorageContext, entry: &ObservedEntry) -> Result<()> {
    let handle = commit_stream::prepare_stream(destination, entry).await?;
    let (tx, rx) = mpsc::channel(2);
    tx.send(PositionedChunk {
        offset: 0,
        data: Bytes::from_static(b"new"),
    })
    .await
    .unwrap();
    drop(tx);
    commit_stream::write_chunk_stream(rx, &handle, Arc::new(|_, _| {})).await?;
    assert_eq!(handle.received_size().await, 3);
    commit_stream::commit_chunk_stream(handle).await
}

#[tokio::test]
async fn stream_metadata_failure_keeps_old_destination() {
    let (root, destination, probe) = destination(true).await;
    let mut entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    entry = crate::observed_fixtures::with_mode(entry, Some(0o640));
    let error = stream(&destination, &entry).await.unwrap_err();
    assert!(probe.called.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
    match error {
        StorageError::Transfer(failure) => {
            assert_eq!(failure.phase(), TransferPhase::Metadata);
            assert!(!failure.final_destination_changed());
            assert!(failure.cleanup_failure().is_none());
            let metadata = failure.metadata_failure().unwrap();
            assert!(!metadata.failures().is_empty());
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
    let entry = {
        let fixture_path: std::path::PathBuf = "file".into();
        crate::observed_fixtures::observation(
            data_mover::model::BackendKind::S3,
            fixture_path,
            data_mover::model::EntryKind::File,
            3,
            [0, 1_234_000_000, 0],
            None,
            None,
            Some(vec![("team".into(), "archive".into())]),
            None,
        )
    };
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
    let mut entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    entry = crate::observed_fixtures::with_mode(entry, Some(0o640));
    entry = crate::observed_fixtures::with_times(entry, Some(2_000_000_000), Some(3_000_000_000));
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
    let _first = manager.source_group(128).unwrap();
    let groups = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let manager = &manager;
                scope.spawn(move || manager.source_group(256).unwrap())
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
        independent
            .source_group(256)
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
    let entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    let error = stream(&destination, &entry).await.unwrap_err();
    match error {
        StorageError::Transfer(failure) => {
            assert_eq!(failure.phase(), TransferPhase::Metadata);
            let metadata = failure.metadata_failure().unwrap();
            assert!(metadata.storage_error().is_some());
            assert!(!metadata.failures().is_empty());
            assert!(failure.cleanup_failure().is_some());
        }
        error => panic!("expected primary error and cleanup evidence: {error:?}"),
    }
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
}

#[tokio::test]
async fn unsupported_modes_fail_before_destination_mutation() {
    let (root, destination, probe) = destination(false).await;
    let entry = Arc::new(observe_path(destination.storage(), Path::new("file")).await.unwrap());
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        probe.resumable.store(false, Ordering::SeqCst);
        let error = copy_transfer::copy_entry(
            &destination,
            &destination,
            &entry,
            CopyOptions {
                transfer_mode: mode,
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StorageError::UnsupportedTransferMode { mode: rejected, .. } if rejected == mode));
        assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
        assert!(!probe.called.load(Ordering::SeqCst));
    }
    assert!(copy_transfer::validate_transfer_mode(&destination, TransferMode::Atomic).is_ok());
}

/// 上游 Local 的生产恢复门槛为 256 MiB；使用共享 chunk 控制内存，验证真实前缀复用。
#[tokio::test]
async fn checkpoint_stream_reuses_partial_prefix_and_verifies_full_payload() {
    use std::io::Read;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"old").unwrap();
    let destination = create_storage(
        root.path().to_str().unwrap(),
        CreateStorageOptions::new(Some(64 * 1024), false),
    )
    .await
    .unwrap();
    let mut entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    let size = 256 * 1024 * 1024 + 17;
    entry = crate::observed_fixtures::with_size(entry, size);
    let first = commit_stream::prepare_stream_with_mode(
        &destination,
        &entry,
        TransferMode::Checkpoint,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(2);
    let prefix = 1024 * 1024 + 3;
    tx.send(PositionedChunk {
        offset: 0,
        data: Bytes::from(vec![b'A'; prefix]),
    })
    .await
    .unwrap();
    drop(tx);
    let error = commit_stream::write_chunk_stream(rx, &first, Arc::new(|_, _| {}))
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Transfer(_)), "{error:?}");
    drop(error);
    first.discard().await.unwrap();
    drop(first);
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");

    let resumed = commit_stream::prepare_stream_with_mode(
        &destination,
        &entry,
        TransferMode::Checkpoint,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        resumed.state.lock().await.session.as_ref().unwrap().write_offset(),
        prefix as u64
    );
    let (tx, rx) = mpsc::channel(4);
    let writer_handle = resumed.clone();
    let writer =
        tokio::spawn(async move { commit_stream::write_chunk_stream(rx, &writer_handle, Arc::new(|_, _| {})).await });
    let chunk = Bytes::from(vec![b'A'; 64 * 1024]);
    let mut expected = Hasher::new();
    let mut offset = 0;
    while offset < size {
        let data = chunk.slice(..((size - offset).min(chunk.len() as u64) as usize));
        expected.update(&data);
        tx.send(PositionedChunk {
            offset,
            data: data.clone(),
        })
        .await
        .unwrap();
        offset += data.len() as u64;
    }
    drop(tx);
    writer.await.unwrap().unwrap();
    commit_stream::commit_chunk_stream(resumed).await.unwrap();
    assert_eq!(std::fs::metadata(root.path().join("file")).unwrap().len(), size);
    let mut reader = std::fs::File::open(root.path().join("file")).unwrap();
    let mut buffer = vec![0; 64 * 1024];
    let mut actual = Hasher::new();
    loop {
        let n = reader.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        actual.update(&buffer[..n]);
    }
    assert_eq!(actual.finalize(), expected.finalize());
}

#[tokio::test]
async fn a_selected_history_version_drives_stream_size_metadata_and_tags() {
    let (root, destination, probe) = destination(false).await;
    let observations: Vec<_> = [("old", 3, 100, "old-tag"), ("current", 7, 200, "current-tag")]
        .into_iter()
        .map(|(id, size, time, tag)| {
            crate::observed_fixtures::observation(
                BackendKind::S3,
                "file".into(),
                EntryKind::File,
                size,
                [time; 3],
                None,
                None,
                Some(vec![("version".into(), tag.into())]),
                Some((Some(id.into()), id == "current", false)),
            )
        })
        .collect();
    let history = ObjectHistory::whole(
        observations
            .iter()
            .flat_map(|entry| entry.history().unwrap().records().to_vec())
            .collect(),
        HistoryOrder::Server,
    )
    .unwrap();
    let original = Arc::new(
        observations
            .into_iter()
            .last()
            .unwrap()
            .with_history(Some(Box::new(history)))
            .unwrap(),
    );
    let selected = ObservedItem {
        entry: Arc::clone(&original),
        version_index: Some(0),
    };
    let handle = commit_stream::prepare_observed_stream(
        &destination,
        selected.view(),
        TransferMode::Atomic,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(StreamChunk::from(PositionedChunk {
        offset: 0,
        data: Bytes::from_static(b"old"),
    }))
    .await
    .unwrap();
    drop(tx);
    commit_stream::write_planned_stream(rx, &handle).await.unwrap();
    commit_stream::commit_chunk_stream(handle).await.unwrap();
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
    assert_eq!(
        probe
            .tags
            .lock()
            .unwrap()
            .iter()
            .map(|tag| (tag.key(), tag.value()))
            .collect::<Vec<_>>(),
        [("version", "old-tag")]
    );
    assert_eq!(
        std::fs::metadata(root.path().join("file"))
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        100
    );
    assert!(Arc::ptr_eq(&selected.entry, &original));
}

#[cfg(unix)]
#[tokio::test]
async fn stream_keeps_true_mode_without_numeric_ownership() {
    use data_mover::model::{PermissionFacts, PrincipalIdentity};
    for backend in [BackendKind::Hdfs, BackendKind::Nfs] {
        let (root, destination, _) = destination(false).await;
        let entry = crate::observed_fixtures::observation(
            backend,
            "file".into(),
            EntryKind::File,
            3,
            [0; 3],
            None,
            None,
            None,
            None,
        )
        .with_permissions(Some(Box::new(PermissionFacts {
            owner: (backend == BackendKind::Hdfs).then(|| PrincipalIdentity::Named("alice".into())),
            group: None,
            mode: Some(0o6640),
        })))
        .unwrap();
        // Use the wire round trip: the receiver only knows the advertised snapshot.
        let entry =
            ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes(), entry.source_identity().backend())
                .unwrap();
        assert_eq!(entry.get_mode(), Some(0o6640));
        assert_eq!(entry.get_uid(), None);
        stream(&destination, &entry).await.unwrap();
        assert_eq!(
            std::fs::metadata(root.path().join("file"))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o640
        );
    }
}

struct PublishedMetadataProbe {
    fail: bool,
    mutations: Mutex<Vec<MetadataMutation>>,
}

#[async_trait]
impl Metadata for PublishedMetadataProbe {
    async fn observe(
        &self, _: &StoragePath, _: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        panic!("已捕获的元数据不能重新观察源端");
    }

    async fn apply(
        &self, _: &StoragePath, mutation: MetadataMutation, _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        if self.fail {
            return Err(role_failure("injected published metadata failure"));
        }
        self.mutations.lock().unwrap().push(mutation);
        Ok(())
    }
}

async fn published_metadata_destination(fail: bool) -> (TempDir, StorageContext, Arc<PublishedMetadataProbe>) {
    let (root, destination, stage) = destination(false).await;
    let metadata = Arc::new(PublishedMetadataProbe {
        fail,
        mutations: Mutex::default(),
    });
    let roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .read_source(destination.roles.read_source().unwrap())
    .staged_destination(stage)
    .namespace(destination.roles.namespace().unwrap())
    .metadata(metadata.clone())
    .build()
    .unwrap();
    (
        root,
        StorageContext {
            roles,
            block_size: 4,
            reconnect_config: None,
        },
        metadata,
    )
}

fn captured_tags(tags: Vec<ObjectTag>) -> ObservedEntry {
    let identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::S3, "captured-tags").unwrap(),
        IdentityStrength::PathScoped,
        b"file",
    )
    .unwrap();
    let metadata = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: tags,
            provenance: MetadataProvenance::Inline,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )
    .unwrap();
    ObservedEntry::new(
        StoragePath::new("file").unwrap(),
        EntryKind::File,
        Some(3),
        None,
        identity,
    )
    .unwrap()
    .with_metadata(metadata)
}

#[tokio::test]
async fn published_metadata_applies_and_clears_tags_without_inventing_timestamps() {
    let (root, destination, probe) = published_metadata_destination(false).await;
    let tag = ObjectTag::new("test", "captured").unwrap();
    for tags in [vec![tag.clone()], Vec::new()] {
        copy_metadata::set_entry_metadata(destination.storage(), &captured_tags(tags))
            .await
            .unwrap();
    }
    let mutations = probe.mutations.lock().unwrap();
    assert_eq!(mutations.len(), 2);
    assert!(matches!(&mutations[0], MetadataMutation::Tags(tags) if tags == &[tag]));
    assert!(matches!(&mutations[1], MetadataMutation::Tags(tags) if tags.is_empty()));
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
}

#[tokio::test]
async fn published_metadata_preserves_typed_application_failure() {
    let (_, destination, _) = published_metadata_destination(true).await;
    let error = copy_metadata::set_entry_metadata(destination.storage(), &captured_tags(Vec::new()))
        .await
        .unwrap_err();
    let StorageError::MetadataApplication(failure) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(failure.storage_error().is_some());
    assert_eq!(failure.family(), MetadataFamily::Tags);
}

#[cfg(unix)]
#[tokio::test]
async fn published_directory_keeps_setgid_without_fabricating_numeric_owner() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("directory")).unwrap();
    let destination = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default())
        .await
        .unwrap();
    let source = crate::observed_fixtures::observation(
        BackendKind::Hdfs,
        "directory".into(),
        EntryKind::Directory,
        0,
        [0; 3],
        None,
        None,
        None,
        None,
    )
    .with_permissions(Some(Box::new(PermissionFacts {
        owner: Some(PrincipalIdentity::Named("hdfs".into())),
        group: Some(PrincipalIdentity::Named("supergroup".into())),
        mode: Some(0o2775),
    })))
    .unwrap();
    copy_metadata::set_entry_metadata(destination.storage(), &source)
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(root.path().join("directory"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o2775
    );
}

#[tokio::test]
async fn stream_cancel_before_publication_keeps_the_original_reason_and_target() {
    let (root, destination, probe) = destination(false).await;
    let entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    let handle = commit_stream::prepare_stream(&destination, &entry).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(PositionedChunk {
        offset: 0,
        data: Bytes::from_static(b"new"),
    })
    .await
    .unwrap();
    drop(tx);
    commit_stream::write_chunk_stream(rx, &handle, Arc::new(|_, _| {}))
        .await
        .unwrap();
    handle.cancel();
    let error = commit_stream::commit_chunk_stream(handle).await.unwrap_err();
    match error {
        StorageError::Transfer(failure) => {
            assert_eq!(failure.phase(), TransferPhase::Verify);
            assert!(failure.to_string().contains("cancelled"));
            assert!(!failure.final_destination_changed());
            assert!(failure.cleanup_failure().is_none());
        }
        error => panic!("expected original typed cancellation reason: {error:?}"),
    }
    assert!(!probe.called.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn cancelling_a_pending_stream_preserves_the_native_reason_and_cleans_the_stage() {
    let (root, destination, probe) = destination(false).await;
    let entry = observe_path(destination.storage(), Path::new("file")).await.unwrap();
    let handle = commit_stream::prepare_stream(&destination, &entry).await.unwrap();
    let polled = Arc::new(Notify::new());
    let reader = Arc::clone(&polled);
    let input: Input = Box::pin(stream::poll_fn(move |_| {
        reader.notify_one();
        Poll::Pending
    }));
    let writer = handle.clone();
    let writing = tokio::spawn(async move { commit_stream::write_input(input, &writer, Arc::new(|_, _| {})).await });
    tokio::time::timeout(Duration::from_secs(2), polled.notified())
        .await
        .unwrap();
    handle.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), writing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let StorageError::Transfer(failure) = error else {
        panic!("expected native transfer failure");
    };
    assert_eq!(failure.phase(), TransferPhase::Transfer);
    assert!(matches!(
        failure.source().and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()),
        Some(StorageRoleFailure::Entry(cause)) if cause.class() == FailureClass::Cancelled
            && cause.operation() == Operation::Write && cause.transience() == Transience::Transient
    ));
    assert!(failure.cleanup_failure().is_none());
    assert!(!failure.final_destination_changed());
    assert_eq!(handle.received_size().await, 0);
    assert!(!handle.input_ended().await);
    assert!(!probe.called.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}
