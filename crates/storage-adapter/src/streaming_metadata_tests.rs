#![allow(clippy::unwrap_used)]

use super::*;
use crate::ObservedEntryExt;
use async_trait::async_trait;
use bytes::Bytes;
use data_mover::storage::{
    BackendConfig, CheckpointObservation, CopiedMetadataTarget, CopiedValueTarget, DestinationPrepareRequest,
    LocalBackendConfig, Metadata, MetadataMutation, PreparedStage, PublicationEvidence, PublicationFailure,
    PublishRequest, StagedDestination, StorageBuilder, StorageRoleFailure, VerificationEvidence, VerifyRequest,
    WriteEvidence, connect_backend,
};
use data_mover::transfer::TransferPhase;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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
        StorageEnum::Local(ConnectedStorage {
            roles,
            block_size: 4,
            reconnect_config: None,
        }),
        probe,
    )
}

async fn stream(destination: &StorageEnum, entry: &ObservedEntry) -> Result<()> {
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
    entry = crate::observed_fixtures::with_mode(entry, Some(0o640));
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
            (Some(vec![Tag {
                key: "team".into(),
                value: "archive".into(),
            }]))
            .map(|tags| tags.into_iter().map(|tag| (tag.key, tag.value)).collect()),
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
    let mut entry = destination.get_metadata(Path::new("file")).await.unwrap();
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

#[tokio::test]
async fn unsupported_modes_fail_before_destination_mutation() {
    let (root, destination, probe) = destination(false).await;
    let entry = Arc::new(destination.get_metadata(Path::new("file")).await.unwrap());
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        probe.resumable.store(false, Ordering::SeqCst);
        let error = StorageEnum::copy_file(
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
    assert!(destination.validate_transfer_mode(TransferMode::Atomic).is_ok());
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
    let mut entry = destination.get_metadata(Path::new("file")).await.unwrap();
    let size = 256 * 1024 * 1024 + 17;
    entry = crate::observed_fixtures::with_size(entry, size);
    let first =
        StorageEnum::prepare_stream_with_mode(&destination, &entry, TransferMode::Checkpoint, CancellationToken::new())
            .await
            .unwrap();
    let (tx, rx) = mpsc::channel(2);
    let prefix = 1024 * 1024 + 3;
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::from(vec![b'A'; prefix]),
    })
    .await
    .unwrap();
    drop(tx);
    let error = StorageEnum::write_chunk_stream(&destination, &entry, rx, &first, None, Arc::new(|_, _| {}))
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Transfer(_)), "{error:?}");
    drop(error);
    first.discard().await.unwrap();
    drop(first);
    assert_eq!(std::fs::read(root.path().join("file")).unwrap(), b"old");

    let resumed =
        StorageEnum::prepare_stream_with_mode(&destination, &entry, TransferMode::Checkpoint, CancellationToken::new())
            .await
            .unwrap();
    assert_eq!(resumed.state.lock().await.resume_offset, prefix as u64);
    let (tx, rx) = mpsc::channel(4);
    let writer_handle = resumed.clone();
    let writer_dest = destination.clone();
    let writer_entry = entry.clone();
    let writer = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(
            &writer_dest,
            &writer_entry,
            rx,
            &writer_handle,
            None,
            Arc::new(|_, _| {}),
        )
        .await
    });
    let chunk = Bytes::from(vec![b'A'; 64 * 1024]);
    let mut expected = Hasher::new();
    let mut offset = 0;
    while offset < size {
        let data = chunk.slice(..((size - offset).min(chunk.len() as u64) as usize));
        expected.update(&data);
        tx.send(DataChunk {
            offset,
            data: data.clone(),
        })
        .await
        .unwrap();
        offset += data.len() as u64;
    }
    drop(tx);
    writer.await.unwrap().unwrap();
    StorageEnum::commit_chunk_stream(&destination, &entry, size, resumed)
        .await
        .unwrap();
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
    let handle = StorageEnum::prepare_observed_stream(
        &destination,
        selected.view(),
        TransferMode::Atomic,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(StreamChunk::from(DataChunk {
        offset: 0,
        data: Bytes::from_static(b"old"),
    }))
    .await
    .unwrap();
    drop(tx);
    StorageEnum::write_planned_stream(rx, &handle).await.unwrap();
    StorageEnum::commit_chunk_stream(&destination, &original, 3, handle)
        .await
        .unwrap();
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

async fn published_metadata_destination(fail: bool) -> (TempDir, StorageEnum, Arc<PublishedMetadataProbe>) {
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
        StorageEnum::S3(ConnectedStorage {
            roles,
            block_size: 4,
            reconnect_config: None,
        }),
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
        destination.set_entry_metadata(&captured_tags(tags)).await.unwrap();
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
    let error = destination
        .set_entry_metadata(&captured_tags(Vec::new()))
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
    destination.set_entry_metadata(&source).await.unwrap();
    assert_eq!(
        std::fs::metadata(root.path().join("directory"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o2775
    );
}
