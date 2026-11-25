//! 本地真实复制与受控 role 故障；远程链路和性能另行验收。
#![allow(clippy::unwrap_used)]
use super::{ControlledCopyCounters, DirectoryBudget, run};
use crate::copy_bytes::CopyBytes;
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, StorageError},
    migration_plan::MigrationPlan,
    receiver::{LocalCopyExecutor, LocalCopyOutcome, LocalCopyPhase, LocalCopyWork, ReceiverConfig},
    scan::{observe_path_with_plan, source::WalkOptions},
    storage_factory::{CreateStorageOptions, StorageContext, create_storage},
};
use async_trait::async_trait;
use data_mover::{
    model::{
        AclMetadata, BackendSessionFailure, EntryIdentityKey, EntryOperationFailure, FailureClass,
        MetadataCapabilities, MetadataObservation, MetadataObservationParts, MetadataObservations, ObservationMode,
        ObservationPlan, Operation, SourceIdentity, StoragePath, SymlinkTarget, Transience,
    },
    storage::{
        BackendCapabilities, ByteStream, CapabilityAvailability, CheckpointObservation, CopiedMetadataObservation,
        CopiedMetadataTarget, DestinationPrepareRequest, ListRequest, ListingPage, ListingStream, Metadata,
        MetadataBatchFailure, MetadataMutation, Namespace, PrepareRequest, PreparedStage, PublicationEvidence,
        PublicationFailure, PublishRequest, SourceDescriptor, StagedDestination, StorageBuilder, StorageRoleFailure,
        UnsupportedReason, VerificationEvidence, VerifyRequest, WriteEvidence,
    },
};
use futures::StreamExt;
use std::{
    fs::{File, FileTimes, Permissions},
    future::pending,
    num::NonZeroUsize,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use task_model::{StorageEntryMessage, TransferMode};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn executor(source: &Path, target: &Path, cancel: CancellationToken) -> TestResult<LocalCopyExecutor> {
    Ok(LocalCopyExecutor {
        id: 0,
        source: Arc::new(create_storage(&source.to_string_lossy(), CreateStorageOptions::default()).await?),
        destination: Arc::new(create_storage(&target.to_string_lossy(), CreateStorageOptions::default()).await?),
        config: Arc::new(ReceiverConfig {
            job_id: "controlled-local".into(),
            enable_integrity_check: false,
            enable_acl: true,
            is_source_reserved: true,
            job_dir: String::new(),
            transfer_mode: TransferMode::Direct,
            cancel: Some(cancel),
        }),
        qos: None,
        bytes: None,
        broadcaster: BroadcastForwarder::new(1),
        active: Arc::new(AtomicUsize::new(0)),
        cleanup: None,
    })
}

fn options() -> WalkOptions {
    WalkOptions {
        observation_plan: Some(
            ObservationPlan::default()
                .with_timestamps(ObservationMode::InlineOnly)
                .with_ownership_mode(ObservationMode::InlineOnly),
        ),
        concurrency: 1,
        ..WalkOptions::default()
    }
}

async fn copy(
    mut executor: LocalCopyExecutor, mut options: WalkOptions, workers: usize, window: usize, directory_count: usize,
) -> TestResult<(crate::error::Result<()>, Vec<StorageEntryMessage>, usize)> {
    let cancel = executor.config.cancel.clone().unwrap();
    options.cancel = Some(cancel.clone());
    let session = executor.source.consumer_traversal(
        &options,
        NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
        NonZeroUsize::new(window).unwrap(),
    )?;
    let mut events = executor.broadcaster.subscribe();
    let counted = Arc::new(AtomicUsize::new(0));
    let counters = ControlledCopyCounters {
        entries: counted.clone(),
        bytes: Arc::new(AtomicU64::new(0)),
        active: Arc::new(AtomicUsize::new(0)),
    };
    let copying = run(
        session,
        vec![executor.clone(); workers],
        executor,
        counters,
        DirectoryBudget {
            count: directory_count,
            bytes: 8 * 1024 * 1024,
        },
        cancel,
    );
    tokio::pin!(copying);
    let mut captured = Vec::new();
    loop {
        tokio::select! {
            event = events.recv() => if let Some(event) = event { captured.push(event); },
            result = &mut copying => {
                while let Ok(event) = events.try_recv() { captured.push(event); }
                return Ok((result, captured, counted.load(Ordering::SeqCst)));
            }
        }
    }
}

fn acl(permission: u16) -> AclMetadata {
    let mut bytes = 2u32.to_le_bytes().to_vec();
    for (tag, bits) in [(1u16, 7u16), (4, permission), (32, 1)] {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&bits.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    }
    AclMetadata::new_posix(None, Some(bytes)).unwrap()
}

#[tokio::test]
async fn first_directory_observation_applies_mode_acl_and_nanosecond_mtime_once() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let expected = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
    std::fs::set_permissions(source.path().join("dir"), Permissions::from_mode(0o751))?;
    executor
        .source
        .storage()
        .metadata()?
        .apply(
            &StoragePath::new("dir")?,
            MetadataMutation::Acl(acl(5)),
            CancellationToken::new(),
        )
        .await?;
    File::open(source.path().join("dir"))?.set_times(FileTimes::new().set_modified(expected))?;
    let plan = options()
        .observation_plan
        .unwrap()
        .with_acl(ObservationMode::BestEffort);
    let entry = Arc::new(observe_path_with_plan(executor.source.storage(), Path::new("dir"), plan).await?);
    assert_eq!(entry.metadata().acl().value(), Some(&acl(5)));
    let mut events = executor.broadcaster.subscribe();
    let prepared = executor
        .execute(LocalCopyWork {
            entry: entry.clone(),
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    let identity = match &prepared.result {
        Ok(LocalCopyOutcome::Prepared(identity)) => *identity,
        other => panic!("prepare failed: {}", other.is_err()),
    };
    executor.publish(prepared).await?;
    assert!(events.try_recv().is_err(), "prepare must not publish New");
    std::fs::set_permissions(source.path().join("dir"), Permissions::from_mode(0o700))?;
    executor
        .source
        .storage()
        .metadata()?
        .apply(
            &StoragePath::new("dir")?,
            MetadataMutation::Acl(acl(0)),
            CancellationToken::new(),
        )
        .await?;
    File::open(source.path().join("dir"))?
        .set_times(FileTimes::new().set_modified(expected + Duration::from_secs(20)))?;
    std::fs::write(target.path().join("dir/child"), b"changes destination directory mtime")?;
    let finished = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    executor.publish(finished).await?;
    assert!(matches!(events.try_recv(), Ok(StorageEntryMessage::New(_))));
    assert!(events.try_recv().is_err());
    let metadata = std::fs::metadata(target.path().join("dir"))?;
    assert_eq!(metadata.modified()?, expected);
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o751);
    let observed = observe_path_with_plan(executor.destination.storage(), Path::new("dir"), plan).await?;
    assert_eq!(observed.metadata().acl().value(), Some(&acl(5)));
    Ok(())
}

#[tokio::test]
async fn target_directory_replacement_is_not_modified_or_deleted() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let entry = Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("dir"),
            options().observation_plan.unwrap(),
        )
        .await?,
    );
    let prepared = executor
        .execute(LocalCopyWork {
            entry: entry.clone(),
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    let LocalCopyOutcome::Prepared(identity) = prepared.result? else {
        unreachable!();
    };
    std::fs::rename(target.path().join("dir"), target.path().join("held"))?;
    std::fs::create_dir(target.path().join("dir"))?;
    std::fs::set_permissions(target.path().join("dir"), Permissions::from_mode(0o711))?;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(matches!(
        completion.result,
        Err(AppError::StorageError(StorageError::TargetPublicationChanged { .. }))
    ));
    assert_eq!(
        std::fs::metadata(target.path().join("dir"))?.permissions().mode() & 0o7777,
        0o711
    );
    assert!(target.path().join("held").is_dir());
    Ok(())
}

struct PrepareRequests {
    inner: Arc<dyn Namespace>,
    calls: Arc<Mutex<Vec<(Operation, StoragePath)>>>,
}

#[async_trait]
impl Namespace for PrepareRequests {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        assert!(fresh, "prepare identity must use fresh stat");
        self.calls.lock().unwrap().push((Operation::Observe, path.clone()));
        self.inner.stat(path, fresh).await
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.calls.lock().unwrap().push((Operation::Namespace, path.clone()));
        self.inner.create_directory(path).await?;
        let failure = match path.as_str() {
            "ready/uncertain" => Some(FailureClass::NotFound),
            "ready/denied" => Some(FailureClass::PermissionDenied),
            _ => None,
        };
        if let Some(class) = failure {
            // 模拟 mkdir 成功后设置权限失败。
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Namespace,
                    class,
                    Transience::Permanent,
                    "failed setting permissions on newly created directory",
                )
                .unwrap(),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn local_prepare_creates_only_the_leaf_when_the_parent_already_exists() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    for entry in [
        "ready/child",
        "missing/child",
        "ready/occupied",
        "ready/uncertain",
        "ready/denied",
    ] {
        std::fs::create_dir_all(source.path().join(entry))?;
    }
    std::fs::create_dir(target.path().join("ready"))?;
    std::fs::write(target.path().join("ready/occupied"), b"existing data")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let destination = Arc::make_mut(&mut executor.destination);
    destination.roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .namespace(Arc::new(PrepareRequests {
        inner: destination.roles.namespace()?,
        calls: calls.clone(),
    }))
    .read_source(destination.roles.read_source()?)
    .staged_destination(destination.roles.staged_destination()?)
    .metadata(destination.roles.metadata()?)
    .build()?;

    let mkdir = Operation::Namespace;
    let stat = Operation::Observe;
    for (entry, expected, ready) in [
        ("ready/child", vec![(mkdir, "ready/child"), (stat, "ready/child")], true),
        ("ready/child", vec![(mkdir, "ready/child"), (stat, "ready/child")], true),
        (
            "missing/child",
            vec![
                (mkdir, "missing/child"),
                (stat, "missing"),
                (mkdir, "missing"),
                (mkdir, "missing/child"),
                (stat, "missing/child"),
            ],
            true,
        ),
        (
            "ready/occupied",
            vec![(mkdir, "ready/occupied"), (stat, "ready/occupied")],
            false,
        ),
        (
            "ready/uncertain",
            vec![(mkdir, "ready/uncertain"), (stat, "ready")],
            false,
        ),
        ("ready/denied", vec![(mkdir, "ready/denied")], false),
    ] {
        let entry = Arc::new(
            observe_path_with_plan(
                executor.source.storage(),
                Path::new(entry),
                options().observation_plan.unwrap(),
            )
            .await?,
        );
        let completed = executor
            .execute(LocalCopyWork {
                entry,
                control: Some(([1; 32], LocalCopyPhase::Prepare)),
            })
            .await;
        let expected = expected
            .into_iter()
            .map(|(op, path)| Ok((op, StoragePath::new(path)?)))
            .collect::<TestResult<Vec<_>>>()?;
        assert_eq!(std::mem::take(&mut *calls.lock().unwrap()), expected);
        assert_eq!(completed.phase_proof.is_ready(), ready);
        assert_eq!(matches!(completed.result, Ok(LocalCopyOutcome::Prepared(_))), ready);
    }
    assert_eq!(std::fs::read(target.path().join("ready/occupied"))?, b"existing data");
    assert!(target.path().join("ready/uncertain").is_dir());
    assert!(target.path().join("ready/denied").is_dir());
    Ok(())
}

#[tokio::test]
async fn wide_deep_and_empty_trees_count_each_directory_once_with_window_one() -> TestResult {
    for window in [1, 8] {
        let source = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        std::fs::create_dir_all(source.path().join("a/b/c/d/e/f/g/empty"))?;
        for index in 0..64 {
            std::fs::write(source.path().join(format!("f-{index}")), vec![index as u8; 100 + index])?;
        }
        let executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
        let (result, events, entries) =
            timeout(Duration::from_secs(10), copy(executor, options(), 4, window, 100)).await??;
        result?;
        assert_eq!(entries, 72);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, StorageEntryMessage::New(_)))
                .count(),
            72
        );
        assert!(
            events
                .iter()
                .all(|event| matches!(event, StorageEntryMessage::New(item) if !item.entry.path().as_str().is_empty()))
        );
        for index in 0..64 {
            assert_eq!(
                std::fs::read(target.path().join(format!("f-{index}")))?,
                vec![index as u8; 100 + index]
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_identity_budget_is_independent_of_window_and_fails_typed() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir_all(source.path().join("a/b/c"))?;
    let executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let (result, _, _) = timeout(Duration::from_secs(3), copy(executor, options(), 1, 1, 1)).await??;
    assert!(matches!(
        result,
        Err(AppError::CopyDirectoryBudget { count_limit: 1, .. })
    ));
    assert!(source.path().join("a/b/c").is_dir());
    Ok(())
}

#[derive(Clone, Copy)]
enum MetadataFault {
    Fields,
    EntryPermissionDenied,
    SessionConnectivity,
}

struct FaultMetadata {
    inner: Arc<dyn Metadata>,
    fail_path: String,
    blocked: Option<CancellationToken>,
    mode: MetadataFault,
}
#[async_trait]
impl Metadata for FaultMetadata {
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if path.as_str() == self.fail_path {
            if let Some(signal) = &self.blocked {
                signal.cancel();
            }
            if matches!(self.mode, MetadataFault::EntryPermissionDenied) {
                return Err(StorageRoleFailure::Entry(
                    EntryOperationFailure::new(
                        path.clone(),
                        Operation::Metadata,
                        FailureClass::PermissionDenied,
                        Transience::Permanent,
                        "whole metadata role permission denied",
                    )
                    .unwrap(),
                ));
            }
            if matches!(self.mode, MetadataFault::SessionConnectivity) {
                return Err(StorageRoleFailure::Session(
                    BackendSessionFailure::new(
                        Operation::Metadata,
                        FailureClass::Connectivity,
                        Transience::Transient,
                        "owned source session failure",
                    )
                    .unwrap(),
                ));
            }
            return Ok(MetadataObservations::new(
                MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                },
                if plan.xattrs() == ObservationMode::Omit {
                    MetadataObservation::NotRequested
                } else {
                    MetadataObservation::NotApplicable
                },
                if plan.tags() == ObservationMode::Omit {
                    MetadataObservation::NotRequested
                } else {
                    MetadataObservation::NotApplicable
                },
                MetadataObservation::NotRequested,
                MetadataObservation::NotRequested,
            )
            .unwrap());
        }
        if path.as_str() == "slow" {
            pending().await
        } else {
            self.inner.observe(path, plan).await
        }
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("read-only source metadata must never be changed")
    }
}
fn read_only_capabilities() -> BackendCapabilities {
    BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Unsupported(UnsupportedReason::new("read-only source").unwrap()),
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    )
}

fn fault_metadata(
    context: &mut StorageContext, fail_path: &str, blocked: Option<CancellationToken>, fatal: bool,
) -> TestResult {
    install_metadata_fault(
        context,
        fail_path,
        blocked,
        if fatal {
            MetadataFault::SessionConnectivity
        } else {
            MetadataFault::Fields
        },
    )
}

fn install_metadata_fault(
    context: &mut StorageContext, fail_path: &str, blocked: Option<CancellationToken>, mode: MetadataFault,
) -> TestResult {
    context.roles = StorageBuilder::new(context.roles.identity().clone(), read_only_capabilities())
        .read_source(context.roles.read_source()?)
        .namespace(context.roles.namespace()?)
        .metadata(Arc::new(FaultMetadata {
            inner: context.roles.metadata()?,
            fail_path: fail_path.into(),
            blocked,
            mode,
        }))
        .build()?;
    Ok(())
}

#[tokio::test]
async fn metadata_failure_still_copies_readable_file_and_has_one_callback_cause() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"readable")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "file", None, false)?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 2, 2, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert_eq!(std::fs::read(target.path().join("file"))?, b"readable");
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], StorageEntryMessage::CopyVersionFailed { first_in_entry:true, reason, .. } if reason.contains("源端元数据获取失败"))
    );
    Ok(())
}

#[tokio::test]
async fn directory_metadata_failure_does_not_block_child_data_or_duplicate_cause() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir/file"), b"readable child")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "dir", None, false)?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 2, 2, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert_eq!(std::fs::read(target.path().join("dir/file"))?, b"readable child");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, StorageEntryMessage::CopyVersionFailed { .. }))
            .count(),
        1
    );
    assert_eq!(events.len(), 2);
    Ok(())
}

#[tokio::test]
async fn cancellation_drains_known_metadata_diagnostic_without_future_entry() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    for name in ["bad", "slow"] {
        std::fs::write(source.path().join(name), b"retained")?;
    }
    let cancel = CancellationToken::new();
    let known = CancellationToken::new();
    let mut executor = executor(source.path(), target.path(), cancel.clone()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "bad", Some(known.clone()), false)?;
    let mut options = options();
    // slow 可能先被列出，必须给触发取消信号的 bad 留一个观察槽。
    options.concurrency = 2;
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let cancelling = async {
        known.cancelled().await;
        tokio::task::yield_now().await;
        cancel.cancel();
    };
    let (copied, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(copy(executor, options, 1, 2, 10), cancelling)
    })
    .await?;
    let (result, events, _) = copied?;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], StorageEntryMessage::Error {path, reason, ..} if path == Path::new("bad") && reason.contains("源端元数据获取失败"))
    );
    assert!(!target.path().join("bad").exists());
    Ok(())
}

struct PanicDirectory(Arc<dyn Namespace>);
#[async_trait]
impl Namespace for PanicDirectory {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.0.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.0.list(request)
    }
    async fn create_directory(&self, _: &StoragePath) -> Result<(), StorageRoleFailure> {
        panic!("owned receiver create-directory panic")
    }
}
fn panic_namespace(context: &mut StorageContext, source_only: bool) -> TestResult {
    let capabilities = if source_only {
        read_only_capabilities()
    } else {
        context.roles.capabilities().clone()
    };
    let mut builder = StorageBuilder::new(context.roles.identity().clone(), capabilities)
        .read_source(context.roles.read_source()?)
        .metadata(context.roles.metadata()?)
        .namespace(Arc::new(PanicDirectory(context.roles.namespace()?)));
    if !source_only {
        builder = builder.staged_destination(context.roles.staged_destination()?);
    }
    context.roles = builder.build()?;
    Ok(())
}

#[tokio::test]
async fn receiver_panic_reports_exact_directory_phase_and_settles_without_hang() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    panic_namespace(Arc::make_mut(&mut executor.destination), false)?;
    let active = executor.active.clone();
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options(), 2, 2, 10)).await??;
    assert!(
        matches!(result, Err(AppError::ReportedCopyFailure {source, ..}) if matches!(&*source, AppError::CopyWorkerFailed { phase:"directory_prepare", path, .. } if path == Path::new("dir")))
    );
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], StorageEntryMessage::CopyVersionFailed { reason, .. } if reason.contains("directory_prepare") && reason.contains("目标写结果未知"))
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(source.path().join("dir").is_dir());
    Ok(())
}

#[tokio::test]
async fn read_only_source_needs_no_mutation_role_or_directory_write() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir/file"), b"read only source")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    panic_namespace(Arc::make_mut(&mut executor.source), true)?;
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options(), 1, 1, 10)).await??;
    result?;
    assert_eq!(events.len(), 2);
    assert_eq!(std::fs::read(target.path().join("dir/file"))?, b"read only source");
    Ok(())
}

#[tokio::test]
async fn callback_consumer_close_stops_admission_and_preserves_published_data() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"published")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    drop(executor.broadcaster.subscribe());
    let active = executor.active.clone();
    let (result, _, _) = timeout(Duration::from_secs(3), copy(executor, options(), 1, 1, 10)).await??;
    assert!(matches!(result, Err(AppError::ConsumerDisconnected { .. })));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(target.path().join("file"))?, b"published");
    Ok(())
}

#[tokio::test]
async fn source_session_cause_survives_cancellation_and_never_becomes_consumer_failure() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    for name in ["bad", "slow"] {
        std::fs::write(source.path().join(name), b"retained")?;
    }
    let cancel = CancellationToken::new();
    let known = CancellationToken::new();
    let mut executor = executor(source.path(), target.path(), cancel.clone()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "bad", Some(known.clone()), true)?;
    let mut options = options();
    // slow 可能先被列出，必须给触发取消信号的 bad 留一个观察槽。
    options.concurrency = 2;
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let cancelling = async {
        known.cancelled().await;
        tokio::task::yield_now().await;
        cancel.cancel();
    };
    let (copied, ()) = timeout(Duration::from_secs(3), async {
        tokio::join!(copy(executor, options, 1, 2, 10), cancelling)
    })
    .await?;
    let (result, _, _) = copied?;
    assert!(
        matches!(result, Err(AppError::StorageError(StorageError::RemoteScan(failure)))
        if matches!(&failure.source, Some(StorageRoleFailure::Session(source))
            if source.class() == FailureClass::Connectivity && source.diagnostic() == "owned source session failure"))
    );
    assert!(!target.path().join("bad").exists());
    Ok(())
}

#[tokio::test]
async fn directory_prepare_and_source_metadata_failure_share_one_entry_terminal() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "dir", None, false)?;
    panic_namespace(Arc::make_mut(&mut executor.destination), false)?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(
        matches!(result, Err(AppError::ReportedCopyFailure {source, ..}) if matches!(*source, AppError::CopyAndSourceMetadataFailed {..}))
    );
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], StorageEntryMessage::CopyVersionFailed {first_in_entry:true, reason, ..}
        if reason.contains("源端元数据") && reason.contains("directory_prepare"))
    );
    Ok(())
}

struct PausedListing {
    inner: Arc<dyn Namespace>,
    closed: CancellationToken,
    release: CancellationToken,
}
#[async_trait]
impl Namespace for PausedListing {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        let mut pages = self.inner.list(request.clone());
        let closed = self.closed.clone();
        let release = self.release.clone();
        ListingStream::from_producer(request.path, move |sink| async move {
            while let Some(page) = pages.next().await {
                match page {
                    Ok(page) => {
                        if sink.send(page).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        sink.fail(error).await;
                        return;
                    }
                }
            }
            tokio::select! { () = sink.closed() => {}, () = release.cancelled() => {} }
            closed.cancel();
        })
    }
}

#[tokio::test]
async fn cancellation_stops_native_ack_wait_and_drains_completed_copy_under_callback_backpressure() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    for name in ["a", "b"] {
        std::fs::write(source.path().join(name), b"copied")?;
    }
    let cancel = CancellationToken::new();
    let closed = CancellationToken::new();
    let release = CancellationToken::new();
    let mut executor = executor(source.path(), target.path(), cancel.clone()).await?;
    let storage = Arc::make_mut(&mut executor.source);
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), read_only_capabilities())
        .read_source(storage.roles.read_source()?)
        .metadata(storage.roles.metadata()?)
        .namespace(Arc::new(PausedListing {
            inner: storage.roles.namespace()?,
            closed: closed.clone(),
            release: release.clone(),
        }))
        .build()?;
    let mut options = options();
    options.cancel = Some(cancel.clone());
    let scan = executor.source.consumer_traversal(
        &options,
        NonZeroUsize::new(1024 * 1024).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )?;
    let mut events = executor.broadcaster.subscribe();
    let active = executor.active.clone();
    let copying = run(
        scan,
        vec![executor.clone(); 2],
        executor,
        ControlledCopyCounters {
            entries: Arc::new(AtomicUsize::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
        },
        DirectoryBudget {
            count: 10,
            bytes: 1024 * 1024,
        },
        cancel.clone(),
    );
    let delaying = async {
        timeout(Duration::from_secs(3), async {
            while !(target.path().join("a").exists()
                && target.path().join("b").exists()
                && active.load(Ordering::SeqCst) == 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        cancel.cancel();
        let mut received = Vec::new();
        while let Some(event) = events.recv().await {
            received.push(event);
        }
        TestResult::Ok(received)
    };
    let (result, events) = timeout(Duration::from_secs(3), async { tokio::join!(copying, delaying) }).await?;
    // 原生取消不等待未知的底层读取；测试自行释放尚在途的fixture请求再确认收尾。
    release.cancel();
    timeout(Duration::from_millis(500), closed.cancelled()).await?;
    assert!(result.is_err());
    let events = events?;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, StorageEntryMessage::New(_)))
            .count(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn whole_role_entry_permission_denial_still_copies_readable_file_once() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"readable after whole role denial")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    install_metadata_fault(
        Arc::make_mut(&mut executor.source),
        "file",
        None,
        MetadataFault::EntryPermissionDenied,
    )?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert!(
        target.path().join("file").exists(),
        "whole-role metadata denial must retain first descriptor and attempt readable data: {events:?}"
    );
    assert_eq!(
        std::fs::read(target.path().join("file"))?,
        b"readable after whole role denial"
    );
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], StorageEntryMessage::CopyVersionFailed {first_in_entry:true, reason, ..} if reason.contains("源端元数据获取失败"))
    );
    Ok(())
}

#[tokio::test]
async fn whole_role_entry_permission_denial_still_copies_directory_children_once() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(
        source.path().join("dir/file"),
        b"readable descendant after whole role denial",
    )?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    install_metadata_fault(
        Arc::make_mut(&mut executor.source),
        "dir",
        None,
        MetadataFault::EntryPermissionDenied,
    )?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert!(
        target.path().join("dir/file").exists(),
        "whole-role directory metadata denial must admit readable descendants: {events:?}"
    );
    assert_eq!(
        std::fs::read(target.path().join("dir/file"))?,
        b"readable descendant after whole role denial"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                StorageEntryMessage::CopyVersionFailed {
                    first_in_entry: true,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(events.len(), 2);
    Ok(())
}

/// 每页仅一个真实descriptor，顺序由fixture明确指定，不依赖目录迭代或worker完成顺序。
struct OrderedNamespace {
    inner: Arc<dyn Namespace>,
    scope: StoragePath,
    pages: Vec<ListingPage>,
}
#[async_trait]
impl Namespace for OrderedNamespace {
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        if request.path == self.scope {
            ListingStream::from_pages(request.path, self.pages.iter().cloned().map(Ok))
        } else {
            self.inner.list(request)
        }
    }
}

async fn metadata_failure_continuation(directory: bool) -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let scope = if directory { "dir" } else { "" };
    if directory {
        std::fs::create_dir(source.path().join(scope))?;
    }
    let bad = if directory { "dir/bad" } else { "bad" };
    let good = if directory { "dir/good" } else { "good" };
    std::fs::write(source.path().join(bad), b"readable but source metadata denied")?;
    std::fs::write(source.path().join(good), b"ordinary readable following sibling")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    let bytes = Arc::new(CopyBytes::default());
    executor.bytes = Some(bytes.clone());
    install_metadata_fault(
        Arc::make_mut(&mut executor.source),
        bad,
        None,
        MetadataFault::EntryPermissionDenied,
    )?;
    let storage = Arc::make_mut(&mut executor.source);
    let namespace = storage.roles.namespace()?;
    let scope = StoragePath::new(scope)?;
    let mut pages = Vec::new();
    for path in [bad, good] {
        let descriptor = namespace.stat(&StoragePath::new(path)?, false).await?;
        pages.push(ListingPage::from_descriptors(&scope, vec![descriptor], Vec::new(), 0));
    }
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), read_only_capabilities())
        .read_source(storage.roles.read_source()?)
        .metadata(storage.roles.metadata()?)
        .namespace(Arc::new(OrderedNamespace {
            inner: namespace,
            scope,
            pages,
        }))
        .build()?;
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    assert_eq!(options.concurrency, 1);
    let (result, events, entries) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert_eq!(entries, if directory { 3 } else { 2 });
    assert_eq!(
        bytes.published(),
        u64::try_from(b"readable but source metadata denied".len() + b"ordinary readable following sibling".len())
            .unwrap()
    );
    assert_eq!(
        std::fs::read(target.path().join(bad))?,
        b"readable but source metadata denied"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                StorageEntryMessage::CopyVersionFailed {
                    first_in_entry: true,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(
        target.path().join(good).exists(),
        "metadata-only failure must not suppress the following selected readable sibling: bad={bad}, good={good}, events={events:?}"
    );
    assert_eq!(
        std::fs::read(target.path().join(good))?,
        b"ordinary readable following sibling"
    );
    assert_eq!(
        std::fs::read(source.path().join(bad))?,
        b"readable but source metadata denied"
    );
    assert_eq!(
        std::fs::read(source.path().join(good))?,
        b"ordinary readable following sibling"
    );
    Ok(())
}

#[tokio::test]
async fn metadata_only_failure_keeps_later_sibling_data_attempts() -> TestResult {
    metadata_failure_continuation(false).await
}

#[tokio::test]
async fn metadata_only_failure_keeps_later_descendant_data_attempts() -> TestResult {
    metadata_failure_continuation(true).await
}

#[tokio::test]
async fn symlink_known_source_failure_is_reported_once_without_hiding_later_copy() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::os::unix::fs::symlink("relative-source-target", source.path().join("link"))?;
    std::fs::write(source.path().join("good"), b"following link")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    // ACL/xattr在symlink上不适用；测试计划实际选中的inline时间事实失败。
    let storage = Arc::make_mut(&mut executor.source);
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), read_only_capabilities())
        .read_source(storage.roles.read_source()?)
        .namespace(storage.roles.namespace()?)
        .metadata(Arc::new(SymlinkInlineFailure(storage.roles.metadata()?)))
        .build()?;
    let plan = MigrationPlan::compile(&executor.source, &executor.destination, true).await?;
    assert_eq!(plan.observation.timestamps(), ObservationMode::InlineOnly);
    let storage = Arc::make_mut(&mut executor.source);
    let namespace = storage.roles.namespace()?;
    let root = StoragePath::root();
    let mut pages = Vec::new();
    for path in ["link", "good"] {
        let mut descriptor = namespace.stat(&StoragePath::new(path)?, false).await?;
        if path == "link" {
            descriptor.inline_timestamps = None;
        }
        pages.push(ListingPage::from_descriptors(&root, vec![descriptor], Vec::new(), 0));
    }
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), read_only_capabilities())
        .read_source(storage.roles.read_source()?)
        .metadata(storage.roles.metadata()?)
        .namespace(Arc::new(OrderedNamespace {
            inner: namespace,
            scope: root,
            pages,
        }))
        .build()?;
    let mut options = options();
    options.observation_plan = Some(plan.observation);
    options.metadata_scope = plan.scope;
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(
        result.as_ref().is_err_and(AppError::is_reported_copy_failure),
        "outcome={result:?}, events={events:?}"
    );
    assert_eq!(
        std::fs::read_link(target.path().join("link"))?,
        Path::new("relative-source-target")
    );
    assert_eq!(std::fs::read(target.path().join("good"))?, b"following link");
    assert_eq!(events.iter().filter(|event| matches!(event, StorageEntryMessage::CopyVersionFailed {first_in_entry:true, reason, ..} if reason.contains("源端元数据获取失败"))).count(),1);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StorageEntryMessage::New(item) if item.entry.path().as_str()=="link"))
    );
    assert_eq!(events.len(), 2);
    Ok(())
}

struct SymlinkInlineFailure(Arc<dyn Metadata>);
#[async_trait]
impl Metadata for SymlinkInlineFailure {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.0.observation_capabilities()
    }
    fn observe_listed(
        &self, descriptor: &SourceDescriptor, plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        if descriptor.path.as_str() != "link" {
            return self.0.observe_listed(descriptor, plan);
        }
        assert_eq!(plan.timestamps(), ObservationMode::InlineOnly);
        assert!(descriptor.inline_timestamps.is_none());
        let mut facts: MetadataObservationParts = descriptor
            .inline_observations(plan, self.observation_capabilities())
            .into();
        facts.timestamps = MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        };
        Some(Ok(facts.build().unwrap()))
    }
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        assert_ne!(
            path.as_str(),
            "link",
            "inline timestamp failure must not cause extra metadata IO"
        );
        self.0.observe(path, plan).await
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("source must remain read-only")
    }
}

struct MetadataProbe {
    inner: Arc<dyn Metadata>,
    refuse: bool,
    applied: Arc<AtomicUsize>,
}
#[async_trait]
impl Metadata for MetadataProbe {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        self.inner.copied_metadata_observation_plan()
    }
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if self.refuse {
            return Err(owned_verification_refusal(path));
        }
        self.inner.observe(path, plan).await
    }
    async fn observe_bound(
        &self, path: &StoragePath, expected: &SourceIdentity, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if self.refuse {
            return Err(owned_verification_refusal(path));
        }
        self.inner.observe_bound(path, expected, plan).await
    }
    async fn observe_copy_bound(
        &self, path: &StoragePath, expected: &SourceIdentity, plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        if self.refuse {
            return Err(owned_verification_refusal(path));
        }
        self.inner.observe_copy_bound(path, expected, plan).await
    }
    async fn apply(
        &self, path: &StoragePath, value: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(path, value, cancel).await
    }

    fn supports_bound_directory_metadata(&self) -> bool {
        self.inner.supports_bound_directory_metadata()
    }

    async fn apply_bound_directory(
        &self, path: &StoragePath, expected: EntryIdentityKey, mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        let count = mutations.len();
        let result = self
            .inner
            .apply_bound_directory(path, expected, mutations, cancel)
            .await;
        self.applied.fetch_add(
            result.as_ref().map_or_else(|error| error.completed, |()| count),
            Ordering::SeqCst,
        );
        result
    }
}
fn owned_verification_refusal(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Verify,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "owned target verification refusal",
        )
        .unwrap(),
    )
}
fn target_metadata_probe(context: &mut StorageContext, refuse: bool) -> TestResult<Arc<AtomicUsize>> {
    let applied = Arc::new(AtomicUsize::new(0));
    context.roles = StorageBuilder::new(context.roles.identity().clone(), context.roles.capabilities().clone())
        .read_source(context.roles.read_source()?)
        .staged_destination(context.roles.staged_destination()?)
        .namespace(context.roles.namespace()?)
        .metadata(Arc::new(MetadataProbe {
            inner: context.roles.metadata()?,
            refuse,
            applied: applied.clone(),
        }))
        .build()?;
    Ok(applied)
}

#[tokio::test]
async fn source_directory_metadata_failure_keeps_children_and_later_sibling_without_apply() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("bad"))?;
    std::fs::write(source.path().join("bad/child"), b"child")?;
    std::fs::write(source.path().join("good"), b"sibling")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    install_metadata_fault(
        Arc::make_mut(&mut executor.source),
        "bad",
        None,
        MetadataFault::EntryPermissionDenied,
    )?;
    let storage = Arc::make_mut(&mut executor.source);
    let namespace = storage.roles.namespace()?;
    let root = StoragePath::root();
    let mut pages = Vec::new();
    for path in ["bad", "good"] {
        pages.push(ListingPage::from_descriptors(
            &root,
            vec![namespace.stat(&StoragePath::new(path)?, false).await?],
            Vec::new(),
            0,
        ));
    }
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), read_only_capabilities())
        .read_source(storage.roles.read_source()?)
        .metadata(storage.roles.metadata()?)
        .namespace(Arc::new(OrderedNamespace {
            inner: namespace,
            scope: root,
            pages,
        }))
        .build()?;
    let applied = target_metadata_probe(Arc::make_mut(&mut executor.destination), false)?;
    let bytes = Arc::new(CopyBytes::default());
    executor.bytes = Some(bytes.clone());
    let mut options = options();
    options.observation_plan = Some(options.observation_plan.unwrap().with_acl(ObservationMode::BestEffort));
    let (result, events, _) = timeout(Duration::from_secs(3), copy(executor, options, 1, 1, 10)).await??;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert_eq!(std::fs::read(target.path().join("bad/child"))?, b"child");
    assert_eq!(std::fs::read(target.path().join("good"))?, b"sibling");
    assert_eq!(bytes.published(), 12);
    assert_eq!(applied.load(Ordering::SeqCst), 0);
    assert_eq!(events.len(), 3);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                StorageEntryMessage::CopyVersionFailed {
                    first_in_entry: true,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event,StorageEntryMessage::New(item) if item.entry.path().as_str()=="bad"))
    );
    Ok(())
}

#[tokio::test]
async fn published_data_with_target_verification_failure_has_no_phase_proof() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"published body")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    target_metadata_probe(Arc::make_mut(&mut executor.destination), true)?;
    let entry = Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("file"),
            options().observation_plan.unwrap(),
        )
        .await?,
    );
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([1; 32], LocalCopyPhase::Copy)),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(
        matches!(completion.result,Err(error) if matches!(error.storage_error(),Some(StorageError::PublishedDataMetadata{source,..}) if matches!(source.as_ref(),StorageError::MetadataVerification(_))))
    );
    assert_eq!(std::fs::read(target.path().join("file"))?, b"published body");
    Ok(())
}

#[tokio::test]
async fn cancelled_copy_with_known_source_failure_never_gets_phase_proof() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"retained")?;
    let cancel = CancellationToken::new();
    let mut executor = executor(source.path(), target.path(), cancel.clone()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "file", None, false)?;
    let entry = Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("file"),
            options()
                .observation_plan
                .unwrap()
                .with_acl(ObservationMode::BestEffort),
        )
        .await?,
    );
    cancel.cancel();
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([1; 32], LocalCopyPhase::Copy)),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(
        completion
            .result
            .as_ref()
            .is_err_and(|error| error.storage_error().is_some_and(StorageError::is_cancelled))
    );
    assert!(!target.path().join("file").exists());
    assert_eq!(std::fs::read(source.path().join("file"))?, b"retained");
    Ok(())
}

struct UnknownPublication(Arc<dyn StagedDestination>);
#[async_trait]
impl StagedDestination for UnknownPublication {
    fn supports_direct(&self) -> bool {
        self.0.supports_direct()
    }
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        self.0.copied_metadata_target()
    }
    fn may_set_owner(&self, uid: u32, gid: u32) -> bool {
        self.0.may_set_owner(uid, gid)
    }
    async fn prepare_direct(
        &self, request: PrepareRequest, cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.0.prepare_direct(request, cancel).await
    }
    async fn prepare_at_destination(
        &self, request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.0.prepare_at_destination(request).await
    }
    async fn observe_checkpoint(&self, stage: &PreparedStage) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.0.observe_checkpoint(stage).await
    }
    async fn write(&self, stage: &PreparedStage, input: ByteStream) -> Result<WriteEvidence, StorageRoleFailure> {
        self.0.write(stage, input).await
    }
    async fn verify(
        &self, stage: &PreparedStage, request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        self.0.verify(stage, request).await
    }
    async fn apply_metadata_batch(
        &self, stage: &PreparedStage, values: Vec<MetadataMutation>, cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        self.0.apply_metadata_batch(stage, values, cancel).await
    }
    async fn publish(
        &self, stage: &PreparedStage, request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        self.0.publish(stage, request).await?;
        Err(PublicationFailure {
            error: StorageRoleFailure::Session(
                BackendSessionFailure::new(
                    Operation::Publish,
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "owned unknown publication reply",
                )
                .unwrap(),
            ),
            final_destination_changed: true,
        })
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.0.discard(stage).await
    }
    async fn remove_failed_direct(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.0.remove_failed_direct(stage).await
    }
}

#[tokio::test]
async fn unknown_publication_and_source_metadata_failure_do_not_prove_phase_ready() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"publication reply unknown")?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "file", None, false)?;
    let destination = Arc::make_mut(&mut executor.destination);
    destination.roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .read_source(destination.roles.read_source()?)
    .namespace(destination.roles.namespace()?)
    .metadata(destination.roles.metadata()?)
    .staged_destination(Arc::new(UnknownPublication(destination.roles.staged_destination()?)))
    .build()?;
    let entry = Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("file"),
            options()
                .observation_plan
                .unwrap()
                .with_acl(ObservationMode::BestEffort),
        )
        .await?,
    );
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([1; 32], LocalCopyPhase::Copy)),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(
        matches!(completion.result,Err(error) if matches!(error.storage_error(),Some(StorageError::TransferAndMetadata{data,metadata})
        if matches!(data.as_ref(),StorageError::Transfer(failure) if failure.final_destination_changed())
            && matches!(metadata.as_ref(),StorageError::SourceMetadataAcquisitionFailed{..})))
    );
    assert!(source.path().join("file").exists());
    Ok(())
}

struct ReplaceAfterFirstStat {
    inner: Arc<dyn Namespace>,
    root: std::path::PathBuf,
    calls: AtomicUsize,
}
#[async_trait]
impl Namespace for ReplaceAfterFirstStat {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        if path.as_str() == "dir" && self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            std::fs::rename(self.root.join("dir"), self.root.join("held")).unwrap();
            std::fs::create_dir(self.root.join("dir")).unwrap();
            std::fs::set_permissions(self.root.join("dir"), Permissions::from_mode(0o711)).unwrap();
        }
        self.inner.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
}

#[tokio::test]
async fn source_failure_then_identity_change_preserves_both_errors_and_never_proves_ready() -> TestResult {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    let mut executor = executor(source.path(), target.path(), CancellationToken::new()).await?;
    fault_metadata(Arc::make_mut(&mut executor.source), "dir", None, false)?;
    let entry = Arc::new(
        observe_path_with_plan(
            executor.source.storage(),
            Path::new("dir"),
            options()
                .observation_plan
                .unwrap()
                .with_acl(ObservationMode::BestEffort),
        )
        .await?,
    );
    let prepared = executor
        .execute(LocalCopyWork {
            entry: entry.clone(),
            control: Some(([1; 32], LocalCopyPhase::Prepare)),
        })
        .await;
    let LocalCopyOutcome::Prepared(identity) = prepared.result? else {
        unreachable!()
    };
    let applied = target_metadata_probe(Arc::make_mut(&mut executor.destination), false)?;
    let destination = Arc::make_mut(&mut executor.destination);
    destination.roles = StorageBuilder::new(
        destination.roles.identity().clone(),
        destination.roles.capabilities().clone(),
    )
    .read_source(destination.roles.read_source()?)
    .staged_destination(destination.roles.staged_destination()?)
    .metadata(destination.roles.metadata()?)
    .namespace(Arc::new(ReplaceAfterFirstStat {
        inner: destination.roles.namespace()?,
        root: target.path().into(),
        calls: AtomicUsize::new(0),
    }))
    .build()?;
    let completion = executor
        .execute(LocalCopyWork {
            entry,
            control: Some(([2; 32], LocalCopyPhase::Finalize { identity })),
        })
        .await;
    assert!(!completion.phase_proof.is_ready());
    assert!(
        matches!(&completion.result,Err(AppError::CopyAndSourceMetadataFailed{source,metadata})
        if matches!(source.storage_error(),Some(StorageError::TargetPublicationChanged{..}))
            && matches!(metadata.as_ref(),StorageError::SourceMetadataAcquisitionFailed{..}))
    );
    let mut events = executor.broadcaster.subscribe();
    assert!(
        executor
            .publish(completion)
            .await
            .as_ref()
            .is_err_and(AppError::is_reported_copy_failure)
    );
    assert!(
        matches!(events.try_recv(),Ok(StorageEntryMessage::CopyVersionFailed{first_in_entry:true,reason,..}) if reason.contains("源端元数据")&&reason.contains("目标发布状态"))
    );
    assert!(events.try_recv().is_err());
    assert_eq!(applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::metadata(target.path().join("dir"))?.permissions().mode() & 0o7777,
        0o711
    );
    assert!(target.path().join("held").is_dir());
    Ok(())
}

#[path = "controlled_freshness_tests.rs"]
mod freshness_tests;
