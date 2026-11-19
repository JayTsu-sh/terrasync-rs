//! `NoDB` 的真实本地目录必须在所有子项完成后复制目录属性。
#![allow(clippy::unwrap_used)]

use std::{
    fs::{File, FileTimes, Permissions},
    num::NonZeroUsize,
    os::unix::fs::PermissionsExt,
    path::Path,
    result::Result as StdResult,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use data_mover::{
    model::{
        BackendSessionFailure, FailureClass, MetadataObservation, MetadataObservations, ObservationMode,
        ObservationPlan, Operation, StoragePath, Transience,
    },
    storage::{
        ListRequest, ListingStream, Metadata, MetadataMutation, Namespace, NamespaceReadPolicy, SourceDescriptor,
        StorageBuilder, StorageRoleFailure,
    },
};
use futures::StreamExt;
use task_model::{SourceCleanupPolicy, StorageEntryMessage, TransferMode};
use tokio::time::timeout;
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use utils::app_config::AppConfig;

use super::{SyncOrchestrator, collect_controlled_receivers, create_storage_pair_with_retry};
use crate::{
    broadcast::BroadcastForwarder,
    config::{JobType, SyncJobConfig, initialize_consumer_config},
    consumer::{
        ConsumerManager,
        stats::{ProgressDetail, callback_state::StatisticsCheckpoint},
    },
    copy_root::CopyRootMetadata,
    error::{AppError, Result, StorageError},
    migration_plan::MigrationPlan,
    receiver::{LocalCopyExecutor, ReceiverConfig},
    scan::source::WalkOptions,
    sender::controlled::{self, ControlledCopyCounters, DirectoryBudget},
    storage_factory::{StorageRole, create_storage_for_role},
};

#[tokio::test]
async fn nodb_recursive_copy_finishes_directory_metadata_after_children() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let job = tempfile::tempdir()?;
    std::fs::create_dir_all(source.path().join("parent/empty"))?;
    std::fs::write(source.path().join("parent/payload"), b"copied before directory mtime")?;
    let expected = UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
    for directory in ["parent", "parent/empty"] {
        let path = source.path().join(directory);
        std::fs::set_permissions(&path, Permissions::from_mode(0o751))?;
        File::open(path)?.set_times(FileTimes::new().set_modified(expected))?;
    }
    let config = SyncJobConfig {
        job_id: "nodb-directory-metadata".into(),
        job_dir: job.path().to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: source.path().to_string_lossy().into_owned(),
        dest_path: target.path().to_string_lossy().into_owned(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: String::new(),
        progress_callback_url: None,
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: Some(2),
    };
    let app: AppConfig = serde_json::from_value(serde_json::json!({
        "log": {"max_size": 1, "max_backups": 1, "level": "error"},
        "scan": {"concurrency": 2, "include_tags": false},
        "sync": {"is_source_reserved": true, "concurrency": 2},
        "database": {"enabled": false, "batch_size": 64, "clickhouse": {
            "dsn": "http://127.0.0.1:1", "dial_timeout": 1, "read_timeout": 1,
            "database": "default", "username": "default", "password": null
        }}
    }))?;
    let consumer = initialize_consumer_config(
        &config.job_id,
        &config.job_dir,
        JobType::Copy,
        String::new(),
        &app,
        None,
    )?;
    let mut manager = ConsumerManager::statistics_only(&consumer)?;
    let source_storage = Arc::new(create_storage_for_role(&config.src_path, None, false, StorageRole::Source).await?);
    let target_storage =
        Arc::new(create_storage_for_role(&config.dest_path, None, false, StorageRole::Destination).await?);
    let plan = MigrationPlan::compile(&source_storage, &target_storage, false).await?;
    let root = CopyRootMetadata::new(
        source_storage,
        target_storage,
        plan.root_metadata,
        plan.observation,
        NamespaceReadPolicy {
            operation_timeout: Duration::from_secs(1),
            recovery_window: Duration::ZERO,
            retry_interval: Duration::from_millis(10),
        },
        CancellationToken::new(),
        false,
    );
    root.capture().await?;
    timeout(
        Duration::from_secs(10),
        SyncOrchestrator::new_local(config).run_sync(Some(plan), &app, &mut manager, None, &root),
    )
    .await
    .unwrap()?;
    assert_eq!(
        std::fs::read(target.path().join("parent/payload"))?,
        b"copied before directory mtime"
    );
    for directory in ["parent", "parent/empty"] {
        let metadata = std::fs::metadata(target.path().join(directory))?;
        assert_eq!(
            metadata.modified()?,
            expected,
            "{directory} must preserve its first nanosecond mtime"
        );
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o751, "{directory}");
    }
    let statistics: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    assert!(matches!(
        statistics.report.detail,
        ProgressDetail::Full {
            scanned_files: 1,
            scanned_dirs: 2,
            error_count: 0,
            ..
        }
    ));
    manager.end_lifecycle().await;
    Ok(())
}

struct StartupSourceFault(Arc<dyn Namespace>);
#[async_trait]
impl Namespace for StartupSourceFault {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> StdResult<SourceDescriptor, StorageRoleFailure> {
        self.0.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        let mut pages = self.0.list(request.clone());
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
            sink.fail(StorageRoleFailure::Session(
                BackendSessionFailure::new(
                    Operation::Traverse,
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "owned source fault during receiver mount",
                )
                .unwrap(),
            ))
            .await;
        })
    }
}

#[tokio::test]
async fn source_failure_cancels_blocked_receiver_startup_before_copy_admission() -> Result<()> {
    startup_source_failure_during_mount(false, false).await
}

async fn startup_source_failure_during_mount(known_fact: bool, startup_failure: bool) -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    if known_fact {
        std::fs::write(source.path().join("known"), b"metadata fact before source fault")?;
        std::fs::write(source.path().join("bad"), b"source session fault")?;
    }
    let mut source =
        create_storage_for_role(&source.path().to_string_lossy(), None, false, StorageRole::Source).await?;
    let target = Arc::new(
        create_storage_for_role(&target.path().to_string_lossy(), None, false, StorageRole::Destination).await?,
    );
    source.roles = StorageBuilder::new(source.roles.identity().clone(), source.roles.capabilities().clone())
        .read_source(source.roles.read_source()?)
        .staged_destination(source.roles.staged_destination()?)
        .metadata(if known_fact {
            Arc::new(StartupMetadataFact)
        } else {
            source.roles.metadata()?
        })
        .namespace(if known_fact {
            source.roles.namespace()?
        } else {
            Arc::new(StartupSourceFault(source.roles.namespace()?))
        })
        .build()
        .unwrap();
    let source = Arc::new(source);
    let cancel = CancellationToken::new();
    let scan = source.consumer_traversal(
        &WalkOptions {
            cancel: Some(cancel.clone()),
            concurrency: 1,
            observation_plan: Some(ObservationPlan::default().with_acl(ObservationMode::BestEffort)),
            ..Default::default()
        },
        NonZeroUsize::new(1024 * 1024).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )?;
    let mut mounts = JoinSet::new();
    if startup_failure {
        mounts.spawn(async {
            Err(AppError::ConfigError(
                "owned independent receiver startup failure".into(),
            ))
        });
    }
    let blocked = cancel.clone();
    mounts.spawn(async move {
        // 实际调用既有mount helper，但零permit确保未发起任何Native连接IO。
        let semaphore = Semaphore::new(0);
        let pair = create_storage_pair_with_retry(
            &semaphore,
            "must-not-open-source",
            "must-not-open-target",
            None,
            "blocked mount",
            Some(&blocked),
        )
        .await?;
        let _ = pair;
        Result::<LocalCopyExecutor>::Err(AppError::CopyWorkProtocol {
            reason: "blocked mount unexpectedly admitted",
        })
    });
    timeout(Duration::from_secs(1), scan.1.failed()).await.unwrap();
    let collected = timeout(
        Duration::from_millis(500),
        collect_controlled_receivers(mounts, &scan.1, &cancel),
    )
    .await;
    if collected.is_err() {
        cancel.cancel();
    }
    let config = Arc::new(ReceiverConfig {
        job_id: "startup-source-fault".into(),
        enable_integrity_check: false,
        enable_acl: false,
        is_source_reserved: true,
        job_dir: String::new(),
        transfer_mode: TransferMode::Direct,
        cancel: Some(cancel.clone()),
    });
    let mut reporter = LocalCopyExecutor {
        id: 0,
        source,
        destination: target,
        config,
        qos: None,
        bytes: None,
        broadcaster: BroadcastForwarder::new(1),
        active: Arc::new(AtomicUsize::new(0)),
        cleanup: None,
    };
    let mut events = reporter.broadcaster.subscribe();
    let outcome = timeout(
        Duration::from_secs(1),
        controlled::run(
            scan,
            Vec::new(),
            reporter,
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
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome,Err(AppError::StorageError(StorageError::RemoteScan(failure)))
        if matches!(&failure.source,Some(StorageRoleFailure::Session(source)) if source.diagnostic()=="owned source fault during receiver mount"))
    );
    assert!(
        collected.is_ok(),
        "identified source fault must cancel blocked startup before waiting for receiver mounts"
    );
    let (executors, failure) = collected.unwrap();
    assert!(executors.is_empty());
    if startup_failure {
        assert!(
            matches!(failure, Some(AppError::ConfigError(reason)) if reason == "owned independent receiver startup failure")
        );
    } else {
        assert!(
            failure.is_none(),
            "source-driven Cancelled must not become a false startup failure"
        );
    }
    if known_fact {
        assert!(
            matches!(events.try_recv(), Ok(StorageEntryMessage::Error {path, reason, ..})
            if path == Path::new("known") && reason.contains("源端元数据获取失败"))
        );
        assert!(
            events.try_recv().is_err(),
            "already known source fact must be drained once"
        );
    }
    assert!(cancel.is_cancelled());
    Ok(())
}

struct StartupMetadataFact;
#[async_trait]
impl Metadata for StartupMetadataFact {
    fn observe_listed(
        &self, descriptor: &SourceDescriptor, _: ObservationPlan,
    ) -> Option<StdResult<MetadataObservations, StorageRoleFailure>> {
        if descriptor.path.as_str() != "known" {
            return None;
        }
        Some(Ok(MetadataObservations::new(
            MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            },
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
        )
        .unwrap()))
    }
    async fn observe(
        &self, _: &StoragePath, _: ObservationPlan,
    ) -> StdResult<MetadataObservations, StorageRoleFailure> {
        Err(StorageRoleFailure::Session(
            BackendSessionFailure::new(
                Operation::Traverse,
                FailureClass::Connectivity,
                Transience::Transient,
                "owned source fault during receiver mount",
            )
            .unwrap(),
        ))
    }
    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> StdResult<(), StorageRoleFailure> {
        panic!("source must remain read-only");
    }
}

#[tokio::test]
async fn source_failure_during_startup_drains_known_metadata_fact_once() -> Result<()> {
    startup_source_failure_during_mount(true, false).await
}

#[tokio::test]
async fn source_failure_during_startup_preserves_independent_startup_failure() -> Result<()> {
    startup_source_failure_during_mount(true, true).await
}
