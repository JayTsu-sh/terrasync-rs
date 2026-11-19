//! 生产controlled/Root/ConsumerManager组件验收；完整CLI与删源安全由独立矩阵验证。
#![allow(clippy::unwrap_used)]
use std::{
    env,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use data_mover::{
    model::{BackendKind, EntryKind, FailureClass, StoragePath},
    storage::{NamespaceReadPolicy, ReadRequest, SourceDescriptor, StorageRoleFailure},
};
use futures::StreamExt;
use serde_json::json;
use task_model::{StorageEntryMessage, TransferMode};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use utils::app_config::AppConfig;
use uuid::Uuid;

use crate::{
    broadcast::BroadcastForwarder,
    callback::CallbackEndpoint,
    config::{JobType, initialize_consumer_config},
    consumer::{
        ConsumerManager,
        stats::{StatsKind, callback_state::StatisticsCheckpoint},
    },
    copy_root::CopyRootMetadata,
    error::{AppError, StorageError},
    migration_plan::MigrationPlan,
    receiver::{LocalCopyExecutor, ReceiverConfig},
    scan::source::WalkOptions,
    sender::controlled::{self, ControlledCopyCounters, DirectoryBudget},
    storage_factory::{CreateStorageOptions, StorageContext, create_storage},
};

#[path = "orchestrator_controlled_real_tests/checkpoint_tests.rs"]
mod checkpoint_tests;
#[path = "orchestrator_controlled_real_tests/local_tests.rs"]
mod local_tests;
#[path = "orchestrator_controlled_real_tests/phase.rs"]
mod phase;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const PAYLOAD: &[u8] = b"main controlled payload before directory finalization";
const LINK_TARGET: &str = "dir/目标-你好";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Normal,
    TargetPre,
    TargetPost,
    BadUtf8,
}
impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::TargetPre => "target_pre",
            Self::TargetPost => "target_post",
            Self::BadUtf8 => "bad_utf8",
        }
    }
    fn parse(value: &str) -> TestResult<Self> {
        match value {
            "normal" => Ok(Self::Normal),
            "target_pre" => Ok(Self::TargetPre),
            "target_post" => Ok(Self::TargetPost),
            "bad_utf8" => Ok(Self::BadUtf8),
            _ => Err("unknown controlled test case".into()),
        }
    }
    fn trigger(self) -> Option<usize> {
        match self {
            Self::TargetPre => Some(2),
            Self::TargetPost => Some(3),
            _ => None,
        }
    }
    fn phase(self) -> &'static str {
        match self {
            Self::TargetPre => "directory_finalize_pre",
            Self::TargetPost => "directory_finalize_post",
            _ => "none",
        }
    }
}

struct Inputs {
    case: Case,
    run: String,
    source_url: String,
    target_url: String,
    callback: CallbackEndpoint,
    control: Option<phase::Control>,
}

fn environment(name: &str) -> TestResult<String> {
    Ok(env::var(name).map_err(|_| format!("explicit owned {name} is required"))?)
}

impl Inputs {
    fn external() -> TestResult<Self> {
        let run = environment("LAB_MAIN_CONTROLLED_RUN")?;
        let suffix = run
            .strip_prefix("nightly-main-nfs-")
            .ok_or("owned nightly-main-nfs UUID run required")?;
        Uuid::parse_str(suffix)?;
        let case = Case::parse(&environment("LAB_MAIN_CONTROLLED_CASE")?)?;
        let source_url = environment("LAB_MAIN_CONTROLLED_SOURCE_URL")?;
        let target_url = environment("LAB_MAIN_CONTROLLED_TARGET_URL")?;
        for value in [&source_url, &target_url] {
            assert!(
                value.starts_with("nfs://") && value.contains(&format!("/ci/{run}/")) && !value.contains(".."),
                "real test must use an owned NFS run prefix"
            );
        }
        let callback = CallbackEndpoint::parse(&environment("LAB_MAIN_CONTROLLED_CALLBACK")?)?;
        assert!(callback.is_loopback(), "callback observer must stay on loopback");
        let control = if case.trigger().is_some() {
            let address = environment("LAB_MAIN_CONTROLLED_CONTROL")?.parse()?;
            let token = environment("LAB_MAIN_CONTROLLED_TOKEN")?;
            assert!(
                (16..=128).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "owned hexadecimal control token required"
            );
            Some(phase::Control {
                address,
                run: run.clone(),
                token,
                case,
            })
        } else {
            None
        };
        Ok(Self {
            case,
            run,
            source_url,
            target_url,
            callback,
            control,
        })
    }
}

fn app_config() -> TestResult<AppConfig> {
    Ok(serde_json::from_value(json!({
        "log": {"max_size": 1, "max_backups": 1, "level": "error"},
        "scan": {"concurrency": 2, "include_tags": false, "operation_timeout_secs": 2, "recovery_window_secs": 3},
        "sync": {"is_source_reserved": true, "concurrency": 2},
        "database": {"enabled": false, "batch_size": 64, "clickhouse": {
            "dsn": "http://127.0.0.1:1", "dial_timeout": 1, "read_timeout": 1,
            "database": "default", "username": "default", "password": null
        }}
    }))?)
}

struct Outcome {
    before: StatisticsCheckpoint,
    after: StatisticsCheckpoint,
    events: Vec<StorageEntryMessage>,
    result: crate::error::Result<()>,
    admitted: usize,
    root_reopened: bool,
    evidence: Arc<phase::PhaseEvidence>,
}

async fn exercise(inputs: &Inputs, source: StorageContext, target: StorageContext) -> TestResult<Outcome> {
    exercise_with_transfer(inputs, source, target, TransferMode::Direct, CancellationToken::new()).await
}

async fn exercise_with_transfer(
    inputs: &Inputs, source: StorageContext, mut target: StorageContext, transfer_mode: TransferMode,
    cancel: CancellationToken,
) -> TestResult<Outcome> {
    let app = app_config()?;
    let job = tempfile::tempdir()?;
    let consumer_config = initialize_consumer_config(
        &format!("{}-{}", inputs.run, inputs.case.name()),
        &job.path().to_string_lossy(),
        JobType::Copy,
        String::new(),
        &app,
        Some(inputs.callback.clone()),
    )?;
    let mut manager = ConsumerManager::statistics_only(&consumer_config)?;
    manager.prepare_copy_consumers(&consumer_config, None).await?;
    let evidence = phase::install(&mut target, inputs.control.clone())?;
    let source = Arc::new(source);
    let target = Arc::new(target);
    let plan = MigrationPlan::compile(&source, &target, false).await?;
    let root = CopyRootMetadata::new(
        source.clone(),
        target.clone(),
        plan.root_metadata,
        plan.observation,
        NamespaceReadPolicy {
            operation_timeout: Duration::from_secs(2),
            recovery_window: Duration::from_secs(3),
            retry_interval: Duration::from_millis(100),
        },
        cancel.clone(),
        false,
    );
    root.capture().await?;
    if inputs.case != Case::BadUtf8 {
        // 同一个目标上下文预热；真正替换在后续guard调用前由外部控制端执行。
        target
            .storage()
            .namespace()?
            .stat(&StoragePath::new("dir")?, false)
            .await?;
    }
    let scan = source.consumer_traversal(
        &WalkOptions {
            cancel: Some(cancel.clone()),
            observation_plan: Some(plan.observation),
            metadata_scope: plan.scope,
            concurrency: 2,
            operation_timeout: Some(Duration::from_secs(2)),
            ..WalkOptions::default()
        },
        NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
    )?;
    let mut broadcaster = BroadcastForwarder::new(8);
    manager.begin_lifecycle().await;
    let handles = manager.start_consumers(&mut broadcaster).await?;
    let mut events_receiver = broadcaster.subscribe();
    let events_task = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = events_receiver.recv().await {
            assert!(events.len() < 32);
            events.push(event);
        }
        events
    });
    let admitted = Arc::new(AtomicUsize::new(0));
    let executor = LocalCopyExecutor {
        id: 0,
        source,
        destination: target,
        config: Arc::new(ReceiverConfig {
            job_id: consumer_config.job_id.clone(),
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            job_dir: job.path().to_string_lossy().into_owned(),
            transfer_mode,
            cancel: Some(cancel.clone()),
        }),
        qos: None,
        bytes: manager.get_bytes_tracker().await,
        broadcaster: broadcaster.clone(),
        active: Arc::new(AtomicUsize::new(0)),
        cleanup: None,
    };
    let active = executor.active.clone();
    let copying = controlled::run(
        scan,
        vec![executor.clone(); 2],
        executor,
        ControlledCopyCounters {
            entries: admitted.clone(),
            bytes: Arc::new(AtomicU64::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
        },
        DirectoryBudget {
            count: 16,
            bytes: 1024 * 1024,
        },
        cancel,
    );
    let copied = timeout(Duration::from_secs(40), copying).await?;
    drop(broadcaster);
    for handle in handles {
        handle.await??;
    }
    let events = events_task.await?;
    let root_reopened = copied.is_ok();
    let result = match copied {
        Ok(()) => {
            // 与run_sync相同：内容结束后为Root收尾重新打开目标；不是假装使用原context。
            let reopened = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
            root.finish(&reopened).await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = &result {
        manager.record_task_error(error).await;
    }
    let before = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    manager.end_lifecycle().await;
    let after = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    Ok(Outcome {
        before,
        after,
        events,
        result,
        admitted: admitted.load(Ordering::SeqCst),
        root_reopened,
        evidence,
    })
}

fn full(snapshot: &StatisticsCheckpoint) -> &crate::consumer::stats::FullStats {
    let StatsKind::Full(stats) = &snapshot.stats else {
        panic!("expected full copy statistics")
    };
    stats
}

fn check_outcome(inputs: &Inputs, outcome: &mut Outcome) {
    let errors = usize::from(inputs.case != Case::Normal);
    assert_eq!(full(&outcome.before).error_stats.total(), errors);
    assert_eq!(
        full(&outcome.after).error_stats.total(),
        errors,
        "HTTP acceptance must not decrement accumulated errors"
    );
    assert_eq!(
        outcome.after.callback.task_error_count, 0,
        "already reported entry must not be counted as a task error"
    );
    assert!(
        outcome.after.callback.outbox.prepare_batch().is_none(),
        "HTTP2xx must release accepted details"
    );
    if inputs.case == Case::Normal {
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        assert_eq!(outcome.admitted, 3);
        assert_eq!(full(&outcome.after).dir_count, 1);
        assert_eq!(full(&outcome.after).regular_file_count, 1);
        assert_eq!(full(&outcome.after).symlink_count, 1);
        assert_eq!(
            outcome
                .events
                .iter()
                .filter(|event| matches!(event, StorageEntryMessage::New(_)))
                .count(),
            3
        );
    } else if inputs.case == Case::BadUtf8 {
        let error = outcome.result.as_ref().err().unwrap();
        assert!(
            matches!(error.storage_error(), Some(StorageError::TraversalEntry { source, .. }) if source.class() == FailureClass::Protocol)
        );
        assert!(
            error.to_string().contains("READLINK") && error.to_string().contains("UTF-8"),
            "{error}"
        );
        assert_eq!(
            outcome.admitted, 0,
            "failed listing observation was never admitted as copy work"
        );
        assert_eq!(outcome.events.len(), 1);
        assert!(
            matches!(&outcome.events[0], StorageEntryMessage::Error { path, reason, .. } if path.to_str() == Some("bad-link") && reason.contains("READLINK") && reason.contains("UTF-8"))
        );
    } else {
        assert!(
            outcome.evidence.confirmed.load(Ordering::SeqCst),
            "external phase handshake was not completed"
        );
        assert!(matches!(
            outcome.result.as_ref().err().and_then(AppError::storage_error),
            Some(StorageError::TargetPublicationChanged { .. })
        ));
        assert_eq!(outcome.admitted, 2);
        assert_eq!(
            outcome
                .events
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
            !outcome.events.iter().any(
                |event| matches!(event, StorageEntryMessage::New(item) if item.entry.kind() == EntryKind::Directory)
            )
        );
    }
}

fn directory_values(descriptor: &SourceDescriptor) -> TestResult<(u32, i128)> {
    let mode = descriptor.inline_mode.ok_or("real mode observation missing")? & 0o7777;
    let modified = descriptor
        .inline_timestamps
        .and_then(|value| value.modified)
        .ok_or("real mtime observation missing")?
        .unix_nanos();
    Ok((mode, modified))
}

async fn inspect_normal(inputs: &Inputs, expected: &SourceDescriptor) -> TestResult {
    let target = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
    let namespace = target.storage().namespace()?;
    let actual = namespace.stat(&StoragePath::new("dir")?, true).await?;
    assert_eq!(directory_values(&actual)?, directory_values(expected)?);
    assert_eq!(
        namespace.read_link(&StoragePath::new("link")?).await?.as_bytes(),
        LINK_TARGET.as_bytes()
    );
    let descriptor = namespace.stat(&StoragePath::new("dir/payload")?, true).await?;
    let mut chunks = target
        .storage()
        .read_source()?
        .read(ReadRequest {
            path: descriptor.path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 4096,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: descriptor.version,
        })
        .await?;
    let mut body = Vec::new();
    while let Some(chunk) = chunks.next().await {
        body.extend_from_slice(&chunk?);
        assert!(body.len() <= 65_536);
    }
    assert_eq!(body, PAYLOAD);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires independently reviewed owned NFS fixture/control relay and HTTP204 callback observer"]
async fn real_main_nfs_controlled_copy_and_callback() -> TestResult {
    let inputs = Inputs::external()?;
    let source = create_storage(&inputs.source_url, CreateStorageOptions::default()).await?;
    let target = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
    assert_eq!(source.storage().kind(), BackendKind::Nfs);
    assert_eq!(target.storage().kind(), BackendKind::Nfs);
    assert_ne!(
        source.storage().identity(),
        target.storage().identity(),
        "owned fixture roots must be distinct"
    );
    let expected = if inputs.case == Case::BadUtf8 {
        None
    } else {
        Some(
            source
                .storage()
                .namespace()?
                .stat(&StoragePath::new("dir")?, false)
                .await?,
        )
    };
    let mut outcome = exercise(&inputs, source, target).await?;
    check_outcome(&inputs, &mut outcome);
    if inputs.case == Case::Normal {
        inspect_normal(&inputs, expected.as_ref().unwrap()).await?;
    }
    if inputs.case.trigger().is_some() {
        let inspected = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
        let namespace = inspected.storage().namespace()?;
        let current = namespace.stat(&StoragePath::new("dir")?, true).await?;
        assert_eq!(
            directory_values(&current)?.0,
            0o711,
            "replacement was modified after the barrier"
        );
        let held = namespace.stat(&StoragePath::new("held-dir")?, true).await?;
        if inputs.case == Case::TargetPost {
            assert_eq!(
                directory_values(&held)?,
                directory_values(expected.as_ref().unwrap())?,
                "post barrier must follow successful real metadata apply/verification"
            );
        } else {
            assert_eq!(
                directory_values(&held)?.0,
                0o777,
                "precheck must not finalize the old directory"
            );
        }
    }
    if inputs.case == Case::BadUtf8 {
        let source = create_storage(&inputs.source_url, CreateStorageOptions::default()).await?;
        assert_eq!(
            source
                .storage()
                .namespace()?
                .stat(&StoragePath::new("bad-link")?, true)
                .await?
                .kind,
            EntryKind::Symlink
        );
        let target = create_storage(&inputs.target_url, CreateStorageOptions::default()).await?;
        assert!(
            matches!(target.storage().namespace()?.stat(&StoragePath::new("bad-link")?, true).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
        );
    }
    println!(
        "MAIN_CONTROLLED_RESULT {}",
        json!({
            "case": inputs.case.name(), "run": inputs.run, "component_not_cli": true,
            "retained_source_does_not_prove_delete_gate": true, "admitted_entries": outcome.admitted,
            "before_callback": outcome.before.report, "after_callback": outcome.after.report,
            "stat_trace": *outcome.evidence.calls.lock().unwrap(), "task_result": outcome.result.as_ref().err().map(ToString::to_string),
            "root_finish_reopened_target": outcome.root_reopened
        })
    );
    Ok(())
}
