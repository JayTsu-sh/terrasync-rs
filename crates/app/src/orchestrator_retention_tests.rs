//! 真实 `ClickHouse` 与 HTTP 回调验证保留目标的状态事件，不把它计为删除或复制失败。
use crate::migration_fixtures::request;
use crate::target_absence::SourceAbsenceCheck;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use task_model::migration::CopyGenerationPhase;

use clickhouse::Client;
use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory, ObservedEntry};
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{SourcePresence, StorageEntryMessage, TransferMode};
use tempfile::tempdir;
use tokio::sync::Mutex;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::SyncOrchestrator;
use crate::broadcast::BroadcastForwarder;
use crate::callback::CallbackEndpoint;
use crate::config::JobType;
use crate::consumer::stats::callback_state::CallbackState;
use crate::consumer::stats::{IncrementalStats, ProgressBar, ProgressDetail, StatisticConsumer, StatsKind};
use crate::error::{AppError, Result};
use crate::observed_fixtures::observation;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

fn version_facts() -> Arc<ObservedEntry> {
    let mut records = Vec::new();
    let mut latest = None;
    for index in 0..20 {
        let entry = observation(
            BackendKind::S3,
            "payload".into(),
            EntryKind::File,
            16,
            [0; 3],
            None,
            None,
            None,
            Some((Some(format!("v{index}")), index == 19, false)),
        );
        if let Some(history) = entry.history() {
            records.extend_from_slice(history.records());
        }
        latest = Some(entry);
    }
    let entry = latest.unwrap_or_else(|| panic!("history fixture must contain records"));
    let history = ObjectHistory::whole(records, HistoryOrder::Server).unwrap_or_else(|error| panic!("{error}"));
    Arc::new(
        entry
            .with_history(Some(Box::new(history)))
            .unwrap_or_else(|error| panic!("{error}")),
    )
}

async fn preserve_case(config: &ClickHouseConfig) -> Result<()> {
    let source_root = tempdir()?;
    let target_root = tempdir()?;
    for root in [source_root.path(), target_root.path()] {
        std::fs::write(root.join("payload"), b"retained payload")?;
        std::fs::create_dir(root.join("folder"))?;
    }
    std::fs::write(target_root.path().join("folder/child"), b"retained child")?;
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let database = ClickHouseDatabase::connect(config, "retained_target").await?;
    database.initialize().await?;
    let descriptor = |reserved| {
        let mut request = request("same-plan", reserved);
        request.binding.target_history = true;
        request
    };
    database.begin_copy_generation(&descriptor(true)).await?;
    let mut entries = Vec::new();
    // 历史来自确定性 S3 观察；真实链路验证范围为 CH、Local 目标与 HTTP callback。
    entries.push(version_facts());
    entries.push(Arc::new(observe_path(source.storage(), Path::new("folder")).await?));
    database.batch_insert_base_record(&entries).await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    std::fs::remove_file(source_root.path().join("payload"))?;
    std::fs::remove_dir(source_root.path().join("folder"))?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&descriptor(false)).await?;
    let mut events = BroadcastForwarder::new(8);
    let mut received = events.subscribe();
    SyncOrchestrator::apply_deletions_and_renames(
        &database,
        source,
        target,
        false,
        &events,
        false,
        None,
        None,
        "retained_target",
        "",
        TransferMode::Direct,
        SourceAbsenceCheck::new(Duration::from_secs(1), Duration::from_secs(3)),
    )
    .await?;
    let consumer = Arc::new(Mutex::new(StatisticConsumer {
        stats: StatsKind::Incremental(IncrementalStats::new(
            JobType::IncrementalCopy,
            "retained_target".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::IncrementalCopy),
        job_dir: String::new(),
        callback_url: Some(CallbackEndpoint::parse(
            &std::env::var("LAB_TARGET_RETENTION_CALLBACK_URL")
                .map_err(|_| AppError::ConfigError("owned callback URL required".into()))?,
        )?),
        generation_committed: None,
        migration: None,
        scan_snapshot: None,
        callback: CallbackState::with_database(Some(database.clone())),
        pb_handle: None,
    }));
    let mut count = 0;
    while let Ok(message) = received.try_recv() {
        if let StorageEntryMessage::TargetPreserved { retention, .. } = &message {
            assert_eq!(retention.source_presence, SourcePresence::Unknown);
        } else {
            panic!("only retention events expected: {message:?}");
        }
        StatisticConsumer::record_message(&consumer, &message).await?;
        count += 1;
    }
    assert_eq!(count, 2);
    StatisticConsumer::end_with_database(Arc::clone(&consumer), None, Some(&database), false).await;
    let mut state = consumer.lock().await;
    assert_eq!(state.to_report(true).target_preserved_count, 2);
    assert!(matches!(
        state.to_report(true).detail,
        ProgressDetail::Incremental {
            error_count: 0,
            deleted_count: 0,
            renamed_count: 0,
            ..
        }
    ));
    assert!(state.callback.outbox.prepare_batch().is_none());
    assert!(!state.callback.diagnostics_incomplete);
    let identity = state.callback.identity.clone();
    drop(state);
    assert!(
        database
            .final_report(&identity.round_id, &identity.attempt_id, identity.sequence)
            .await?
            .is_some()
    );
    assert_eq!(database.get_count("base").await?, 21);
    assert_eq!(std::fs::read(target_root.path().join("payload"))?, b"retained payload");
    assert_eq!(
        std::fs::read(target_root.path().join("folder/child"))?,
        b"retained child"
    );
    eprintln!(
        "target-retention cluster={} entry_count=2 history_records=20 errors=0 deleted=0 callback_accepted=true final_report_archived=true base_rows=21",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair and HTTP callback from run-notification-archive-local.py --retention-events"]
async fn real_retention_events_preserve_targets_and_archive_successful_callbacks() -> Result<()> {
    let dsn = std::env::var("LAB_COPY_GENERATION_DSN")
        .map_err(|_| AppError::ConfigError("isolated ClickHouse URL required".into()))?;
    let admin = Client::default().with_url(&dsn).with_user("default");
    for cluster in [false, true] {
        let config = ClickHouseConfig {
            mode: if cluster {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.then(|| "notification_cluster".into()),
            dsn: dsn.clone(),
            database: format!("ts_target_retention_{}", Uuid::new_v4().simple()),
            username: "default".into(),
            password: None,
            dial_timeout: 10,
            read_timeout: 60,
        };
        let on_cluster = if cluster {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        admin
            .query(&format!("CREATE DATABASE {}{on_cluster}", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from)?;
        let result = preserve_case(&config).await;
        let cleanup = admin
            .query(&format!("DROP DATABASE {}{on_cluster} SYNC", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from);
        result?;
        cleanup?;
    }
    Ok(())
}
