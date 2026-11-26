//! 真实 Single／Cluster：保留源的全量之后，增量搬迁跳过的数据仍受全量校验约束。
use std::{
    env,
    fs::{File, FileTimes, Permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use clickhouse::Client;
use db::catalogue::CopyChangeArchiveCoverage;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{
    RetentionScope, SourceCleanupPolicy, SourcePresence, TargetRetentionReason,
    migration::{CopyGenerationPhase, MigrationMode},
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::tests::job;
use crate::callback::CallbackEndpoint;
use crate::consumer::stats::{ProgressDetail, ProgressReport, delivery::FinalCallbackReport};
use crate::error::{AppError, Result, StorageError};
use crate::orchestrator::SyncOrchestrator;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn case(config: &ClickHouseConfig, deployment: &str, case: &str) -> Result<()> {
    let root = PathBuf::from(environment("LAB_COPY_CONTENT_ROOT")?).join(format!("{deployment}-{case}"));
    let source = root.join("source");
    let target = root.join("target");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir(&target)?;
    let source_file = source.join("payload");
    let target_file = target.join("payload");
    std::fs::write(&source_file, b"source!")?;
    std::fs::write(source.join("old-only"), b"retained at the target")?;
    std::fs::set_permissions(&source_file, Permissions::from_mode(0o640))?;
    let mut command = job(&root.join("job"));
    command.job_id = format!("content_{case}");
    command.src_path = source.to_string_lossy().into_owned();
    command.dest_path = target.to_string_lossy().into_owned();
    command.enable_integrity_check = true;
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_COPY_CONTENT_CALLBACK")?)?);
    SyncOrchestrator::new_local(command.clone()).run().await?;
    assert_eq!(std::fs::read(&target_file)?, b"source!");
    std::fs::remove_file(source.join("old-only"))?;
    let damaged = case != "unchanged";
    if damaged {
        let before = std::fs::metadata(&target_file)?;
        std::fs::write(&target_file, b"damaged")?;
        File::open(&target_file)?.set_times(FileTimes::new().set_modified(before.modified()?))?;
    }
    if case == "metadata" {
        std::fs::set_permissions(&source_file, Permissions::from_mode(0o600))?;
    }
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    let result = SyncOrchestrator::new_local(command.clone()).run().await;
    if damaged {
        assert!(
            matches!(
                result.as_ref().err().and_then(AppError::storage_error),
                Some(StorageError::MismatchData(_))
            ),
            "{result:?}"
        );
        assert_eq!(std::fs::read(&source_file)?, b"source!");
        assert_eq!(std::fs::read(&target_file)?, b"damaged");
    } else {
        result?;
        assert!(!source_file.exists());
        assert_eq!(std::fs::read(&target_file)?, b"source!");
    }
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("generation missing".into()))?;
    assert_eq!(generation.sequence, 2);
    assert_eq!(generation.plan.mode, MigrationMode::Incremental);
    assert_eq!(
        generation.phase,
        if damaged {
            CopyGenerationPhase::Failed
        } else {
            CopyGenerationPhase::Completed
        }
    );
    assert_eq!(database.get_count("base").await?, 0);
    let page = database
        .incremental_copy_changes(&generation.request.execution_id, 0, 1)
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    let retained = page
        .target_preserved_range
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(retained.scope, RetentionScope::SourceRange);
    assert_eq!(retained.source_presence, SourcePresence::Unknown);
    assert_eq!(retained.reason, TargetRetentionReason::SourceCleanupPolicy);
    assert!(retained.source_path.is_none() && retained.target_path.is_none());
    assert!(page.changes.iter().all(|row| row.path != "old-only"));
    assert_eq!(std::fs::read(target.join("old-only"))?, b"retained at the target");
    assert_eq!(
        page.archive_coverage,
        if damaged {
            CopyChangeArchiveCoverage::Partial
        } else {
            CopyChangeArchiveCoverage::Complete
        }
    );
    let report = final_report(config, &command.job_id, &generation.request.execution_id).await?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, u64::from(damaged));
    assert_eq!(report.target_preserved_count, u64::from(damaged));
    match report.detail {
        ProgressDetail::Incremental {
            transferred_bytes,
            new_count,
            changed_count,
            ..
        } => {
            assert_eq!(transferred_bytes, 0);
            assert_eq!(new_count, 0);
            assert_eq!(changed_count, u64::from(case == "metadata"));
        }
        other @ ProgressDetail::Full { .. } => panic!("incremental report required, got {other:?}"),
    }
    println!(
        "content migration deployment={deployment} case={case} generations=2 second_mode=incremental second_data_bytes=0 source_retained={damaged} base_empty=true final_report_saved=true"
    );
    Ok(())
}

async fn final_report(config: &ClickHouseConfig, job: &str, execution_id: &str) -> Result<ProgressReport> {
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client
        .query(&format!("SELECT payload_json FROM notifications_{job} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1"))
        .bind(execution_id)
        .fetch_one::<String>()
        .await
        .map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    Ok(serde_json::from_str(&saved.payload_json)?)
}

async fn incremental_reports_actual_bytes_and_paths(config: &ClickHouseConfig, deployment: &str) -> Result<()> {
    let root = PathBuf::from(environment("LAB_COPY_CONTENT_ROOT")?).join(format!("{deployment}-bytes"));
    let source = root.join("source");
    let target = root.join("target");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir(&target)?;
    for name in ["content", "metadata", "both", "removed", "already_absent"] {
        std::fs::write(source.join(name), b"abc")?;
        std::fs::set_permissions(source.join(name), Permissions::from_mode(0o640))?;
    }
    let mut command = job(&root.join("job"));
    command.job_id = "content_bytes".into();
    command.src_path = source.to_string_lossy().into_owned();
    command.dest_path = target.to_string_lossy().into_owned();
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_COPY_CONTENT_CALLBACK")?)?);
    command.concurrency = Some(2);
    SyncOrchestrator::new_local(command.clone()).run().await?;
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let first = database
        .latest_copy_generation()
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    let full = final_report(config, &command.job_id, &first.request.execution_id).await?;
    assert!(matches!(
        full.detail,
        ProgressDetail::Full {
            transferred_bytes: 15,
            ..
        }
    ));

    std::fs::write(source.join("added"), b"added")?;
    std::fs::write(source.join("content"), b"content")?;
    std::fs::write(source.join("both"), b"both")?;
    for name in ["metadata", "both"] {
        std::fs::set_permissions(source.join(name), Permissions::from_mode(0o600))?;
    }
    std::fs::remove_file(source.join("removed"))?;
    std::fs::remove_file(source.join("already_absent"))?;
    std::fs::remove_file(target.join("already_absent"))?;
    SyncOrchestrator::new_local(command.clone()).run().await?;
    let run = database
        .latest_copy_generation()
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    let report = final_report(config, &command.job_id, &run.request.execution_id).await?;
    assert!(
        matches!(
            report.detail,
            ProgressDetail::Incremental {
                transferred_bytes: 16,
                new_count: 1,
                changed_count: 3,
                deleted_count: 2,
                ..
            }
        ),
        "{:?}",
        report.detail
    );
    assert_eq!(std::fs::read(target.join("content"))?, b"content");
    assert_eq!(std::fs::read(target.join("metadata"))?, b"abc");
    assert_eq!(
        std::fs::metadata(target.join("metadata"))?.permissions().mode() & 0o777,
        0o600
    );
    assert!(!target.join("removed").exists());
    let page = database
        .incremental_copy_changes(&run.request.execution_id, 0, 10)
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(
        page.changes
            .iter()
            .map(|row| (row.path.as_str(), row.kind.as_str()))
            .collect::<Vec<_>>(),
        [
            ("added", "new"),
            ("already_absent", "source_deleted"),
            ("both", "both_changed"),
            ("content", "data_changed"),
            ("metadata", "metadata_changed"),
            ("removed", "source_deleted"),
        ]
    );
    assert_eq!(page.changes[1].target_outcome.as_deref(), Some("already_absent"));
    assert_eq!(page.changes[2].target_outcome.as_deref(), Some("completed"));
    assert_eq!(page.changes[5].target_outcome.as_deref(), Some("deleted"));
    assert_eq!(page.archive_coverage, CopyChangeArchiveCoverage::Complete);
    Ok(())
}

async fn incremental_archives_failed_changes(config: &ClickHouseConfig, deployment: &str) -> Result<()> {
    let root = PathBuf::from(environment("LAB_COPY_CONTENT_ROOT")?).join(format!("{deployment}-failed-changes"));
    let source = root.join("source");
    let target = root.join("target");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir(&target)?;
    std::fs::write(source.join("changed"), b"original")?;
    let mut command = job(&root.join("job"));
    command.job_id = "content_failed_changes".into();
    command.src_path = source.to_string_lossy().into_owned();
    command.dest_path = target.to_string_lossy().into_owned();
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_COPY_CONTENT_CALLBACK")?)?);
    SyncOrchestrator::new_local(command.clone()).run().await?;

    std::fs::write(source.join("changed"), b"changed contents")?;
    std::fs::write(source.join("added"), b"new contents")?;
    std::fs::remove_file(target.join("changed"))?;
    for path in ["added", "changed"] {
        std::fs::create_dir(target.join(path))?;
        std::fs::write(target.join(path).join("keep"), b"keep")?;
    }
    assert!(SyncOrchestrator::new_local(command.clone()).run().await.is_err());

    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let run = database
        .latest_copy_generation()
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    let page = database
        .incremental_copy_changes(&run.request.execution_id, 0, 10)
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(page.phase, CopyGenerationPhase::Failed);
    assert_eq!(page.archive_coverage, CopyChangeArchiveCoverage::Partial);
    assert_eq!(
        page.changes
            .iter()
            .map(|row| (row.path.as_str(), row.kind.as_str(), row.target_outcome.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("added", "new", Some("failed")),
            ("changed", "data_changed", Some("failed")),
        ]
    );
    assert!(
        page.changes
            .iter()
            .all(|row| row.reason.as_ref().is_some_and(|reason| !reason.is_empty()))
    );
    let report = final_report(config, &command.job_id, &run.request.execution_id).await?;
    assert!(matches!(
        report.detail,
        ProgressDetail::Incremental {
            new_count: 0,
            changed_count: 0,
            transferred_bytes: 0,
            ..
        }
    ));
    assert_eq!(std::fs::read(source.join("added"))?, b"new contents");
    assert_eq!(std::fs::read(source.join("changed"))?, b"changed contents");
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-notification-archive-local.py --copy-content owned ClickHouse pair and callback"]
async fn real_incremental_move_checks_existing_data_before_source_removal() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let dsn = environment("LAB_COPY_GENERATION_DSN")?;
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
            database: format!("ts_content_{}", Uuid::new_v4().simple()),
            username: "default".into(),
            password: None,
            dial_timeout: 10,
            read_timeout: 60,
        };
        let suffix = if cluster {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        admin
            .query(&format!("CREATE DATABASE {}{suffix}", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from)?;
        AppConfig::override_with(|global| {
            global.database.enabled = true;
            global.database.clickhouse = config.clone();
        })?;
        let result: Result<()> = async {
            let deployment = if cluster { "cluster" } else { "single" };
            for name in ["unchanged", "corrupt", "metadata"] {
                case(&config, deployment, name).await?;
            }
            incremental_reports_actual_bytes_and_paths(&config, deployment).await?;
            incremental_archives_failed_changes(&config, deployment).await
        }
        .await;
        let cleanup = admin
            .query(&format!("DROP DATABASE {}{suffix} SYNC", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from);
        result?;
        cleanup?;
    }
    Ok(())
}
