//! 真实 S3 历史、迁移入口及 Single／Cluster 基线共同验证版本槽与逻辑计数。
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::TransferMode;
use task_model::migration::{CopyGenerationPhase, MigrationMode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::tests::job;
use crate::callback::CallbackEndpoint;
use crate::config::SyncJobConfig;
use crate::consumer::stats::delivery::FinalCallbackReport;
use crate::consumer::stats::{ProgressDetail, ProgressReport};
use crate::error::{AppError, Result};
use crate::file_copy::{CopyOptions, copy_file};
use crate::orchestrator::SyncOrchestrator;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

async fn control_action(action: &str, prefix: &str) -> Result<()> {
    let control = std::env::var("LAB_VERSION_CATALOGUE_CONTROL")
        .map_err(|_| AppError::ConfigError("owned S3 control required".into()))?;
    timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(&control).await?;
        stream
            .write_all(format!("GET /{action}/{prefix} HTTP/1.0\r\nHost: {control}\r\n\r\n").as_bytes())
            .await?;
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await?;
        assert!(reply.starts_with(b"HTTP/1.0 204"));
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned control timed out".into()))?
}

async fn failed_incremental_history(config: &ClickHouseConfig, command: &SyncJobConfig) -> Result<()> {
    let result = SyncOrchestrator::new_local(command.clone()).run().await;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("failed generation required".into()))?;
    assert_eq!(generation.phase, CopyGenerationPhase::Failed);
    assert_eq!(database.get_count("base").await?, 4);
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw=client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, 0, "已交付逐版本错误不能再次计任务错误");
    assert!(matches!(
        report.detail,
        ProgressDetail::Incremental {
            scanned_files: 1,
            error_count: 1,
            new_count: 0,
            changed_count: 0,
            transferred_bytes: 0,
            ..
        }
    ));
    Ok(())
}

async fn run_round(config: &ClickHouseConfig, command: &SyncJobConfig, expected_bytes: u64, rows: u64) -> Result<u64> {
    SyncOrchestrator::new_local(command.clone()).run().await?;
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    assert_eq!(database.get_count("base").await?, rows);
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("accepted version migration required".into()))?;
    assert_eq!(generation.phase, CopyGenerationPhase::Completed);
    assert_eq!(
        generation.plan.mode,
        if generation.sequence == 1 {
            MigrationMode::Full
        } else {
            MigrationMode::Incremental
        }
    );
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    if !generation.request.binding.target_history {
        let keys = client
            .query(&format!(
                "SELECT version_id,source_version_id FROM base_{} FINAL",
                command.job_id
            ))
            .fetch_all::<(String, String)>()
            .await
            .map_err(DatabaseError::from)?;
        assert_eq!(keys.len(), 1);
        assert!(keys[0].0.is_empty() && !keys[0].1.is_empty());
    }
    let raw=client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final);
    assert_eq!(report.task_error_count, 0);
    assert_eq!(report.migration, Some(generation.report()));
    match report.detail {
        ProgressDetail::Full {
            scanned_files,
            error_count,
            transferred_bytes,
            ..
        } => {
            assert_eq!((scanned_files, error_count, transferred_bytes), (1, 0, expected_bytes));
        }
        ProgressDetail::Incremental {
            scanned_files,
            error_count,
            transferred_bytes,
            new_count,
            changed_count,
            deleted_count,
            renamed_count,
            ..
        } => {
            assert_eq!(
                (
                    scanned_files,
                    error_count,
                    transferred_bytes,
                    deleted_count,
                    renamed_count
                ),
                (1, 0, expected_bytes, 0, 0)
            );
            assert_eq!(new_count + changed_count, u64::from(expected_bytes > 0));
        }
    }
    Ok(report.target_preserved_count)
}

async fn copy_versions_case(config: &ClickHouseConfig, prefix: &str) -> Result<()> {
    let environment = |kind: &str| -> Result<String> {
        Ok(format!(
            "{}/{prefix}",
            std::env::var(format!("LAB_VERSION_CATALOGUE_{kind}"))
                .map_err(|_| AppError::ConfigError("owned S3 catalogue fixture required".into()))?
        ))
    };
    let directory = tempfile::tempdir()?;
    let mut latest = job(directory.path());
    latest.job_id = "version_latest".into();
    latest.src_path = environment("SOURCE")?;
    latest.dest_path = environment("LATEST")?;
    latest.concurrency = Some(2);
    latest.progress_callback_url = Some(CallbackEndpoint::parse(
        &std::env::var("LAB_VERSION_CATALOGUE_CALLBACK")
            .map_err(|_| AppError::ConfigError("owned version callback required".into()))?,
    )?);
    let mut history = latest.clone();
    history.job_id = "version_history".into();
    history.dest_path = environment("HISTORY")?;
    run_round(config, &latest, 9, 1).await?;
    run_round(config, &history, 36, 4).await?;
    let mut failed = history.clone();
    failed.job_id = "version_failed_history".into();
    failed.src_path = environment("FAULT_SOURCE")?;
    failed.dest_path.push_str("/failure");
    run_round(config, &failed, 36, 4).await?;

    let upload = tempfile::tempdir()?;
    let upload_storage = create_storage(&upload.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let source = create_storage(&latest.src_path, CreateStorageOptions::default()).await?;
    for content in [b"changed-payload".as_slice(), b"final-payload".as_slice()] {
        std::fs::write(upload.path().join("payload"), content)?;
        let entry = Arc::new(observe_path(upload_storage.storage(), Path::new("payload")).await?);
        copy_file(
            &upload_storage,
            &source,
            &entry,
            CopyOptions {
                transfer_mode: TransferMode::Direct,
                ..Default::default()
            },
            None,
        )
        .await?;
    }
    run_round(config, &latest, 13, 1).await?;
    run_round(config, &history, 28, 6).await?;
    run_round(config, &latest, 0, 1).await?;
    run_round(config, &history, 0, 6).await?;
    control_action("data-fault", prefix).await?;
    failed_incremental_history(config, &failed).await?;
    control_action("marker", prefix).await?;
    assert_eq!(
        run_round(config, &latest, 0, 1).await?,
        1,
        "已识别删除标记的保留决定必须明确进入 callback"
    );
    eprintln!(
        "version-catalogue cluster={} real_rounds=9 latest_rows=1 history_rows=6 unchanged_bytes=0 changed_versions=2 logical_entries_per_change=1 failed_entries=1 failed_version_details=2 latest_marker_preserves_target_with_callback=true reports_archived=true",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned S3 and ClickHouse from run-notification-archive-local.py --version-catalogue"]
async fn real_versioned_copy_catalogues_follow_destination_history() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let dsn = std::env::var("LAB_COPY_GENERATION_DSN")
        .map_err(|_| AppError::ConfigError("owned ClickHouse required".into()))?;
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
            database: format!("ts_version_catalogue_{}", Uuid::new_v4().simple()),
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
        AppConfig::override_with(|global| {
            global.database.enabled = true;
            global.database.clickhouse = config.clone();
        })?;
        let result = copy_versions_case(&config, if cluster { "cluster" } else { "single" }).await;
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
