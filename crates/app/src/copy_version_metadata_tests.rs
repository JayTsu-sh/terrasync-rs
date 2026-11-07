//! 真实 S3＋Single／Cluster 验证历史元数据修复不重传数据，连续失败保持数据完成事实。
use std::{env, time::Duration};

use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
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
use crate::orchestrator::SyncOrchestrator;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn action(mode: &str, prefix: &str) -> Result<()> {
    let address = environment("LAB_VERSION_METADATA_CATALOGUE_CONTROL")?;
    timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(&address).await?;
        stream
            .write_all(format!("GET /{mode}/{prefix} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(response.starts_with(b"HTTP/1.0 204"));
        Ok::<_, AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("metadata test control timed out".into()))?
}

async fn round(
    config: &ClickHouseConfig, command: &SyncJobConfig, round: usize, failed: bool, changes: u64,
) -> Result<()> {
    let result = SyncOrchestrator::new_local(command.clone()).run().await;
    if failed {
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.is_reported_copy_failure() && !error.reported_copy_data_failed()),
            "{result:?}"
        );
    } else {
        result?;
    }
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("generation missing".into()))?;
    assert_eq!(generation.sequence, round as u64);
    assert_eq!(
        generation.phase,
        if failed {
            CopyGenerationPhase::Failed
        } else {
            CopyGenerationPhase::Completed
        }
    );
    assert_eq!(
        generation.plan.mode,
        if round == 1 {
            MigrationMode::Full
        } else {
            MigrationMode::Incremental
        }
    );
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let rows = client.query(&format!("SELECT source_version_id,copy_target_version,copy_data_confirmed,copy_path_confirmed FROM base_{} FINAL ORDER BY source_version_id",command.job_id))
        .fetch_all::<(String,Option<String>,bool,bool)>().await.map_err(DatabaseError::from)?;
    assert_eq!(
        rows.len(),
        if generation.request.binding.target_history {
            4
        } else {
            1
        }
    );
    assert!(rows.iter().all(|row| row.2 && row.3 != failed), "{rows:?}");
    if generation.request.binding.target_history {
        assert!(
            rows.iter().all(|row| row
                .1
                .as_ref()
                .is_some_and(|target| !target.is_empty() && target != &row.0)),
            "{rows:?}"
        );
    }
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, 0);
    match report.detail {
        ProgressDetail::Full {
            error_count,
            transferred_bytes,
            ..
        } => {
            assert_eq!(round, 1);
            assert_eq!(error_count, 1);
            assert_eq!(
                transferred_bytes,
                if generation.request.binding.target_history {
                    36
                } else {
                    9
                }
            );
        }
        ProgressDetail::Incremental {
            error_count,
            new_count,
            changed_count,
            transferred_bytes,
            ..
        } => {
            assert_eq!(error_count, u64::from(failed));
            assert_eq!(new_count, 0);
            assert_eq!(changed_count, changes);
            assert_eq!(transferred_bytes, 0);
        }
    }
    Ok(())
}

async fn case(config: &ClickHouseConfig, deployment: &str, history: bool, full: bool) -> Result<()> {
    let prefix = format!(
        "{deployment}/{}/{}",
        if history { "history" } else { "latest" },
        if full { "full" } else { "quick" }
    );
    let directory = tempfile::tempdir()?;
    let mut command = job(directory.path());
    command.job_id = format!("metadata_{history}_{full}");
    command.src_path = format!("{}/{prefix}", environment("LAB_VERSION_METADATA_CATALOGUE_SOURCE")?);
    command.dest_path = format!(
        "{}/{prefix}",
        environment(if history {
            "LAB_VERSION_METADATA_CATALOGUE_HISTORY"
        } else {
            "LAB_VERSION_METADATA_CATALOGUE_LATEST"
        })?
    );
    command.enable_integrity_check = full;
    command.concurrency = Some(4);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment(
        "LAB_VERSION_METADATA_CATALOGUE_CALLBACK",
    )?)?);
    action("source-fail", &prefix).await?;
    round(config, &command, 1, true, 0).await?;
    round(config, &command, 2, true, 0).await?;
    action("target-fail", &prefix).await?;
    round(config, &command, 3, true, 0).await?;
    round(config, &command, 4, true, 0).await?;
    action("normal", &prefix).await?;
    round(config, &command, 5, false, 1).await?;
    round(config, &command, 6, false, 0).await?;
    action("old-tags", &prefix).await?;
    round(config, &command, 7, false, u64::from(history)).await?;
    eprintln!(
        "metadata catalogue deployment={deployment} history={history} full={full} rounds=7 repeated_failures=4 retries_data_bytes=0 logical_errors_each=1"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned S3 and ClickHouse from run-notification-archive-local.py --version-metadata"]
async fn historical_metadata_repair_preserves_versions_and_data_after_repeated_failures() -> Result<()> {
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
            database: format!("ts_metadata_{}", Uuid::new_v4().simple()),
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
        let result = async {
            for history in [false, true] {
                for full in [false, true] {
                    case(&config, if cluster { "cluster" } else { "single" }, history, full).await?;
                }
            }
            Ok::<_, AppError>(())
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
