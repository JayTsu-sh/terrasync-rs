//! 目录 SETATTR 阻塞与 ClickHouse 故障共存时，整轮失败保源并完整交付终态。
#![allow(clippy::unwrap_used)]
use std::{env, time::Duration};

use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{
    SourceCleanupPolicy,
    migration::{CopyGenerationPhase, MigrationMode},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout},
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::tests::job;
use crate::{
    callback::CallbackEndpoint,
    config::SyncJobConfig,
    consumer::stats::{ProgressDetail, ProgressReport, delivery::FinalCallbackReport},
    error::{AppError, Result},
    orchestrator::SyncOrchestrator,
};

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn control(action: &str) -> Result<()> {
    let address = environment("LAB_DIRECTORY_CANCEL_CONTROL")?;
    timeout(Duration::from_secs(15), async {
        let mut stream = TcpStream::connect(&address).await?;
        stream
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: loopback\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(
            response.starts_with(b"HTTP/1.0 204"),
            "owned directory control refused: {response:?}"
        );
        Ok::<_, AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned directory control timeout".into()))?
}

async fn saved(config: &ClickHouseConfig, command: &SyncJobConfig, mode: MigrationMode, failed: bool) -> Result<()> {
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database.latest_copy_generation().await?.unwrap();
    assert_eq!(generation.plan.mode, mode);
    assert_eq!(
        generation.phase,
        if failed {
            CopyGenerationPhase::Failed
        } else {
            CopyGenerationPhase::Completed
        }
    );
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1", command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let report: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&report.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    let (errors, bytes) = match &report.detail {
        ProgressDetail::Full {
            error_count,
            transferred_bytes,
            ..
        }
        | ProgressDetail::Incremental {
            error_count,
            transferred_bytes,
            ..
        } => (*error_count, *transferred_bytes),
    };
    if failed {
        assert_eq!((errors, report.task_error_count), (3, 1), "{report:?}");
        if mode == MigrationMode::Incremental {
            assert_eq!(bytes, 0, "目录属性变化不能重传未变文件数据");
        }
        assert_eq!(database.get_count("base").await?, 0, "删源失败代次结束也要退役 base");
    } else {
        assert_eq!((errors, report.task_error_count), (0, 0));
        if command.source_cleanup == SourceCleanupPolicy::DeleteSelectedReady {
            assert_eq!(database.get_count("base").await?, 0);
        }
    }
    Ok(())
}

async fn case(config: &ClickHouseConfig, deployment: &str, incremental: bool) -> Result<()> {
    let name = if incremental { "incremental" } else { "full" };
    let directory = tempfile::tempdir()?;
    let run = environment("LAB_DIRECTORY_CANCEL_RUN")?;
    let prefix = format!("/srv/nfs/v4:/ci/{run}/{deployment}/{name}");
    let mut command = job(directory.path());
    command.job_id = format!("dircancel_{deployment}_{name}");
    command.concurrency = Some(4);
    command.src_path = format!("nfs://10.10.1.13{prefix}/source?version=4.1&noresvport=true&uid=0&gid=0");
    command.dest_path = format!(
        "nfs://127.0.0.1{prefix}/target?version=4.1&noresvport=true&uid=0&gid=0&nfsport={}",
        environment("LAB_DIRECTORY_CANCEL_NFS_PORT")?
    );
    command.progress_callback_url = Some(CallbackEndpoint::parse(&format!(
        "http://{}/callback",
        environment("LAB_DIRECTORY_CANCEL_CONTROL")?
    ))?);
    if incremental {
        command.source_cleanup = SourceCleanupPolicy::Retain;
        SyncOrchestrator::new_local(command.clone()).run().await?;
        saved(config, &command, MigrationMode::Full, false).await?;
    }
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    control(&format!("arm/{deployment}/{name}")).await?;
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(35),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await
    .map_err(|_| AppError::ConfigError("owned directory cancellation job timed out".into()))?;
    assert!(result.is_err(), "directory/database fault cannot complete successfully");
    println!(
        "directory cancellation deployment={deployment} mode={name} elapsed_ms={} error={}",
        started.elapsed().as_millis(),
        result.unwrap_err()
    );
    saved(
        config,
        &command,
        if incremental {
            MigrationMode::Incremental
        } else {
            MigrationMode::Full
        },
        true,
    )
    .await?;
    control(&format!("failed/{deployment}/{name}")).await?;
    timeout(
        Duration::from_secs(40),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await
    .map_err(|_| AppError::ConfigError("owned directory recovery timed out".into()))??;
    saved(config, &command, MigrationMode::Full, false).await?;
    control(&format!("recovered/{deployment}/{name}")).await?;
    println!(
        "directory cancellation deployment={deployment} mode={name} source_kept_until_ready=true recovery_full=true final_report_saved=true"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-directory-cancel-nfs.py, owned NFS prefixes and isolated ClickHouse"]
async fn real_directory_metadata_cancellation_keeps_sources_and_recovers() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let dsn = format!("http://{}", environment("LAB_DIRECTORY_CANCEL_CONTROL")?);
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
            database: format!("ts_dircancel_{}", Uuid::new_v4().simple()),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
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
            for incremental in [false, true] {
                case(&config, if cluster { "cluster" } else { "single" }, incremental).await?;
            }
            Ok(())
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
