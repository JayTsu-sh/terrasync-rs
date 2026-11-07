//! 相同大小／mtime／版本槽被覆盖时，必须按新的源身份重新复制。
use std::time::Duration;

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
use crate::error::AppError;
use crate::orchestrator::SyncOrchestrator;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn environment(name: &str) -> TestResult<String> {
    Ok(std::env::var(format!("LAB_MUTABLE_CATALOGUE_{name}"))
        .map_err(|_| AppError::ConfigError("owned mutable object fixture required".into()))?)
}

async fn replace_source(prefix: &str) -> TestResult {
    let control = environment("CONTROL")?;
    timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(&control).await?;
        stream
            .write_all(format!("GET /replace/{prefix} HTTP/1.0\r\nHost: {control}\r\n\r\n").as_bytes())
            .await?;
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await?;
        assert!(reply.starts_with(b"HTTP/1.0 204"));
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned source replacement timed out".into()))??;
    Ok(())
}

async fn run_round(config: &ClickHouseConfig, command: &SyncJobConfig, round: u64) -> TestResult<String> {
    SyncOrchestrator::new_local(command.clone()).run().await?;
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    assert_eq!(database.get_count("base").await?, 1);
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("accepted mutable source generation required".into()))?;
    assert_eq!(generation.phase, CopyGenerationPhase::Completed);
    assert_eq!(generation.sequence, round);
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
    let (identity, size, modified, raw_version) = client
        .query(&format!(
            "SELECT identity_key,size,toUnixTimestamp64Nano(mtime),source_version_id FROM base_{} FINAL",
            command.job_id
        ))
        .fetch_one::<(String, u64, i64, String)>()
        .await
        .map_err(DatabaseError::from)?;
    assert_eq!((size, modified), (11, 1_704_067_200_000_000_000));
    assert_eq!(
        raw_version,
        if command.job_id.starts_with("null_") {
            "null"
        } else {
            ""
        }
    );
    let raw=client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!((report.task_error_count, report.target_preserved_count), (0, 0));
    match report.detail {
        ProgressDetail::Full {
            scanned_files,
            error_count,
            transferred_bytes,
            ..
        } => {
            assert_eq!((scanned_files, error_count, transferred_bytes), (1, 0, 11));
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
                (scanned_files, error_count, new_count, deleted_count, renamed_count),
                (1, 0, 0, 0, 0)
            );
            assert_eq!(
                (changed_count, transferred_bytes),
                if round == 2 { (1, 11) } else { (0, 0) }
            );
        }
    }
    Ok(identity)
}

async fn copy_case(config: &ClickHouseConfig, deployment: &str, kind: &str, history: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    let prefix = format!("{deployment}/{kind}/{history}");
    let mut command = job(directory.path());
    command.job_id = format!("{kind}_{history}");
    command.src_path = format!("{}/{prefix}", environment(&kind.to_uppercase())?);
    command.dest_path = format!("{}/{prefix}", environment(if history { "HISTORY" } else { "LATEST" })?);
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("CALLBACK")?)?);
    let original = run_round(config, &command, 1).await?;
    replace_source(&prefix).await?;
    let replaced = run_round(config, &command, 2).await?;
    assert_ne!(original, replaced, "大小／时间／版本号相同，但源身份确实变化");
    assert_eq!(replaced, run_round(config, &command, 3).await?);
    eprintln!(
        "mutable-object deployment={deployment} source={kind} target_history={history} rounds=3 copied_after_identity_change=1 unchanged_bytes=0 same_size_mtime_version=true logical_entry_count=1"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned S3 and ClickHouse from run-notification-archive-local.py --mutable-catalogue"]
async fn real_mutable_object_identities_are_compared_even_with_equal_size_time_and_version() -> TestResult {
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
            database: format!("ts_mutable_catalogue_{}", Uuid::new_v4().simple()),
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
        let result: TestResult = async {
            for kind in ["ordinary", "null"] {
                for history in [false, true] {
                    copy_case(&config, if cluster { "cluster" } else { "single" }, kind, history).await?;
                }
            }
            Ok(())
        }
        .await;
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
