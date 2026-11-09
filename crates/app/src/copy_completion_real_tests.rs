//! 独占目录的权限故障，覆盖真实代次、目录收尾、保源及分页通知。
#![allow(clippy::unwrap_used)]
use std::{
    env,
    fs::{File, FileTimes, Permissions},
    os::unix::fs::{PermissionsExt, chown},
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{
    SourceCleanupPolicy,
    migration::{CopyGenerationPhase, MigrationMode},
};
use tokio::{process::Command, time::timeout};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use crate::{
    callback::CallbackEndpoint,
    config::SyncJobConfig,
    consumer::stats::{ProgressDetail, ProgressReport, delivery::FinalCallbackReport},
    error::{AppError, Result},
    orchestrator::SyncOrchestrator,
};

use super::tests::job;

const FAILED_DIRECTORIES: usize = 70;
const BODY: &[u8] = b"directory completion payload";
const MTIME: u64 = 1_700_000_000;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

fn command(root: &Path, name: &str) -> Result<SyncJobConfig> {
    let mut command = job(&root.join("job"));
    command.job_id = format!("dir_finalize_{name}");
    command.src_path = root.join("source").to_string_lossy().into_owned();
    command.dest_path = root.join("target").to_string_lossy().into_owned();
    command.concurrency = Some(8);
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_DIRECTORY_CALLBACK")?)?);
    Ok(command)
}

fn owned(path: &Path) -> Result<()> {
    chown(path, Some(65534), Some(65534))?;
    Ok(())
}

fn writable(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    std::fs::set_permissions(path, Permissions::from_mode(0o777))?;
    Ok(())
}

async fn saved_report(
    config: &ClickHouseConfig, command: &SyncJobConfig, mode: MigrationMode, failed: bool,
) -> Result<()> {
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
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, 0);
    let errors = match report.detail {
        ProgressDetail::Full { error_count, .. } | ProgressDetail::Incremental { error_count, .. } => error_count,
    };
    assert_eq!(errors, if failed { FAILED_DIRECTORIES as u64 } else { 0 });
    if !failed && command.source_cleanup == SourceCleanupPolicy::DeleteSelectedReady {
        assert_eq!(database.get_count("base").await?, 0);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned unprivileged child from --directory-completion"]
async fn real_unprivileged_directory_metadata_failures_keep_all_sources() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let config: ClickHouseConfig = serde_json::from_str(&environment("LAB_DIRECTORY_CHILD_CONFIG")?)?;
    AppConfig::override_with(|global| {
        global.database.enabled = true;
        global.database.clickhouse = config.clone();
    })?;
    let root = PathBuf::from(environment("LAB_DIRECTORY_CHILD_ROOT")?);
    let name = environment("LAB_DIRECTORY_CHILD_NAME")?;
    let command = command(&root, &name)?;
    let result = timeout(
        Duration::from_secs(60),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await
    .map_err(|_| AppError::ConfigError("owned directory failure timed out".into()))?;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.is_reported_copy_failure() && !error.reported_copy_data_failed()),
        "{result:?}"
    );
    for index in 0..=FAILED_DIRECTORIES {
        let path = format!("d{index:03}/payload");
        assert_eq!(std::fs::read(root.join("source").join(&path))?, BODY);
        assert_eq!(std::fs::read(root.join("target").join(&path))?, BODY);
    }
    assert_eq!(
        std::fs::metadata(root.join("target").join(format!("d{FAILED_DIRECTORIES:03}")))?
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs(),
        MTIME,
        "one failed directory must not prevent a later writable directory from finishing"
    );
    let mode = if name.ends_with("incremental") {
        MigrationMode::Incremental
    } else {
        MigrationMode::Full
    };
    saved_report(&config, &command, mode, true).await
}

async fn case(config: &ClickHouseConfig, deployment: &str, incremental: bool) -> Result<()> {
    let name = format!("{deployment}_{}", if incremental { "incremental" } else { "full" });
    let root = PathBuf::from(environment("LAB_DIRECTORY_ROOT")?).join(&name);
    for directory in [&root, &root.join("source"), &root.join("target"), &root.join("job")] {
        writable(directory)?;
    }
    owned(&root.join("job"))?;
    for index in 0..=FAILED_DIRECTORIES {
        let path = root.join("source").join(format!("d{index:03}"));
        writable(&path)?;
        owned(&path)?;
        std::fs::write(path.join("payload"), BODY)?;
        owned(&path.join("payload"))?;
    }
    let mut command = command(&root, &name)?;
    if incremental {
        command.source_cleanup = SourceCleanupPolicy::Retain;
        SyncOrchestrator::new_local(command.clone()).run().await?;
        saved_report(config, &command, MigrationMode::Full, false).await?;
        owned(&root.join("job/job.lock"))?;
        command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    }
    for index in 0..=FAILED_DIRECTORIES {
        let path = format!("d{index:03}");
        let target = root.join("target").join(&path);
        writable(&target)?;
        chown(
            &target,
            Some(if index == FAILED_DIRECTORIES { 65534 } else { 0 }),
            Some(65534),
        )?;
        File::open(root.join("source").join(&path))?
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(MTIME)))?;
    }
    let executable = root.join("test-program");
    let current = env::current_exe()?;
    if std::fs::hard_link(&current, &executable).is_err() {
        std::fs::copy(&current, &executable)?;
    }
    std::fs::set_permissions(&executable, Permissions::from_mode(0o755))?;
    let status = Command::new(&executable).uid(65534).gid(65534)
        .args(["copy_execution::directory_completion_tests::real_unprivileged_directory_metadata_failures_keep_all_sources", "--ignored", "--exact", "--nocapture"])
        .env("LAB_DIRECTORY_CHILD_CONFIG", serde_json::to_string(config)?)
        .env("LAB_DIRECTORY_CHILD_ROOT", &root).env("LAB_DIRECTORY_CHILD_NAME", &name)
        .status().await?;
    assert!(status.success(), "owned directory child failed: {status}");
    // 权限恢复后由同一个 job 再执行；上一代删源，必须重新全量。
    SyncOrchestrator::new_local(command.clone()).run().await?;
    saved_report(config, &command, MigrationMode::Full, false).await?;
    for index in 0..=FAILED_DIRECTORIES {
        let path = format!("d{index:03}");
        assert!(!root.join("source").join(&path).exists());
        assert_eq!(std::fs::read(root.join("target").join(&path).join("payload"))?, BODY);
        assert_eq!(
            std::fs::metadata(root.join("target").join(&path))?
                .modified()?
                .duration_since(UNIX_EPOCH)?
                .as_secs(),
            MTIME
        );
    }
    println!(
        "directory completion deployment={deployment} incremental={incremental} failed_entries=70 good_directory_completed=true all_sources_preserved=true recovered_full=true base_empty=true final_report_saved=true"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires --directory-completion isolated ClickHouse, callback and owned uid-65534 child"]
async fn real_directory_metadata_failures_are_all_reported_and_recoverable() -> Result<()> {
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
            database: format!("ts_directory_{}", Uuid::new_v4().simple()),
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
