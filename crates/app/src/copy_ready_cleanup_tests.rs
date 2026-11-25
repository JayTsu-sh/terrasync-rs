//! 真实 `ClickHouse` 回执阻塞时，目标就绪的小窗口不等待入库；目录及失败项仍保留原门槛。
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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Command,
    time::timeout,
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::tests::job;
use crate::callback::CallbackEndpoint;
use crate::config::SyncJobConfig;
use crate::consumer::stats::{ProgressDetail, ProgressReport, delivery::FinalCallbackReport};
use crate::error::{AppError, Result};
use crate::orchestrator::SyncOrchestrator;

const BODY: &[u8] = b"ready data";
const DIRECTORY_TIME: u64 = 1_700_000_000;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn control(action: &str) -> Result<()> {
    let address = environment("LAB_READY_CONTROL")?;
    timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(&address).await?;
        stream
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(response.starts_with(b"HTTP/1.0 204"), "owned control failed");
        Ok::<_, AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned cleanup control timed out".into()))?
}

fn command(root: &Path, deployment: &str, name: &str) -> Result<SyncJobConfig> {
    let mut command = job(&root.join("job"));
    command.job_id = format!("ready_{deployment}_{name}");
    command.src_path = root.join("source").to_string_lossy().into_owned();
    command.dest_path = root.join("target").to_string_lossy().into_owned();
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    command.concurrency = Some(if matches!(name, "metadata" | "data") { 1 } else { 2 });
    command.enable_integrity_check = name == "success";
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_READY_CALLBACK")?)?);
    Ok(command)
}

pub(super) async fn check_report(config: &ClickHouseConfig, command: &SyncJobConfig, failed: bool) -> Result<()> {
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("generation missing".into()))?;
    assert_eq!(generation.sequence, 1);
    assert_eq!(generation.plan.mode, MigrationMode::Full);
    assert_eq!(
        generation.phase,
        if failed {
            CopyGenerationPhase::Failed
        } else {
            CopyGenerationPhase::Completed
        }
    );
    assert_eq!(database.get_count("base").await?, 0);
    let report = saved_report(config, command, &generation.request.execution_id).await?;
    assert!(report.is_final && report.diagnostics_complete);
    if command.job_id.ends_with("reject") {
        assert_eq!(report.task_error_count, 1);
    }
    Ok(())
}

async fn saved_report(
    config: &ClickHouseConfig, command: &SyncJobConfig, execution_id: &str,
) -> Result<ProgressReport> {
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    Ok(serde_json::from_str(&saved.payload_json)?)
}

async fn case(config: &ClickHouseConfig, deployment: &str, name: &str) -> Result<()> {
    let root = PathBuf::from(environment("LAB_READY_ROOT")?).join(format!("{deployment}-{name}"));
    std::fs::create_dir_all(root.join("source"))?;
    std::fs::create_dir(root.join("target"))?;
    for directory in [&root, &root.join("source"), &root.join("target")] {
        std::fs::set_permissions(directory, Permissions::from_mode(0o777))?;
    }
    if matches!(name, "metadata" | "data") {
        // 非特权子进程须能迁移根属性，只在文件本身保留本例的故障。
        for role in ["source", "target"] {
            chown(root.join(role), Some(65534), Some(65534))?;
        }
    }
    let paths: Vec<&str> = match name {
        "directory" => {
            std::fs::create_dir(root.join("source/tree"))?;
            vec!["tree/a", "b"]
        }
        "fallback" => vec!["a", "b", "c", "d"],
        _ => vec!["a", "b"],
    };
    for path in &paths {
        std::fs::write(root.join("source").join(path), BODY)?;
        std::fs::set_permissions(
            root.join("source").join(path),
            Permissions::from_mode(if name == "data" { 0o000 } else { 0o644 }),
        )?;
    }
    if name == "directory" {
        File::open(root.join("source/tree"))?
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(DIRECTORY_TIME)))?;
    }
    let command = command(&root, deployment, name)?;
    control(&format!("arm/{deployment}/{name}")).await?;
    if matches!(name, "metadata" | "data") {
        run_failure_child(config, &root, deployment, name).await?;
    } else {
        let result = timeout(
            Duration::from_secs(40),
            SyncOrchestrator::new_local(command.clone()).run(),
        )
        .await
        .map_err(|_| AppError::ConfigError("owned early cleanup job timed out".into()))?;
        if matches!(name, "reject" | "data") {
            assert!(result.is_err(), "{name} must fail");
            if name == "reject" {
                assert!(result.unwrap_err().to_string().contains("owned ready fact rejection"));
            }
        } else {
            result?;
        }
        let retained = name == "data";
        for path in &paths {
            assert_eq!(root.join("source").join(path).exists(), retained);
            if !retained {
                assert_eq!(std::fs::read(root.join("target").join(path))?, BODY);
            }
        }
        if name == "directory" {
            assert!(!root.join("source/tree").exists());
            assert_eq!(
                std::fs::metadata(root.join("target/tree"))?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs(),
                DIRECTORY_TIME
            );
        }
        check_report(config, &command, matches!(name, "reject" | "data")).await?;
    }
    control(&format!("finish/{deployment}/{name}")).await?;
    println!("ready cleanup deployment={deployment} case={name} full=true final_report_saved=true base_empty=true");
    Ok(())
}

async fn incremental_case(config: &ClickHouseConfig, deployment: &str, name: &str) -> Result<()> {
    let root = PathBuf::from(environment("LAB_READY_ROOT")?).join(format!("{deployment}-{name}"));
    let source = root.join("source");
    let target = root.join("target");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir(&target)?;
    std::fs::write(source.join("a"), b"old content")?;
    std::fs::write(source.join("previous"), b"unchanged")?;

    let mut migration = command(&root, deployment, name)?;
    migration.enable_integrity_check = true;
    let baseline = SyncJobConfig {
        source_cleanup: SourceCleanupPolicy::Retain,
        ..migration.clone()
    };
    SyncOrchestrator::new_local(baseline).run().await?;
    std::fs::write(source.join("a"), BODY)?;
    std::fs::write(source.join("b"), BODY)?;

    let mut active_config = config.clone();
    if name == "incremental_stall" {
        active_config.read_timeout = 1;
        AppConfig::override_with(|global| global.database.clickhouse = active_config)?;
    }
    control(&format!("arm/{deployment}/{name}")).await?;
    let result = timeout(
        Duration::from_secs(40),
        SyncOrchestrator::new_local(migration.clone()).run(),
    )
    .await;
    if name == "incremental_stall" {
        AppConfig::override_with(|global| global.database.clickhouse = config.clone())?;
    }
    let result = result.map_err(|_| AppError::ConfigError("owned incremental cleanup job timed out".into()))?;
    let failed = matches!(
        name,
        "incremental_reject" | "incremental_disconnect" | "incremental_stall"
    );
    if name == "incremental_reject" {
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("owned ready fact rejection")),
            "{result:?}"
        );
    } else if failed {
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("ClickHouse unavailable after")),
            "{result:?}"
        );
    } else {
        result?;
    }
    control(&format!("finish/{deployment}/{name}")).await?;
    for path in ["a", "b"] {
        assert!(!source.join(path).exists(), "{path} was retained after publication");
        assert_eq!(std::fs::read(target.join(path))?, BODY);
    }
    assert_eq!(source.join("previous").exists(), failed);
    assert_eq!(std::fs::read(target.join("previous"))?, b"unchanged");

    let database = ClickHouseDatabase::connect(config, &migration.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("incremental generation missing".into()))?;
    assert_eq!(generation.sequence, 2);
    assert_eq!(generation.plan.mode, MigrationMode::Incremental);
    assert_eq!(
        generation.phase,
        if failed {
            CopyGenerationPhase::Failed
        } else {
            CopyGenerationPhase::Completed
        }
    );
    if !failed {
        assert_eq!(database.get_count("base").await?, 0);
    }
    let report = saved_report(config, &migration, &generation.request.execution_id).await?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(report.task_error_count, u64::from(failed));
    assert!(
        matches!(
            report.detail,
            ProgressDetail::Incremental { new_count: 1, changed_count: 1, transferred_bytes, .. }
                if transferred_bytes == (BODY.len() * 2) as u64
        ),
        "{:?}",
        report.detail
    );
    println!(
        "ready cleanup deployment={deployment} case={name} incremental=true phase={:?}",
        generation.phase
    );
    Ok(())
}

async fn run_failure_child(config: &ClickHouseConfig, root: &Path, deployment: &str, name: &str) -> Result<()> {
    let executable = root.join("test-program");
    let current = env::current_exe()?;
    if std::fs::hard_link(&current, &executable).is_err() {
        std::fs::copy(&current, &executable)?;
    }
    std::fs::set_permissions(&executable, Permissions::from_mode(0o755))?;
    let status = Command::new(&executable)
        .uid(65534)
        .gid(65534)
        .args([
            "copy_execution::ready_cleanup_tests::real_full_move_unprivileged_failure_preserves_sources",
            "--ignored",
            "--exact",
            "--nocapture",
        ])
        .env("LAB_READY_CHILD_CONFIG", serde_json::to_string(config)?)
        .env("LAB_READY_CHILD_ROOT", root)
        .env("LAB_READY_CHILD_DEPLOYMENT", deployment)
        .env("LAB_READY_CHILD_CASE", name)
        .status()
        .await?;
    assert!(status.success());
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned unprivileged child from --copy-ready-cleanup"]
async fn real_full_move_unprivileged_failure_preserves_sources() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let config: ClickHouseConfig = serde_json::from_str(&environment("LAB_READY_CHILD_CONFIG")?)?;
    AppConfig::override_with(|global| {
        global.database.enabled = true;
        global.database.clickhouse = config.clone();
    })?;
    let root = PathBuf::from(environment("LAB_READY_CHILD_ROOT")?);
    let name = environment("LAB_READY_CHILD_CASE")?;
    let command = command(&root, &environment("LAB_READY_CHILD_DEPLOYMENT")?, &name)?;
    let result = SyncOrchestrator::new_local(command.clone()).run().await;
    assert!(
        result.as_ref().is_err_and(
            |error| error.is_reported_copy_failure() && error.reported_copy_data_failed() == (name == "data")
        ),
        "{result:?}"
    );
    for path in ["a", "b"] {
        assert!(root.join("source").join(path).exists());
        if name == "metadata" {
            assert_eq!(std::fs::read(root.join("source").join(path))?, BODY);
            assert_eq!(std::fs::read(root.join("target").join(path))?, BODY);
        } else {
            assert!(!root.join("target").join(path).exists());
        }
    }
    check_report(&config, &command, true).await
}

#[tokio::test]
#[ignore = "requires run-notification-archive-local.py --copy-ready-cleanup owned ClickHouse and held fact receipts"]
async fn real_full_move_ready_window_does_not_wait_for_fact_receipts() -> Result<()> {
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
            database: format!("ts_ready_{}", Uuid::new_v4().simple()),
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
            for name in ["success", "reject", "directory", "fallback", "data", "metadata"] {
                case(&config, deployment, name).await?;
            }
            for name in [
                "incremental_success",
                "incremental_reject",
                "incremental_lost",
                "incremental_disconnect",
                "incremental_stall",
            ] {
                incremental_case(&config, deployment, name).await?;
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
