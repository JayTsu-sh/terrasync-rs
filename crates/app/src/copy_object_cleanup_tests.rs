//! 未版本化对象源的永久清理：真实协议、数据库及终态通知共同核验。
#![allow(clippy::unwrap_used)]
use std::{env, path::PathBuf, time::Duration};

use clickhouse::Client;
use data_mover::{model::FailureClass, storage::StorageRoleFailure};
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{
    SourceCleanupPolicy,
    migration::{CopyGenerationPhase, MigrationMode},
};
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
use crate::consumer::stats::{ProgressDetail, ProgressReport, delivery::FinalCallbackReport};
use crate::error::{AppError, Result, StorageError};
use crate::orchestrator::SyncOrchestrator;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn control(action: &str) -> Result<()> {
    let address = environment("LAB_OBJECT_CLEANUP_CONTROL")?;
    // 大历史的独占种子准备不计迁移耗时，控制请求单独容纳 256 次版本写入。
    let budget = if action.starts_with("arm/") && action.ends_with("/large") {
        60
    } else {
        5
    };
    timeout(Duration::from_secs(budget), async {
        let mut stream = TcpStream::connect(&address).await?;
        stream
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(response.starts_with(b"HTTP/1.0 204"), "owned object control failed");
        Ok::<_, AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned object control timed out".into()))?
}

async fn report(config: &ClickHouseConfig, command: &SyncJobConfig, sequence: u64, failed: bool) -> Result<()> {
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    let generation = database
        .latest_copy_generation()
        .await?
        .ok_or_else(|| AppError::ConfigError("object generation missing".into()))?;
    assert_eq!(generation.sequence, sequence);
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
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
    assert!(report.is_final && report.diagnostics_complete);
    assert_eq!(
        report.task_error_count,
        u64::from(
            failed
                && !matches!(
                    command.job_id.rsplit('_').next(),
                    Some("data" | "metadata" | "verify" | "cleanup403" | "cleanupunknown")
                )
        )
    );
    Ok(())
}

async fn round(config: &ClickHouseConfig, command: &SyncJobConfig, mode: &str, sequence: u64) -> Result<()> {
    let started = tokio::time::Instant::now();
    let budget = if command.job_id.ends_with("_large") { 90 } else { 35 };
    let result = timeout(
        Duration::from_secs(budget),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await
    .map_err(|_| AppError::ConfigError("owned object cleanup job timed out".into()))?;
    if mode == "normal" {
        result?;
    } else if matches!(mode, "data" | "metadata" | "verify" | "cleanup403" | "cleanupunknown") {
        assert!(
            result.as_ref().is_err_and(AppError::is_reported_copy_failure),
            "{result:?}"
        );
        assert_eq!(result.unwrap_err().reported_copy_data_failed(), mode != "metadata");
    } else if matches!(mode, "historyguard" | "missingcurrent" | "metaread" | "metachanged") {
        let error = result.unwrap_err();
        assert!(error.source_cleanup_entry().is_some(), "{error}");
        assert!(
            matches!(
                (mode, error.storage_error()),
                (
                    "historyguard",
                    Some(StorageError::PermanentVersionCleanupUnavailable { .. })
                ) | (
                    "missingcurrent",
                    Some(StorageError::SourceHistoryCurrentUnconfirmed { .. })
                ) | ("metaread", Some(StorageError::NamespaceRead(_)))
                    | ("metachanged", Some(StorageError::SourceMetadataChanged { .. }))
            ),
            "{error}"
        );
    } else {
        let error = result.unwrap_err();
        assert!(error.source_cleanup_entry().is_some(), "{error}");
        assert!(
            matches!(error.storage_error(), Some(StorageError::SourcePurgeRefused { .. })),
            "{error}"
        );
        if let Some(StorageError::SourcePurgeRefused { source, .. }) = error.storage_error() {
            let class = match source {
                StorageRoleFailure::Entry(cause) => cause.class(),
                StorageRoleFailure::Session(cause) => cause.class(),
            };
            assert_eq!(
                class,
                match mode {
                    "readonly" => FailureClass::PermissionDenied,
                    "stall" => FailureClass::Timeout,
                    "sourcechanged" => FailureClass::Conflict,
                    "contradictory" => FailureClass::Protocol,
                    _ => FailureClass::Connectivity,
                }
            );
        }
    }
    report(config, command, sequence, mode != "normal").await?;
    println!(
        "object cleanup job={} mode={mode} generation={sequence} elapsed_ms={} full=true report_saved=true base_empty=true",
        command.job_id,
        started.elapsed().as_millis()
    );
    Ok(())
}

async fn case(config: &ClickHouseConfig, deployment: &str, target: &str, full: bool, mode: &str) -> Result<()> {
    let verification = if full { "full" } else { "quick" };
    let prefix = format!("{deployment}-{target}-{verification}-{mode}");
    let root = PathBuf::from(environment("LAB_OBJECT_CLEANUP_ROOT")?).join(&prefix);
    std::fs::create_dir_all(root.join("target"))?;
    let mut command = job(&root.join("job"));
    command.job_id = format!("objclean_{deployment}_{target}_{verification}_{mode}");
    command.src_path = format!("{}/{prefix}", environment("LAB_OBJECT_CLEANUP_SOURCE")?);
    command.dest_path = if target == "local" {
        root.join("target").to_string_lossy().into_owned()
    } else {
        format!(
            "{}/{prefix}",
            environment(&format!("LAB_OBJECT_CLEANUP_{}", target.to_uppercase()))?
        )
    };
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    command.enable_integrity_check = full;
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_OBJECT_CLEANUP_CALLBACK")?)?);
    control(&format!("arm/{prefix}/{mode}")).await?;
    let result_mode = if mode == "large" { "normal" } else { mode };
    round(config, &command, result_mode, 1).await?;
    control(&format!("inspect/{prefix}/{result_mode}")).await?;
    if !matches!(mode, "normal" | "historyguard" | "missingcurrent" | "large") {
        control(&format!("recover/{prefix}/{mode}")).await?;
        round(config, &command, "normal", 2).await?;
        control(&format!("inspect/{prefix}/normal")).await?;
    }
    Ok(())
}

async fn unchanged_versioned_move(config: &ClickHouseConfig, deployment: &str, corrupt: bool) -> Result<()> {
    let mode = if corrupt { "unchangedbad" } else { "unchanged" };
    let prefix = format!("{deployment}-latest-full-{mode}");
    let root = PathBuf::from(environment("LAB_OBJECT_CLEANUP_ROOT")?).join(&prefix);
    std::fs::create_dir_all(root.join("job"))?;
    let mut command = job(&root.join("job"));
    command.job_id = format!("objclean_{deployment}_latest_full_{mode}");
    command.src_path = format!("{}/{prefix}", environment("LAB_OBJECT_CLEANUP_SOURCE")?);
    command.dest_path = format!("{}/{prefix}", environment("LAB_OBJECT_CLEANUP_LATEST")?);
    command.enable_integrity_check = true;
    command.concurrency = Some(2);
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_OBJECT_CLEANUP_CALLBACK")?)?);
    control(&format!("arm/{prefix}/{mode}")).await?;
    for sequence in 1..=if corrupt { 3 } else { 2 } {
        command.source_cleanup = SourceCleanupPolicy::from_reserved(sequence == 1);
        if sequence == 2 && corrupt {
            control(&format!("damage/{prefix}/{mode}")).await?;
        } else if sequence == 3 {
            control(&format!("recover/{prefix}/normal")).await?;
        }
        let failed = sequence == 2 && corrupt;
        let result = timeout(
            Duration::from_secs(35),
            Box::pin(SyncOrchestrator::new_local(command.clone()).run()),
        )
        .await
        .map_err(|_| AppError::ConfigError("owned unchanged historical move timed out".into()))?;
        if failed {
            let error = result.unwrap_err();
            assert!(error.source_cleanup_entry().is_some(), "{error}");
            assert!(
                matches!(error.storage_error(), Some(StorageError::MismatchData(_))),
                "{error}"
            );
        } else {
            result?;
        }
        let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
        assert_eq!(database.get_count("base").await?, u64::from(sequence == 1));
        let generation = database
            .latest_copy_generation()
            .await?
            .ok_or_else(|| AppError::ConfigError("unchanged generation missing".into()))?;
        assert_eq!(generation.sequence, sequence);
        assert_eq!(
            generation.plan.mode,
            if sequence == 2 {
                MigrationMode::Incremental
            } else {
                MigrationMode::Full
            }
        );
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
        assert_eq!(report.task_error_count, u64::from(failed));
        if sequence == 2 {
            assert!(
                matches!(
                    report.detail,
                    ProgressDetail::Incremental {
                        scanned_files: 1,
                        new_count: 0,
                        changed_count: 0,
                        transferred_bytes: 0,
                        error_count: 0,
                        ..
                    }
                ),
                "{report:?}"
            );
        }
        let inspection = if sequence == 1 {
            "retained"
        } else if failed {
            "unverified"
        } else {
            "normal"
        };
        control(&format!("inspect/{prefix}/{inspection}")).await?;
        eprintln!(
            "unchanged-versioned-move cluster={} corrupt={corrupt} generation={sequence} incremental={} failed={failed} reports_archived=true base_retained_only_while_source_reserved=true",
            config.cluster.is_some(),
            sequence == 2
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-notification-archive-local.py --copy-object-cleanup owned RustFS and ClickHouse"]
async fn real_unversioned_object_source_cleanup_and_failures() -> Result<()> {
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
            database: format!("ts_objclean_{}", Uuid::new_v4().simple()),
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
        let deployment = if cluster { "cluster" } else { "single" };
        let result: Result<()> = async {
            let targets = if environment("LAB_OBJECT_CLEANUP_VERSIONED").is_ok_and(|value| value == "true") {
                vec!["local", "latest"]
            } else {
                vec!["local", "latest", "history"]
            };
            for target in targets {
                for full in [false, true] {
                    case(&config, deployment, target, full, "normal").await?;
                }
            }
            for mode in ["readonly", "disconnect", "stall", "lostack", "data", "metadata"] {
                case(&config, deployment, "latest", false, mode).await?;
            }
            for mode in ["verify", "cleanup403", "cleanupunknown"] {
                case(&config, deployment, "latest", true, mode).await?;
            }
            if environment("LAB_OBJECT_CLEANUP_VERSIONED").is_ok_and(|value| value == "true") {
                for mode in [
                    "missingcurrent",
                    "sourcechanged",
                    "metaread",
                    "metachanged",
                    "contradictory",
                ] {
                    case(&config, deployment, "latest", false, mode).await?;
                }
                case(&config, deployment, "history", false, "historyguard").await?;
                for corrupt in [false, true] {
                    Box::pin(unchanged_versioned_move(&config, deployment, corrupt)).await?;
                }
                case(&config, deployment, "latest", false, "large").await?;
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
