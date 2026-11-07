//! 任务接受时的策略同时控制代次记录与远程准入；全局配置不得反向改变它。
use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use std::time::{Duration, Instant};
use std::{path::Path, sync::Arc};
use task_model::SourceCleanupPolicy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::{begin_copy_catalogue, tests::job};
use crate::callback::CallbackEndpoint;
use crate::consumer::stats::delivery::FinalCallbackReport;
use crate::consumer::stats::{ProgressDetail, ProgressReport};
use crate::error::{AppError, Result};
use crate::orchestrator::SyncOrchestrator;
use crate::scan::observe_path;
use crate::storage_factory::{StorageRole, create_storage_for_role};

async fn frozen_catalogue(config: &ClickHouseConfig) -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let directory = tempfile::tempdir()?;
    let mut command = job(directory.path());
    command.job_id = "frozen_command".into();
    command.src_path = source.path().to_string_lossy().into_owned();
    command.dest_path = target.path().to_string_lossy().into_owned();
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    database.initialize().await?;
    // 每轮刻意让全局值与请求相反；包括尚未提交的失败代次。
    for policy in [
        SourceCleanupPolicy::Retain,
        SourceCleanupPolicy::DeleteSelectedReady,
        SourceCleanupPolicy::Retain,
    ] {
        command.source_cleanup = policy;
        AppConfig::override_with(|global| global.sync.is_source_reserved = !policy.retains_source())?;
        begin_copy_catalogue(&database, &command, None).await?;
        let observed = ClickHouseDatabase::connect(config, &command.job_id).await?;
        assert_eq!(
            observed.copy_catalogue_source_reserved().await?,
            Some(policy.retains_source())
        );
    }
    eprintln!(
        "frozen-source-policy cluster={} accepted_attempts=3 global_opposite=true persisted_request_policy=true",
        config.cluster.is_some()
    );
    Ok(())
}

async fn remote_rejection(config: &ClickHouseConfig, retain: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut command = job(directory.path());
    command.job_id = format!("remote_policy_{retain}");
    command.source_cleanup = SourceCleanupPolicy::from_reserved(retain);
    // 本用例必须在解析远端地址／访问存储前完成准入及错误通知。
    command.src_path = "invalid-source://must-not-open".into();
    command.dest_path = "invalid-target://must-not-open".into();
    command.raw_command_line =
        "sync --token policy auth secret s3://policy-access:policy-secret@bucket.invalid/x target".into();
    command.progress_callback_url = Some(CallbackEndpoint::parse(
        &std::env::var("LAB_REQUEST_POLICY_CALLBACK_URL")
            .map_err(|_| AppError::ConfigError("owned callback URL required".into()))?,
    )?);
    AppConfig::override_with(|global| global.sync.is_source_reserved = !retain)?;
    let result = SyncOrchestrator::new_remote(
        command.clone(),
        "invalid-remote-address",
        None,
        Some("policy auth secret".into()),
    )
    .run()
    .await;
    if retain {
        assert!(matches!(result, Err(AppError::CopyError(_))));
    } else {
        assert!(matches!(result, Err(AppError::RemoteSourceCleanupUnavailable)));
    }
    let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
    assert!(!database.table_exists(&format!("base_{}", command.job_id)).await?);
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let json = client
        .query(&format!(
            "SELECT payload_json FROM notifications_{} WHERE kind=2 ORDER BY sequence DESC LIMIT 1",
            command.job_id
        ))
        .fetch_one::<String>()
        .await
        .map_err(DatabaseError::from)?;
    assert!(!json.contains("policy-secret"));
    assert!(!json.contains("policy-access"));
    assert!(!json.contains("policy auth secret"));
    let archived: FinalCallbackReport = serde_json::from_str(&json)?;
    let report: ProgressReport = serde_json::from_str(&archived.payload_json)?;
    assert!(report.is_final);
    assert_eq!(report.task_error_count, 1);
    assert!(report.diagnostics_complete);
    let identity = &archived.identity;
    assert!(
        database
            .final_report(&identity.round_id, &identity.attempt_id, identity.sequence)
            .await?
            .is_some()
    );
    eprintln!(
        "remote-source-policy cluster={} retain={retain} startup_rejected=true task_errors=1 callback_received=true final_report_archived=true base_not_created=true credentials_redacted=true",
        config.cluster.is_some()
    );
    Ok(())
}

async fn local_rejections(config: &ClickHouseConfig) -> Result<()> {
    for case in [
        "packaged",
        "filter",
        "file_list",
        "binding",
        "database",
        "disconnect",
        "stall",
    ] {
        let directory = tempfile::tempdir()?;
        let source = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let other_target = tempfile::tempdir()?;
        std::fs::write(source.path().join("payload"), b"source-retained")?;
        let mut command = job(directory.path());
        command.job_id = format!("local_preflight_{case}");
        command.src_path = source.path().to_string_lossy().into_owned();
        command.dest_path = target.path().to_string_lossy().into_owned();
        command.raw_command_line = "sync s3://policy-access:policy-secret@bucket.invalid/x target".into();
        command.progress_callback_url = Some(CallbackEndpoint::parse(
            &std::env::var("LAB_REQUEST_POLICY_CALLBACK_URL")
                .map_err(|_| AppError::ConfigError("owned callback URL required".into()))?,
        )?);
        let database = ClickHouseDatabase::connect(config, &command.job_id).await?;
        match case {
            "packaged" => command.packaged = true,
            "filter" => command.exclude = Some("invalid_field == 1".into()),
            "file_list" => {
                command.file_list = Some(directory.path().join("missing-list").to_string_lossy().into_owned());
            }
            "database" => {
                AppConfig::override_with(|global| {
                    global.database.clickhouse.mode = ClickHouseMode::Cluster;
                    global.database.clickhouse.cluster = None;
                })?;
            }
            "disconnect" | "stall" => {
                AppConfig::override_with(|global| {
                    global.database.clickhouse.dial_timeout = 1;
                    global.database.clickhouse.read_timeout = 1;
                })?;
                startup_fault(&format!("startup_{case}")).await?;
            }
            _ => {
                database.initialize().await?;
                begin_copy_catalogue(&database, &command, None).await?;
                let storage = create_storage_for_role(&command.src_path, None, false, StorageRole::Source).await?;
                database
                    .batch_insert_base_record(&[Arc::new(observe_path(storage.storage(), Path::new("payload")).await?)])
                    .await?;
                std::fs::write(target.path().join("payload"), b"confirmed-target")?;
                command.dest_path = other_target.path().to_string_lossy().into_owned();
            }
        }
        let started = Instant::now();
        let result = SyncOrchestrator::new_local(command.clone()).run().await;
        if matches!(case, "disconnect" | "stall") {
            startup_fault("normal").await?;
            assert!(
                started.elapsed() < Duration::from_secs(8),
                "{case}: {:?}",
                started.elapsed()
            );
            eprintln!(
                "local-preflight network={case} cluster={} total_including_final_notification_ms={}",
                config.cluster.is_some(),
                started.elapsed().as_millis()
            );
        }
        AppConfig::override_with(|global| global.database.clickhouse = config.clone())?;
        match case {
            "packaged" => assert!(matches!(result, Err(AppError::ConfigError(_)))),
            "filter" => assert!(matches!(result, Err(AppError::ScanFilter(_)))),
            "file_list" => {
                assert!(
                    matches!(result, Err(AppError::IoError(error)) if error.kind() == std::io::ErrorKind::NotFound)
                );
            }
            "database" => assert!(matches!(
                result,
                Err(AppError::DatabaseError(DatabaseError::UtilsError(_)))
            )),
            "disconnect" | "stall" => assert!(matches!(result, Err(AppError::DatabaseError(_))), "{result:?}"),
            _ => {
                assert!(matches!(
                    result,
                    Err(AppError::DatabaseError(DatabaseError::ConfigError(_)))
                ));
                assert_eq!(database.get_count("base").await?, 1);
                assert_eq!(database.copy_catalogue_source_reserved().await?, Some(true));
                assert_eq!(std::fs::read(target.path().join("payload"))?, b"confirmed-target");
            }
        }
        assert_eq!(std::fs::read(source.path().join("payload"))?, b"source-retained");
        assert!(!other_target.path().join("payload").exists());
        if case == "database" {
            assert!(
                !database
                    .table_exists(&format!("notifications_{}", command.job_id))
                    .await?
            );
            eprintln!(
                "local-preflight cluster={} case=invalid-database typed_failure=true callback_fallback_without_database=true source_retained=true",
                config.cluster.is_some()
            );
            continue;
        }
        let client = Client::default()
            .with_url(&config.dsn)
            .with_user("default")
            .with_database(&config.database);
        let json = client
            .query(&format!(
                "SELECT payload_json FROM notifications_{} WHERE kind=2 ORDER BY sequence DESC LIMIT 1",
                command.job_id,
            ))
            .fetch_one::<String>()
            .await
            .map_err(DatabaseError::from)?;
        assert!(!json.contains("policy-access") && !json.contains("policy-secret"));
        let archived: FinalCallbackReport = serde_json::from_str(&json)?;
        let report: ProgressReport = serde_json::from_str(&archived.payload_json)?;
        assert!(report.is_final && report.diagnostics_complete);
        assert_eq!(report.task_error_count, 1);
        eprintln!(
            "local-preflight cluster={} case={case} task_errors=1 callback_received=true final_report_archived=true source_retained=true",
            config.cluster.is_some()
        );
    }
    Ok(())
}

async fn startup_fault(mode: &str) -> Result<()> {
    let control = std::env::var("LAB_COPY_GENERATION_CONTROL")
        .map_err(|_| AppError::ConfigError("owned fault control required".into()))?;
    timeout(Duration::from_secs(1), async {
        let mut stream = TcpStream::connect(&control).await?;
        stream
            .write_all(format!("GET /{mode} HTTP/1.0\r\nHost: {control}\r\n\r\n").as_bytes())
            .await?;
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await?;
        assert!(reply.starts_with(b"HTTP/1.0 200"));
        Ok(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned fault control timed out".into()))?
}

async fn local_pipeline_reports(config: &ClickHouseConfig) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let mut command = job(directory.path());
    command.job_id = "local_pipeline".into();
    command.src_path = source.path().to_string_lossy().into_owned();
    command.dest_path = target.path().to_string_lossy().into_owned();
    command.concurrency = Some(2);
    // 不配置 callback，成功报告也必须独立归档；两轮应有各自唯一的执行身份。
    for content in [b"initial".as_slice(), b"incremental-change".as_slice()] {
        std::fs::write(source.path().join("payload"), content)?;
        SyncOrchestrator::new_local(command.clone()).run().await?;
        assert_eq!(std::fs::read(source.path().join("payload"))?, content);
        assert_eq!(std::fs::read(target.path().join("payload"))?, content);
    }
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let rows = client
        .query("SELECT payload_json FROM notifications_local_pipeline FINAL WHERE kind=2")
        .fetch_all::<String>()
        .await
        .map_err(DatabaseError::from)?;
    assert_eq!(rows.len(), 2);
    let mut identities = Vec::new();
    let mut kinds = Vec::new();
    for row in rows {
        let archived: FinalCallbackReport = serde_json::from_str(&row)?;
        let report: ProgressReport = serde_json::from_str(&archived.payload_json)?;
        assert!(report.is_final && report.diagnostics_complete);
        assert_eq!(report.task_error_count, 0);
        assert!(report.diagnostics.is_none());
        identities.push(archived.identity.attempt_id);
        match report.detail {
            ProgressDetail::Full {
                error_count,
                scanned_files,
                ..
            } => {
                assert_eq!(error_count, 0);
                assert_eq!(scanned_files, 1);
                kinds.push("full");
            }
            ProgressDetail::Incremental {
                error_count,
                changed_count,
                ..
            } => {
                assert_eq!(error_count, 0);
                assert_eq!(changed_count, 1);
                kinds.push("incremental");
            }
        }
    }
    kinds.sort_unstable();
    assert_eq!(kinds, ["full", "incremental"]);
    assert_ne!(identities[0], identities[1]);
    eprintln!(
        "local-pipeline cluster={} full=true incremental=true bytes_verified=true unique_attempts=true final_reports_without_callback=2",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair and HTTP callback from run-notification-archive-local.py --request-policy"]
async fn real_copy_requests_freeze_cleanup_and_report_preflight_rejections() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
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
            database: format!("ts_copy_request_{}", Uuid::new_v4().simple()),
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
        let result: Result<()> = async {
            frozen_catalogue(&config).await?;
            remote_rejection(&config, false).await?;
            remote_rejection(&config, true).await?;
            local_rejections(&config).await?;
            local_pipeline_reports(&config).await
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
