//! 任务接受时的策略同时控制代次记录与远程准入；全局配置不得反向改变它。
use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::SourceCleanupPolicy;
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::{begin_copy_catalogue, tests::job};
use crate::callback::CallbackEndpoint;
use crate::consumer::stats::ProgressReport;
use crate::consumer::stats::delivery::FinalCallbackReport;
use crate::error::{AppError, Result};
use crate::orchestrator::SyncOrchestrator;

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
            remote_rejection(&config, true).await
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
