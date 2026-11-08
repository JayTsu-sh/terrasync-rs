//! 真实 Single／Cluster：保留源的全量之后，增量搬迁跳过的数据仍受全量校验约束。
use std::{
    env,
    fs::{File, FileTimes, Permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use clickhouse::Client;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{
    SourceCleanupPolicy,
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
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    let raw = client.query(&format!("SELECT payload_json FROM notifications_{} FINAL WHERE kind=2 AND attempt_id=? ORDER BY sequence DESC LIMIT 1",command.job_id))
        .bind(&generation.request.execution_id).fetch_one::<String>().await.map_err(DatabaseError::from)?;
    let saved: FinalCallbackReport = serde_json::from_str(&raw)?;
    let report: ProgressReport = serde_json::from_str(&saved.payload_json)?;
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
            for name in ["unchanged", "corrupt", "metadata"] {
                case(&config, if cluster { "cluster" } else { "single" }, name).await?;
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
