//! 真实目录查询与 Local 命名空间：无过滤保留源迁移确认缺失后执行 rename。
use crate::migration_fixtures::request;
use crate::target_absence::SourceAbsenceCheck;
use std::sync::Arc;
use std::time::Duration;
use task_model::migration::CopyGenerationPhase;

use clickhouse::Client;
use data_mover::model::{BackendKind, EntryKind};
use db::catalogue::ComparisonMode;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::{StorageEntryMessage, TransferMode};
use tempfile::tempdir;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::SyncOrchestrator;
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result};
use crate::file_copy::metadata::set_entry_metadata;
use crate::observed_fixtures::observation;
use crate::storage_factory::{CreateStorageOptions, create_storage};

async fn apply_case(config: &ClickHouseConfig, directory: bool) -> Result<()> {
    let reserved = true;
    let job = format!("rename_{reserved}_{directory}");
    let database = ClickHouseDatabase::connect(config, &job).await?;
    database.initialize().await?;
    database.begin_copy_generation(&request("same-plan", true)).await?;
    let source_root = tempdir()?;
    let target_root = tempdir()?;
    if directory {
        std::fs::create_dir(source_root.path().join("new"))?;
        std::fs::create_dir(target_root.path().join("old"))?;
        std::fs::write(source_root.path().join("new/child"), b"payload")?;
        std::fs::write(target_root.path().join("old/child"), b"payload")?;
    } else {
        std::fs::write(source_root.path().join("new"), b"payload")?;
        std::fs::write(target_root.path().join("old"), b"payload")?;
    }
    let kind = if directory {
        EntryKind::Directory
    } else {
        EntryKind::File
    };
    let entry = |path: &str| {
        observation(
            BackendKind::Nfs,
            path.into(),
            kind,
            7,
            [0; 3],
            None,
            Some(b"stable-source".to_vec()),
            None,
            None,
        )
    };
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    set_entry_metadata(target.storage(), &entry("old")).await?;
    database.batch_insert_base_record(&[entry("old").into()]).await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&request("same-plan", reserved)).await?;
    assert!(
        database
            .compare_catalogue_batch(&[entry("new").into()], ComparisonMode::AwaitCopyAcknowledgement)
            .await?
            .is_empty()
    );
    let mut events = BroadcastForwarder::new(8);
    let mut received = events.subscribe();
    SyncOrchestrator::apply_deletions_and_renames(
        &database,
        source,
        target,
        reserved,
        &events,
        false,
        None,
        None,
        &job,
        "",
        TransferMode::Direct,
        SourceAbsenceCheck::new(Duration::from_secs(1), Duration::from_secs(3)),
    )
    .await?;
    assert!(
        source_root.path().join("new").exists(),
        "source purge belongs to target-ready cleanup"
    );
    assert!(!target_root.path().join("old").exists());
    let path = if directory { "new/child" } else { "new" };
    assert_eq!(std::fs::read(target_root.path().join(path))?, b"payload");
    assert!(
        matches!(received.try_recv(), Ok(StorageEntryMessage::Renamed((from, to)))
        if from.path().as_str()=="old" && to.path().as_str()=="new")
    );
    eprintln!(
        "rename-apply cluster={} source_reserved={reserved} directory={directory} target_moved=true source_not_purged_early=true",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair from run-notification-archive-local.py --rename-generations"]
async fn real_retained_incremental_applies_confirmed_rename() -> Result<()> {
    let dsn = std::env::var("LAB_COPY_GENERATION_DSN")
        .map_err(|_| AppError::ConfigError("LAB_COPY_GENERATION_DSN must select an isolated server".into()))?;
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
            database: format!("ts_rename_apply_{}", Uuid::new_v4().simple()),
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
        let result: Result<()> = async {
            for directory in [false, true] {
                apply_case(&config, directory).await?;
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
