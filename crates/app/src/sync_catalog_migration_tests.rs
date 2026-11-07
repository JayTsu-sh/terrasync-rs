//! 保留源端后切换搬迁：基线无变化不传数据，仍清理对应源项。

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use clickhouse::Client;
use db::catalogue::ComparisonMode;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::TransferMode;
use tempfile::tempdir;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::determine_sync_type;
use crate::copy_completion;
use crate::error::{AppError, Result};
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::{ScanType, observe_path};
use crate::source_cleanup::remove_confirmed_sources;
use crate::storage_factory::{CreateStorageOptions, create_storage};

async fn unchanged_copy_is_deleted(config: &ClickHouseConfig) -> Result<()> {
    let source_root = tempdir()?;
    let target_root = tempdir()?;
    std::fs::write(source_root.path().join("payload"), b"already copied payload")?;
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let entry = Arc::new(observe_path(source.storage(), Path::new("payload")).await?);
    let database = ClickHouseDatabase::connect(config, "unchanged_move").await?;
    database.initialize().await?;
    database.begin_copy_generation("same-source-target", true).await?;
    copy_file(
        &source,
        &target,
        &entry,
        CopyOptions {
            transfer_mode: TransferMode::Direct,
            ..Default::default()
        },
        None,
    )
    .await?;
    database.batch_insert_base_record(&[Arc::clone(&entry)]).await?;
    database.commit_scan_generation().await?;
    drop(database);
    let metadata = std::fs::metadata(target_root.path().join("payload"))?;
    let database = ClickHouseDatabase::connect(config, "unchanged_move").await?;
    assert_eq!(determine_sync_type(&database, false).await?, ScanType::Incremental);
    database.begin_copy_generation("same-source-target", false).await?;
    let current = Arc::new(observe_path(source.storage(), Path::new("payload")).await?);
    let changes = database
        .compare_catalogue_batch(&[current], ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert!(changes.is_empty(), "unchanged entries must produce no copy work");
    copy_completion::complete(database.clone(), Arc::clone(&target)).await?;
    remove_confirmed_sources(database, source, target).await?;
    assert!(!source_root.path().join("payload").exists());
    assert_eq!(
        std::fs::read(target_root.path().join("payload"))?,
        b"already copied payload"
    );
    let after = std::fs::metadata(target_root.path().join("payload"))?;
    assert_eq!(metadata.ino(), after.ino());
    assert_eq!(
        (metadata.ctime(), metadata.ctime_nsec()),
        (after.ctime(), after.ctime_nsec())
    );
    eprintln!(
        "unchanged-move cluster={} classification=unchanged copy_work=0 source_deleted=true target_inode_and_ctime_unchanged=true",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair from run-notification-archive-local.py --copy-generations"]
async fn real_retained_copy_becomes_incremental_move_without_rewriting_the_target() -> Result<()> {
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
            database: format!("ts_unchanged_move_{}", Uuid::new_v4().simple()),
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
        let result = unchanged_copy_is_deleted(&config).await;
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
