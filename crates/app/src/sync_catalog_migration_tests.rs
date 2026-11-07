//! 源保留策略切换：未变化项可搬迁，已搬走的项不能反向删除目标。
use crate::migration_fixtures::request;
use task_model::migration::CopyGenerationPhase;

use std::fs::{File, FileTimes};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use clickhouse::Client;
use db::catalogue::ComparisonMode;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError};
use task_model::TransferMode;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::determine_sync_type;
use crate::copy_completion;
use crate::error::{AppError, Result};
use crate::file_copy::metadata::set_entry_metadata;
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::{ScanType, observe_path};
use crate::source_cleanup::{SourceCleanupCheck, remove_confirmed_sources};
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
    database
        .begin_copy_generation(&request("same-source-target", true))
        .await?;
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
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    drop(database);
    let metadata = std::fs::metadata(target_root.path().join("payload"))?;
    let database = ClickHouseDatabase::connect(config, "unchanged_move").await?;
    assert_eq!(determine_sync_type(&database).await?, ScanType::Incremental);
    database
        .begin_copy_generation(&request("same-source-target", false))
        .await?;
    let current = Arc::new(observe_path(source.storage(), Path::new("payload")).await?);
    let changes = database
        .compare_catalogue_batch(&[current], ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert!(changes.is_empty(), "unchanged entries must produce no copy work");
    copy_completion::complete(database.clone(), Arc::clone(&target)).await?;
    remove_confirmed_sources(
        database,
        source,
        target,
        SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new()),
    )
    .await?;
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

async fn moved_copy_is_retained(config: &ClickHouseConfig, incomplete: bool) -> Result<()> {
    let source_root = tempdir()?;
    let target_root = tempdir()?;
    std::fs::write(source_root.path().join("payload"), b"moved payload")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("payload")).await?);
    let job = format!("moved_then_retained_{incomplete}");
    let database = ClickHouseDatabase::connect(config, &job).await?;
    database.initialize().await?;
    database
        .begin_copy_generation(&request("same-source-target", false))
        .await?;
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
    source
        .storage()
        .namespace()?
        .purge_observed(&entry, CancellationToken::new())
        .await?;
    if !incomplete {
        database.commit_scan_generation().await?;
        database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    }
    drop(database);
    let before = std::fs::metadata(target_root.path().join("payload"))?;
    let database = ClickHouseDatabase::connect(config, &job).await?;
    assert_eq!(determine_sync_type(&database).await?, ScanType::Full);
    database
        .begin_copy_generation(&request("same-source-target", true))
        .await?;
    assert_eq!(database.get_count("base").await?, 0);
    assert_eq!(database.detect_deleted_items().await?.count(), 0);
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    assert!(!source_root.path().join("payload").exists());
    assert_eq!(std::fs::read(target_root.path().join("payload"))?, b"moved payload");
    let after = std::fs::metadata(target_root.path().join("payload"))?;
    assert_eq!(
        (before.ino(), before.ctime(), before.ctime_nsec()),
        (after.ino(), after.ctime(), after.ctime_nsec())
    );
    eprintln!(
        "moved-then-retained cluster={} incomplete={incomplete} next_full=true reverse_deletions=0 target_inode_and_ctime_unchanged=true",
        config.cluster.is_some()
    );
    Ok(())
}

async fn only_current_selection_authorizes_source_cleanup(config: &ClickHouseConfig) -> Result<()> {
    let source_root = tempdir()?;
    let target_root = tempdir()?;
    for name in ["selected", "outside"] {
        std::fs::write(source_root.path().join(name), name.as_bytes())?;
    }
    std::fs::create_dir(source_root.path().join("outside-dir"))?;
    std::fs::create_dir(target_root.path().join("outside-dir"))?;
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let database = ClickHouseDatabase::connect(config, "selected_cleanup").await?;
    database.initialize().await?;
    database
        .begin_copy_generation(&request("same-source-target", true))
        .await?;
    let mut entries = Vec::new();
    for name in ["selected", "outside", "outside-dir"] {
        let entry = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
        if name == "outside-dir" {
            set_entry_metadata(target.storage(), &entry).await?;
        } else {
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
        }
        entries.push(entry);
    }
    database.batch_insert_base_record(&entries).await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    drop(database);
    let client = Client::default()
        .with_url(&config.dsn)
        .with_user("default")
        .with_database(&config.database);
    // 三轮已经更新 state 但未提交，再成功提交一轮；每次重新打开执行域。
    for round in 0..4 {
        let database = ClickHouseDatabase::connect(config, "selected_cleanup").await?;
        database
            .begin_copy_generation(&request("same-source-target", true))
            .await?;
        let changes = database
            .compare_catalogue_batch(&entries[..1], ComparisonMode::AwaitCopyAcknowledgement)
            .await?;
        assert!(changes.is_empty(), "selected unchanged entry must not retransmit");
        let state = client
            .query("SELECT current_state FROM base_selected_cleanup FINAL WHERE relative_path='selected'")
            .fetch_one::<u8>()
            .await
            .map_err(DatabaseError::from)?;
        assert_eq!(
            state, 0,
            "real database state must already have changed before the uncommitted exit"
        );
        if round == 3 {
            database.commit_scan_generation().await?;
            database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
        }
    }
    // 下一轮 W=1，准备前未选中的旧记录也恰为 1；不能靠这个相等关系授权删除。
    let state = client
        .query("SELECT current_state FROM base_selected_cleanup FINAL WHERE relative_path='outside'")
        .fetch_one::<u8>()
        .await
        .map_err(DatabaseError::from)?;
    assert_eq!(state, 1);
    let database = ClickHouseDatabase::connect(config, "selected_cleanup").await?;
    database
        .begin_copy_generation(&request("same-source-target", false))
        .await?;
    let changes = database
        .compare_catalogue_batch(&entries[..1], ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert!(changes.is_empty());
    let state = client
        .query("SELECT current_state FROM base_selected_cleanup FINAL WHERE relative_path='outside'")
        .fetch_one::<u8>()
        .await
        .map_err(DatabaseError::from)?;
    assert_eq!(state, 0, "新代次统一回标旧事实，执行身份仍负责限定本轮清理范围");
    let untouched_time = UNIX_EPOCH + Duration::from_secs(1_234_567);
    File::open(target_root.path().join("outside-dir"))?.set_times(FileTimes::new().set_modified(untouched_time))?;
    copy_completion::complete(database.clone(), Arc::clone(&target)).await?;
    remove_confirmed_sources(
        database,
        source,
        target,
        SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new()),
    )
    .await?;
    assert!(!source_root.path().join("selected").exists());
    assert_eq!(std::fs::read(source_root.path().join("outside"))?, b"outside");
    assert!(source_root.path().join("outside-dir").is_dir());
    assert_eq!(
        std::fs::metadata(target_root.path().join("outside-dir"))?.modified()?,
        untouched_time
    );
    for name in ["selected", "outside"] {
        assert_eq!(std::fs::read(target_root.path().join(name))?, name.as_bytes());
    }
    eprintln!(
        "selected-cleanup cluster={} uncommitted_rounds=3 old_state_collides_with_working=true unchanged_copy_work=0 only_selected_source_deleted=true old_source_and_directory_preserved=true unselected_target_mtime_untouched=true",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair from run-notification-archive-local.py --copy-generations"]
async fn real_copy_source_policy_transitions_preserve_target_data() -> Result<()> {
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
        let result: Result<()> = async {
            unchanged_copy_is_deleted(&config).await?;
            only_current_selection_authorizes_source_cleanup(&config).await?;
            for incomplete in [false, true] {
                moved_copy_is_retained(&config, incomplete).await?;
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
