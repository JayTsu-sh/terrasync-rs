//! 真实目录查询与 Local 命名空间：无过滤保留源迁移确认缺失后执行 rename。
use crate::migration_fixtures::request;
use crate::target_absence::SourceAbsenceCheck;
use std::sync::Arc;
use std::time::Duration;
use task_model::migration::CopyGenerationPhase;

use clickhouse::Client;
use data_mover::model::{BackendKind, EntryKind};
use db::catalogue::ComparisonMode;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use task_model::{ChangeKind, StorageEntryMessage, TransferMode};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::SyncOrchestrator;
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::metadata::set_entry_metadata;
use crate::observed_fixtures::observation;
use crate::storage_factory::{CreateStorageOptions, create_storage};

async fn apply_case(config: &ClickHouseConfig, case: &str) -> Result<()> {
    let reserved = true;
    let directory = case == "directory";
    let changed_failure = case == "change_failed";
    let rename_failure = case == "rename_failed";
    let cancelled = case == "cancelled";
    let job = format!("rename_{case}");
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
    if rename_failure {
        std::fs::create_dir(target_root.path().join("new"))?;
        std::fs::write(target_root.path().join("new/keep"), b"unowned target")?;
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
            if changed_failure && path == "new" { 8 } else { 7 },
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
    set_entry_metadata(target.storage(), &entry("old"), None).await?;
    database.batch_insert_base_record(&[entry("old").into()]).await?;
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let database = database.new_execution_scope();
    let run = request("same-plan", reserved);
    database.begin_copy_generation(&run).await?;
    assert!(
        database
            .compare_catalogue_batch(&[entry("new").into()], ComparisonMode::AwaitCopyAcknowledgement)
            .await?
            .is_empty()
    );
    let mut events = BroadcastForwarder::new(8);
    let mut received = events.subscribe();
    let cancel = CancellationToken::new();
    if cancelled {
        cancel.cancel();
    }
    let result = SyncOrchestrator::apply_deletions_and_renames(
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
        SourceAbsenceCheck::new(Duration::from_secs(1), Duration::from_secs(3)).with_cancellation(cancel),
    )
    .await;
    assert!(
        source_root.path().join("new").exists(),
        "source purge belongs to target-ready cleanup"
    );
    assert_eq!(
        database
            .current_copy_generation()
            .await?
            .target_reconciliation_completed,
        Some(!cancelled),
    );
    if cancelled {
        assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
        assert_eq!(std::fs::read(target_root.path().join("old"))?, b"payload");
        assert!(!target_root.path().join("new").exists());
        assert!(received.try_recv().is_err());
        return Ok(());
    }
    assert_eq!(target_root.path().join("old").exists(), rename_failure);
    let path = if directory { "new/child" } else { "new" };
    if changed_failure || rename_failure {
        assert!(result.is_err());
        let Ok(event) = received.try_recv() else {
            panic!("failed rename must publish its classified outcome");
        };
        assert!(matches!(
            &event,
            StorageEntryMessage::CopyRenameFailed { from, to, change, reason }
            if from.path().as_str() == "old" && to.path().as_str() == "new"
                && *change == changed_failure.then_some(ChangeKind::DataOnly) && !reason.is_empty()
        ));
        assert!(
            received.try_recv().is_err(),
            "failed follow-up must not publish a baseline success"
        );
        database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
        database.batch_insert_incremental_record(&[event]).await?;
        let page = database
            .incremental_copy_changes(&run.execution_id, 0, 10)
            .await?
            .ok_or(DatabaseError::CopyExecutionUnavailable)?;
        assert_eq!(page.changes.len(), if changed_failure { 2 } else { 1 });
        let rename = page
            .changes
            .iter()
            .find(|row| row.kind == "rename")
            .ok_or(DatabaseError::CopyExecutionUnavailable)?;
        assert_eq!(rename.previous_path.as_deref(), Some("old"));
        assert_eq!(rename.path, "new");
        assert_eq!(
            rename.target_outcome.as_deref(),
            Some(if changed_failure {
                "moved_update_failed"
            } else {
                "failed"
            })
        );
        assert!(rename.reason.as_ref().is_some_and(|reason| !reason.is_empty()));
        assert!(page.counts.iter().all(|row| row.count == 1));
        if changed_failure {
            assert!(
                page.changes
                    .iter()
                    .any(|row| row.kind == "data_changed" && row.target_outcome.as_deref() == Some("failed"))
            );
        }
    } else {
        result?;
        assert_eq!(std::fs::read(target_root.path().join(path))?, b"payload");
        assert!(
            matches!(received.try_recv(), Ok(StorageEntryMessage::Renamed((from, to)))
            if from.path().as_str()=="old" && to.path().as_str()=="new")
        );
    }
    eprintln!(
        "rename-apply cluster={} source_reserved={reserved} case={case} source_not_purged_early=true",
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
            for case in ["file", "directory", "change_failed", "rename_failed", "cancelled"] {
                apply_case(&config, case).await?;
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
