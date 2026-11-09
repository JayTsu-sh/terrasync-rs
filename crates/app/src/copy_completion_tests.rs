use crate::migration_fixtures::request;
use crate::scan::observe_path;
use std::future::pending;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{ConsoleConfig, ConsumerConfig, JobType};
use crate::consumer::{
    ConsumerManager,
    stats::{ProgressDetail, callback_state::StatisticsCheckpoint},
};
use clickhouse::Client;
use data_mover::model::ObservedEntry;
use db::catalogue::ClickHouseDatabase;
use db::{ClickHouseConfig, CopyGenerationStatus, DatabaseError};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedItem;
use tempfile::TempDir;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Duration, timeout};
use utils::app_config::{ClickHouseMode, DatabaseConfig};

use super::*;
use crate::storage_factory::{StorageRole, create_storage_for_role};

fn catalogue(entry: ObservedEntry) -> CopyCatalogueEntry {
    CopyCatalogueEntry {
        completion: ObservedItem::from(Arc::new(entry)).into(),
        history_segment: 0,
        history_index: 0,
        history_records: 0,
        history_complete: true,
    }
}

async fn target(root: &TempDir) -> Result<Arc<StorageContext>> {
    Ok(Arc::new(
        create_storage_for_role(&root.path().to_string_lossy(), None, false, StorageRole::Destination).await?,
    ))
}

async fn directory_entry(source: &TempDir, name: &str) -> Result<ObservedEntry> {
    std::fs::create_dir_all(source.path().join(name))?;
    let storage = create_storage_for_role(&source.path().to_string_lossy(), None, false, StorageRole::Source).await?;
    let mut entry = observe_path(storage.storage(), Path::new(name)).await?;
    entry = crate::observed_fixtures::with_times(entry, None, Some(1_234_567_890_000_000_000));
    Ok(entry)
}

fn verify_mtime(target: &TempDir, name: &str, expected: i64) -> Result<()> {
    let actual = std::fs::metadata(target.path().join(name))?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_nanos();
    if actual != expected as u128 {
        return Err(AppError::CopyError(format!("directory mtime {actual} != {expected}")));
    }
    Ok(())
}

#[tokio::test]
async fn directory_query_failure_stays_typed_and_drains_updates() -> Result<()> {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let entry = directory_entry(&source, "present").await?;
    let expected = entry.get_mtime();
    std::fs::create_dir(destination.path().join("present"))?;
    let (sender, receiver) = mpsc::channel(2);
    let query = tokio::spawn(async move {
        sender
            .send(catalogue(entry))
            .await
            .map_err(|_| AppError::ScanWorkerStopped)?;
        Err(DatabaseError::ConfigError("directory query regression fixture".into()).into())
    });
    let result = apply_directories(query, receiver, target(&destination).await?, None).await;
    assert!(matches!(
        result,
        Err(AppError::DatabaseError(DatabaseError::ConfigError(_)))
    ));
    verify_mtime(&destination, "present", expected)
}

#[tokio::test]
async fn directory_write_failure_stays_typed_and_drains_later_updates() -> Result<()> {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let missing = directory_entry(&source, "missing").await?;
    let present = directory_entry(&source, "present").await?;
    let expected = present.get_mtime();
    std::fs::create_dir(destination.path().join("present"))?;
    let (sender, receiver) = mpsc::channel(2);
    let query = tokio::spawn(async move {
        sender
            .send(catalogue(missing))
            .await
            .map_err(|_| AppError::ScanWorkerStopped)?;
        sender
            .send(catalogue(present))
            .await
            .map_err(|_| AppError::ScanWorkerStopped)?;
        Ok(())
    });
    let result = apply_directories(query, receiver, target(&destination).await?, None).await;
    assert!(matches!(result, Err(AppError::StorageError(_))));
    verify_mtime(&destination, "present", expected)
}

#[tokio::test]
async fn directory_query_panic_is_observed() -> Result<()> {
    let destination = TempDir::new()?;
    let (sender, receiver) = mpsc::channel(1);
    let query = tokio::spawn(async move {
        let _sender = sender;
        panic!("directory query regression fixture");
    });
    let result = apply_directories(query, receiver, target(&destination).await?, None).await;
    assert!(matches!(result, Err(AppError::TaskJoinError(error)) if error.is_panic()));
    Ok(())
}

#[tokio::test]
async fn cancelled_metadata_stage_does_not_detach_query() -> Result<()> {
    struct QueryDrop(Arc<Notify>);
    impl Drop for QueryDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let destination = TempDir::new()?;
    let (sender, receiver) = mpsc::channel(1);
    let (started, ready) = oneshot::channel();
    let dropped = Arc::new(Notify::new());
    let signal = dropped.clone();
    let query = tokio::spawn(async move {
        let _guard = QueryDrop(signal);
        let _ = started.send(());
        pending::<()>().await;
        drop(sender);
        Ok(())
    });
    let task = tokio::spawn(apply_directories(query, receiver, target(&destination).await?, None));
    ready.await.map_err(|_| AppError::ScanWorkerStopped)?;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(timeout(Duration::from_secs(1), dropped.notified()).await.is_ok());
    Ok(())
}

async fn isolated_generation_case(mode: ClickHouseMode) -> Result<()> {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let entry = directory_entry(&source, "missing").await?;
    let name = format!(
        "ts_copy_completion_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let cluster = if mode == ClickHouseMode::Cluster {
        Some(std::env::var("LAB_CLICKHOUSE_CLUSTER").map_err(|_| {
            AppError::ConfigError("LAB_CLICKHOUSE_CLUSTER must explicitly select a test cluster".into())
        })?)
    } else {
        None
    };
    let config = ClickHouseConfig {
        mode,
        cluster: cluster.clone(),
        dsn: std::env::var("LAB_CLICKHOUSE_DSN")
            .map_err(|_| AppError::ConfigError("LAB_CLICKHOUSE_DSN must explicitly select a test server".into()))?,
        dial_timeout: 10,
        read_timeout: 60,
        database: name.clone(),
        username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok().filter(|s| !s.is_empty()),
    };
    config.cluster_name()?;
    let mut admin = Client::default().with_url(&config.dsn).with_user(&config.username);
    if let Some(password) = &config.password {
        admin = admin.with_password(password);
    }
    if let Some(cluster) = &cluster {
        let members = admin
            .query("SELECT count() FROM system.clusters WHERE cluster=?")
            .bind(cluster)
            .fetch_one::<u64>()
            .await
            .map_err(DatabaseError::from)?;
        if members != 6 {
            return Err(AppError::ConfigError(format!(
                "integration fixture requires six replicas, got {members}"
            )));
        }
    }
    let on_cluster = cluster
        .as_ref()
        .map_or(String::new(), |name| format!(" ON CLUSTER {name}"));
    admin
        .query(&format!("CREATE DATABASE {name}{on_cluster}"))
        .execute()
        .await
        .map_err(DatabaseError::from)?;
    let outcome = async {
        let database: ClickHouseDatabase = ClickHouseDatabase::new(&config, "completion")?;
        database.initialize().await?;
        database
            .begin_copy_generation(&request("directory-completion-regression", true))
            .await?;
        database.batch_insert_base_record(&[Arc::new(entry.clone())]).await?;
        let result = complete(database.clone(), target(&destination).await?, None).await;
        if !matches!(result, Err(AppError::StorageError(_))) {
            return Err(AppError::CopyError(format!(
                "metadata failure was not reported: {result:?}"
            )));
        }
        if database
            .begin_copy_generation(&request("directory-completion-regression", true))
            .await?
            != CopyGenerationStatus::Recovering
        {
            return Err(AppError::CopyError(
                "directory metadata failure incorrectly committed generation".into(),
            ));
        }
        std::fs::create_dir(destination.path().join("missing"))?;
        database.batch_insert_base_record(&[Arc::new(entry.clone())]).await?;
        complete(database.clone(), target(&destination).await?, None).await?;
        verify_mtime(&destination, "missing", entry.get_mtime())?;
        if database
            .begin_copy_generation(&request("directory-completion-regression", true))
            .await?
            != CopyGenerationStatus::Fresh
        {
            return Err(AppError::CopyError(
                "successful metadata completion left generation dirty".into(),
            ));
        }
        Ok(())
    }
    .await;
    let cleanup = admin
        .query(&format!("DROP DATABASE IF EXISTS {name}{on_cluster} SYNC"))
        .execute()
        .await
        .map_err(DatabaseError::from);
    outcome?;
    cleanup?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires real single ClickHouse; isolated database and local directories only"]
async fn single_metadata_failure_keeps_generation_dirty_until_retry_succeeds() -> Result<()> {
    isolated_generation_case(ClickHouseMode::Single).await
}

#[tokio::test]
#[ignore = "requires explicitly configured real ClickHouse cluster; isolated database and local directories only"]
async fn cluster_metadata_failure_keeps_generation_dirty_until_retry_succeeds() -> Result<()> {
    isolated_generation_case(ClickHouseMode::Cluster).await
}

fn statistics_manager(job_type: JobType) -> Result<ConsumerManager> {
    ConsumerManager::statistics_only(&ConsumerConfig {
        job_id: "directory-errors".into(),
        job_type,
        job_dir: String::new(),
        db_config: DatabaseConfig {
            enabled: false,
            batch_size: 256,
            clickhouse: ClickHouseConfig {
                mode: ClickHouseMode::Single,
                cluster: None,
                dsn: "http://127.0.0.1:1".into(),
                dial_timeout: 1,
                read_timeout: 1,
                database: "default".into(),
                username: "default".into(),
                password: None,
            },
        },
        console_config: ConsoleConfig {
            raw_command_line: String::new(),
        },
        progress_callback_url: None,
    })
}

#[tokio::test]
async fn every_failed_directory_is_reported_once_and_later_directories_finish() -> Result<()> {
    for job_type in [JobType::Copy, JobType::IncrementalCopy] {
        let source = TempDir::new()?;
        let destination = TempDir::new()?;
        let manager = statistics_manager(job_type)?;
        let first = directory_entry(&source, "missing-first").await?;
        let second = directory_entry(&source, "missing-second").await?;
        let present = directory_entry(&source, "present").await?;
        let expected = present.get_mtime();
        std::fs::create_dir(destination.path().join("present"))?;
        let (sender, receiver) = mpsc::channel(3);
        let query = tokio::spawn(async move {
            for entry in [first, second, present] {
                sender
                    .send(catalogue(entry))
                    .await
                    .map_err(|_| AppError::ScanWorkerStopped)?;
            }
            Ok(())
        });
        let result = apply_directories(
            query,
            receiver,
            target(&destination).await?,
            Some(manager.task_statistics()),
        )
        .await;
        let error = result
            .err()
            .ok_or_else(|| AppError::ConfigError("missing metadata failure".into()))?;
        manager.record_task_error(&error).await;
        let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
        let events = saved.callback.outbox.archive_prefix();
        assert_eq!(events.len(), 2, "each failed directory needs its own diagnostic");
        for name in ["missing-first", "missing-second"] {
            assert!(
                events.iter().any(|event| event.path.as_deref() == Some(name)
                    && event.message.contains("directory_metadata_finalize")
                    && event.message.contains("backend=Local")
                    && event.code == "Copy"),
                "missing path/phase/backend: {events:?}"
            );
        }
        assert!(matches!(
            saved.report.detail,
            ProgressDetail::Full { error_count: 2, .. } | ProgressDetail::Incremental { error_count: 2, .. }
        ));
        assert_eq!(
            saved.report.task_error_count, 0,
            "already reported directory failures must not be counted again"
        );
        assert!(
            matches!(error, AppError::ReportedCopyFailure { source, data_failed: false }
            if matches!(*source, AppError::StorageError(_)))
        );
        verify_mtime(&destination, "present", expected)?;
    }
    Ok(())
}

#[tokio::test]
async fn directory_worker_panic_retains_path_and_typed_failure() -> Result<()> {
    let source = TempDir::new()?;
    let manager = statistics_manager(JobType::Copy)?;
    let entry = Arc::new(directory_entry(&source, "panicked-directory").await?);
    let destination = TempDir::new()?;
    let mut updates = DirectoryUpdates::new(target(&destination).await?, Some(manager.task_statistics()));
    let task = updates.tasks.spawn(async { panic!("owned directory worker panic") });
    updates.paths.insert(task.id(), entry);
    let result = updates
        .tasks
        .join_next_with_id()
        .await
        .ok_or(AppError::ScanWorkerStopped)?;
    updates.record_update(result).await;
    assert!(updates.paths.is_empty());
    assert!(matches!(updates.failure, Some(AppError::TaskJoinError(error)) if error.is_panic()));
    let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    let events = saved.callback.outbox.archive_prefix();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].path.as_deref(), Some("panicked-directory"));
    assert!(events[0].message.contains("owned directory worker panic"));
    Ok(())
}

#[tokio::test]
async fn query_failure_does_not_hide_already_failed_directory() -> Result<()> {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let manager = statistics_manager(JobType::Copy)?;
    let entry = directory_entry(&source, "missing-directory").await?;
    let (sender, receiver) = mpsc::channel(1);
    let query = tokio::spawn(async move {
        sender
            .send(catalogue(entry))
            .await
            .map_err(|_| AppError::ScanWorkerStopped)?;
        Err(DatabaseError::ConfigError("owned directory query failure".into()).into())
    });
    let result = apply_directories(
        query,
        receiver,
        target(&destination).await?,
        Some(manager.task_statistics()),
    )
    .await;
    let error = result.err().ok_or(AppError::ScanWorkerStopped)?;
    assert!(matches!(error, AppError::DatabaseError(DatabaseError::ConfigError(_))));
    manager.record_task_error(&error).await;
    let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    let events = saved.callback.outbox.archive_prefix();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .any(|event| event.path.as_deref() == Some("missing-directory"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.message.contains("owned directory query failure"))
    );
    assert_eq!(saved.report.task_error_count, 1);
    assert!(matches!(
        saved.report.detail,
        ProgressDetail::Full { error_count: 1, .. }
    ));
    Ok(())
}
