use super::*;
use crate::storage_factory::{StorageRole, create_storage_for_role};
use clickhouse::Client;
use db::catalogue::ClickHouseDatabase;
use db::{ClickHouseConfig, CopyGenerationStatus, DatabaseError};
use std::future::pending;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Duration, timeout};
use utils::app_config::ClickHouseMode;

async fn target(root: &TempDir) -> Result<Arc<StorageEnum>> {
    Ok(Arc::new(
        create_storage_for_role(&root.path().to_string_lossy(), None, false, StorageRole::Destination).await?,
    ))
}

async fn directory_entry(source: &TempDir, name: &str) -> Result<EntryEnum> {
    std::fs::create_dir_all(source.path().join(name))?;
    let storage = create_storage_for_role(&source.path().to_string_lossy(), None, false, StorageRole::Source).await?;
    let mut entry = storage.get_metadata(Path::new(name)).await?;
    if let EntryEnum::NAS(directory) = &mut entry {
        directory.mtime = 1_234_567_890_000_000_000;
    }
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
        sender.send(entry).await.map_err(|_| AppError::ScanWorkerStopped)?;
        Err(DatabaseError::ConfigError("directory query regression fixture".into()).into())
    });
    let result = apply_directories(query, receiver, target(&destination).await?).await;
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
        sender.send(missing).await.map_err(|_| AppError::ScanWorkerStopped)?;
        sender.send(present).await.map_err(|_| AppError::ScanWorkerStopped)?;
        Ok(())
    });
    let result = apply_directories(query, receiver, target(&destination).await?).await;
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
    let result = apply_directories(query, receiver, target(&destination).await?).await;
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
    let task = tokio::spawn(apply_directories(query, receiver, target(&destination).await?));
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
        let database: Box<dyn Database> = Box::new(ClickHouseDatabase::new(&config, "completion")?);
        database.initialize().await?;
        database
            .begin_copy_generation("directory-completion-regression")
            .await?;
        database.batch_insert_base_record(&[Arc::new(entry.clone())]).await?;
        let result = complete(database.clone(), target(&destination).await?).await;
        if !matches!(result, Err(AppError::StorageError(_))) {
            return Err(AppError::CopyError(format!(
                "metadata failure was not reported: {result:?}"
            )));
        }
        if database
            .begin_copy_generation("directory-completion-regression")
            .await?
            != CopyGenerationStatus::Recovering
        {
            return Err(AppError::CopyError(
                "directory metadata failure incorrectly committed generation".into(),
            ));
        }
        std::fs::create_dir(destination.path().join("missing"))?;
        database.batch_insert_base_record(&[Arc::new(entry.clone())]).await?;
        complete(database.clone(), target(&destination).await?).await?;
        verify_mtime(&destination, "missing", entry.get_mtime())?;
        if database
            .begin_copy_generation("directory-completion-regression")
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
