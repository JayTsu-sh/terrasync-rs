//! 目录收尾共享执行取消：未启动项不写入、不伪造失败，在途项保留路径诊断。
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use data_mover::model::{
    EntryOperationFailure, FailureClass, MetadataCapabilities, MetadataObservations, ObservationPlan, Operation,
    StoragePath, Transience,
};
use data_mover::storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure};
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn execution(cancel: CancellationToken) -> Result<(ClickHouseDatabase, CopyDatabaseExecution)> {
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: "http://127.0.0.1:9".into(),
        database: "directory_cancel_fixture".into(),
        username: "fixture".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    let mut database = ClickHouseDatabase::new(&config, "directory_cancel")?;
    let execution = CopyDatabaseExecution::prepare(&mut database, cancel)?;
    Ok((database, execution))
}

#[tokio::test]
async fn precancelled_completion_never_queries_or_commits_generation() -> TestResult {
    let destination = TempDir::new()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (database, execution) = execution(cancel)?;
    let result = timeout(
        Duration::from_millis(300),
        complete(database, target(&destination).await?, None, Some(execution)),
    )
    .await?;
    assert!(
        matches!(result, Err(AppError::DatabaseError(DatabaseError::ScanCancelled))),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn precancelled_directory_queue_never_mutates_or_counts_unstarted_entries() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let manager = statistics_manager(JobType::Copy)?;
    let entry = directory_entry(&source, "queued").await?;
    std::fs::create_dir(destination.path().join("queued"))?;
    let before = std::fs::metadata(destination.path().join("queued"))?.modified()?;
    let (sender, receiver) = mpsc::channel(128);
    for _ in 0..128 {
        sender.send(catalogue(entry.clone())).await?;
    }
    drop(sender);
    let query = tokio::spawn(async { Ok(()) });
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (_, execution) = execution(cancel)?;
    let result = apply_directories(
        query,
        receiver,
        target(&destination).await?,
        Some(manager.task_statistics()),
        Some(execution),
    )
    .await;
    assert!(
        matches!(result, Err(AppError::DatabaseError(DatabaseError::ScanCancelled))),
        "{result:?}"
    );
    assert_eq!(
        std::fs::metadata(destination.path().join("queued"))?.modified()?,
        before
    );
    let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    assert!(saved.callback.outbox.archive_prefix().is_empty());
    Ok(())
}

#[tokio::test]
async fn directory_cancellation_aborts_idle_query_without_dropping_its_owner() -> TestResult {
    struct QueryDrop(Arc<Notify>);
    impl Drop for QueryDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let destination = TempDir::new()?;
    let dropped = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let query_entered = entered.clone();
    let query_drop = dropped.clone();
    let (sender, receiver) = mpsc::channel(1);
    let query = tokio::spawn(async move {
        let _guard = QueryDrop(query_drop);
        query_entered.notify_one();
        pending::<()>().await;
        drop(sender);
        Ok(())
    });
    let cancel = CancellationToken::new();
    let (_, execution) = execution(cancel.clone())?;
    let operation = apply_directories(query, receiver, target(&destination).await?, None, Some(execution));
    tokio::pin!(operation);
    tokio::select! {
        result = &mut operation => panic!("目录查询未等待取消: {result:?}"),
        () = entered.notified() => {}
    }
    cancel.cancel();
    let result = timeout(Duration::from_millis(300), operation).await?;
    assert!(
        matches!(result, Err(AppError::DatabaseError(DatabaseError::ScanCancelled))),
        "{result:?}"
    );
    timeout(Duration::from_millis(300), dropped.notified()).await?;
    Ok(())
}

struct PendingDirectoryMetadata {
    inner: Arc<dyn Metadata>,
    entered: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Metadata for PendingDirectoryMetadata {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        self.inner.observe(path, plan).await
    }
    async fn apply(
        &self, path: &StoragePath, _: MetadataMutation, cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == DIRECTORY_CONCURRENCY {
            self.entered.notify_one();
        }
        cancel.cancelled().await;
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Metadata,
                FailureClass::Cancelled,
                Transience::Unknown,
                "owned in-flight metadata write is unconfirmed",
            )
            .unwrap_or_else(|error| panic!("{error}")),
        ))
    }
}

#[tokio::test]
async fn directory_cancel_drains_started_writes_without_admitting_the_next_entry() -> TestResult {
    let source = TempDir::new()?;
    let destination = TempDir::new()?;
    let manager = statistics_manager(JobType::Copy)?;
    let entered = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut target = Arc::try_unwrap(target(&destination).await?).map_err(|_| "target shared")?;
    let original = target.storage();
    target.roles = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .namespace(original.namespace()?)
        .staged_destination(original.staged_destination()?)
        .metadata(Arc::new(PendingDirectoryMetadata {
            inner: original.metadata()?,
            entered: entered.clone(),
            calls: calls.clone(),
        }))
        .build()?;
    let (sender, receiver) = mpsc::channel(DIRECTORY_CONCURRENCY + 1);
    for index in 0..=DIRECTORY_CONCURRENCY {
        let name = format!("d{index:03}");
        std::fs::create_dir(destination.path().join(&name))?;
        sender.send(catalogue(directory_entry(&source, &name).await?)).await?;
    }
    drop(sender);
    let query = tokio::spawn(async { Ok(()) });
    let cancel = CancellationToken::new();
    let (_, execution) = execution(cancel.clone())?;
    let operation = apply_directories(
        query,
        receiver,
        Arc::new(target),
        Some(manager.task_statistics()),
        Some(execution),
    );
    let cancelling = async {
        timeout(Duration::from_secs(1), entered.notified()).await?;
        cancel.cancel();
        TestResult::Ok(())
    };
    let (result, cancelled) = timeout(Duration::from_secs(2), async { tokio::join!(operation, cancelling) }).await?;
    cancelled?;
    assert!(
        matches!(result, Err(AppError::DatabaseError(DatabaseError::ScanCancelled))),
        "{result:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), DIRECTORY_CONCURRENCY);
    let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    let events = saved.callback.outbox.archive_prefix();
    assert_eq!(events.len(), DIRECTORY_CONCURRENCY);
    for index in 0..DIRECTORY_CONCURRENCY {
        let name = format!("d{index:03}");
        assert!(
            events
                .iter()
                .any(|event| event.path.as_deref() == Some(&name)
                    && event.message.contains("directory_metadata_finalize"))
        );
    }
    assert!(!events.iter().any(|event| event.path.as_deref() == Some("d032")));
    assert!(matches!(
        saved.report.detail,
        ProgressDetail::Full { error_count: 32, .. }
    ));
    Ok(())
}

#[tokio::test]
async fn directory_cancel_preserves_an_already_completed_query_error() -> TestResult {
    let destination = TempDir::new()?;
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    let query = tokio::spawn(async { Err(DatabaseError::ConfigError("owned completed query failure".into()).into()) });
    timeout(Duration::from_millis(300), async {
        while !query.is_finished() {
            yield_now().await;
        }
    })
    .await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (_, execution) = execution(cancel)?;
    let result = apply_directories(query, receiver, target(&destination).await?, None, Some(execution)).await;
    assert!(
        matches!(result, Err(AppError::DatabaseError(DatabaseError::ConfigError(reason)))
        if reason == "owned completed query failure")
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_completion_preserves_the_saved_execution_root_cause() -> TestResult {
    let destination = TempDir::new()?;
    let (database, execution) = execution(CancellationToken::new())?;
    let first = execution
        .run_child(async { Err(AppError::ConfigError("owned execution root cause".into())) })
        .await;
    assert!(first.is_err());
    let result = complete(database, target(&destination).await?, None, Some(execution)).await;
    assert!(matches!(result, Err(AppError::CopyExecutionStopped { source })
        if matches!(source.as_ref(), AppError::ConfigError(reason) if reason == "owned execution root cause")));
    Ok(())
}
