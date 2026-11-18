//! 首个清理失败须在唤醒下一项之前保存原因和取消执行。
use std::{
    io::{Error as IoError, ErrorKind},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use data_mover::{
    model::{EntryOperationFailure, FailureClass, ObservedEntry, Operation, StoragePath, Transience},
    storage::{
        ListRequest, ListingStream, Namespace, NamespaceCapabilities, PurgeFailure, PurgeOutcome, SourceDescriptor,
        StorageBuilder, StorageRoleFailure,
    },
};
use db::ClickHouseDatabase;
use tokio::{sync::Notify, task::yield_now, time::timeout};
use tokio_util::sync::CancellationToken;
use utils::app_config::{ClickHouseMode, ClickhouseConfig};

use super::ReadyFileCleanup;
use crate::{
    copy_execution::CopyDatabaseExecution,
    error::{AppError, StorageError},
    file_copy::{CopyOptions, copy_file},
    scan::observe_path,
    source_cleanup::SourceCleanupCheck,
    storage_factory::{CreateStorageOptions, StorageContext, create_storage},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Default)]
struct Calls {
    entered: Notify,
    release: Notify,
    persist: AtomicUsize,
    purge: AtomicUsize,
}

struct ObjectNamespace {
    inner: Arc<dyn Namespace>,
    calls: Arc<Calls>,
}

#[async_trait]
impl Namespace for ObjectNamespace {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            directory_objects: true,
            ..self.inner.capabilities()
        }
    }
    fn allows_paged_delete(&self) -> bool {
        true
    }
    async fn persist_observed(&self, entry: &SourceDescriptor, _: CancellationToken) -> Result<(), StorageRoleFailure> {
        self.calls.persist.fetch_add(1, Ordering::SeqCst);
        self.calls.entered.notify_one();
        self.calls.release.notified().await;
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                entry.path.clone(),
                Operation::Publish,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "owned readiness refusal",
            )
            .unwrap_or_else(|error| panic!("fixture: {error}")),
        ))
    }
    async fn purge_observed(
        &self, entry: &ObservedEntry, cancel: CancellationToken,
    ) -> Result<PurgeOutcome, PurgeFailure> {
        self.calls.purge.fetch_add(1, Ordering::SeqCst);
        self.inner.purge_observed(entry, cancel).await
    }
}

fn object_namespace(storage: &mut StorageContext, calls: Arc<Calls>) -> TestResult {
    let original = storage.storage();
    storage.roles = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .namespace(Arc::new(ObjectNamespace {
            inner: original.namespace()?,
            calls,
        }))
        .read_source(original.read_source()?)
        .metadata(original.metadata()?)
        .staged_destination(original.staged_destination()?)
        .build()?;
    Ok(())
}

async fn failure_case(peer_failed: bool) -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    for name in ["a", "b"] {
        std::fs::write(source_root.path().join(name), b"verified data")?;
    }
    let mut source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let a = Arc::new(observe_path(source.storage(), Path::new("a")).await?);
    let b = Arc::new(observe_path(source.storage(), Path::new("b")).await?);
    for item in [&a, &b] {
        copy_file(&source, &target, item, CopyOptions::default(), None).await?;
    }
    let calls = Arc::new(Calls::default());
    object_namespace(&mut source, calls.clone())?;
    object_namespace(&mut target, calls.clone())?;
    let source = Arc::new(source);
    let target = Arc::new(target);
    let token = CancellationToken::new();
    // 只构造已有执行作用域，不连接数据库；该测试仅观察清理门闩及最初原因。
    let config = ClickhouseConfig {
        mode: ClickHouseMode::default(),
        cluster: None,
        dsn: "http://127.0.0.1:9".into(),
        database: "fixture".into(),
        username: "fixture".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    let mut database = ClickHouseDatabase::new(&config, "ready_gate")?;
    let execution = CopyDatabaseExecution::prepare(&mut database, token.clone())?;
    let cleaner = Arc::new(ReadyFileCleanup::new(
        SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), token.clone()),
        execution.clone(),
    ));
    let first = {
        let cleaner = cleaner.clone();
        let source = source.clone();
        let target = target.clone();
        tokio::spawn(async move { cleaner.remove(&source, &target, &a, None).await })
    };
    timeout(Duration::from_secs(2), calls.entered.notified()).await?;
    let observed_cleanup = cleaner.clone();
    let second = tokio::spawn(async move { cleaner.remove(&source, &target, &b, None).await });
    yield_now().await;
    if peer_failed {
        assert!(
            execution
                .run_child(async { Err(IoError::new(ErrorKind::TimedOut, "owned peer failure").into()) })
                .await
                .is_err()
        );
    }
    calls.release.notify_one();
    let (first, second) = timeout(Duration::from_secs(2), async { tokio::join!(first, second) }).await?;
    let first = first?.err().ok_or("readiness failure was lost")?;
    if peer_failed {
        assert!(first.to_string().contains("owned peer failure"));
        assert!(execution.stopped_error().source_cleanup_entry().is_none());
    } else {
        assert_eq!(first.source_cleanup_entry().map(|(path, _)| path), Some(Path::new("a")));
        assert_eq!(
            execution.stopped_error().source_cleanup_entry().map(|(path, _)| path),
            Some(Path::new("a"))
        );
    }
    let cleanup = observed_cleanup.failure().ok_or("actual cleanup failure was lost")?;
    assert_eq!(
        cleanup.source_cleanup_entry().map(|(path, _)| path),
        Some(Path::new("a"))
    );
    assert!(matches!(
        cleanup.storage_error(),
        Some(StorageError::TargetPersistenceUnconfirmed { .. })
    ));
    assert!(cleanup.to_string().contains("owned readiness refusal"));
    assert!(matches!(second?, Err(AppError::StorageError(StorageError::Cancelled))));
    assert!(token.is_cancelled());
    assert_eq!(calls.persist.load(Ordering::SeqCst), 1);
    assert_eq!(calls.purge.load(Ordering::SeqCst), 0);
    for name in ["a", "b"] {
        assert_eq!(std::fs::read(source_root.path().join(name))?, b"verified data");
        assert_eq!(std::fs::read(target_root.path().join(name))?, b"verified data");
    }
    Ok(())
}

#[tokio::test]
async fn first_readiness_failure_cancels_queued_cleanup_before_releasing_the_gate() -> TestResult {
    failure_case(false).await
}

#[tokio::test]
async fn concurrent_execution_failure_keeps_the_actual_cleanup_path_and_cause() -> TestResult {
    failure_case(true).await
}
