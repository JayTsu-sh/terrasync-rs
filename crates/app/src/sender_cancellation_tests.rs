//! 取消须中断等待与背压，不能让诱发的 Cancelled 遮住最先失败的 worker 原因。

use std::{
    error::Error,
    fs,
    future::pending,
    io::{Error as IoError, ErrorKind},
    path::Path,
    result::Result as StdResult,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use data_mover::model::ObservedEntry;
use db::ClickHouseDatabase;
use task_model::StorageEntryMessage;
use tokio::{sync::Notify, task::yield_now, time::timeout};
use tokio_util::sync::CancellationToken;
use utils::app_config::ClickhouseConfig;

use super::{
    SenderWorkerConfig,
    copy_queue::{CopyCommand, CopyCommandSink},
    sender_worker,
};
use crate::{
    broadcast::BroadcastForwarder,
    copy_execution::CopyDatabaseExecution,
    error::{AppError, Result, StorageError},
    receiver::ReceiverConfig,
    scan::observe_path,
    storage_factory::{CreateStorageOptions, StorageContext, create_storage},
    task_stream::AsyncReceiver,
};

type TestResult = StdResult<(), Box<dyn Error>>;

#[derive(Default)]
struct BlockedTransport {
    started: Notify,
    attempts: AtomicUsize,
    dropped: AtomicBool,
}

struct MarkDrop<'a>(&'a AtomicBool);
impl Drop for MarkDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl CopyCommandSink for BlockedTransport {
    async fn send(&self, _message: CopyCommand) -> Result<()> {
        let _guard = MarkDrop(&self.dropped);
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        pending().await
    }
}

async fn fixture() -> Result<(tempfile::TempDir, Arc<StorageContext>, Arc<ObservedEntry>)> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("file"), b"retained source")?;
    let storage = Arc::new(create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let entry = Arc::new(observe_path(storage.storage(), Path::new("file")).await?);
    Ok((root, storage, entry))
}

async fn run(
    receiver: async_channel::Receiver<StorageEntryMessage>, storage: Arc<StorageContext>,
    transport: Arc<BlockedTransport>, cancel: CancellationToken, active: Arc<AtomicUsize>,
) -> Result<()> {
    let config = SenderWorkerConfig {
        target_history: None,
        enable_integrity_check: false,
        enable_acl: false,
        is_source_reserved: true,
        large_file_log_threshold: u64::MAX,
    };
    let copy = ReceiverConfig {
        job_id: "sender_cancel".into(),
        enable_integrity_check: false,
        enable_acl: false,
        is_source_reserved: true,
        job_dir: String::new(),
        transfer_mode: Default::default(),
        cancel: Some(cancel),
    };
    sender_worker(
        0,
        AsyncReceiver::new(receiver),
        storage.clone(),
        storage,
        transport,
        &BroadcastForwarder::new(4),
        &config,
        None,
        None,
        &copy,
        active,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicU64::new(0)),
    )
    .await
}

#[tokio::test]
async fn pre_cancelled_sender_does_not_start_a_transport_send() -> TestResult {
    let (root, storage, entry) = fixture().await?;
    let (sender, receiver) = async_channel::bounded(1);
    sender.send(StorageEntryMessage::Scanned(entry)).await?;
    let transport = Arc::new(BlockedTransport::default());
    let token = CancellationToken::new();
    token.cancel();
    let result = timeout(
        Duration::from_millis(500),
        run(
            receiver,
            storage,
            transport.clone(),
            token,
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .await?;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    assert_eq!(transport.attempts.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(root.path().join("file"))?, b"retained source");
    Ok(())
}

#[tokio::test]
async fn sender_waiting_for_scan_input_observes_cancellation() -> TestResult {
    let (_root, storage, _) = fixture().await?;
    let (keep_open, receiver) = async_channel::bounded(1);
    let token = CancellationToken::new();
    let signal = token.clone();
    let operation = run(
        receiver,
        storage,
        Arc::new(BlockedTransport::default()),
        token,
        Arc::new(AtomicUsize::new(0)),
    );
    let cancelled = async {
        yield_now().await;
        assert!(!keep_open.is_closed());
        signal.cancel();
    };
    let (result, ()) = timeout(Duration::from_millis(500), async { tokio::join!(operation, cancelled) }).await?;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    Ok(())
}

#[tokio::test]
async fn peer_failure_releases_send_backpressure_and_preserves_the_original_typed_cause() -> TestResult {
    let (root, storage, entry) = fixture().await?;
    let (sender, receiver) = async_channel::bounded(1);
    sender.send(StorageEntryMessage::Scanned(entry)).await?;
    let token = CancellationToken::new();
    // 构造作用域不发送数据库请求；此用例只验证已有作用域对子任务失败的取消传播。
    let config = ClickhouseConfig {
        mode: Default::default(),
        cluster: None,
        dsn: "http://127.0.0.1:9".into(),
        database: "fixture".into(),
        username: "fixture".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    let mut database = ClickHouseDatabase::new(&config, "sender_cancel")?;
    let execution = CopyDatabaseExecution::prepare(&mut database, token.clone())?;
    let transport = Arc::new(BlockedTransport::default());
    let active = Arc::new(AtomicUsize::new(0));
    let operation = execution.run_child(run(receiver, storage, transport.clone(), token, active.clone()));
    let failure = execution.run_child(async {
        transport.started.notified().await;
        Err(IoError::new(ErrorKind::TimedOut, "original receiver failure").into())
    });
    let (sender_result, peer_result) =
        timeout(Duration::from_millis(500), async { tokio::join!(operation, failure) }).await?;
    match (sender_result, peer_result) {
        (
            Err(AppError::CopyExecutionStopped { source: first }),
            Err(AppError::CopyExecutionStopped { source: peer }),
        ) => {
            assert!(Arc::ptr_eq(&first, &peer));
            assert!(matches!(first.as_ref(), AppError::IoError(cause) if cause.kind() == ErrorKind::TimedOut));
            assert!(first.to_string().contains("original receiver failure"));
        }
        other => panic!("original peer failure was lost: {other:?}"),
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(transport.dropped.load(Ordering::SeqCst));
    assert_eq!(fs::read(root.path().join("file"))?, b"retained source");
    Ok(())
}
