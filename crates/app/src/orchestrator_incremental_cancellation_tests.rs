//! 增量 worker 取消后不再取队列，也不把未开始条目虚构为复制失败。
use std::{path::Path, sync::Arc, time::Duration};

use data_mover::model::ObservedEntry;
use task_model::{SourceCleanupPolicy, TransferMode};
use tempfile::TempDir;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    copy_history::HistoryCompletion,
    error::StorageError,
    scan::observe_path,
    storage_factory::{CreateStorageOptions, create_storage},
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Worker {
    _source: TempDir,
    target: TempDir,
    entry: Arc<ObservedEntry>,
    sender: async_channel::Sender<StorageEntryMessage>,
    messages: mpsc::Receiver<StorageEntryMessage>,
    task: JoinHandle<Result<()>>,
    cancel: CancellationToken,
}

async fn worker() -> TestResult<Worker> {
    let source = TempDir::new()?;
    let target = TempDir::new()?;
    std::fs::write(source.path().join("file"), b"retained source")?;
    let storage = create_storage(&source.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(storage.storage(), Path::new("file")).await?);
    let command = SyncJobConfig {
        job_id: "incremental-worker-cancel".into(),
        job_dir: source.path().join("job").to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: source.path().to_string_lossy().into_owned(),
        dest_path: target.path().to_string_lossy().into_owned(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: String::new(),
        progress_callback_url: None,
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: Some(1),
    };
    let (sender, receiver) = async_channel::bounded(128);
    let mut broadcaster = BroadcastForwarder::new(128);
    let messages = broadcaster.subscribe();
    let cancel = CancellationToken::new();
    let mut tasks = spawn_handlers(
        &command,
        vec![AsyncReceiver::new(receiver)],
        broadcaster,
        CopyOptions {
            cancel: Some(cancel.clone()),
            ..Default::default()
        },
        None,
        HistoryCopyPolicy {
            active_entry_limit: 1,
            target_history: Some(false),
            completion: HistoryCompletion::Incremental,
            recovering: false,
        },
        None,
    )
    .await;
    let task = tasks.pop().ok_or("worker was not created")?;
    Ok(Worker {
        _source: source,
        target,
        entry,
        sender,
        messages,
        task,
        cancel,
    })
}

#[tokio::test]
async fn idle_incremental_worker_exits_while_the_input_producer_is_open() -> TestResult {
    let worker = worker().await?;
    assert!(!worker.sender.is_closed());
    worker.cancel.cancel();
    let result = timeout(Duration::from_millis(500), worker.task).await??;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    Ok(())
}

#[tokio::test]
async fn cancelled_incremental_queue_does_not_invent_metadata_errors() -> TestResult {
    let mut worker = worker().await?;
    for _ in 0..128 {
        worker
            .sender
            .send(StorageEntryMessage::Changed {
                entry: worker.entry.clone().into(),
                kind: ChangeKind::MetadataOnly,
            })
            .await?;
    }
    worker.cancel.cancel();
    let result = timeout(Duration::from_millis(500), worker.task).await??;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    assert!(worker.messages.try_recv().is_err());
    assert!(!worker.target.path().join("file").exists());
    Ok(())
}

#[tokio::test]
async fn incremental_cancel_preserves_the_metadata_failure_already_reported() -> TestResult {
    let mut worker = worker().await?;
    worker
        .sender
        .send(StorageEntryMessage::Changed {
            entry: worker.entry.clone().into(),
            kind: ChangeKind::MetadataOnly,
        })
        .await?;
    let event = timeout(Duration::from_secs(1), worker.messages.recv())
        .await?
        .ok_or("metadata error missing")?;
    assert!(matches!(
        event,
        StorageEntryMessage::CopyChangeFailed {
            entry,
            change: EntryChange::Changed(ChangeKind::MetadataOnly),
            reason,
        } if entry.entry.path().as_str() == "file" && !reason.is_empty()
    ));
    worker.cancel.cancel();
    let result = timeout(Duration::from_millis(500), worker.task).await??;
    assert!(matches!(
        result,
        Err(AppError::StorageError(StorageError::MetadataApplication(_)))
    ));
    assert!(worker.messages.try_recv().is_err());
    Ok(())
}
