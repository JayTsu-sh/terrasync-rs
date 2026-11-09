//! 取消优先于排队条目；仅已开始且真实失败的复制产生明细。
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use task_model::{StorageEntryMessage, TransferMode};
use tokio::{task::yield_now, time::timeout};
use tokio_util::sync::CancellationToken;
use transport::{
    in_process::{InProcessSenderTransport, create_in_process_pair_with_capacity},
    message::SenderMsg,
    traits::SenderTransport,
};

use super::LocalCopyReceiver;
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, StorageError},
    receiver::ReceiverConfig,
    scan::observe_path,
    storage_factory::{CreateStorageOptions, create_storage},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture(
    cancel: CancellationToken,
) -> Result<
    (
        tempfile::TempDir,
        tempfile::TempDir,
        LocalCopyReceiver,
        InProcessSenderTransport,
    ),
    Box<dyn std::error::Error>,
> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"retained source")?;
    let source_storage =
        Arc::new(create_storage(&source.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let destination =
        Arc::new(create_storage(&target.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let (sender, receiver) = create_in_process_pair_with_capacity(128);
    Ok((
        source,
        target,
        LocalCopyReceiver {
            id: 0,
            transport: Arc::new(receiver),
            source: source_storage,
            destination,
            config: Arc::new(ReceiverConfig {
                job_id: "receiver_cancel".into(),
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                job_dir: String::new(),
                transfer_mode: TransferMode::Direct,
                cancel: Some(cancel),
            }),
            qos: None,
            bytes: None,
            broadcaster: BroadcastForwarder::new(4),
            active: Arc::new(AtomicUsize::new(0)),
        },
        sender,
    ))
}

#[tokio::test]
async fn cancelled_receiver_does_not_drain_queued_entries_or_emit_invented_failures() -> TestResult {
    let token = CancellationToken::new();
    let (source, target, mut receiver, sender) = fixture(token.clone()).await?;
    let mut events = receiver.broadcaster.subscribe();
    let item = Arc::new(observe_path(receiver.source.storage(), Path::new("file")).await?);
    for _ in 0..128 {
        sender.send(SenderMsg::CopyEntry { entry: item.clone() }).await?;
    }
    let active = receiver.active.clone();
    token.cancel();
    assert!(matches!(
        timeout(Duration::from_millis(500), receiver.run()).await?,
        Err(AppError::StorageError(StorageError::Cancelled))
    ));
    assert!(sender.send(SenderMsg::TransferDone).await.is_err());
    assert!(events.recv().await.is_none());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(source.path().join("file"))?, b"retained source");
    assert_eq!(std::fs::read_dir(target.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn idle_receiver_exits_on_cancellation_with_producer_still_open() -> TestResult {
    let token = CancellationToken::new();
    let (_source, target, receiver, sender) = fixture(token.clone()).await?;
    let cancel = async {
        yield_now().await;
        token.cancel();
    };
    let (result, ()) = timeout(Duration::from_millis(500), async {
        tokio::join!(receiver.run(), cancel)
    })
    .await?;
    assert!(matches!(result, Err(AppError::StorageError(StorageError::Cancelled))));
    assert!(sender.send(SenderMsg::TransferDone).await.is_err());
    assert_eq!(std::fs::read_dir(target.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn cancellation_preserves_a_previously_reported_real_failure() -> TestResult {
    let token = CancellationToken::new();
    let (source, target, mut receiver, sender) = fixture(token.clone()).await?;
    let mut events = receiver.broadcaster.subscribe();
    let item = Arc::new(observe_path(receiver.source.storage(), Path::new("file")).await?);
    std::fs::remove_file(source.path().join("file"))?;
    sender.send(SenderMsg::CopyEntry { entry: item }).await?;
    let cancel = async {
        let event = events.recv().await.ok_or("missing real copy failure")?;
        assert!(matches!(
            event,
            StorageEntryMessage::CopyVersionFailed {
                first_in_entry: true,
                ..
            }
        ));
        token.cancel();
        TestResult::Ok(())
    };
    let (result, cancelled) = timeout(Duration::from_secs(3), async { tokio::join!(receiver.run(), cancel) }).await?;
    cancelled?;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert!(
        matches!(result, Err(AppError::ReportedCopyFailure {source, ..}) if matches!(source.storage_error(), Some(StorageError::Transfer(_))))
    );
    assert!(events.recv().await.is_none());
    assert_eq!(std::fs::read_dir(target.path())?.count(), 0);
    Ok(())
}
