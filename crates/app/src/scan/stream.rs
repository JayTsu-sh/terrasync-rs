//! 不落库扫描的轻量通道；与持久化通道共享取消及故障域。
use crate::{
    broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY},
    config::{JobType, initialize_consumer_config, initialize_scan_config},
    consumer::ConsumerManager,
    dir_walker::scan_walkdir,
    error::{AppError, Result},
    scan::{ScanRequest, ScanType, cancellation::ScanCancellation},
};
use std::time::Duration;
use storage_adapter::{StorageEntryMessage, WalkDirAsyncIterator};
use tokio::task::JoinHandle;
use utils::app_config::AppConfig;

pub(super) async fn run(request: ScanRequest<'_>, config: AppConfig) -> Result<()> {
    let cancel = ScanCancellation::new(request.cancel.clone());
    let scan_config = initialize_scan_config(
        request.job_id,
        request.path,
        request.depth,
        ScanType::Full,
        request.include,
        request.exclude,
        config.scan.concurrency,
        config.scan.include_tags,
        &None,
        false,
        0,
    )?;
    let iterator = cancel
        .interrupt(scan_walkdir(
            scan_config,
            Duration::from_secs(config.scan.operation_timeout_secs),
            Duration::from_secs(config.scan.recovery_window_secs),
        ))
        .await??;
    let _failure_watch = iterator
        .failure_signal()
        .map(|failure| cancel.watch_storage_failure(failure));
    let consumer_config = initialize_consumer_config(
        request.job_id,
        request.job_dir,
        JobType::Scan,
        request.command,
        &config,
        request.callback_url,
    )?;
    let mut manager = ConsumerManager::new(&consumer_config).await?;
    let mut forwarder = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
    manager.begin_lifecycle().await;
    let handles = manager.start_consumers(&mut forwarder).await?;
    let result = consume(&iterator, &forwarder, &cancel).await;
    iterator.close();
    drop(forwarder);
    let consumers = finish_consumers(handles, &cancel).await;
    let result = if request.cancel.is_cancelled() {
        Err(AppError::ObservationScanCancelled)
    } else if let Some(error) = iterator.take_error() {
        Err(error.into())
    } else if result.is_err() {
        result
    } else {
        consumers
    };
    if let Err(error) = &result
        && !request.cancel.is_cancelled()
    {
        manager.record_task_failures(&[error.to_string()]).await;
    }
    manager.set_generation_committed(result.is_ok()).await;
    if request.cancel.is_cancelled() {
        manager.end_cancelled_lifecycle().await;
    } else {
        manager.end_lifecycle().await;
    }
    result
}

async fn consume(
    iterator: &WalkDirAsyncIterator, forwarder: &BroadcastForwarder<StorageEntryMessage>, cancel: &ScanCancellation,
) -> Result<()> {
    while let Some(message) = cancel.interrupt(iterator.next()).await? {
        cancel.interrupt(forwarder.broadcast(message)).await?;
    }
    Ok(())
}

async fn finish_consumers(handles: Vec<JoinHandle<Result<()>>>, cancel: &ScanCancellation) -> Result<()> {
    let mut failure = None;
    for mut handle in handles {
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => { handle.abort(); handle.await }
            result = &mut handle => result,
        };
        match result {
            Ok(Ok(())) => {}
            Err(error) if error.is_cancelled() && cancel.token().is_cancelled() => {}
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(error) => {
                failure.get_or_insert(error.into());
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::io::{Error as IoError, ErrorKind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use storage_adapter::{AsyncReceiver, ErrorEvent};
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn consumer_failure_keeps_type_and_drains_later_tasks() {
        let completed = Arc::new(AtomicBool::new(false));
        let done = completed.clone();
        let failed = tokio::spawn(async { Err(AppError::IoError(IoError::from(ErrorKind::TimedOut))) });
        let later = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            done.store(true, Ordering::Release);
            Ok(())
        });
        let cancel = ScanCancellation::new(CancellationToken::new());
        let result = finish_consumers(vec![failed, later], &cancel).await;
        assert!(matches!(result, Err(AppError::IoError(error)) if error.kind() == ErrorKind::TimedOut));
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn consumer_panic_preserves_join_error() {
        let handle = tokio::spawn(async { panic!("consumer regression fixture") });
        let cancel = ScanCancellation::new(CancellationToken::new());
        assert!(
            matches!(finish_consumers(vec![handle], &cancel).await, Err(AppError::TaskJoinError(error)) if error.is_panic())
        );
    }

    #[tokio::test]
    async fn cancellation_drains_aborted_consumers() {
        let handle = tokio::spawn(async { pending::<Result<()>>().await });
        let user = CancellationToken::new();
        user.cancel();
        let cancel = ScanCancellation::new(user);
        assert!(finish_consumers(vec![handle], &cancel).await.is_ok());
    }

    #[tokio::test]
    async fn cancellation_interrupts_output_backpressure() -> Result<()> {
        let (sender, receiver) = async_channel::bounded(2);
        for _ in 0..2 {
            sender
                .send(StorageEntryMessage::Error {
                    event: ErrorEvent::Scan,
                    path: "fixture".into(),
                    entry: None,
                    reason: "fixture".into(),
                })
                .await
                .map_err(|error| AppError::ScanError(error.to_string()))?;
        }
        drop(sender);
        let iterator = AsyncReceiver::new(receiver);
        let mut forwarder = BroadcastForwarder::new(1);
        let _blocked_consumer = forwarder.subscribe();
        let user = CancellationToken::new();
        let cancel = ScanCancellation::new(user.clone());
        let (result, ()) = tokio::join!(
            timeout(Duration::from_secs(1), consume(&iterator, &forwarder, &cancel)),
            async {
                sleep(Duration::from_millis(30)).await;
                user.cancel();
            },
        );
        assert!(matches!(result, Ok(Err(AppError::ObservationScanCancelled))));
        Ok(())
    }
}
