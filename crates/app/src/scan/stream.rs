//! 不落库扫描的轻量通道；与持久化通道共享取消及故障域。
#[cfg(test)]
use crate::broadcast::BroadcastForwarder;
use crate::{
    config::{JobType, initialize_consumer_config},
    consumer::{ConsumerManager, ScanStatistics},
    error::{AppError, Result},
    scan::{ScanRequest, cancellation::ScanCancellation, directory_ledger::DirectoryLedger},
    storage_factory::{StorageRole, create_storage_for_role},
};
use data_mover::{model::ObservedEntry, traversal::TraversalOutcome};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use storage_adapter::{ResumableScan, ResumableScanItem, StorageError, WalkOptions, filter::parse_filter_expression};
#[cfg(test)]
use storage_adapter::{StorageEntryMessage, WalkDirAsyncIterator};
#[cfg(test)]
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use utils::app_config::AppConfig;

pub(super) async fn run(request: ScanRequest<'_>, config: AppConfig) -> Result<()> {
    let cancel = ScanCancellation::new(request.cancel.clone());
    let storage = cancel
        .interrupt(create_storage_for_role(request.path, None, false, StorageRole::Source))
        .await??;
    let options = WalkOptions {
        depth: NonZeroUsize::new(request.depth as usize).map(NonZeroUsize::get),
        match_expressions: request.include.as_deref().map(parse_filter_expression).transpose()?,
        exclude_expressions: request.exclude.as_deref().map(parse_filter_expression).transpose()?,
        concurrency: config.scan.concurrency,
        include_tags: config.scan.include_tags,
        operation_timeout: Some(Duration::from_secs(config.scan.operation_timeout_secs)),
        recovery_window: Some(Duration::from_secs(config.scan.recovery_window_secs)),
        ..Default::default()
    };
    let mut source = storage.resumable_scan(&options, None, cancel.token().clone(), Duration::from_secs(60))?;
    let failure = source.failure_signal();
    let _failure_watch = cancel.watch_storage_failure(failure.clone());
    let consumer_config = initialize_consumer_config(
        request.job_id,
        request.job_dir,
        JobType::Scan,
        request.command,
        &config,
        request.callback_url,
    )?;
    let mut manager = ConsumerManager::new(&consumer_config).await?;
    manager
        .initialize_scan_statistics(&storage, options.include_tags, None)
        .await?;
    let statistics = manager.scan_statistics();
    let recovery_progress = statistics.watch_recovery(source.recovery_status()).await;
    let mut ledger = DirectoryLedger::new(None, 1).await?;
    manager.begin_lifecycle().await;
    let result = consume_observed(&mut source, &statistics, &mut ledger, &cancel).await;
    let result = match result {
        Ok(()) => match source.finish().await {
            Ok(TraversalOutcome::Completed(value))
                if value.totals.failed_children == 0 && value.totals.listing_failures == 0 =>
            {
                statistics.complete_scan().await;
                Ok(())
            }
            Ok(_) => Err(AppError::ScanError("Scan did not complete".into())),
            Err(error) => Err(error.into()),
        },
        Err(error) => {
            let _ = source.stop().await;
            Err(error)
        }
    };
    if let Some(progress) = recovery_progress {
        progress.finish().await;
    }
    let result = if request.cancel.is_cancelled() {
        Err(AppError::ObservationScanCancelled)
    } else if let Some(error) = failure.failure() {
        Err(StorageError::from(error).into())
    } else {
        result
    };
    if let Err(error) = &result
        && !request.cancel.is_cancelled()
    {
        manager.record_task_failures(&[error.to_string()]).await;
    }
    manager.set_generation_committed(result.is_ok()).await;
    if cancel.token().is_cancelled() {
        manager.end_cancelled_lifecycle().await;
    } else {
        manager.end_lifecycle().await;
    }
    result
}

async fn consume_observed(
    source: &mut ResumableScan, statistics: &ScanStatistics, ledger: &mut DirectoryLedger, cancel: &ScanCancellation,
) -> Result<()> {
    let mut entries = Vec::<Arc<ObservedEntry>>::with_capacity(256);
    let mut tick = interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let item = tokio::select! {
            biased;
            ()=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
            item=source.next()=>item,
            _=tick.tick(),if !entries.is_empty() || !ledger.is_empty()=>{
                statistics.record_observations(&entries).await; entries.clear(); ledger.flush(statistics).await?; continue;
            }
        };
        match item {
            Some(Ok(ResumableScanItem::Entry(entry))) => {
                entries.push(Arc::from(entry));
                if entries.len() >= 256 {
                    statistics.record_observations(&entries).await;
                    entries.clear();
                }
            }
            Some(Ok(ResumableScanItem::Statistics(value))) => {
                cancel.interrupt(ledger.push(*value, statistics)).await??;
            }
            Some(Ok(ResumableScanItem::Checkpoint(_))) => {}
            Some(Err(error)) => return Err(error.into()),
            None => break,
        }
    }
    statistics.record_observations(&entries).await;
    cancel.interrupt(ledger.flush(statistics)).await??;
    Ok(())
}

#[cfg(test)]
async fn consume(
    iterator: &WalkDirAsyncIterator, forwarder: &BroadcastForwarder<StorageEntryMessage>, cancel: &ScanCancellation,
) -> Result<()> {
    while let Some(message) = cancel.interrupt(iterator.next()).await? {
        cancel.interrupt(forwarder.broadcast(message)).await?;
    }
    Ok(())
}

#[cfg(test)]
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
