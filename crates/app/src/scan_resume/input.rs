//! 确认检查点时，有界预读后续遍历项；交付仍保持原始检查点顺序。
use crate::error::{AppError, Result};
use data_mover::traversal::TraversalOutcome;
use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::Arc,
    task::Poll,
    vec::IntoIter,
};
use storage_adapter::{ResumableScan, ResumableScanItem};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{
    select,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

const PREFETCH_CHUNK: usize = 256;

pub(super) struct ScanBatch {
    pub(super) items: Vec<ResumableScanItem>,
    pub(super) failure: Option<AppError>,
}

pub(super) struct PrefetchedBatch {
    items: Vec<ResumableScanItem>,
    permit: OwnedSemaphorePermit,
}

pub(super) enum ScanInput {
    Direct(Option<ResumableScan>),
    Finishing(Option<AbortOnDropHandle<Result<TraversalOutcome>>>),
    Prefetched {
        receiver: mpsc::Receiver<PrefetchedBatch>,
        current: IntoIter<ResumableScanItem>,
        permit: Option<OwnedSemaphorePermit>,
        reader: Option<AbortOnDropHandle<Result<TraversalOutcome>>>,
        discard: CancellationToken,
    },
}

impl ScanInput {
    /// 一次取走已就绪的小批次；源端等待时立即交付尾部，不等待凑满批次。
    /// 每次 poll 内要么不取条目，要么返回整批，因此外层定时器取消等待不会丢条目。
    pub(super) async fn next_batch(&mut self, limit: usize) -> Option<ScanBatch> {
        if let Self::Prefetched {
            receiver,
            current,
            permit,
            ..
        } = self
            && limit >= PREFETCH_CHUNK
            && current.len() == 0
            && receiver.len() <= 1
        {
            // 稳态直接移交原缓冲区，避免为每个源端小批次再次分配和逐项 poll。
            permit.take();
            let batch = receiver.recv().await?;
            return Some(ScanBatch {
                items: batch.items,
                failure: None,
            });
        }
        poll_fn(|cx| {
            let mut batch = ScanBatch {
                items: Vec::with_capacity(limit.clamp(1, 16)),
                failure: None,
            };
            loop {
                let mut next = pin!(self.next());
                match next.as_mut().poll(cx) {
                    Poll::Ready(Some(Ok(item))) => batch.items.push(item),
                    Poll::Ready(Some(Err(error))) => {
                        batch.failure = Some(error);
                        break;
                    }
                    Poll::Ready(None) | Poll::Pending if !batch.items.is_empty() => break,
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
                if batch.items.len() >= limit.max(1) {
                    break;
                }
            }
            Poll::Ready(Some(batch))
        })
        .await
    }

    pub(super) fn new(source: ResumableScan, capacity: Option<usize>) -> Self {
        let Some(capacity) = capacity else {
            return Self::Direct(Some(source));
        };
        // 以实际条目数限流，部分就绪的小批次也能充分利用同一预读预算。
        let capacity = capacity.clamp(1, Semaphore::MAX_PERMITS.min(u32::MAX as usize));
        let room = Arc::new(Semaphore::new(capacity));
        let (sender, receiver) = mpsc::channel(capacity);
        let discard = CancellationToken::new();
        let stop = discard.clone();
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut source = Self::Direct(Some(source));
            loop {
                let items = select! {
                    biased;
                    ()=stop.cancelled()=>{
                        source.stop().await;
                        return Err(AppError::ObservationScanCancelled);
                    }
                    items=source.next_batch(PREFETCH_CHUNK.min(capacity))=>items,
                };
                let Some(ScanBatch { items: batch, failure }) = items else {
                    break;
                };
                let sent = if batch.is_empty() {
                    Ok(())
                } else {
                    let permit = select! {
                        biased;
                        ()=stop.cancelled()=>None,
                        permit=Arc::clone(&room).acquire_many_owned(batch.len() as u32)=>permit.ok(),
                    };
                    let Some(permit) = permit else {
                        source.stop().await;
                        return Err(failure.unwrap_or(AppError::ObservationScanCancelled));
                    };
                    let batch = PrefetchedBatch { items: batch, permit };
                    match sender.try_send(batch) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(batch)) => select! {
                            biased;
                            ()=stop.cancelled()=>Err(AppError::ObservationScanCancelled),
                            result=sender.send(batch)=>result.map_err(|_| consumer_stopped()),
                        },
                        Err(TrySendError::Closed(_)) => Err(consumer_stopped()),
                    }
                };
                if let Some(error) = failure {
                    source.stop().await;
                    return Err(error);
                }
                if let Err(error) = sent {
                    source.stop().await;
                    return Err(error);
                }
            }
            source.finish().await
        }));
        Self::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: Some(reader),
            discard,
        }
    }

    pub(super) async fn next(&mut self) -> Option<Result<ResumableScanItem>> {
        match self {
            Self::Direct(Some(source)) => source.next().await.map(|item| item.map_err(AppError::from)),
            Self::Direct(None) => None,
            Self::Finishing(_) => None,
            Self::Prefetched {
                receiver,
                current,
                permit,
                ..
            } => loop {
                if let Some(item) = current.next() {
                    break Some(Ok(item));
                }
                permit.take();
                let batch = receiver.recv().await?;
                *current = batch.items.into_iter();
                *permit = Some(batch.permit);
            },
        }
    }

    pub(super) async fn finish(&mut self) -> Result<TraversalOutcome> {
        if let Self::Direct(source) = self {
            let source = source.take().ok_or_else(missing_outcome)?;
            *self = Self::Finishing(Some(AbortOnDropHandle::new(tokio::spawn(async move {
                source.finish().await.map_err(AppError::from)
            }))));
        }
        match self {
            Self::Direct(_) => Err(missing_outcome()),
            Self::Prefetched { reader, .. } | Self::Finishing(reader) => {
                // await 时保留所有权；finish future 被取消后，stop 仍能等待同一个任务。
                let result = reader.as_mut().ok_or_else(missing_outcome)?.await;
                reader.take();
                result?
            }
        }
    }

    pub(super) async fn stop(&mut self) {
        match self {
            Self::Direct(source) => {
                if let Some(source) = source.take() {
                    let _ = source.stop().await;
                }
            }
            Self::Finishing(reader) => {
                if let Some(reader) = reader.take() {
                    let _ = reader.await;
                }
            }
            Self::Prefetched {
                reader,
                receiver,
                discard,
                ..
            } => {
                discard.cancel();
                receiver.close();
                if let Some(reader) = reader.take() {
                    let _ = reader.await;
                }
            }
        }
    }
}

fn consumer_stopped() -> AppError {
    AppError::ConsumerError("Scan traversal consumer stopped".into())
}

fn missing_outcome() -> AppError {
    AppError::ConsumerError("Scan traversal outcome already consumed".into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::storage_factory::{StorageRole, create_storage_for_role};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use storage_adapter::WalkOptions;
    use tokio::{
        sync::{Notify, oneshot},
        time::{Duration, sleep, timeout},
    };

    #[tokio::test]
    async fn stopping_a_real_source_with_a_full_prefetch_budget_joins_the_reader() {
        let directory = tempfile::tempdir().unwrap();
        for index in 0..100 {
            std::fs::write(directory.path().join(index.to_string()), []).unwrap();
        }
        let storage = create_storage_for_role(&directory.path().to_string_lossy(), None, false, StorageRole::Source)
            .await
            .unwrap();
        let parent = CancellationToken::new();
        let source = storage
            .resumable_scan(&WalkOptions::default(), None, parent.clone(), Duration::from_secs(60))
            .unwrap();
        let mut input = ScanInput::new(source, Some(1));
        timeout(Duration::from_secs(2), async {
            loop {
                if matches!(&input, ScanInput::Prefetched { receiver, .. } if receiver.len() == 1) {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_millis(500), input.stop()).await.unwrap();
        assert!(matches!(input, ScanInput::Prefetched { reader: None, .. }));
        assert!(!parent.is_cancelled());
    }

    fn buffered(items: Vec<ResumableScanItem>) -> PrefetchedBatch {
        let room = Arc::new(Semaphore::new(items.len()));
        let permit = room.try_acquire_many_owned(items.len() as u32).unwrap();
        PrefetchedBatch { items, permit }
    }

    #[tokio::test]
    async fn ready_batches_preserve_checkpoints_across_a_cancelled_empty_wait() {
        let (sender, receiver) = mpsc::channel(4);
        let mut input = ScanInput::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: None,
            discard: CancellationToken::new(),
        };
        sender
            .send(buffered(vec![
                ResumableScanItem::Checkpoint(vec![1]),
                ResumableScanItem::Checkpoint(vec![2]),
            ]))
            .await
            .unwrap();
        // 尾部两项已就绪，不能因上限为八项而等待仍未到达的源端数据。
        let batch = timeout(Duration::from_millis(200), input.next_batch(PREFETCH_CHUNK))
            .await
            .unwrap()
            .unwrap();
        let markers: Vec<_> = batch
            .items
            .into_iter()
            .map(|item| match item {
                ResumableScanItem::Checkpoint(bytes) => bytes[0],
                ResumableScanItem::Entry(_) => panic!("unexpected entry"),
            })
            .collect();
        assert_eq!(markers, [1, 2]);
        assert!(
            timeout(Duration::from_millis(10), input.next_batch(PREFETCH_CHUNK))
                .await
                .is_err()
        );
        for marker in [3, 4] {
            sender
                .send(buffered(vec![ResumableScanItem::Checkpoint(vec![marker])]))
                .await
                .unwrap();
        }
        drop(sender);
        for marker in [3, 4] {
            let batch = input.next_batch(1).await.unwrap();
            assert_eq!(batch.items.len(), 1);
            assert!(batch.failure.is_none());
            assert!(matches!(&batch.items[0], ResumableScanItem::Checkpoint(bytes) if bytes == &[marker]));
        }
        assert!(input.next_batch(8).await.is_none());
    }

    #[tokio::test]
    async fn prefetch_budget_is_released_only_after_the_buffered_batch_is_consumed() {
        let room = Arc::new(Semaphore::new(2));
        let permit = Arc::clone(&room).acquire_many_owned(2).await.unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(PrefetchedBatch {
                items: vec![
                    ResumableScanItem::Checkpoint(vec![1]),
                    ResumableScanItem::Checkpoint(vec![2]),
                ],
                permit,
            })
            .await
            .unwrap();
        let mut input = ScanInput::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: None,
            discard: CancellationToken::new(),
        };
        for marker in [1, 2] {
            assert!(matches!(input.next().await, Some(Ok(ResumableScanItem::Checkpoint(bytes))) if bytes == [marker]));
            assert_eq!(room.available_permits(), 0);
        }
        assert!(timeout(Duration::from_millis(10), input.next()).await.is_err());
        assert_eq!(room.available_permits(), 2);
    }

    #[tokio::test]
    async fn interrupted_finish_retains_the_reader_until_stop_has_joined_it() {
        let release = Arc::new(Notify::new());
        let allow = release.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = dropped.clone();
        let (sender, receiver) = mpsc::channel(1);
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            let _sender = sender;
            let _guard = Dropped(flag);
            allow.notified().await;
            Err(AppError::ObservationScanCancelled)
        }));
        let mut input = ScanInput::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: Some(reader),
            discard: CancellationToken::new(),
        };
        assert!(timeout(Duration::from_millis(10), input.finish()).await.is_err());
        let stopping = input.stop();
        tokio::pin!(stopping);
        assert!(timeout(Duration::from_millis(10), &mut stopping).await.is_err());
        assert!(!dropped.load(Ordering::SeqCst));
        release.notify_one();
        stopping.await;
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn buffered_items_keep_order_and_reader_failure_is_not_completion() {
        let (sender, receiver) = mpsc::channel(2);
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            sender
                .send(buffered(vec![ResumableScanItem::Checkpoint(vec![1])]))
                .await
                .unwrap();
            sender
                .send(buffered(vec![ResumableScanItem::Checkpoint(vec![2])]))
                .await
                .unwrap();
            Err(AppError::ScanError("reader failed".into()))
        }));
        let mut input = ScanInput::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: Some(reader),
            discard: CancellationToken::new(),
        };
        for expected in [1, 2] {
            assert!(
                matches!(input.next().await,Some(Ok(ResumableScanItem::Checkpoint(bytes))) if bytes==vec![expected])
            );
        }
        assert!(input.next().await.is_none());
        assert!(matches!(input.finish().await,Err(AppError::ScanError(message)) if message=="reader failed"));
    }

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stopping_waits_for_the_prefetch_task_to_exit() {
        let (sender, receiver) = mpsc::channel(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = dropped.clone();
        let (started, ready) = oneshot::channel();
        let discard = CancellationToken::new();
        let stop = discard.clone();
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            let _sender = sender;
            let _guard = Dropped(flag);
            started.send(()).unwrap();
            stop.cancelled().await;
            Err(AppError::ObservationScanCancelled)
        }));
        let mut input = ScanInput::Prefetched {
            receiver,
            current: Vec::new().into_iter(),
            permit: None,
            reader: Some(reader),
            discard,
        };
        ready.await.unwrap();
        input.stop().await;
        assert!(dropped.load(Ordering::SeqCst));
    }
}
