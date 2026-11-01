//! 增量比较确认检查点时，有界预读后续遍历项；交付仍保持原始检查点顺序。
use crate::error::{AppError, Result};
use data_mover::traversal::TraversalOutcome;
use storage_adapter::{ResumableScan, ResumableScanItem};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{select, sync::mpsc};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

pub(super) enum ScanInput {
    Direct(Option<ResumableScan>),
    Finishing(Option<AbortOnDropHandle<Result<TraversalOutcome>>>),
    Prefetched {
        receiver: mpsc::Receiver<ResumableScanItem>,
        reader: Option<AbortOnDropHandle<Result<TraversalOutcome>>>,
        discard: CancellationToken,
    },
}

impl ScanInput {
    pub(super) fn new(mut source: ResumableScan, capacity: Option<usize>) -> Self {
        let Some(capacity) = capacity else {
            return Self::Direct(Some(source));
        };
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let discard = CancellationToken::new();
        let stop = discard.clone();
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                if stop.is_cancelled() {
                    return source.stop().await.map_err(AppError::from);
                }
                let item = source.next().await;
                if stop.is_cancelled() {
                    return source.stop().await.map_err(AppError::from);
                }
                let item = match item {
                    None => break,
                    Some(Ok(item)) => item,
                    Some(Err(error)) => {
                        let _ = source.stop().await;
                        return Err(error.into());
                    }
                };
                // 队列有空位时沿用同步快路径，仅背压时注册取消等待。
                let item = match sender.try_send(item) {
                    Ok(()) => continue,
                    Err(TrySendError::Full(item)) => item,
                    Err(TrySendError::Closed(_)) => {
                        let _ = source.stop().await;
                        return Err(AppError::ConsumerError("Scan traversal consumer stopped".into()));
                    }
                };
                select! {
                    biased;
                    _=stop.cancelled()=>return source.stop().await.map_err(AppError::from),
                    result=sender.send(item)=>if result.is_err(){
                        let _=source.stop().await;
                        return Err(AppError::ConsumerError("Scan traversal consumer stopped".into()));
                    }
                }
            }
            source.finish().await.map_err(AppError::from)
        }));
        Self::Prefetched {
            receiver,
            reader: Some(reader),
            discard,
        }
    }

    pub(super) async fn next(&mut self) -> Option<Result<ResumableScanItem>> {
        match self {
            Self::Direct(Some(source)) => source.next().await.map(|item| item.map_err(AppError::from)),
            Self::Direct(None) => None,
            Self::Finishing(_) => None,
            Self::Prefetched { receiver, .. } => receiver.recv().await.map(Ok),
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

fn missing_outcome() -> AppError {
    AppError::ConsumerError("Scan traversal outcome already consumed".into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::{
        sync::{Notify, oneshot},
        time::{Duration, timeout},
    };

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
            sender.send(ResumableScanItem::Checkpoint(vec![1])).await.unwrap();
            sender.send(ResumableScanItem::Checkpoint(vec![2])).await.unwrap();
            Err(AppError::ScanError("reader failed".into()))
        }));
        let mut input = ScanInput::Prefetched {
            receiver,
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
            reader: Some(reader),
            discard,
        };
        ready.await.unwrap();
        input.stop().await;
        assert!(dropped.load(Ordering::SeqCst));
    }
}
