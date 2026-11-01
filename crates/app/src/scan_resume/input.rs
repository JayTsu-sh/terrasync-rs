//! 增量比较确认检查点时，有界预读后续遍历项；交付仍保持原始检查点顺序。
use crate::error::{AppError, Result};
use data_mover::traversal::TraversalOutcome;
use storage_adapter::{ResumableScan, ResumableScanItem};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

pub(super) enum ScanInput {
    Direct(Option<ResumableScan>),
    Prefetched {
        receiver: mpsc::Receiver<ResumableScanItem>,
        reader: Option<AbortOnDropHandle<Result<TraversalOutcome>>>,
    },
}

impl ScanInput {
    pub(super) fn new(mut source: ResumableScan, capacity: Option<usize>) -> Self {
        let Some(capacity) = capacity else {
            return Self::Direct(Some(source));
        };
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            while let Some(item) = source.next().await {
                sender
                    .send(item?)
                    .await
                    .map_err(|_| AppError::ConsumerError("Scan traversal consumer stopped".into()))?;
            }
            source.finish().await.map_err(AppError::from)
        }));
        Self::Prefetched {
            receiver,
            reader: Some(reader),
        }
    }

    pub(super) async fn next(&mut self) -> Option<Result<ResumableScanItem>> {
        match self {
            Self::Direct(Some(source)) => source.next().await.map(|item| item.map_err(AppError::from)),
            Self::Direct(None) => None,
            Self::Prefetched { receiver, .. } => receiver.recv().await.map(Ok),
        }
    }

    pub(super) async fn finish(&mut self) -> Result<TraversalOutcome> {
        match self {
            Self::Direct(source) => source
                .take()
                .ok_or_else(missing_outcome)?
                .finish()
                .await
                .map_err(AppError::from),
            Self::Prefetched { reader, .. } => reader.take().ok_or_else(missing_outcome)?.await?,
        }
    }

    pub(super) async fn stop(&mut self) {
        if let Self::Prefetched { reader, .. } = self
            && let Some(reader) = reader.take()
        {
            reader.abort();
            let _ = reader.await;
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
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tokio::sync::oneshot;

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
        let reader = AbortOnDropHandle::new(tokio::spawn(async move {
            let _sender = sender;
            let _guard = Dropped(flag);
            started.send(()).unwrap();
            pending::<Result<TraversalOutcome>>().await
        }));
        let mut input = ScanInput::Prefetched {
            receiver,
            reader: Some(reader),
        };
        ready.await.unwrap();
        input.stop().await;
        assert!(dropped.load(Ordering::SeqCst));
    }
}
