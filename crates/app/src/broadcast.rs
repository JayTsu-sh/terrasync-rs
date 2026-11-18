use futures::future::try_join_all;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use thiserror::Error;
use tokio::sync::{mpsc, mpsc::error::SendError};
use tracing::trace;

/// 默认广播通道容量（条目数）。
///
/// 所有 job 类型（sync / scan / integrity-check）共享同一缺省值——之前各模块自定义
/// 副本曾导致调整时遗漏；现统一在此处 SSOT。
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

#[derive(Error)]
#[error("required consumer {consumer_index} disconnected: {source}")]
pub struct BroadcastError<T> {
    pub consumer_index: usize,
    #[source]
    pub source: SendError<T>,
}

impl<T> fmt::Debug for BroadcastError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 消息可能带完整元数据，错误诊断不输出载荷。
        formatter
            .debug_struct("BroadcastError")
            .field("consumer_index", &self.consumer_index)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct BroadcastForwarder<T> {
    buffer: usize,
    senders: Vec<mpsc::Sender<T>>,
    failed_consumer: Arc<AtomicUsize>,
}

impl<T> BroadcastForwarder<T>
where
    T: Clone + Send + std::fmt::Debug + 'static,
{
    pub fn new(buffer: usize) -> Self {
        Self {
            buffer,
            senders: Vec::new(),
            failed_consumer: Arc::new(AtomicUsize::new(usize::MAX)),
        }
    }

    /// 添加一个新消费者，返回对应的 Receiver
    pub fn subscribe(&mut self) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel(self.buffer);
        self.senders.push(tx);
        rx
    }

    pub fn is_closed(&self) -> bool {
        self.failed_consumer.load(Ordering::Relaxed) != usize::MAX
    }

    /// 广播消息，等待所有消费者通道有空位（背压）。
    ///
    /// 必需消费者退出立即返回类型化错误，不等待其他已满通道。
    /// 已接收消息的消费者不会回滚；调用方必须阻止后续成功及源删除阶段。
    /// 当所有 `BroadcastForwarder` 克隆体都被丢弃时，所有消费者的通道自动关闭。
    pub async fn broadcast(&self, msg: T) -> Result<(), BroadcastError<T>> {
        let consumer_index = self.failed_consumer.load(Ordering::Relaxed);
        if consumer_index != usize::MAX {
            return Err(BroadcastError {
                consumer_index,
                source: SendError(msg),
            });
        }
        trace!("Broadcasting message: {:?}", msg);
        let sends = self.senders.iter().enumerate().map(|(consumer_index, sender)| {
            let message = msg.clone();
            async move {
                sender
                    .send(message)
                    .await
                    .map_err(|source| BroadcastError { consumer_index, source })
            }
        });
        match try_join_all(sends).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let _ = self.failed_consumer.compare_exchange(
                    usize::MAX,
                    error.consumer_index,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn closed_consumer_fails_while_another_queue_is_full_and_preserves_failed_message() {
        let mut forwarder = BroadcastForwarder::new(1);
        let mut full = forwarder.subscribe();
        let closed = forwarder.subscribe();
        forwarder.broadcast("first").await.unwrap();
        let peer = forwarder.clone();
        drop(closed);
        let failure = timeout(Duration::from_millis(100), forwarder.broadcast("credential-canary"))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(failure.consumer_index, 1);
        assert!(peer.is_closed());
        assert_eq!(peer.broadcast("later").await.unwrap_err().consumer_index, 1);
        assert_eq!(failure.source.0, "credential-canary");
        assert!(!format!("{failure:?} {failure}").contains("credential-canary"));
        assert_eq!(full.recv().await, Some("first"));
        assert!(
            full.try_recv().is_err(),
            "blocked send must be cancelled on peer failure"
        );
    }

    #[tokio::test]
    async fn slow_consumers_keep_backpressure_and_cancelled_waits_do_not_deliver_later() {
        let mut forwarder = BroadcastForwarder::new(1);
        let mut first = forwarder.subscribe();
        let mut second = forwarder.subscribe();
        forwarder.broadcast(1).await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), forwarder.broadcast(2))
                .await
                .is_err()
        );
        assert_eq!(first.recv().await, Some(1));
        assert_eq!(second.recv().await, Some(1));
        forwarder.broadcast(3).await.unwrap();
        assert_eq!(first.recv().await, Some(3));
        assert_eq!(second.recv().await, Some(3));
        drop(forwarder);
        assert_eq!(first.recv().await, None);
        assert_eq!(second.recv().await, None);
    }
}
