//! 必要消费者的有界可靠分发；入队确认与检查点处理确认分开。
use crate::error::{AppError, Result};
use tokio::sync::{mpsc, oneshot};

pub(crate) enum Delivery<T> {
    Data(T),
    Barrier(oneshot::Sender<()>),
}
struct Subscriber<T> {
    name: &'static str,
    sender: mpsc::Sender<Delivery<T>>,
}
pub(crate) struct ObservedForwarder<T> {
    subscribers: Vec<Subscriber<T>>,
}
impl<T: Clone> ObservedForwarder<T> {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: Vec::new(),
        }
    }
    pub(crate) fn subscribe(&mut self, name: &'static str, capacity: usize) -> mpsc::Receiver<Delivery<T>> {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        self.subscribers.push(Subscriber { name, sender });
        receiver
    }
    pub(crate) async fn send(&self, data: T) -> Result<()> {
        // 先取得所有队列的空位，再同步投递；不能静默跳过退出的必要消费者。
        let mut permits = Vec::with_capacity(self.subscribers.len());
        for subscriber in &self.subscribers {
            permits.push(subscriber.sender.reserve().await.map_err(|_| closed(subscriber.name))?);
        }
        for permit in permits {
            permit.send(Delivery::Data(data.clone()));
        }
        Ok(())
    }
    pub(crate) async fn barrier(&self) -> Result<()> {
        let mut acknowledgements = Vec::with_capacity(self.subscribers.len());
        for subscriber in &self.subscribers {
            let (tx, rx) = oneshot::channel();
            subscriber
                .sender
                .send(Delivery::Barrier(tx))
                .await
                .map_err(|_| closed(subscriber.name))?;
            acknowledgements.push((subscriber.name, rx));
        }
        for (name, ack) in acknowledgements {
            ack.await.map_err(|_| closed(name))?;
        }
        Ok(())
    }
}
fn closed(name: &str) -> AppError {
    AppError::ConsumerError(format!("Required scan consumer {name} stopped before acknowledgement"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};
    #[tokio::test]
    async fn every_consumer_receives_the_same_shared_entry_in_order() -> Result<()> {
        let mut forwarder = ObservedForwarder::new();
        let mut a = forwarder.subscribe("a", 2);
        let mut b = forwarder.subscribe("b", 2);
        let entry = Arc::new(7);
        forwarder.send(entry.clone()).await?;
        for rx in [&mut a, &mut b] {
            let Some(Delivery::Data(received)) = rx.recv().await else {
                panic!("missing entry")
            };
            assert!(Arc::ptr_eq(&entry, &received));
        }
        Ok(())
    }
    #[tokio::test]
    async fn slowest_consumer_applies_backpressure_without_serial_processing() -> Result<()> {
        let mut forwarder = ObservedForwarder::new();
        let mut fast = forwarder.subscribe("fast", 2);
        let mut slow = forwarder.subscribe("slow", 1);
        forwarder.send(1).await?;
        assert!(matches!(fast.recv().await, Some(Delivery::Data(1))));
        assert!(timeout(Duration::from_millis(20), forwarder.send(2)).await.is_err());
        assert!(fast.try_recv().is_err());
        assert!(matches!(slow.recv().await, Some(Delivery::Data(1))));
        forwarder.send(2).await?;
        assert!(matches!(fast.recv().await, Some(Delivery::Data(2))));
        assert!(matches!(slow.recv().await, Some(Delivery::Data(2))));
        Ok(())
    }
    #[tokio::test]
    async fn barrier_waits_for_every_consumer_and_propagates_missing_ack() -> Result<()> {
        let mut forwarder: ObservedForwarder<u8> = ObservedForwarder::new();
        let mut a = forwarder.subscribe("a", 2);
        let mut b = forwarder.subscribe("b", 2);
        let task = async {
            let Some(Delivery::Barrier(ack_a)) = a.recv().await else {
                panic!("missing barrier")
            };
            let Some(Delivery::Barrier(ack_b)) = b.recv().await else {
                panic!("missing barrier")
            };
            let _ = ack_a.send(());
            drop(ack_b);
        };
        let (result, ()) = tokio::join!(forwarder.barrier(), task);
        assert!(result.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn exited_required_consumer_is_fatal() -> Result<()> {
        let mut forwarder = ObservedForwarder::new();
        let _fast = forwarder.subscribe("fast", 1);
        let slow = forwarder.subscribe("clickhouse", 1);
        drop(slow);
        assert!(forwarder.send(1).await.is_err());
        Ok(())
    }
}

#[cfg(test)]
mod barrier_order_tests {
    use super::*;
    use tokio::time::{Duration, timeout};
    #[tokio::test]
    async fn checkpoint_cannot_pass_a_consumer_still_processing_the_previous_entry() -> Result<()> {
        let mut forwarder = ObservedForwarder::new();
        let mut fast = forwarder.subscribe("statistics", 2);
        let mut slow = forwarder.subscribe("clickhouse", 2);
        forwarder.send(42).await?;
        let barrier = forwarder.barrier();
        tokio::pin!(barrier);
        assert!(timeout(Duration::from_millis(10), &mut barrier).await.is_err());
        assert!(matches!(fast.recv().await, Some(Delivery::Data(42))));
        let Some(Delivery::Barrier(fast_ack)) = fast.recv().await else {
            panic!("statistics barrier missing")
        };
        let _ = fast_ack.send(());
        assert!(timeout(Duration::from_millis(10), &mut barrier).await.is_err());
        assert!(matches!(slow.recv().await, Some(Delivery::Data(42))));
        let Some(Delivery::Barrier(slow_ack)) = slow.recv().await else {
            panic!("database barrier missing")
        };
        let _ = slow_ack.send(());
        barrier.await?;
        Ok(())
    }
}
