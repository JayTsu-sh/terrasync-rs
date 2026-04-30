use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::trace;

/// 默认广播通道容量（条目数）。
///
/// 所有 job 类型（sync / scan / integrity-check）共享同一缺省值——之前各模块自定义
/// 副本曾导致调整时遗漏；现统一在此处 SSOT。
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

#[derive(Clone)]
pub struct BroadcastForwarder<T> {
    buffer: usize,
    senders: Vec<mpsc::Sender<T>>,
}

impl<T> BroadcastForwarder<T>
where
    T: Clone + Send + std::fmt::Debug + 'static,
{
    pub fn new(buffer: usize) -> Self {
        Self {
            buffer,
            senders: Vec::new(),
        }
    }

    /// 添加一个新消费者，返回对应的 Receiver
    pub fn subscribe(&mut self) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel(self.buffer);
        self.senders.push(tx);
        rx
    }

    /// 广播消息，等待所有消费者通道有空位（背压）。
    ///
    /// 消费者已退出（Receiver 被丢弃）时，对应的发送失败会被静默忽略，
    /// 这与 `tokio::sync::broadcast` 的行为一致。
    /// 当所有 `BroadcastForwarder` 克隆体都被丢弃时，所有消费者的通道自动关闭。
    pub async fn broadcast(&self, msg: T) {
        trace!("Broadcasting message: {:?}", msg);
        let futures: Vec<_> = self.senders.iter().map(|s| s.send(msg.clone())).collect();
        join_all(futures).await;
    }
}
