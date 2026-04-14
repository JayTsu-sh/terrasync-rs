/// 通用异步通道接收器封装
///
/// 基于 `async_channel::Receiver<T>`，天然支持多 worker 并发 clone，
/// 无需外部 Mutex，各 worker 独立等待，消除锁竞争。
pub struct AsyncReceiver<T> {
    pub rx: async_channel::Receiver<T>,
}

impl<T> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self { rx: self.rx.clone() }
    }
}

impl<T> AsyncReceiver<T> {
    pub fn new(rx: async_channel::Receiver<T>) -> Self {
        Self { rx }
    }

    /// 异步获取下一个元素，通道关闭时返回 None
    pub async fn next(&self) -> Option<T> {
        self.rx.recv().await.ok()
    }
}
