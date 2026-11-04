use data_mover::traversal::{ScanFailureSignal, TraversalRecoveryStatus};

use crate::StorageError;

/// 通用异步通道接收器封装。
///
/// 基于 [`async_channel::Receiver<T>`]，天然支持多 worker 并发 clone，
/// 无需外部 Mutex，各 worker 独立等待，消除锁竞争。
///
/// 通道关闭（所有发送端 drop）时 [`next`](AsyncReceiver::next) 返回 `None`，
/// 可作为流式迭代的自然终止信号。
///
/// # 线程安全
///
/// `AsyncReceiver<T>` 可在线程间 Send + Sync（与 `async_channel::Receiver` 一致）。
pub struct AsyncReceiver<T> {
    rx: async_channel::Receiver<T>,
    error: std::sync::Arc<std::sync::Mutex<Option<StorageError>>>,
    guard: Option<ScanFailureSignal>,
    recovery: Option<TraversalRecoveryStatus>,
}

impl<T> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            error: self.error.clone(),
            guard: self.guard.clone(),
            recovery: self.recovery.clone(),
        }
    }
}

impl<T> AsyncReceiver<T> {
    #[must_use]
    pub fn new(rx: async_channel::Receiver<T>) -> Self {
        Self {
            rx,
            error: Default::default(),
            guard: None,
            recovery: None,
        }
    }

    /// 连接生产者共享的首个终态错误；消费结束后由调用方读取 [`Self::take_error`]。
    #[must_use]
    pub fn with_error(mut self, error: std::sync::Arc<std::sync::Mutex<Option<StorageError>>>) -> Self {
        self.error = error;
        self
    }

    pub(crate) fn with_scan_failure(
        mut self, error: std::sync::Arc<std::sync::Mutex<Option<StorageError>>>, guard: ScanFailureSignal,
    ) -> Self {
        self.error = error;
        self.guard = Some(guard);
        self
    }

    pub(crate) fn with_recovery(mut self, recovery: Option<TraversalRecoveryStatus>) -> Self {
        self.recovery = recovery;
        self
    }

    /// 供调用方展示正在恢复和恢复成功的状态。
    pub fn recovery_status(&self) -> Option<&TraversalRecoveryStatus> {
        self.recovery.as_ref()
    }

    /// 允许扫描协调层独立中止背压及消费者排空等待。
    pub fn failure_signal(&self) -> Option<ScanFailureSignal> {
        self.guard.clone()
    }

    /// 停止生产者向本轮已结束的扫描继续发送结果。
    pub fn close(&self) {
        self.rx.close();
    }

    /// worker 结束后读取最终错误；连接故障不依赖缓冲消息是否已排空。
    pub fn take_error(&self) -> Option<StorageError> {
        if let Some(failure) = self.guard.as_ref().and_then(ScanFailureSignal::failure) {
            return Some(failure.into());
        }
        self.error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// 异步获取下一个元素，通道关闭时返回 None
    pub async fn next(&self) -> Option<T> {
        if let Some(guard) = &self.guard {
            tokio::select! {
                biased;
                () = guard.failed() => { self.rx.close(); None }
                item = self.rx.recv() => item.ok(),
            }
        } else {
            self.rx.recv().await.ok()
        }
    }
}
