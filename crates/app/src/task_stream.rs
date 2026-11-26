use data_mover::traversal::{ScanFailureSignal, TraversalRecoveryStatus};
use std::sync::Arc;

use crate::error::{AppError, Result, StorageError};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

struct Producer {
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

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
    producer: Option<Arc<Producer>>,
}

impl<T> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            error: self.error.clone(),
            guard: self.guard.clone(),
            recovery: self.recovery.clone(),
            producer: self.producer.clone(),
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
            producer: None,
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

    pub(crate) fn with_producer(mut self, task: JoinHandle<()>, cancel: CancellationToken) -> Self {
        self.producer = Some(Arc::new(Producer {
            cancel,
            task: Mutex::new(Some(task)),
        }));
        self
    }

    /// 协调者关闭流后等待生产任务和其原生遍历收束，再读取终态错误。
    pub(crate) async fn wait_for_producer(&self) {
        if let Some(producer) = &self.producer {
            // 锁跨 await 持有，所有 clone 的等待都必须观察到同一次 join 已完成。
            let mut task = producer.task.lock().await;
            if let Some(handle) = task.as_mut() {
                if let Err(error) = handle.await {
                    let mut stored = self.error.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    // 关闭缓冲产生的取消不能遮住 panic；已有真实错误与 join 原因一起保留。
                    *stored = Some(match stored.take() {
                        Some(preceding) if !preceding.is_cancelled() => StorageError::StreamProducerFailed {
                            source: error,
                            preceding: Box::new(preceding),
                        },
                        _ => error.into(),
                    });
                }
                *task = None;
            }
        }
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
        if let Some(producer) = &self.producer {
            if !self.rx.is_empty() {
                self.error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert(StorageError::Cancelled);
            }
            self.rx.close();
            producer.cancel.cancel();
        } else {
            self.rx.close();
        }
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
        if let Some(producer) = &self.producer {
            tokio::select! {
                biased;
                () = producer.cancel.cancelled() => { self.close(); None }
                item = self.receive() => item,
            }
        } else {
            self.receive().await
        }
    }

    async fn receive(&self) -> Option<T> {
        if let Some(guard) = &self.guard {
            tokio::select! {
                biased;
                () = guard.failed() => { self.close(); None }
                item = self.rx.recv() => item.ok(),
            }
        } else {
            self.rx.recv().await.ok()
        }
    }
}

/// 完整等待后台任务；校验等非迁移入口仍返回首个错误。
pub(crate) async fn finish_phase(tasks: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    drain_phase(tasks).await.into_iter().next().map_or(Ok(()), Err)
}

/// 迁移仅在所有阶段任务成功退出后推进，并保留每个任务的终态。
pub(crate) async fn finish_copy_phase(tasks: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    combine_copy_results(drain_phase(tasks).await.into_iter().map(Err))
}

async fn drain_phase(tasks: Vec<JoinHandle<Result<()>>>) -> Vec<AppError> {
    let mut failures = Vec::new();
    for task in tasks {
        let result = match task.await {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            error!(%error, "Catalogue phase task failed");
            failures.push(error);
        }
    }
    failures
}

/// 结果数由后台角色和 worker 数量决定，不随条目数增长。
pub(crate) fn combine_copy_results(results: impl IntoIterator<Item = Result<()>>) -> Result<()> {
    let mut failures = results.into_iter().filter_map(Result::err);
    let Some(source) = failures.next() else {
        return Ok(());
    };
    let additional: Vec<_> = failures.collect();
    if additional.is_empty() {
        return Err(source);
    }
    Err(AppError::CopyPhaseFailures {
        source: Box::new(source),
        additional,
    })
}

#[cfg(test)]
#[path = "task_stream/producer_tests.rs"]
mod producer_tests;
