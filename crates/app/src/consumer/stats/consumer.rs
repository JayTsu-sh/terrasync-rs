//! `StatisticConsumer` — 聚合累计统计、进度条、HTTP 进度回调，对外提供统一 API。
//!
//! 生命周期拆分为三段，便于跨阶段复用同一实例（如增量拷贝 Phase A/B 共享统计累加）：
//! - [`StatisticConsumer::begin`]：一次性启动进度条显示线程 + HTTP 回调轮询任务
//! - [`StatisticConsumer::process`]：消费 `StorageEntryMessage` 通道直到关闭，可重复调用
//! - [`StatisticConsumer::end`]：一次性 abort 回调 + 发送 final 回调 + 打印合并报告
//!
//! 调用方目前均走 `ConsumerManager::begin_lifecycle` / `start_consumers` / `end_lifecycle`
//! 三段式编排；不再暴露一站式 `run`。

// 标准库
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

// 外部crate
use storage_v2::StorageEntryMessage;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, trace, warn};

// 内部模块
use super::accumulator::StatsKind;
use super::progress_bar::ProgressBar;
use super::report::ProgressReport;

/// HTTP 进度回调间隔（秒）
const CALLBACK_INTERVAL_SECS: u64 = 2;

/// 回调守卫：周期性 HTTP 回调任务句柄 + 共享的 `reqwest::Client`（`end()` 时 abort + 复用）。
pub type CallbackGuard = (JoinHandle<()>, reqwest::Client);

#[derive(Debug)]
pub struct StatisticConsumer {
    pub stats: StatsKind,
    pub progress_bar: ProgressBar,
    pub job_dir: String,
    pub callback_url: Option<String>,
    /// progress bar display 线程句柄，`finalize()` join 以确保正常退出
    pub(crate) pb_handle: Option<std::thread::JoinHandle<()>>,
}

impl StatisticConsumer {
    /// 启动终端进度条（后台线程），并将句柄保存在 `self.pb_handle` 中。
    pub fn start_progress_bar(&mut self) {
        self.pb_handle = Some(self.progress_bar.start());
    }

    /// 结束进度条 + 打印最终统计 + join display 线程；调用后 Drop 成为 no-op
    pub fn finalize(&mut self) {
        if let Some(handle) = self.pb_handle.take() {
            self.progress_bar.finish();
            if let Err(e) = handle.join() {
                error!("[ProgressBar] Display thread panicked: {e:?}");
            }
        }
        println!("\n");
        println!("{}", self.stats);
    }

    /// 更新累计统计 + 进度条原子计数器
    pub fn update_statistics(&mut self, message: &StorageEntryMessage) {
        trace!("Updating statistics for message: {:?}", message);
        self.stats.update_from_message(message);
        self.progress_bar.update_statistics(message);
    }

    /// 获取 per-chunk 实时字节计数器（Copy job 使用）
    pub fn get_bytes_tracker(&self) -> Arc<AtomicU64> {
        self.progress_bar.get_real_time_bytes_counter()
    }

    // ─── 报告 ───

    /// 构建进度报告（实时快照 + 可选 `final_stats`）
    pub fn to_report(&self, is_final: bool) -> ProgressReport {
        let mut report = self.progress_bar.to_report(self.stats.job_id());
        report.is_final = is_final;
        if is_final {
            report.final_stats = Some(self.stats.to_final_stats());
        }
        report
    }

    // ─── HTTP 回调 ───

    /// 构建共享的 HTTP 客户端（带超时），供回调循环和最终回调复用
    fn build_callback_client() -> Option<reqwest::Client> {
        match reqwest::Client::builder().timeout(Duration::from_secs(10)).build() {
            Ok(c) => Some(c),
            Err(e) => {
                error!("[ProgressCallback] Failed to build HTTP client, aborting callbacks: {e}");
                None
            }
        }
    }

    /// 启动周期性 HTTP 回调任务（每 2 秒 POST `ProgressReport`）
    ///
    /// 仅在配置了 `callback_url` 时启动。
    /// 返回 ([`JoinHandle`], Client) 供调用方在消息循环结束后 abort 并复用 client；无 URL 返回 None。
    pub async fn start_callback_loop(consumer: Arc<Mutex<Self>>) -> Option<CallbackGuard> {
        let url = {
            let c = consumer.lock().await;
            c.callback_url.clone()
        };

        let url = url?;
        let client = Self::build_callback_client()?;
        info!("[ProgressCallback] Starting callback loop → {url}");
        let sc = consumer.clone();
        let loop_client = client.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(CALLBACK_INTERVAL_SECS));
            loop {
                interval.tick().await;
                let report = {
                    let consumer = sc.lock().await;
                    consumer.to_report(false)
                };
                match loop_client.post(&url).json(&report).send().await {
                    Ok(resp) if !resp.status().is_success() => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_else(|e| format!("<body read error: {e}>"));
                        warn!("[ProgressCallback] POST returned {status}: {body}");
                    }
                    Err(e) => {
                        warn!("[ProgressCallback] POST failed: {e}");
                    }
                    _ => {}
                }
            }
        });
        Some((handle, client))
    }

    /// 发送最终回调（`is_final=true`），优先复用已有 client
    pub async fn send_final_callback(consumer: &Arc<Mutex<Self>>, client: Option<&reqwest::Client>) {
        let (url, report) = {
            let consumer = consumer.lock().await;
            let url = consumer.callback_url.clone();
            let report = consumer.to_report(true);
            (url, report)
        };

        if let Some(ref url) = url {
            let owned;
            let client = match client {
                Some(c) => c,
                None => match Self::build_callback_client() {
                    Some(c) => {
                        owned = c;
                        &owned
                    }
                    None => return,
                },
            };
            match client.post(url).json(&report).send().await {
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_else(|e| format!("<body read error: {e}>"));
                    error!("[ProgressCallback] Final POST returned {status}: {body}");
                }
                Ok(_) => {
                    info!("[ProgressCallback] Final progress report sent successfully (is_final=true)");
                }
                Err(e) => {
                    error!("[ProgressCallback] Final POST failed: {e}");
                }
            }
        }
    }

    // ─── 生命周期拆分 ───

    /// 启动一次性生命周期：进度条显示线程 + HTTP 回调轮询任务。
    /// 与 [`end()`](Self::end) 成对调用；多阶段 pipeline 只调一次 begin。
    pub async fn begin(consumer: Arc<Mutex<Self>>) -> Option<CallbackGuard> {
        {
            let mut c = consumer.lock().await;
            c.start_progress_bar();
        }
        Self::start_callback_loop(consumer).await
    }

    /// 消息循环：消费 rx 直到 channel 关闭。只更新累计统计与进度条，不涉及生命周期。
    ///
    /// 最小化持锁时间：每条消息更新后立即释放，避免 callback loop 饥饿。
    pub async fn process(consumer: Arc<Mutex<Self>>, mut rx: mpsc::Receiver<StorageEntryMessage>) {
        while let Some(message) = rx.recv().await {
            let mut c = consumer.lock().await;
            c.update_statistics(&message);
        }
    }

    /// 结束生命周期：abort HTTP 回调循环 → 发送 final 回调 → finalize（打印报告 + join 显示线程）。
    pub async fn end(consumer: Arc<Mutex<Self>>, callback: Option<CallbackGuard>) {
        let shared_client = if let Some((handle, client)) = callback {
            handle.abort();
            Some(client)
        } else {
            None
        };

        Self::send_final_callback(&consumer, shared_client.as_ref()).await;

        let mut c = consumer.lock().await;
        c.finalize();
    }
}

/// 兜底 join display 线程（panic/提前 drop 路径）。
///
/// display 线程 `finish()` 后最多还要一轮 `DISPLAY_INTERVAL_SECS` 才退出，直接 join 会阻塞
/// 当前线程（可能是 tokio worker）~2s，故派发到独立 std thread。
impl Drop for StatisticConsumer {
    fn drop(&mut self) {
        if let Some(handle) = self.pb_handle.take() {
            self.progress_bar.finish();
            std::thread::spawn(move || {
                if let Err(e) = handle.join() {
                    error!("[ProgressBar] Display thread panicked during drop: {e:?}");
                }
            });
        }
    }
}
