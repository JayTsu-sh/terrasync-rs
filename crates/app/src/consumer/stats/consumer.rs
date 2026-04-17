//! `StatisticConsumer` — 聚合累计统计、进度条、HTTP 进度回调，对外提供统一 API。

// 标准库
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

// 外部crate
use storage_v2::StorageEntryMessage;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

// 内部模块
use super::accumulator::StatsKind;
use super::progress_bar::ProgressBar;
use super::report::ProgressReport;

/// HTTP 进度回调间隔（秒）
const CALLBACK_INTERVAL_SECS: u64 = 2;

#[derive(Debug)]
pub struct StatisticConsumer {
    pub stats: StatsKind,
    pub progress_bar: ProgressBar,
    pub job_dir: String,
    pub callback_url: Option<String>,
    /// progress bar display 线程句柄，`finalize()` join 以确保正常退出
    pub(crate) pb_handle: Option<std::thread::JoinHandle<()>>,
    /// Phase B 增量消息缓冲，由编排器在 Phase B 结束后 drain 批量写入 `incremental_<job>` 表
    pub(crate) phase_b_buffer: Vec<StorageEntryMessage>,
}

impl StatisticConsumer {
    /// 启动终端进度条（后台线程），并将句柄保存在 `self.pb_handle` 中。
    pub fn start_progress_bar(&mut self) {
        self.pb_handle = Some(self.progress_bar.start());
    }

    /// 结束进度条 + 打印最终统计 + join display 线程；调用后 Drop 成为 no-op
    pub fn finalize(&mut self) {
        self.progress_bar.finish();
        println!("\n");
        println!("{}", self.stats);
        if let Some(handle) = self.pb_handle.take() {
            if let Err(e) = handle.join() {
                error!("[ProgressBar] Display thread panicked: {e:?}");
            }
        }
    }

    /// 更新累计统计 + 进度条原子计数器
    pub fn update_statistics(&mut self, message: &StorageEntryMessage) {
        trace!("Updating statistics for message: {:?}", message);
        self.stats.update_from_message(message);
        self.progress_bar.update_statistics(message);
    }

    /// Phase B 专用：更新 stats/progress bar + 缓冲消息以供批量 DB 写入
    pub fn record_phase_b(&mut self, message: StorageEntryMessage) {
        self.update_statistics(&message);
        self.phase_b_buffer.push(message);
    }

    /// 取出 Phase B 缓冲，供 `batch_insert_incremental_record` 批量写入
    pub fn take_phase_b_buffer(&mut self) -> Vec<StorageEntryMessage> {
        std::mem::take(&mut self.phase_b_buffer)
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

    // ─── HTTP 回调（从 manager.rs 移入）───

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
    /// 返回 (`JoinHandle`, Client) 供调用方在消息循环结束后 abort 并复用 client；无 URL 返回 None。
    pub async fn start_callback_loop(
        consumer: Arc<tokio::sync::Mutex<Self>>,
    ) -> Option<(tokio::task::JoinHandle<()>, reqwest::Client)> {
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

    /// 完整运行生命周期：进度条 → 回调循环 → 消息处理 → 停止回调 → 最终回调 → finalize。
    ///
    /// `defer_finalize=true`：run() 返回后显示线程继续运行，编排器在 Phase B 结束后调用
    /// `finalize_stats()` → `finalize()` 合并 Deleted/Renamed 统计并 join。
    pub async fn run(
        consumer: Arc<tokio::sync::Mutex<Self>>, mut rx: mpsc::Receiver<StorageEntryMessage>, defer_finalize: bool,
    ) {
        {
            let mut c = consumer.lock().await;
            c.start_progress_bar();
        }

        let callback = Self::start_callback_loop(consumer.clone()).await;

        while let Some(message) = rx.recv().await {
            // 最小化持锁时间：更新后立即释放，避免 callback loop 饥饿
            {
                let mut c = consumer.lock().await;
                c.update_statistics(&message);
            }
        }

        let shared_client = if let Some((handle, client)) = callback {
            handle.abort();
            Some(client)
        } else {
            None
        };

        Self::send_final_callback(&consumer, shared_client.as_ref()).await;

        if !defer_finalize {
            let mut c = consumer.lock().await;
            c.finalize();
        }
    }

    /// 发送最终回调（`is_final=true`），优先复用已有 client
    pub async fn send_final_callback(consumer: &Arc<tokio::sync::Mutex<Self>>, client: Option<&reqwest::Client>) {
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
