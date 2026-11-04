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
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

// 外部crate
use data_mover::model::{EntryKind, ObservedEntry};
use reqwest::Response;
use storage_adapter::StorageEntryMessage;
use storage_adapter::observed_scan::{EntryStatistics, ObservedScanEvent, ObservedView};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{error, info, trace, warn};

// 内部模块
use super::accumulator::StatsKind;
use super::callback_state::{CallbackState, ScanRuntimePhase, errors};
use super::diagnostics::{CallbackAck, safe_diagnostic_text};
use super::progress_bar::ProgressBar;
use super::report::ProgressDetail;
use super::report::ProgressReport;
use crate::scan::statistics::{ScanCounts, ScanSnapshot};

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
    /// 纯增量扫描的 generation 是否成功提交；其他任务保持 `None`。
    pub generation_committed: Option<bool>,
    pub scan_snapshot: Option<ScanSnapshot>,
    pub callback: CallbackState,
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
        if let Some(scan) = &self.scan_snapshot {
            println!("{scan}\nErrors: {}", errors(&self.stats).total());
        } else {
            println!("{}", self.stats);
        }
    }

    /// 更新累计统计 + 进度条原子计数器
    pub fn update_observed_entry(&mut self, entry: &ObservedEntry) {
        if let Some(scan) = &self.scan_snapshot {
            if scan.distributions_complete && entry.history().is_none() {
                let statistics = match &mut self.stats {
                    StatsKind::Full(value) => value,
                    StatsKind::Incremental(value) => &mut value.scanned,
                };
                for view in ObservedView::all(entry) {
                    statistics.update_distributions(&view, entry.kind() == EntryKind::File);
                }
            }
            return;
        }
        for view in ObservedView::all(entry) {
            match &mut self.stats {
                StatsKind::Full(stats) => stats.update(&view),
                StatsKind::Incremental(stats) => stats.scanned.update(&view),
            }
            self.progress_bar.update_observed("scanned", &view);
        }
    }
    pub(crate) fn clear_scan_distributions(&mut self) {
        let value = match &mut self.stats {
            StatsKind::Full(value) => value,
            StatsKind::Incremental(value) => &mut value.scanned,
        };
        value.dir_size = 0;
        value.symlink_size = 0;
        value.total_dir_depth = 0;
        value.max_dir_depth = 0;
        value.total_name_length = 0;
        value.max_name_length = 0;
        value.extension_stats.clear();
        value.time_range_stats.clear();
        value.file_size_range_stats.clear();
    }
    pub(crate) fn sync_scan_progress(&mut self) {
        if let Some(scan) = &self.scan_snapshot {
            let statistics = match &mut self.stats {
                StatsKind::Full(value) => value,
                StatsKind::Incremental(value) => &mut value.scanned,
            };
            statistics.dir_count = scan.counts.retained.directories as usize;
            statistics.regular_file_count = scan.counts.retained.regular_files as usize;
            statistics.symlink_count = scan.counts.retained.symlinks as usize;
            statistics.regular_file_size = scan.counts.regular_file_bytes as i64;
            if !scan.distributions_complete && !scan.distributions_reconciled {
                statistics.extension_stats.clear();
                statistics.time_range_stats.clear();
                statistics.file_size_range_stats.clear();
            }
            self.progress_bar.update_scan(scan);
            if let StatsKind::Incremental(stats) = &self.stats {
                self.progress_bar.update_scan_events(stats);
            }
        }
    }
    pub fn update_observed_event(&mut self, event: &ObservedScanEvent) {
        if matches!(event, ObservedScanEvent::Candidate(_)) {
            return;
        }
        self.update_recorded_event(event.operation(), &event.item().view());
    }
    pub fn update_recorded_event(&mut self, operation: &str, entry: &impl EntryStatistics) {
        if let StatsKind::Incremental(stats) = &mut self.stats {
            match operation {
                "new" => stats.new.add(entry),
                "deleted" => stats.deleted.add(entry),
                "rename" => stats.renamed.add(entry),
                _ => stats.changed.add(entry),
            }
        }
        self.progress_bar.update_observed(operation, entry);
    }

    pub fn update_statistics(&mut self, message: &StorageEntryMessage) {
        trace!("Updating statistics for message: {:?}", message);
        self.stats.update_from_message(message);
        self.progress_bar.update_statistics(message);
        if let StorageEntryMessage::Error {
            event, path, reason, ..
        } = message
        {
            let reason = safe_diagnostic_text(reason);
            self.callback
                .outbox
                .enqueue("operation", &format!("{event:?}"), &reason, path.to_str());
            self.callback.runtime.current_error = Some(reason);
            self.callback.runtime.state = ScanRuntimePhase::Failed;
        }
    }

    /// 获取 per-chunk 实时字节计数器（Copy job 使用）
    pub fn get_bytes_tracker(&self) -> Arc<AtomicU64> {
        self.progress_bar.get_real_time_bytes_counter()
    }

    // ─── 报告 ───

    /// 构建进度报告（实时快照 + 可选 `final_stats`）
    pub fn to_report(&self, is_final: bool) -> ProgressReport {
        self.report_for_scan(is_final, self.scan_snapshot.as_ref())
    }
    pub(crate) fn to_report_for_scan_prefix(&self, counts: &ScanCounts) -> ProgressReport {
        let mut snapshot = self.scan_snapshot.clone();
        if let Some(scan) = &mut snapshot {
            scan.counts = counts.clone();
            scan.scanned_entries = counts.scanned.total();
            scan.filtered_entries = counts.filtered.total();
            scan.retained_entries = counts.retained.total();
            scan.distributions_complete = counts.retained.total() == 0;
        }
        self.report_for_scan(false, snapshot.as_ref())
    }
    fn report_for_scan(&self, is_final: bool, snapshot: Option<&ScanSnapshot>) -> ProgressReport {
        let mut report = self.progress_bar.to_report(self.stats.job_id());
        report.is_final = is_final;
        report.generation_committed = self.generation_committed;
        report.scan = snapshot.cloned();
        report.callback_identity = Some(self.callback.identity.clone());
        if snapshot.is_some() {
            report.scan_runtime = Some(self.callback.runtime.clone());
            if let Some(scan) = snapshot {
                let counts = scan.counts.retained;
                match &mut report.detail {
                    ProgressDetail::Full {
                        scanned_files,
                        scanned_dirs,
                        total_bytes,
                        error_count,
                        ..
                    } => {
                        *scanned_files = counts.regular_files + counts.symlinks;
                        *scanned_dirs = counts.directories;
                        *total_bytes = scan.counts.regular_file_bytes;
                        *error_count = errors(&self.stats).total() as u64;
                    }
                    ProgressDetail::Incremental {
                        scanned_files,
                        scanned_dirs,
                        scanned_symlinks,
                        scanned_size,
                        error_count,
                        ..
                    } => {
                        *scanned_files = counts.regular_files;
                        *scanned_dirs = counts.directories;
                        *scanned_symlinks = counts.symlinks;
                        *scanned_size = scan.counts.regular_file_bytes;
                        *error_count = errors(&self.stats).total() as u64;
                    }
                }
            }
        }
        if is_final {
            report.final_stats = Some(self.stats.to_final_stats());
            if let (Some(scan), Some(final_stats)) = (snapshot, &mut report.final_stats) {
                final_stats.dirs = scan.counts.retained.directories as usize;
                final_stats.regular_files = scan.counts.retained.regular_files as usize;
                final_stats.symlinks = scan.counts.retained.symlinks as usize;
                final_stats.regular_file_size = scan.counts.regular_file_bytes as i64;
                if !scan.distributions_reconciled || scan.counts.versions > 0 || scan.counts.delete_markers > 0 {
                    final_stats.dir_size = None;
                    final_stats.symlink_size = None;
                }
                let ordinary = scan.counts.retained.directories
                    + scan.counts.retained.regular_files
                    + scan.counts.retained.symlinks;
                if !scan.complete
                    || scan.counts.versions > 0
                    || scan.counts.delete_markers > 0
                    || ordinary != scan.counts.retained.total()
                    || (!scan.distributions_complete && !scan.distributions_reconciled)
                {
                    // 旧分布按版本/交付流累积，不能冒充去重后的逻辑 entry 分布。
                    final_stats.extension_stats = None;
                    final_stats.time_range_stats = None;
                    final_stats.file_size_range_stats = None;
                }
            }
        }
        report
    }

    async fn post_callback(consumer: &Arc<Mutex<Self>>, client: &reqwest::Client, url: &str, is_final: bool) -> bool {
        let report = {
            let mut consumer = consumer.lock().await;
            consumer.callback.identity.sequence = consumer.callback.identity.sequence.saturating_add(1);
            let batch = consumer.callback.outbox.prepare_batch();
            let mut report = consumer.to_report(is_final);
            report.diagnostics = batch;
            report
        };
        let response = match client
            .post(url)
            .json(&report)
            .send()
            .await
            .and_then(Response::error_for_status)
        {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "Progress callback failed; batch retained");
                return false;
            }
        };
        let Some(batch) = report.diagnostics else { return true };
        let ack = match response.json::<CallbackAck>().await {
            Ok(ack) if ack.batch_id.as_deref() == Some(batch.batch_id.as_str()) => ack,
            _ => {
                warn!("Progress callback did not acknowledge the diagnostic batch");
                return false;
            }
        };
        if let Some(batch_id) = ack.batch_id {
            consumer.lock().await.callback.outbox.ack(&batch_id);
        }
        true
    }

    // ─── HTTP 回调 ───

    fn callback_is_loopback(url: &str) -> bool {
        let Ok(parsed) = reqwest::Url::parse(url) else {
            return false;
        };
        let Some(host) = parsed.host_str() else {
            return false;
        };
        let address_host = host.trim_start_matches('[').trim_end_matches(']');
        host.eq_ignore_ascii_case("localhost")
            || address_host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    }

    /// 构建共享的 HTTP 客户端（带超时），供回调循环和最终回调复用。
    /// 本机回调绕过系统代理，外部回调继续遵循代理环境变量。
    fn build_callback_client(url: &str) -> Option<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if Self::callback_is_loopback(url) {
            builder = builder.no_proxy();
        }
        match builder.build() {
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
        let client = Self::build_callback_client(&url)?;
        info!("[ProgressCallback] Starting callback loop → {url}");
        let sc = consumer.clone();
        let loop_client = client.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(CALLBACK_INTERVAL_SECS));
            loop {
                interval.tick().await;
                Self::post_callback(&sc, &loop_client, &url, false).await;
            }
        });
        Some((handle, client))
    }

    /// 发送最终回调（`is_final=true`），优先复用已有 client
    pub async fn send_final_callback(consumer: &Arc<Mutex<Self>>, client: Option<&reqwest::Client>) {
        let url = consumer.lock().await.callback_url.clone();
        let Some(url) = url else {
            return;
        };
        let owned;
        let client = match client {
            Some(client) => client,
            None => {
                let Some(client) = Self::build_callback_client(&url) else {
                    return;
                };
                owned = client;
                &owned
            }
        };
        loop {
            if !Self::post_callback(consumer, client, &url, true).await {
                break;
            }
            if consumer.lock().await.callback.outbox.prepare_batch().is_none() {
                break;
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
        let budget = consumer
            .lock()
            .await
            .scan_snapshot
            .as_ref()
            .map(|_| Duration::from_secs(2));
        Self::end_with_budget(consumer, callback, budget).await;
    }

    pub(crate) async fn end_cancelled(consumer: Arc<Mutex<Self>>, callback: Option<CallbackGuard>) {
        Self::end_with_budget(consumer, callback, Some(Duration::from_millis(500))).await;
    }

    async fn end_with_budget(consumer: Arc<Mutex<Self>>, callback: Option<CallbackGuard>, budget: Option<Duration>) {
        let shared_client = if let Some((handle, client)) = callback {
            handle.abort();
            let _ = handle.await;
            Some(client)
        } else {
            None
        };

        let callback = Self::send_final_callback(&consumer, shared_client.as_ref());
        if let Some(budget) = budget {
            if timeout(budget, callback).await.is_err() {
                warn!("[ProgressCallback] Cancelled scan final report exceeded its time budget");
            }
        } else {
            callback.await;
        }

        let mut c = consumer.lock().await;
        c.finalize();
    }
}

#[cfg(test)]
mod tests {
    use super::StatisticConsumer;
    use crate::config::JobType;
    use crate::consumer::stats::{FullStats, IncrementalStats, ProgressBar, StatsKind};
    use crate::error::Result;
    use std::{
        future::pending,
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::{net::TcpListener, sync::Mutex};

    #[tokio::test]
    async fn cancelled_lifecycle_does_not_wait_for_a_stalled_final_callback() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let stalled = tokio::spawn(async move {
            let _connection = listener.accept().await;
            pending::<()>().await;
        });
        let consumer = Arc::new(Mutex::new(StatisticConsumer {
            stats: StatsKind::Full(FullStats::new(
                JobType::Scan,
                "cancel_fixture".into(),
                String::new(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::Scan),
            job_dir: String::new(),
            callback_url: Some(format!("http://{address}/progress")),
            generation_committed: Some(false),
            pb_handle: None,
            scan_snapshot: None,
            callback: Default::default(),
        }));
        consumer.lock().await.start_progress_bar();
        let started = Instant::now();
        StatisticConsumer::end_cancelled(consumer.clone(), None).await;
        stalled.abort();
        let _ = stalled.await;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(
            consumer.lock().await.pb_handle.is_none(),
            "display thread must also be joined"
        );
        Ok(())
    }

    #[test]
    fn callback_proxy_bypass_is_limited_to_loopback_hosts() {
        for url in [
            "http://localhost:3000/callback",
            "http://127.0.0.1:3000/callback",
            "http://[::1]:3000/callback",
        ] {
            assert!(
                StatisticConsumer::callback_is_loopback(url),
                "expected loopback URL: {url}"
            );
        }

        for url in [
            "https://callbacks.example.com/progress",
            "http://10.10.1.12:9000/callback",
            "not a URL",
        ] {
            assert!(
                !StatisticConsumer::callback_is_loopback(url),
                "expected external URL: {url}"
            );
        }
    }

    #[test]
    fn final_report_exposes_incremental_scan_generation_commit_status() {
        let mut consumer = StatisticConsumer {
            stats: StatsKind::Incremental(IncrementalStats::new(
                JobType::IncrementalScan,
                "scan-job".to_string(),
                "scan".to_string(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::IncrementalScan),
            job_dir: String::new(),
            callback_url: None,
            generation_committed: Some(false),
            pb_handle: None,
            scan_snapshot: None,
            callback: Default::default(),
        };

        assert_eq!(consumer.to_report(true).generation_committed, Some(false));
        consumer.generation_committed = Some(true);
        assert_eq!(consumer.to_report(true).generation_committed, Some(true));
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

#[cfg(test)]
#[path = "callback_tests.rs"]
mod callback_tests;
