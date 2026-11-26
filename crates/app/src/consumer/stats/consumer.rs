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
use crate::copy_bytes::CopyBytes;
use std::sync::Arc;
use std::time::Duration;

// 外部crate
use data_mover::model::{EntryKind, ObservedEntry};
use reqwest::redirect::Policy;
use task_model::migration::MigrationReport;
use task_model::observation::{EntryProgress, EntryStatistics, ObservedScanEvent, ObservedView};
use task_model::{ErrorEvent, GenerationCommitResult, RetentionScope, StorageEntryMessage};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tracing::{error, info, trace, warn};

// 内部模块
use super::accumulator::StatsKind;
use super::callback_state::{CallbackState, errors};
use super::delivery::{self, DeliveryError, PreparedCallback};
use super::journal;
use super::progress_bar::ProgressBar;
use crate::callback::CallbackEndpoint;
use crate::config::JobType;
use crate::error::{AppError, Result};
use crate::report::{ProgressDetail, ProgressReport, ScanRuntimePhase};
use crate::scan::statistics::{ScanCounts, ScanSnapshot};
use db::ClickHouseDatabase;
use task_model::diagnostics::{MAX_DIAGNOSTIC_BATCH_EVENTS, safe_diagnostic_text};

/// HTTP 进度回调间隔（秒）
const CALLBACK_INTERVAL_SECS: u64 = 2;

/// 回调守卫：周期性 HTTP 回调任务句柄 + 共享的 `reqwest::Client`（`end()` 时 abort + 复用）。
pub type CallbackGuard = (JoinHandle<()>, reqwest::Client);

#[derive(Debug)]
pub struct StatisticConsumer {
    pub stats: StatsKind,
    pub progress_bar: ProgressBar,
    pub job_dir: String,
    pub callback_url: Option<CallbackEndpoint>,
    /// 持久化任务的代次提交结果。
    pub generation_commit_result: Option<GenerationCommitResult>,
    pub migration: Option<MigrationReport>,
    pub scan_snapshot: Option<ScanSnapshot>,
    pub callback: CallbackState,
    /// progress bar display 线程句柄，`finalize()` join 以确保正常退出
    pub(crate) pb_handle: Option<std::thread::JoinHandle<()>>,
}

impl StatisticConsumer {
    pub(crate) async fn record_message(consumer: &Arc<Mutex<Self>>, message: &StorageEntryMessage) -> Result<()> {
        match journal::record(consumer, message).await {
            Ok(()) => Ok(()),
            Err(error) => {
                Self::diagnostic_failure(consumer, error).await;
                Err(AppError::DiagnosticJournalStopped {
                    source: consumer
                        .lock()
                        .await
                        .callback
                        .journal
                        .terminal
                        .clone()
                        .unwrap_or_else(|| Arc::new(AppError::DiagnosticDeliveryUnavailable)),
                })
            }
        }
    }
    /// 启动终端进度条（后台线程），并将句柄保存在 `self.pb_handle` 中。
    pub fn start_progress_bar(&mut self) {
        self.pb_handle = Some(self.progress_bar.start());
    }

    /// 结束进度条 + 打印最终统计 + join display 线程；调用后 Drop 成为 no-op
    pub fn finalize(&mut self) {
        if let Some(handle) = self.pb_handle.take() {
            self.progress_bar.finish();
            handle.thread().unpark();
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
        if let Some(outcome) = self.generation_commit_result {
            println!("Generation commit result: {outcome}");
        }
        if self.callback.task_error_count > 0 {
            println!("Task errors: {}", self.callback.task_error_count);
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
            self.update_scanned_view(view);
        }
    }
    pub(crate) fn update_scanned_view(&mut self, view: ObservedView<'_>) {
        match &mut self.stats {
            StatsKind::Full(stats) => stats.update(&view),
            StatsKind::Incremental(stats) => stats.scanned.update(&view),
        }
        self.progress_bar.update_observed("scanned", &view);
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
            statistics.set_scan_counts(&scan.counts);
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
        if let StorageEntryMessage::Scanned(entry) = message
            && let StatsKind::Incremental(stats) = &mut self.stats
            && matches!(stats.job_type, JobType::IncrementalCopy)
        {
            let inventory =
                EntryProgress::copy_inventory(entry, self.migration.as_ref().is_some_and(|plan| !plan.target_history));
            stats.scanned.update(&inventory);
            self.progress_bar.update_observed("scanned", &inventory);
            return;
        }
        self.stats.update_from_message(message);
        if let StorageEntryMessage::TargetPreserved { retention, reason } = message {
            if retention.scope == RetentionScope::Entry {
                self.callback.target_preserved_count = self.callback.target_preserved_count.saturating_add(1);
            }
            let retained = self.callback.outbox.enqueue_retention(retention, reason);
            self.callback.diagnostics_incomplete |= !retained;
        }
        if let StorageEntryMessage::CopySkipped { entry, reason } = message {
            let retained = self
                .callback
                .outbox
                .enqueue("copy", "copy_skipped", reason, Some(entry.path().as_str()));
            self.callback.diagnostics_incomplete |= !retained;
        }
        self.progress_bar.update_statistics(message);
        if let StorageEntryMessage::CopyVersionFailed {
            entry,
            phase,
            backend,
            reason,
            ..
        } = message
        {
            let version = if entry.view().version_id().is_empty() {
                "current"
            } else {
                entry.view().version_id()
            };
            let reason = safe_diagnostic_text(&format!(
                "version={} backend={backend:?} phase={phase}: {reason}",
                version
            ));
            let path = entry.view().source_path().ok();
            let retained = self.callback.outbox.enqueue(
                "copy",
                "copy_version_failed",
                &reason,
                path.as_ref().map(|path| path.as_str()),
            );
            self.callback.diagnostics_incomplete |= !retained;
            self.callback.runtime.current_error = Some(reason);
            self.callback.runtime.state = ScanRuntimePhase::Failed;
        }
        let diagnostic = match message {
            StorageEntryMessage::Error {
                event, path, reason, ..
            }
            | StorageEntryMessage::Diagnostic { event, path, reason } => Some((*event, path.to_str(), reason)),
            StorageEntryMessage::CopyChangeFailed { entry, reason, .. } => {
                Some((ErrorEvent::Copy, Some(entry.entry.path().as_str()), reason))
            }
            StorageEntryMessage::CopyRenameFailed { to, reason, .. } => message
                .counted_error()
                .map(|event| (event, Some(to.path().as_str()), reason)),
            _ => None,
        };
        if let Some((event, path, reason)) = diagnostic {
            let reason = safe_diagnostic_text(reason);
            let retained = self
                .callback
                .outbox
                .enqueue("operation", &format!("{event:?}"), &reason, path);
            self.callback.diagnostics_incomplete |= !retained;
            self.callback.runtime.current_error = Some(reason);
            self.callback.runtime.state = ScanRuntimePhase::Failed;
        }
    }

    /// 获取 per-chunk 实时字节计数器（Copy job 使用）
    pub fn get_bytes_tracker(&self) -> Arc<CopyBytes> {
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
        report.skipped_count = self.stats.skipped_count();
        report.target_preserved_count = self.callback.target_preserved_count;
        report.task_error_count = self.callback.task_error_count;
        if is_final {
            report.generation_commit_result = self.generation_commit_result;
        }
        report.migration = self.migration.clone();
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

    pub(crate) async fn post_callback(
        consumer: &Arc<Mutex<Self>>, client: &reqwest::Client, url: &CallbackEndpoint, is_final: bool,
    ) -> Option<bool> {
        let gate = consumer.lock().await.callback.journal.delivery.clone();
        let _guard = gate.lock().await;
        let prepared = match PreparedCallback::prepare(consumer, is_final).await {
            Ok(value) => value,
            Err(error) => {
                Self::diagnostic_failure(consumer, error).await;
                return None;
            }
        };
        if let Err(error) = prepared.send(client, url).await {
            let state = consumer.lock().await;
            warn!(job_id = %state.stats.job_id(), %error, "Progress callback failed; batch retained");
            return None;
        }
        prepared.received(consumer).await;
        let failed = consumer.lock().await.callback.journal.failed;
        if !failed && let Err(error) = journal::confirm_pending(consumer, false).await {
            Self::diagnostic_failure(consumer, error).await;
        }
        Some(prepared.batch_id.is_some())
    }

    pub(crate) async fn diagnostic_failure(consumer: &Arc<Mutex<Self>>, error: AppError) {
        warn!(error = %safe_diagnostic_text(&error.to_string()), "Diagnostic persistence is incomplete");
        let mut state = consumer.lock().await;
        if state.callback.journal.terminal.is_none() {
            state.callback.task_error_count = state.callback.task_error_count.saturating_add(1);
            state
                .callback
                .outbox
                .enqueue("diagnostics", "journal_unavailable", &error.to_string(), None);
        }
        state.callback.diagnostics_incomplete = true;
        state.callback.journal.failed = true;
        state.callback.journal.terminal.get_or_insert_with(|| Arc::new(error));
    }

    // ─── HTTP 回调 ───

    /// 构建共享的 HTTP 客户端（带超时），供回调循环和最终回调复用。
    /// 本机回调绕过系统代理，外部回调继续遵循代理环境变量。
    pub(crate) fn build_callback_client(url: &CallbackEndpoint) -> std::result::Result<reqwest::Client, DeliveryError> {
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(Policy::none());
        if url.is_loopback() {
            builder = builder.no_proxy();
        }
        builder
            .build()
            .map_err(|error| DeliveryError::Http(error.without_url()))
    }

    /// 启动周期性 HTTP 回调任务（每 2 秒 POST `ProgressReport`）
    ///
    /// 仅在配置了 `callback_url` 时启动。
    /// 返回 ([`JoinHandle`], Client) 供调用方在消息循环结束后 abort 并复用 client；无 URL 返回 None。
    pub async fn start_callback_loop(consumer: Arc<Mutex<Self>>) -> Option<CallbackGuard> {
        let (url, job_id) = {
            let c = consumer.lock().await;
            (c.callback_url.clone()?, c.stats.job_id().to_owned())
        };
        let client = match Self::build_callback_client(&url) {
            Ok(client) => client,
            Err(error) => {
                warn!(%job_id, %error, "Could not start callback loop");
                return None;
            }
        };
        info!(%job_id, "[ProgressCallback] Starting callback loop");
        let sc = consumer.clone();
        let loop_client = client.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(CALLBACK_INTERVAL_SECS));
            loop {
                interval.tick().await;
                let until = Instant::now() + Duration::from_secs(1);
                while let Some(had_details) = Self::post_callback(&sc, &loop_client, &url, false).await {
                    if !had_details || Instant::now() >= until {
                        break;
                    }
                }
            }
        });
        Some((handle, client))
    }

    /// 发送最终回调（`is_final=true`），优先复用已有 client
    pub async fn send_final_callback(consumer: &Arc<Mutex<Self>>, client: Option<&reqwest::Client>) {
        Self::send_final_with_database(consumer, client, None, Duration::from_secs(2)).await;
    }

    async fn send_final_with_database(
        consumer: &Arc<Mutex<Self>>, client: Option<&reqwest::Client>, database: Option<&ClickHouseDatabase>,
        budget: Duration,
    ) {
        let url = consumer.lock().await.callback_url.clone();
        let owned = match (&url, client) {
            (Some(url), None) => match Self::build_callback_client(url) {
                Ok(client) => Some(client),
                Err(error) => {
                    warn!(%error, "Final callback client could not be built; preserving final report");
                    None
                }
            },
            _ => None,
        };
        delivery::send_final(
            consumer,
            client.or(owned.as_ref()),
            url.as_ref(),
            database,
            budget,
            budget.min(Duration::from_secs(1)),
        )
        .await;
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
    pub async fn process(consumer: Arc<Mutex<Self>>, mut rx: mpsc::Receiver<StorageEntryMessage>) -> Result<()> {
        while let Some(message) = rx.recv().await {
            if let Err(error) = journal::record(&consumer, &message).await {
                rx.close();
                Self::diagnostic_failure(&consumer, error).await;
                // 已接受的有界缓冲继续累计；可用 callback 尽力排空，不能再生成新工作。
                let deadline = Instant::now() + Duration::from_secs(2);
                while let Ok(message) = rx.try_recv() {
                    let mut state = consumer.lock().await;
                    if state.callback.outbox.pending_len() >= MAX_DIAGNOSTIC_BATCH_EVENTS {
                        drop(state);
                        let url = consumer.lock().await.callback_url.clone();
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        let mut delivered = false;
                        if !remaining.is_zero()
                            && let Some(url) = url
                            && let Ok(client) = Self::build_callback_client(&url)
                        {
                            delivered = timeout(remaining, Self::post_callback(&consumer, &client, &url, false))
                                .await
                                .ok()
                                .flatten()
                                .is_some();
                        }
                        if !delivered {
                            journal::log_unrouted(&consumer).await;
                        }
                        state = consumer.lock().await;
                    }
                    state.update_statistics(&message);
                }
                let state = consumer.lock().await;
                return Err(AppError::DiagnosticJournalStopped {
                    source: state
                        .callback
                        .journal
                        .terminal
                        .clone()
                        .unwrap_or_else(|| Arc::new(AppError::DiagnosticDeliveryUnavailable)),
                });
            }
        }
        if let Err(error) = journal::flush_all(&consumer).await {
            Self::diagnostic_failure(&consumer, error).await;
            return Err(AppError::DiagnosticJournalStopped {
                source: consumer
                    .lock()
                    .await
                    .callback
                    .journal
                    .terminal
                    .clone()
                    .unwrap_or_else(|| Arc::new(AppError::DiagnosticDeliveryUnavailable)),
            });
        }
        Ok(())
    }

    /// 结束生命周期：abort HTTP 回调循环 → 发送 final 回调 → finalize（打印报告 + join 显示线程）。
    pub async fn end(consumer: Arc<Mutex<Self>>, callback: Option<CallbackGuard>) {
        Self::end_with_database(consumer, callback, None, false).await;
    }

    pub(crate) async fn end_with_database(
        consumer: Arc<Mutex<Self>>, callback: Option<CallbackGuard>, database: Option<&ClickHouseDatabase>,
        cancelled: bool,
    ) {
        if let Some(database) = database {
            consumer.lock().await.callback.journal.database = Some(database.clone());
        }
        let budget = if cancelled {
            Duration::from_millis(500)
        } else {
            Duration::from_secs(2)
        };
        let shared_client = if let Some((handle, client)) = callback {
            handle.abort();
            let _ = handle.await;
            Some(client)
        } else {
            None
        };

        Self::send_final_with_database(&consumer, shared_client.as_ref(), database, budget).await;

        let mut c = consumer.lock().await;
        c.finalize();
    }
}

#[cfg(test)]
mod tests {
    use super::{GenerationCommitResult, StatisticConsumer};
    use crate::callback::CallbackEndpoint;
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
            callback_url: Some(CallbackEndpoint::parse(&format!("http://{address}/progress"))?),
            generation_commit_result: Some(GenerationCommitResult::NotCommitted),
            migration: None,
            pb_handle: None,
            scan_snapshot: None,
            callback: Default::default(),
        }));
        consumer.lock().await.start_progress_bar();
        let started = Instant::now();
        StatisticConsumer::end_with_database(consumer.clone(), None, None, true).await;
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
                CallbackEndpoint::parse(url).is_ok_and(|endpoint| endpoint.is_loopback()),
                "expected loopback URL: {url}"
            );
        }

        for url in [
            "https://callbacks.example.com/progress",
            "http://10.10.1.12:9000/callback",
            "not a URL",
        ] {
            assert!(
                !CallbackEndpoint::parse(url).is_ok_and(|endpoint| endpoint.is_loopback()),
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
            generation_commit_result: Some(GenerationCommitResult::NotCommitted),
            migration: None,
            pb_handle: None,
            scan_snapshot: None,
            callback: Default::default(),
        };

        assert_eq!(
            consumer.to_report(true).generation_commit_result,
            Some(GenerationCommitResult::NotCommitted)
        );
        consumer.generation_commit_result = Some(GenerationCommitResult::Committed);
        assert_eq!(
            consumer.to_report(true).generation_commit_result,
            Some(GenerationCommitResult::Committed)
        );
    }
}

/// 兜底 join display 线程（panic/提前 drop 路径）。
///
/// 先发布结束状态并唤醒 display；兜底回收仍派发到独立 std thread，避免阻塞 tokio worker。
impl Drop for StatisticConsumer {
    fn drop(&mut self) {
        if let Some(handle) = self.pb_handle.take() {
            self.progress_bar.finish();
            handle.thread().unpark();
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

#[cfg(test)]
#[path = "consumer_wakeup_tests.rs"]
mod wakeup_tests;
