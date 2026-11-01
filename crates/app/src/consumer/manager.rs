//! 消费者管理器模块
//!
//! 该模块负责管理和协调多个消费者实例，提供统一的消息广播机制和消费者生命周期管理。
//!
//! 生命周期拆分：
//! - [`ConsumerManager::begin_lifecycle`] / [`ConsumerManager::end_lifecycle`] 包裹整个 pipeline，
//!   启停进度条、HTTP 回调循环，并在最后打印合并统计报告。
//! - [`ConsumerManager::start_consumers`] 可多次调用：增量拷贝两阶段流水线（Phase A/B）共用同一个
//!   `StatisticConsumer` 实例，但每阶段各起一轮 `DatabaseConsumer::start` + `StatisticConsumer::process`。
//! - 单阶段调用方（scan / sync / integrity_check）仍然只调一次 `start_consumers`。

// 标准库
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

// 外部crate
use data_mover::model::ObservedEntry;
use storage_adapter::observed_scan::ObservedScanEvent;
use storage_adapter::{ErrorEvent, StorageEntryMessage};
use tokio::sync::Mutex;
use tracing::{Instrument, debug, error, info_span, warn};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::config::{ConsumerConfig, JobType};
use crate::consumer::DatabaseConsumer;
use crate::consumer::stats::{FileSizeRange, ProgressDetail, TimeRange};
use crate::consumer::stats::{
    FullStats, IncrementalStats, ProgressBar, ProgressReport, StatisticConsumer, StatsKind, consumer::CallbackGuard,
};
use crate::consumer::traits::Consumer;
use crate::error::{AppError, Result};
use db::clickhouse::scan_resume_statistics::ScanResumeStatistics;
use db::scan_store::ScanFinalEvent;

/// 消费者管理器 - 负责创建、启动和管理多个消费者实例
pub struct ConsumerManager {
    /// 消费者列表，存储所有已注册的消费者实例
    consumers: Vec<Box<dyn Consumer>>,
    /// 统计消费者，通过 `Arc<Mutex>` 持有以便任务完成后读取统计结果
    stats_consumer: Option<Arc<Mutex<StatisticConsumer>>>,
    /// `begin_lifecycle()` 启动的周期 HTTP 回调守卫，在 `end_lifecycle()` 中 abort + 复用 client
    callback_guard: Option<CallbackGuard>,
}

/// 扫描工作任务共享统计状态，不持有生命周期守卫或其他消费者。
#[derive(Clone)]
pub(crate) struct ScanStatistics {
    inner: Option<Arc<Mutex<StatisticConsumer>>>,
}

impl ScanStatistics {
    pub(crate) async fn record_observed_events(&self, events: &[ObservedScanEvent]) {
        if let Some(sc) = &self.inner {
            let mut sc = sc.lock().await;
            for event in events {
                sc.update_observed_event(event);
            }
        }
    }

    pub(crate) async fn record_observations(&self, entries: &[Arc<ObservedEntry>]) {
        if let Some(sc) = &self.inner {
            let mut sc = sc.lock().await;
            for entry in entries {
                sc.update_observed_entry(entry);
            }
        }
    }

    pub(crate) async fn statistics_checkpoint(&self) -> Result<String> {
        match &self.inner {
            Some(sc) => {
                let sc = sc.lock().await;
                Ok(serde_json::to_string(&(&sc.stats, sc.to_report(false)))?)
            }
            None => Ok(String::new()),
        }
    }
}

impl ConsumerManager {
    pub(crate) fn scan_statistics(&self) -> ScanStatistics {
        ScanStatistics {
            inner: self.stats_consumer.clone(),
        }
    }

    pub async fn record_observations(&self, entries: &[Arc<ObservedEntry>]) {
        self.scan_statistics().record_observations(entries).await;
    }
    pub async fn record_observed_events(&self, events: &[ObservedScanEvent]) {
        self.scan_statistics().record_observed_events(events).await;
    }
    pub async fn record_final_events(&self, events: &[ScanFinalEvent]) {
        if let Some(sc) = &self.stats_consumer {
            let mut sc = sc.lock().await;
            for event in events {
                sc.update_recorded_event(event.operation(), event.item());
            }
        }
    }
    /// 扫描条目按实际输入流更新，变更事件落盘后更新；保存游标前等待两者处理完成。
    pub async fn record_statistics(&self, messages: impl IntoIterator<Item = StorageEntryMessage>) {
        if let Some(sc) = &self.stats_consumer {
            let mut sc = sc.lock().await;
            for message in messages {
                sc.update_statistics(&message);
            }
        }
    }

    /// 最终报告以持久化唯一条目为准；恢复时重放的目录不能重复计入结果统计。
    pub async fn reconcile_scan_statistics(&self, values: ScanResumeStatistics) {
        if let Some(sc) = &self.stats_consumer {
            let mut sc = sc.lock().await;
            let deltas = if let StatsKind::Incremental(incremental) = &mut sc.stats {
                incremental.new = Default::default();
                incremental.changed = Default::default();
                incremental.deleted = Default::default();
                incremental.renamed = Default::default();
                for (operation, directory, symlink, count, size) in values.events {
                    let target = match operation.as_str() {
                        "new" => &mut incremental.new,
                        "data_changed" | "metadata_changed" | "both_changed" => &mut incremental.changed,
                        "deleted" => &mut incremental.deleted,
                        "rename" => &mut incremental.renamed,
                        _ => continue,
                    };
                    if directory {
                        target.dir_count += count as usize;
                        target.dir_size += size;
                    } else if symlink {
                        target.symlink_count += count as usize;
                        target.symlink_size += size;
                    } else {
                        target.regular_file_count += count as usize;
                        target.regular_file_size += size;
                    }
                }
                Some((
                    incremental.new.clone(),
                    incremental.changed.clone(),
                    incremental.deleted.clone(),
                    incremental.renamed.clone(),
                ))
            } else {
                None
            };
            let full = match &mut sc.stats {
                StatsKind::Full(full) => full,
                StatsKind::Incremental(incremental) => &mut incremental.scanned,
            };
            let s = values.summary;
            full.dir_count = s.dirs as usize;
            full.dir_size = s.dir_bytes;
            full.regular_file_count = s.files as usize;
            full.regular_file_size = s.file_bytes;
            full.symlink_count = s.links as usize;
            full.symlink_size = s.link_bytes;
            full.total_dir_depth = s.total_depth;
            full.max_dir_depth = s.max_depth as usize;
            full.total_name_length = s.total_name_length;
            full.max_name_length = s.max_name_length as usize;
            full.extension_stats = values
                .extensions
                .into_iter()
                .map(|(key, size, count)| (key, (size, count as usize)))
                .collect();
            let ages = [
                TimeRange::WithinDay,
                TimeRange::DayToWeek,
                TimeRange::WeekToMonth,
                TimeRange::MonthToHalfYear,
                TimeRange::HalfYearToYear,
                TimeRange::OverYear,
            ];
            full.time_range_stats = values
                .ages
                .into_iter()
                .filter_map(|(key, size, count)| ages.get(key as usize).map(|range| (*range, (size, count as usize))))
                .collect();
            let sizes = [
                FileSizeRange::LessThan4K,
                FileSizeRange::Between4KAnd64K,
                FileSizeRange::Between64KAnd128K,
                FileSizeRange::Between128KAnd256K,
                FileSizeRange::Between256KAnd512K,
                FileSizeRange::Between512KAnd1M,
                FileSizeRange::Between1MAnd2M,
                FileSizeRange::Between2MAnd16M,
                FileSizeRange::Between16MAnd64M,
                FileSizeRange::Between64MAnd128M,
                FileSizeRange::Between128MAnd512M,
                FileSizeRange::Over512M,
            ];
            full.file_size_range_stats = values
                .sizes
                .into_iter()
                .filter_map(|(key, size, count)| sizes.get(key as usize).map(|range| (*range, (size, count as usize))))
                .collect();
            let mut report = sc.to_report(false);
            match &mut report.detail {
                ProgressDetail::Full {
                    scanned_files,
                    scanned_dirs,
                    total_bytes,
                    ..
                } => {
                    *scanned_files = s.files + s.links;
                    *scanned_dirs = s.dirs;
                    *total_bytes = (s.file_bytes + s.dir_bytes + s.link_bytes) as u64;
                }
                ProgressDetail::Incremental {
                    scanned_files,
                    scanned_dirs,
                    scanned_symlinks,
                    scanned_size,
                    ..
                } => {
                    *scanned_files = s.files;
                    *scanned_dirs = s.dirs;
                    *scanned_symlinks = s.links;
                    *scanned_size = (s.file_bytes + s.dir_bytes + s.link_bytes) as u64;
                }
            }
            if let (
                Some((new, changed, deleted, renamed)),
                ProgressDetail::Incremental {
                    new_count,
                    new_size,
                    changed_count,
                    changed_size,
                    deleted_count,
                    deleted_size,
                    renamed_count,
                    renamed_size,
                    ..
                },
            ) = (deltas, &mut report.detail)
            {
                *new_count = new.total_count() as u64;
                *new_size = new.total_size() as u64;
                *changed_count = changed.total_count() as u64;
                *changed_size = changed.total_size() as u64;
                *deleted_count = deleted.total_count() as u64;
                *deleted_size = deleted.total_size() as u64;
                *renamed_count = renamed.total_count() as u64;
                *renamed_size = renamed.total_size() as u64;
            }
            // report 从内部合法计数器产生，类型和时长保持不变。
            if let Err(error) = sc.progress_bar.restore(&report) {
                error!(%error, "Failed to reconcile scan progress");
            }
        }
    }

    pub async fn statistics_checkpoint(&self) -> Result<String> {
        self.scan_statistics().statistics_checkpoint().await
    }

    pub async fn restore_statistics(&self, saved: &str) -> Result<()> {
        if saved.is_empty() {
            return Ok(());
        }
        let (mut stats, report): (StatsKind, ProgressReport) = serde_json::from_str(saved)?;
        let duration = Duration::try_from_secs_f64(report.elapsed_secs)
            .map_err(|_| AppError::ConfigError("Invalid saved scan duration".into()))?;
        let start = Instant::now().checked_sub(duration).unwrap_or_else(Instant::now);
        if let Some(sc) = &self.stats_consumer {
            let mut sc = sc.lock().await;
            if stats.job_id() != sc.stats.job_id() || stats.job_type() != sc.stats.job_type() {
                return Err(AppError::ConfigError(
                    "Saved scan statistics belong to another job".into(),
                ));
            }
            match (&mut stats, &sc.stats) {
                (StatsKind::Full(saved), StatsKind::Full(current)) => {
                    saved.command.clone_from(&current.command);
                    saved.log_path.clone_from(&current.log_path);
                    saved.start_time = start;
                }
                (StatsKind::Incremental(saved), StatsKind::Incremental(current)) => {
                    saved.command.clone_from(&current.command);
                    saved.log_path.clone_from(&current.log_path);
                    saved.start_time = start;
                    saved.scanned.start_time = start;
                }
                _ => return Err(AppError::ConfigError("Saved scan statistics kind differs".into())),
            }
            sc.progress_bar.restore(&report)?;
            sc.stats = stats;
        }
        Ok(())
    }

    /// 创建新的消费者管理器实例
    pub async fn new(consumer_config: &ConsumerConfig) -> Result<Self> {
        let enable_db = consumer_config.db_config.enabled;

        let mut manager = Self {
            consumers: Vec::new(),
            stats_consumer: None,
            callback_guard: None,
        };

        // 根据配置初始化数据库消费者
        if enable_db {
            match DatabaseConsumer::try_new(consumer_config).await {
                Ok(db_consumer) => {
                    manager.add_consumer(Box::new(db_consumer));
                    debug!("Database consumer initialized successfully");
                }
                Err(e) => {
                    error!("Failed to initialize database consumer: {}", e);
                }
            }
        }

        // 根据 JobType 初始化对应的统计消费者
        {
            let job_type = consumer_config.job_type.clone();
            let job_id = consumer_config.job_id.clone();
            let command = consumer_config.console_config.raw_command_line.clone();
            let log_path = utils::logger::get_current_app_log_path();
            let job_dir = consumer_config.job_dir.clone();
            let callback_url = consumer_config.progress_callback_url.clone();

            let sc = match job_type {
                JobType::Scan | JobType::Copy | JobType::IntegrityCheck => StatisticConsumer {
                    stats: StatsKind::Full(FullStats::new(job_type.clone(), job_id, command, log_path)),
                    progress_bar: ProgressBar::new(job_type),
                    job_dir,
                    callback_url,
                    generation_committed: None,
                    pb_handle: None,
                },
                JobType::IncrementalScan | JobType::IncrementalCopy => StatisticConsumer {
                    stats: StatsKind::Incremental(IncrementalStats::new(job_type.clone(), job_id, command, log_path)),
                    progress_bar: ProgressBar::new(job_type),
                    job_dir,
                    callback_url,
                    generation_committed: None,
                    pb_handle: None,
                },
            };
            manager.stats_consumer = Some(Arc::new(Mutex::new(sc)));
            debug!("Statistic consumer initialized successfully");
        }

        // 验证至少有一个消费者被成功初始化
        if manager.consumers.is_empty() && manager.stats_consumer.is_none() {
            warn!("No consumers were successfully initialized");
        }

        Ok(manager)
    }

    /// 向消费者管理器添加一个消费者
    pub fn add_consumer(&mut self, consumer: Box<dyn Consumer + Send + Sync>) {
        self.consumers.push(consumer);
        debug!("Added consumer, total consumers: {}", self.consumers.len());
    }

    /// 启动一次性生命周期：进度条 + HTTP 回调循环。必须与 [`end_lifecycle`](Self::end_lifecycle) 成对调用。
    ///
    /// 多阶段 pipeline（如增量拷贝的 Phase A/B）只调用一次 begin + 一次 end；中间可多次
    /// [`start_consumers`](Self::start_consumers) 为每一阶段建立独立的 consumer 任务。
    pub async fn begin_lifecycle(&mut self) {
        if let Some(ref sc) = self.stats_consumer {
            self.callback_guard = StatisticConsumer::begin(sc.clone()).await;
        }
    }

    /// 启动所有已注册的消费者
    ///
    /// 为每个消费者通过 [`BroadcastForwarder`] 订阅独立的 mpsc 通道并启动异步任务。
    /// 可多次调用：每次给所有 `consumers` 重新 `start()`、给 `StatisticConsumer` 订阅新 rx + 启动
    /// [`StatisticConsumer::process`] 任务，共用同一个 `Arc<Mutex<StatisticConsumer>>` 累计统计。
    pub async fn start_consumers(
        &mut self, broadcaster: &mut BroadcastForwarder<StorageEntryMessage>,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        debug!("Starting {} consumers", self.consumers.len());
        let mut handles = Vec::new();

        for consumer in &mut self.consumers {
            let receiver = broadcaster.subscribe();
            let consumer_handle = consumer.start(receiver).await?;
            handles.push(consumer_handle);
            debug!("Consumer started successfully");
        }

        // 统计消费者：只负责消费消息累积统计，生命周期由 begin_lifecycle/end_lifecycle 管理
        if let Some(stats_consumer) = &self.stats_consumer {
            let stats_rx = broadcaster.subscribe();
            let stats_consumer_clone = stats_consumer.clone();
            let span = info_span!("consumer", kind = "statistics");
            let stats_handle = tokio::spawn(
                async move {
                    StatisticConsumer::process(stats_consumer_clone, stats_rx).await;
                    Ok(())
                }
                .instrument(span),
            );
            handles.push(stats_handle);
            debug!("Statistic consumer process task started successfully");
        }

        debug!("All consumers started successfully");
        Ok(handles)
    }

    /// 获取 per-chunk 实时字节计数器，用于在 `copy_file` 中每个 chunk 写入后更新带宽统计
    pub async fn get_bytes_tracker(&self) -> Option<Arc<AtomicU64>> {
        match &self.stats_consumer {
            Some(sc) => Some(sc.lock().await.get_bytes_tracker()),
            None => None,
        }
    }

    /// 获取已注册的消费者数量
    pub fn get_consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// 在 consumer 任务已经退出后，把任务级失败写入同一份最终统计。
    pub async fn record_task_failures(&self, failures: &[String]) {
        let Some(stats_consumer) = &self.stats_consumer else {
            return;
        };
        let mut stats_consumer = stats_consumer.lock().await;
        for reason in failures {
            stats_consumer.update_statistics(&StorageEntryMessage::Error {
                event: ErrorEvent::Scan,
                path: PathBuf::new(),
                entry: None,
                reason: reason.clone(),
            });
        }
    }

    /// 设置纯增量扫描在最终回调中暴露的 generation 提交分类。
    pub async fn set_generation_committed(&self, committed: bool) {
        if let Some(stats_consumer) = &self.stats_consumer {
            stats_consumer.lock().await.generation_committed = Some(committed);
        }
    }

    /// 结束一次性生命周期：abort HTTP 回调 → 发送 final 回调 → finalize（打印合并统计 + join 显示线程）。
    ///
    /// 必须在所有 `start_consumers` 产生的任务全部 await 完成之后调用（此时不会再有并发
    /// `update_statistics` 触发锁竞争）。无 `stats_consumer` 时为 no-op。
    pub async fn end_lifecycle(&mut self) {
        if let Some(sc) = self.stats_consumer.clone() {
            let callback = self.callback_guard.take();
            StatisticConsumer::end(sc, callback).await;
        }
    }

    pub(crate) async fn end_cancelled_lifecycle(&mut self) {
        if let Some(sc) = self.stats_consumer.clone() {
            let callback = self.callback_guard.take();
            StatisticConsumer::end_cancelled(sc, callback).await;
        }
    }
}

/// 兜底回收 callback 回调任务：若 `end_lifecycle` 因 panic/early-return 未被调用，
/// 这里必须 abort 掉周期性 HTTP 回调任务，避免 tokio task 泄漏（`JoinHandle` drop 不会 abort）。
impl Drop for ConsumerManager {
    fn drop(&mut self) {
        if let Some((handle, _client)) = self.callback_guard.take() {
            handle.abort();
            warn!("ConsumerManager dropped without end_lifecycle(); aborted callback loop task");
        }
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;
    use crate::consumer::stats::ProgressDetail;

    #[tokio::test]
    async fn statistics_checkpoint_restores_counts_without_replaying_old_messages() -> Result<()> {
        let mut full = FullStats::new(JobType::Scan, "resume".into(), "redacted".into(), String::new());
        full.regular_file_count = 7;
        full.regular_file_size = 1234;
        full.dir_count = 2;
        let mut stats = StatisticConsumer {
            stats: StatsKind::Full(full),
            progress_bar: ProgressBar::new(JobType::Scan),
            job_dir: String::new(),
            callback_url: None,
            generation_committed: None,
            pb_handle: None,
        };
        stats.progress_bar.restore(&ProgressReport {
            job_id: "resume".into(),
            job_type: "scan".into(),
            elapsed_secs: 12.0,
            speed_bytes_per_sec: 0.0,
            detail: ProgressDetail::Full {
                scanned_files: 7,
                scanned_dirs: 2,
                total_bytes: 1234,
                transferred_bytes: 0,
                error_count: 0,
            },
            is_final: false,
            final_stats: None,
            generation_committed: None,
        })?;
        let manager = ConsumerManager {
            consumers: vec![],
            stats_consumer: Some(Arc::new(Mutex::new(stats))),
            callback_guard: None,
        };
        let json = manager.statistics_checkpoint().await?;
        assert!(
            !json.contains("redacted"),
            "runtime command must not enter durable statistics"
        );
        manager.restore_statistics(&json).await?;
        let restored = manager
            .stats_consumer
            .as_ref()
            .ok_or_else(|| AppError::ConfigError("fixture".into()))?
            .lock()
            .await;
        let report = restored.to_report(true);
        assert!(report.elapsed_secs >= 12.0);
        assert!(matches!(
            report.detail,
            ProgressDetail::Full {
                scanned_files: 7,
                scanned_dirs: 2,
                total_bytes: 1234,
                ..
            }
        ));
        Ok(())
    }
}
