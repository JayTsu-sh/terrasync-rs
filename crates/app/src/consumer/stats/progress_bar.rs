//! 统一进度条 — 合并 `FullProgressBar` + `IncrementalProgressBar`，
//! 通过内部 `CounterSet` 枚举区分全量/增量计数器。
//!
//! 另保留独立的 `DirectoryMetadataProgressBar`（目录元数据同步进度）。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use data_mover::StorageEntryMessage;
use indicatif::{ProgressBar as IndicatifBar, ProgressStyle};
use tracing::info;

use super::accumulator::{fmt_elapsed, format_bytes};
use super::report::{ProgressDetail, ProgressReport};
use crate::config::JobType;

/// 进度条 display 线程刷新间隔（秒）
const DISPLAY_INTERVAL_SECS: u64 = 2;
/// 进度信息写入日志的节流间隔（秒）
const LOG_INTERVAL_SECS: u64 = 30;
/// finish 检测粒度（毫秒）：每 100ms 检查一次，最坏 join 等待时间即为此值
const FINISH_POLL_MS: u64 = 100;

/// 分段等待直到 `is_finished` 返回 true 或超时。
/// 将大 sleep 拆成 `FINISH_POLL_MS` 小段，大幅缩短 join 等待时间。
fn sleep_interruptible(checks: u64, is_finished: impl Fn() -> bool) {
    for _ in 0..checks {
        std::thread::sleep(Duration::from_millis(FINISH_POLL_MS));
        if is_finished() {
            break;
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CounterSet — 全量 / 增量计数器
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone)]
enum CounterSet {
    Full(FullCounters),
    Incremental(IncrCounters),
}

#[derive(Debug, Clone)]
struct FullCounters {
    total_file_count: Arc<AtomicU64>,
    total_dir_count: Arc<AtomicU64>,
    total_size: Arc<AtomicU64>,
    total_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct IncrCounters {
    scanned_regular_file_count: Arc<AtomicU64>,
    scanned_dir_count: Arc<AtomicU64>,
    scanned_symlink_count: Arc<AtomicU64>,
    scanned_size: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
    new_count: Arc<AtomicU64>,
    new_size: Arc<AtomicU64>,
    changed_count: Arc<AtomicU64>,
    changed_size: Arc<AtomicU64>,
    deleted_count: Arc<AtomicU64>,
    deleted_size: Arc<AtomicU64>,
    renamed_count: Arc<AtomicU64>,
    renamed_size: Arc<AtomicU64>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// RateTracker — 速率快照
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
struct RateTracker {
    last_update_time: Option<Instant>,
    last_count: u64,
    last_bytes: u64,
    last_log_time: Instant,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ProgressBar — 统一进度条
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 统一进度条 — 替代 `FullProgressBar` + `IncrementalProgressBar` + `ProgressBarKind`
#[derive(Debug, Clone)]
pub struct ProgressBar {
    pb: IndicatifBar,
    job_type: JobType,
    start_time: Instant,
    real_time_bytes: Arc<AtomicU64>,
    counters: CounterSet,
    rate: RateTracker,
}

impl ProgressBar {
    /// 根据 `JobType` 创建对应的全量或增量进度条
    pub fn new(job_type: JobType) -> Self {
        let now = Instant::now();
        let counters = match job_type {
            JobType::Scan | JobType::Copy | JobType::IntegrityCheck => CounterSet::Full(FullCounters {
                total_file_count: Arc::new(AtomicU64::new(0)),
                total_dir_count: Arc::new(AtomicU64::new(0)),
                total_size: Arc::new(AtomicU64::new(0)),
                total_count: Arc::new(AtomicU64::new(0)),
                error_count: Arc::new(AtomicU64::new(0)),
            }),
            JobType::IncrementalScan | JobType::IncrementalCopy => CounterSet::Incremental(IncrCounters {
                scanned_regular_file_count: Arc::new(AtomicU64::new(0)),
                scanned_dir_count: Arc::new(AtomicU64::new(0)),
                scanned_symlink_count: Arc::new(AtomicU64::new(0)),
                scanned_size: Arc::new(AtomicU64::new(0)),
                error_count: Arc::new(AtomicU64::new(0)),
                new_count: Arc::new(AtomicU64::new(0)),
                new_size: Arc::new(AtomicU64::new(0)),
                changed_count: Arc::new(AtomicU64::new(0)),
                changed_size: Arc::new(AtomicU64::new(0)),
                deleted_count: Arc::new(AtomicU64::new(0)),
                deleted_size: Arc::new(AtomicU64::new(0)),
                renamed_count: Arc::new(AtomicU64::new(0)),
                renamed_size: Arc::new(AtomicU64::new(0)),
            }),
        };

        let pb = {
            let bar = IndicatifBar::new_spinner();
            // 模板只解析一次；数据通过 set_message() 更新，避免每次刷新重复调用
            // Template::from_str（火焰图中的热点）
            if let Ok(style) = ProgressStyle::default_spinner().template("{spinner}{msg}") {
                bar.set_style(style);
            }
            bar
        };

        Self {
            pb,
            job_type,
            start_time: now,
            real_time_bytes: Arc::new(AtomicU64::new(0)),
            counters,
            rate: RateTracker {
                last_update_time: None,
                last_count: 0,
                last_bytes: 0,
                last_log_time: now,
            },
        }
    }

    /// 获取 `real_time_bytes` 原子计数器（Copy job 使用 chunk 粒度带宽）
    pub fn get_real_time_bytes_counter(&self) -> Arc<AtomicU64> {
        self.real_time_bytes.clone()
    }

    // ─── 统计更新 ───

    /// 根据消息类型更新对应的原子计数器
    pub fn update_statistics(&self, message: &StorageEntryMessage) {
        match &self.counters {
            CounterSet::Full(c) => Self::update_full(c, message),
            CounterSet::Incremental(c) => Self::update_incremental(c, message),
        }
    }

    fn update_full(c: &FullCounters, message: &StorageEntryMessage) {
        let entry = match message {
            // Scan job 收 Scanned/Packaged，Copy job 收 New
            StorageEntryMessage::Scanned(e) | StorageEntryMessage::Packaged(e) | StorageEntryMessage::New(e) => e,
            StorageEntryMessage::IntegrityChecked(e) => {
                if e.get_is_dir() {
                    c.total_dir_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    c.total_file_count.fetch_add(1, Ordering::Relaxed);
                }
                c.total_count.fetch_add(1, Ordering::Relaxed);
                c.total_size.fetch_add(e.get_size(), Ordering::Relaxed);
                return;
            }
            StorageEntryMessage::Error { .. } => {
                c.error_count.fetch_add(1, Ordering::Relaxed);
                return;
            }
            _ => return,
        };
        if entry.get_is_dir() {
            c.total_dir_count.fetch_add(1, Ordering::Relaxed);
        } else {
            c.total_file_count.fetch_add(1, Ordering::Relaxed);
        }
        c.total_count.fetch_add(1, Ordering::Relaxed);
        c.total_size.fetch_add(entry.get_size(), Ordering::Relaxed);
    }

    fn update_incremental(c: &IncrCounters, message: &StorageEntryMessage) {
        match message {
            StorageEntryMessage::Scanned(entry) | StorageEntryMessage::Packaged(entry) => {
                if entry.get_is_dir() {
                    c.scanned_dir_count.fetch_add(1, Ordering::Relaxed);
                } else if entry.get_is_symlink() {
                    c.scanned_symlink_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    c.scanned_regular_file_count.fetch_add(1, Ordering::Relaxed);
                }
                c.scanned_size.fetch_add(entry.get_size(), Ordering::Relaxed);
            }
            StorageEntryMessage::New(entry) => {
                c.new_count.fetch_add(1, Ordering::Relaxed);
                c.new_size.fetch_add(entry.get_size(), Ordering::Relaxed);
            }
            StorageEntryMessage::Changed { entry, .. } => {
                c.changed_count.fetch_add(1, Ordering::Relaxed);
                c.changed_size.fetch_add(entry.get_size(), Ordering::Relaxed);
            }
            StorageEntryMessage::Deleted(entry) => {
                c.deleted_count.fetch_add(1, Ordering::Relaxed);
                c.deleted_size.fetch_add(entry.get_size(), Ordering::Relaxed);
            }
            StorageEntryMessage::Renamed((_, to_entry)) => {
                c.renamed_count.fetch_add(1, Ordering::Relaxed);
                c.renamed_size.fetch_add(to_entry.get_size(), Ordering::Relaxed);
            }
            StorageEntryMessage::Error { .. } => {
                c.error_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    // ─── 显示 ───

    /// 刷新进度条显示（由 display thread 定时调用）
    pub fn display(&mut self) {
        match &self.counters {
            CounterSet::Full(_) => self.display_full(),
            CounterSet::Incremental(_) => self.display_incremental(),
        }
    }

    fn display_full(&mut self) {
        let CounterSet::Full(c) = &self.counters else { return };

        let total_count = c.total_count.load(Ordering::Relaxed);
        let total_file_count = c.total_file_count.load(Ordering::Relaxed);
        let total_dir_count = c.total_dir_count.load(Ordering::Relaxed);
        let total_size = c.total_size.load(Ordering::Relaxed);
        let real_time_bytes = self.real_time_bytes.load(Ordering::Relaxed);
        let current_time = Instant::now();

        let is_copy_job = matches!(self.job_type, JobType::Copy);
        let current_bytes = if is_copy_job { real_time_bytes } else { total_size };
        let last_bytes = self.rate.last_bytes;

        let elapsed = self.start_time.elapsed().as_secs_f64();
        let (item_rate, size_rate) = self.compute_rate(current_time, total_count, current_bytes, last_bytes, elapsed);

        // 更新速率快照
        self.rate.last_update_time = Some(current_time);
        self.rate.last_count = total_count;
        self.rate.last_bytes = if is_copy_job { real_time_bytes } else { total_size };
        self.pb.set_position(total_count);

        let display_size = if is_copy_job { real_time_bytes } else { total_size };
        let formatted_size = format_bytes(display_size as f64, true);
        let formatted_size_rate = format_bytes(size_rate, true);
        let formatted_time = fmt_elapsed(elapsed);
        let op = self.job_type.to_operation_name();

        let msg = match self.job_type {
            JobType::IntegrityCheck => format!(
                "{op} Completed: files {total_file_count} | Rate: {item_rate:.0} items/sec | Size: {formatted_size} | Rate: {formatted_size_rate}/sec | Runtime: {formatted_time}                  "
            ),
            _ => format!(
                "{op} Completed: total {total_count} (files {total_file_count}, dirs {total_dir_count}) | Rate: {item_rate:.0} items/sec | Size: {formatted_size} | Rate: {formatted_size_rate}/sec | Runtime: {formatted_time}                  "
            ),
        };

        self.log_and_set_msg(&msg);
    }

    fn display_incremental(&mut self) {
        let CounterSet::Incremental(c) = &self.counters else {
            return;
        };

        let scanned_file = c.scanned_regular_file_count.load(Ordering::Relaxed);
        let scanned_dir = c.scanned_dir_count.load(Ordering::Relaxed);
        let scanned_symlink = c.scanned_symlink_count.load(Ordering::Relaxed);
        let scanned_size = c.scanned_size.load(Ordering::Relaxed);
        let real_time_bytes = self.real_time_bytes.load(Ordering::Relaxed);
        let current_time = Instant::now();

        let scanned_count = scanned_file + scanned_dir + scanned_symlink;
        let is_copy = matches!(self.job_type, JobType::IncrementalCopy);
        let current_bytes = if is_copy { real_time_bytes } else { scanned_size };
        let last_bytes = self.rate.last_bytes;

        let elapsed = self.start_time.elapsed().as_secs_f64();
        let (scan_rate, size_rate) = self.compute_rate(current_time, scanned_count, current_bytes, last_bytes, elapsed);

        // 更新速率快照
        self.rate.last_update_time = Some(current_time);
        self.rate.last_count = scanned_count;
        self.rate.last_bytes = if is_copy { real_time_bytes } else { scanned_size };
        self.pb.set_position(scanned_count);

        let new_count = c.new_count.load(Ordering::Relaxed);
        let new_size = format_bytes(c.new_size.load(Ordering::Relaxed) as f64, true);
        let changed_count = c.changed_count.load(Ordering::Relaxed);
        let changed_size = format_bytes(c.changed_size.load(Ordering::Relaxed) as f64, true);
        let deleted_count = c.deleted_count.load(Ordering::Relaxed);
        let renamed_count = c.renamed_count.load(Ordering::Relaxed);

        let scanned_size_fmt = format_bytes(scanned_size as f64, true);
        let size_rate_fmt = format_bytes(size_rate, true);
        let op = self.job_type.to_operation_name();
        let formatted_time = fmt_elapsed(elapsed);

        let msg = format!(
            "{op}: scanned {scanned_count} (files {scanned_file}, dirs {scanned_dir}) | {scanned_size_fmt} | {scan_rate:.0} items/sec | {size_rate_fmt}/sec | Runtime: {formatted_time}\n         \u{21b3} New {new_count} ({new_size}) | Changed {changed_count} ({changed_size}) | Deleted {deleted_count} | Renamed {renamed_count}                  "
        );

        self.log_and_set_msg(&msg);
    }

    /// 共享的速率计算逻辑
    fn compute_rate(
        &self, current_time: Instant, current_count: u64, current_bytes: u64, last_bytes: u64, elapsed: f64,
    ) -> (f64, f64) {
        match self.rate.last_update_time {
            Some(last_time) => {
                let dt = current_time.duration_since(last_time).as_secs_f64();
                let dc = (current_count.saturating_sub(self.rate.last_count)) as f64;
                let db = (current_bytes.saturating_sub(last_bytes)) as f64;
                if dt > 0.1 && db > 0.0 {
                    (dc / dt, db / dt)
                } else if elapsed > 0.0 {
                    // 回退到平均速率
                    (current_count as f64 / elapsed, current_bytes as f64 / elapsed)
                } else {
                    (0.0, 0.0)
                }
            }
            None if elapsed > 0.0 => (current_count as f64 / elapsed, current_bytes as f64 / elapsed),
            None => (0.0, 0.0),
        }
    }

    /// 共享的日志输出 + 进度条消息更新（模板已在 `new()` 中解析过一次，此处只更新消息内容）
    fn log_and_set_msg(&mut self, msg: &str) {
        let now = Instant::now();
        if now.duration_since(self.rate.last_log_time).as_secs() >= LOG_INTERVAL_SECS {
            info!("{}", msg);
            self.rate.last_log_time = now;
        }
        self.pb.set_message(msg.to_string());
    }

    // ─── 生命周期 ───

    /// 启动后台 display 线程，每 2 秒刷新一次
    pub fn start(&self) -> std::thread::JoinHandle<()> {
        let mut pb = self.clone();
        std::thread::spawn(move || {
            loop {
                if pb.pb.is_finished() {
                    break;
                }
                pb.display();
                sleep_interruptible(DISPLAY_INTERVAL_SECS * 1000 / FINISH_POLL_MS, || pb.pb.is_finished());
            }
        })
    }

    /// 最后刷新一次并结束进度条
    pub fn finish(&mut self) {
        self.display();
        self.pb.finish();
    }

    // ─── 快照输出 ───

    /// 构建实时进度报告（用于 Web 回调）
    pub fn to_report(&self, job_id: &str) -> ProgressReport {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let real_time = self.real_time_bytes.load(Ordering::Relaxed);

        let (is_copy, primary_bytes) = match &self.counters {
            CounterSet::Full(c) => {
                let is_copy = matches!(self.job_type, JobType::Copy);
                (is_copy, c.total_size.load(Ordering::Relaxed))
            }
            CounterSet::Incremental(c) => {
                let is_copy = matches!(self.job_type, JobType::IncrementalCopy);
                (is_copy, c.scanned_size.load(Ordering::Relaxed))
            }
        };

        let speed_bytes = if is_copy { real_time } else { primary_bytes };
        let speed = if elapsed_secs > 0.0 {
            speed_bytes as f64 / elapsed_secs
        } else {
            0.0
        };

        let detail = match &self.counters {
            CounterSet::Full(c) => ProgressDetail::Full {
                scanned_files: c.total_file_count.load(Ordering::Relaxed),
                scanned_dirs: c.total_dir_count.load(Ordering::Relaxed),
                total_bytes: c.total_size.load(Ordering::Relaxed),
                transferred_bytes: real_time,
                error_count: c.error_count.load(Ordering::Relaxed),
            },
            CounterSet::Incremental(c) => ProgressDetail::Incremental {
                scanned_files: c.scanned_regular_file_count.load(Ordering::Relaxed),
                scanned_dirs: c.scanned_dir_count.load(Ordering::Relaxed),
                scanned_symlinks: c.scanned_symlink_count.load(Ordering::Relaxed),
                scanned_size: c.scanned_size.load(Ordering::Relaxed),
                transferred_bytes: real_time,
                error_count: c.error_count.load(Ordering::Relaxed),
                new_count: c.new_count.load(Ordering::Relaxed),
                new_size: c.new_size.load(Ordering::Relaxed),
                changed_count: c.changed_count.load(Ordering::Relaxed),
                changed_size: c.changed_size.load(Ordering::Relaxed),
                deleted_count: c.deleted_count.load(Ordering::Relaxed),
                deleted_size: c.deleted_size.load(Ordering::Relaxed),
                renamed_count: c.renamed_count.load(Ordering::Relaxed),
                renamed_size: c.renamed_size.load(Ordering::Relaxed),
            },
        };

        ProgressReport {
            job_id: job_id.to_string(),
            job_type: self.job_type.to_callback_name().to_string(),
            elapsed_secs,
            speed_bytes_per_sec: speed,
            detail,
            is_final: false,
            final_stats: None,
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// DirectoryMetadataProgressBar（保持原有实现不变）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone)]
pub struct DirectoryMetadataProgressBar {
    pb: IndicatifBar,
    total_dir_count: Arc<AtomicU64>,
}

impl Default for DirectoryMetadataProgressBar {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectoryMetadataProgressBar {
    pub fn new() -> Self {
        let pb = IndicatifBar::new_spinner();
        if let Ok(style) = ProgressStyle::with_template(
            "Updating dir metadata: {spinner} [{elapsed_precise}] \u{5df2}\u{5904}\u{7406} {msg} \u{4e2a}\u{76ee}\u{5f55}",
        ) {
            pb.set_style(style);
        }
        pb.set_message("0");
        Self {
            pb,
            total_dir_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(&self) -> std::thread::JoinHandle<()> {
        // 首帧立即渲染，避免短任务（空 DB 或极少目录）在首个 tick 之前就 finish
        self.pb.tick();
        let progress_bar_c = self.clone();
        std::thread::spawn(move || {
            loop {
                if progress_bar_c.pb.is_finished() {
                    break;
                }
                progress_bar_c.pb.tick();
                sleep_interruptible(5, || progress_bar_c.pb.is_finished());
            }
        })
    }

    pub fn increment_dir_count(&self) -> u64 {
        let count = self.total_dir_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.pb.set_message(count.to_string());
        count
    }

    pub fn finish(&mut self) {
        self.pb.finish();
    }

    pub fn get_total_dir_count(&self) -> u64 {
        self.total_dir_count.load(Ordering::Relaxed)
    }
}
