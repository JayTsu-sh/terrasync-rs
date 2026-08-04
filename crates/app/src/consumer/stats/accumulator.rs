// 标准库
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::{Instant, SystemTime};

// 外部crate
use data_mover::{ErrorEvent, StorageEntryMessage};
use serde::{Deserialize, Serialize};
use tracing::trace;

// 内部模块
use crate::config::JobType;
use crate::integrity_check::{FixStatus, IntegrityIssue, IssueKind};

// ─────────────────────────────────────────────────
// 枚举：时间区间 / 文件大小区间
// ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeRange {
    WithinDay,       // 一天以内
    DayToWeek,       // 大于一天到一周内
    WeekToMonth,     // 大于一周到一月内
    MonthToHalfYear, // 大于一月到半年内
    HalfYearToYear,  // 大于半年到1年内
    OverYear,        // 大于一年
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithinDay => write!(f, "1天以内"),
            Self::DayToWeek => write!(f, "1天~1周"),
            Self::WeekToMonth => write!(f, "1周~1月"),
            Self::MonthToHalfYear => write!(f, "1月~半年"),
            Self::HalfYearToYear => write!(f, "半年~1年"),
            Self::OverYear => write!(f, "1年以上"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileSizeRange {
    LessThan4K,
    Between4KAnd64K,
    Between64KAnd128K,
    Between128KAnd256K,
    Between256KAnd512K,
    Between512KAnd1M,
    Between1MAnd2M,
    Between2MAnd16M,
    Between16MAnd64M,
    Between64MAnd128M,
    Between128MAnd512M,
    Over512M,
}

impl fmt::Display for FileSizeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LessThan4K => write!(f, "<4KB"),
            Self::Between4KAnd64K => write!(f, "4KB~64KB"),
            Self::Between64KAnd128K => write!(f, "64KB~128KB"),
            Self::Between128KAnd256K => write!(f, "128KB~256KB"),
            Self::Between256KAnd512K => write!(f, "256KB~512KB"),
            Self::Between512KAnd1M => write!(f, "512KB~1MB"),
            Self::Between1MAnd2M => write!(f, "1MB~2MB"),
            Self::Between2MAnd16M => write!(f, "2MB~16MB"),
            Self::Between16MAnd64M => write!(f, "16MB~64MB"),
            Self::Between64MAnd128M => write!(f, "64MB~128MB"),
            Self::Between128MAnd512M => write!(f, "128MB~512MB"),
            Self::Over512M => write!(f, ">512MB"),
        }
    }
}

// ─────────────────────────────────────────────────
// 工具函数
// ─────────────────────────────────────────────────

pub fn calculate_time_range(mtime: i64) -> TimeRange {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let time_diff = if now > mtime { now - mtime } else { 0 };
    let days = time_diff as f64 / (24u64 * 60 * 60 * 1_000_000_000) as f64;
    if days < 1.0 {
        TimeRange::WithinDay
    } else if days < 7.0 {
        TimeRange::DayToWeek
    } else if days < 30.0 {
        TimeRange::WeekToMonth
    } else if days < 180.0 {
        TimeRange::MonthToHalfYear
    } else if days < 365.0 {
        TimeRange::HalfYearToYear
    } else {
        TimeRange::OverYear
    }
}

pub fn calculate_file_size_range(size: u64) -> FileSizeRange {
    match size {
        s if s < 4 * 1024 => FileSizeRange::LessThan4K,
        s if s < 64 * 1024 => FileSizeRange::Between4KAnd64K,
        s if s < 128 * 1024 => FileSizeRange::Between64KAnd128K,
        s if s < 256 * 1024 => FileSizeRange::Between128KAnd256K,
        s if s < 512 * 1024 => FileSizeRange::Between256KAnd512K,
        s if s < 1024 * 1024 => FileSizeRange::Between512KAnd1M,
        s if s < 2 * 1024 * 1024 => FileSizeRange::Between1MAnd2M,
        s if s < 16 * 1024 * 1024 => FileSizeRange::Between2MAnd16M,
        s if s < 64 * 1024 * 1024 => FileSizeRange::Between16MAnd64M,
        s if s < 128 * 1024 * 1024 => FileSizeRange::Between64MAnd128M,
        s if s < 512 * 1024 * 1024 => FileSizeRange::Between128MAnd512M,
        _ => FileSizeRange::Over512M,
    }
}

fn calculate_depth(path: &Path) -> usize {
    path.components().count().max(1)
}

pub fn format_bytes(bytes: f64, with_decimals: bool) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes;
    let mut unit_index = 0;
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    if with_decimals {
        format!("{:.2} {}", size, UNITS[unit_index])
    } else {
        format!("{:.0} {}", size, UNITS[unit_index])
    }
}

// ─────────────────────────────────────────────────
// FullStats：Scan / Copy / IntegrityCheck 的最终统计
// ─────────────────────────────────────────────────

/// 按 dirs / `regular_files` / symlinks 三维度存储数量和 size
#[derive(Debug, Clone)]
pub struct FullStats {
    // 三维度 count + size
    pub dir_count: usize,
    pub dir_size: i64,
    pub regular_file_count: usize,
    pub regular_file_size: i64,
    pub symlink_count: usize,
    pub symlink_size: i64,

    // 目录深度
    pub total_dir_depth: i64,
    pub max_dir_depth: usize,
    // 名称长度
    pub total_name_length: i64,
    pub max_name_length: usize,

    // 分布统计（仅对 regular_files）
    pub extension_stats: HashMap<String, (i64, usize)>, // (total_size, count)
    pub time_range_stats: HashMap<TimeRange, (i64, usize)>,
    pub file_size_range_stats: HashMap<FileSizeRange, (i64, usize)>,

    pub error_stats: ErrorStats,

    // 元数据
    pub command: String,
    pub job_id: String,
    pub job_type: JobType,
    pub log_path: String,
    pub start_time: Instant,
}

impl FullStats {
    pub fn new(job_type: JobType, job_id: String, command: String, log_path: String) -> Self {
        Self {
            dir_count: 0,
            dir_size: 0,
            regular_file_count: 0,
            regular_file_size: 0,
            symlink_count: 0,
            symlink_size: 0,
            total_dir_depth: 0,
            max_dir_depth: 0,
            total_name_length: 0,
            max_name_length: 0,
            extension_stats: HashMap::new(),
            time_range_stats: HashMap::new(),
            file_size_range_stats: HashMap::new(),
            error_stats: ErrorStats::default(),
            command,
            job_id,
            job_type,
            log_path,
            start_time: Instant::now(),
        }
    }

    // ── 派生计算 ──
    pub fn total_file_count(&self) -> usize {
        self.regular_file_count + self.symlink_count
    }

    pub fn total_file_size(&self) -> i64 {
        self.regular_file_size + self.symlink_size
    }

    pub fn total_count(&self) -> usize {
        self.dir_count + self.regular_file_count + self.symlink_count
    }

    pub fn total_size(&self) -> i64 {
        self.dir_size + self.regular_file_size + self.symlink_size
    }

    /// 根据 entry 类型更新对应维度的统计
    pub fn update(&mut self, entry: &data_mover::EntryEnum) {
        let size = entry.get_size() as i64;
        let name_len = entry.get_name().len();

        self.total_name_length += name_len as i64;
        if name_len > self.max_name_length {
            self.max_name_length = name_len;
        }

        if entry.get_is_dir() {
            self.dir_count += 1;
            self.dir_size += size;
            let depth = calculate_depth(entry.get_relative_path());
            self.total_dir_depth += depth as i64;
            if depth > self.max_dir_depth {
                self.max_dir_depth = depth;
            }
        } else if entry.get_is_symlink() {
            self.symlink_count += 1;
            self.symlink_size += size;
        } else {
            self.regular_file_count += 1;
            self.regular_file_size += size;
            // 扩展名统计
            if let Some(ext) = entry.get_extension() {
                let e = self.extension_stats.entry(ext.to_lowercase()).or_insert((0, 0));
                e.0 += size;
                e.1 += 1;
            }
            // 文件大小分布
            let range = calculate_file_size_range(entry.get_size());
            let r = self.file_size_range_stats.entry(range).or_insert((0, 0));
            r.0 += size;
            r.1 += 1;
        }

        // 修改时间分布（所有条目）
        let tr = calculate_time_range(entry.get_mtime());
        let t = self.time_range_stats.entry(tr).or_insert((0, 0));
        t.0 += size;
        t.1 += 1;
    }

    pub fn update_from_message(&mut self, message: &StorageEntryMessage) {
        match message {
            StorageEntryMessage::Scanned(entry)
            | StorageEntryMessage::New(entry)
            | StorageEntryMessage::Packaged(entry)
            | StorageEntryMessage::IntegrityChecked(entry) => {
                trace!("[FullStats] update: {:?}", entry.get_relative_path());
                self.update(entry.as_ref());
            }
            StorageEntryMessage::Error { event, .. } => self.error_stats.record(*event),
            _ => {}
        }
    }

    pub fn get_top_extensions(&self) -> Vec<(&String, &(i64, usize))> {
        let mut v: Vec<_> = self.extension_stats.iter().collect();
        v.sort_by(|a, b| b.1.0.cmp(&a.1.0));
        v.into_iter().take(5).collect()
    }
}

impl fmt::Display for FullStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let avg_file_size = if self.regular_file_count > 0 {
            self.regular_file_size as f64 / self.regular_file_count as f64
        } else {
            0.0
        };
        let avg_name_len = if self.total_count() > 0 {
            self.total_name_length as f64 / self.total_count() as f64
        } else {
            0.0
        };
        let avg_dir_depth = if self.dir_count > 0 {
            self.total_dir_depth as f64 / self.dir_count as f64
        } else {
            0.0
        };

        writeln!(f, "{:=<80}", "")?;
        writeln!(f, "{:^80}", "Job Completion Summary")?;
        writeln!(f, "{:=<80}", "")?;
        writeln!(f, "  Job Information:")?;
        writeln!(f, "   ├─ Command:    {}", self.command)?;
        writeln!(f, "   ├─ Job ID:     {}", self.job_id)?;
        writeln!(f, "   ├─ Log Path:   {}", self.log_path)?;
        writeln!(f, "   └─ Total Time: {elapsed:.1}s")?;
        writeln!(f)?;

        writeln!(f, "  Basic Statistics:")?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Dirs:          {:>12}  ({})", self.dir_count, format_bytes(self.dir_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Regular Files: {:>12}  ({})", self.regular_file_count, format_bytes(self.regular_file_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Symlinks:      {:>12}  ({})", self.symlink_count, format_bytes(self.symlink_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Total Files:   {:>12}  ({})", self.total_file_count(), format_bytes(self.total_file_size() as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   └─ Total:         {:>12}  ({})", self.total_count(), format_bytes(self.total_size() as f64, true))?;
        writeln!(f)?;

        fmt_error_stats(f, &self.error_stats)?;

        // IntegrityCheck 不需要 Extended Statistics 和分布表格
        if !matches!(self.job_type, JobType::IntegrityCheck) {
            writeln!(f, "  Extended Statistics:")?;
            writeln!(f, "   ├─ Avg File Size: {:>19}", format_bytes(avg_file_size, true))?;
            writeln!(f, "   ├─ Max Name Len:  {:>15}", self.max_name_length)?;
            writeln!(f, "   ├─ Avg Name Len:  {avg_name_len:>15.1}")?;
            writeln!(f, "   ├─ Max Dir Depth: {:>15}", self.max_dir_depth)?;
            writeln!(f, "   └─ Avg Dir Depth: {avg_dir_depth:>15.1}")?;
            writeln!(f)?;

            fmt_time_range_table(f, &self.time_range_stats, self.total_count(), self.total_size())?;
            fmt_file_size_range_table(
                f,
                &self.file_size_range_stats,
                self.regular_file_count,
                self.regular_file_size,
            )?;
            fmt_top_extensions(
                f,
                self.get_top_extensions(),
                self.regular_file_count,
                self.regular_file_size,
            )?;
        }

        Ok(())
    }
}

// ─────────────────────────────────────────────────
// ErrorStats：按事件类型分类的错误统计
// ─────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorStats {
    pub scan: usize,
    pub copy: usize,
    pub copy_acl: usize,
    pub copy_xattr: usize,
    pub delete: usize,
    pub rename: usize,
    pub symlink_op: usize,
    pub pack: usize,
    pub integrity_check: usize,
}

impl ErrorStats {
    pub fn record(&mut self, event: ErrorEvent) {
        match event {
            ErrorEvent::Scan => self.scan += 1,
            ErrorEvent::Copy => self.copy += 1,
            ErrorEvent::CopyAcl => self.copy_acl += 1,
            ErrorEvent::CopyXattr => self.copy_xattr += 1,
            ErrorEvent::Delete => self.delete += 1,
            ErrorEvent::Rename => self.rename += 1,
            ErrorEvent::SymlinkOp => self.symlink_op += 1,
            ErrorEvent::Pack => self.pack += 1,
            ErrorEvent::IntegrityCheck => self.integrity_check += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.scan
            + self.copy
            + self.copy_acl
            + self.copy_xattr
            + self.delete
            + self.rename
            + self.symlink_op
            + self.pack
            + self.integrity_check
    }

    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

// ─────────────────────────────────────────────────
// IncrementalStats：IncrementalScan / IncrementalCopy
// ─────────────────────────────────────────────────

/// 增量操作的三维度统计辅助结构（内联字段展开到 `IncrementalStats`）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaCounts {
    pub dir_count: usize,
    pub dir_size: i64,
    pub regular_file_count: usize,
    pub regular_file_size: i64,
    pub symlink_count: usize,
    pub symlink_size: i64,
}

impl DeltaCounts {
    pub fn total_count(&self) -> usize {
        self.dir_count + self.regular_file_count + self.symlink_count
    }

    pub fn total_file_count(&self) -> usize {
        self.regular_file_count + self.symlink_count
    }

    pub fn total_size(&self) -> i64 {
        self.dir_size + self.regular_file_size + self.symlink_size
    }

    pub fn add(&mut self, entry: &data_mover::EntryEnum) {
        let size = entry.get_size() as i64;
        if entry.get_is_dir() {
            self.dir_count += 1;
            self.dir_size += size;
        } else if entry.get_is_symlink() {
            self.symlink_count += 1;
            self.symlink_size += size;
        } else {
            self.regular_file_count += 1;
            self.regular_file_size += size;
        }
    }
}

#[derive(Debug, Clone)]
pub struct IncrementalStats {
    pub scanned: FullStats,
    pub new: DeltaCounts,
    pub changed: DeltaCounts,
    pub deleted: DeltaCounts,
    pub renamed: DeltaCounts,
    pub error_stats: ErrorStats,
    pub command: String,
    pub job_id: String,
    pub job_type: JobType,
    pub log_path: String,
    pub start_time: Instant,
}

impl IncrementalStats {
    pub fn new(job_type: JobType, job_id: String, command: String, log_path: String) -> Self {
        // scanned 的元数据字段填充（仅用于分布统计，不用于最终 Display）
        let scanned = FullStats::new(job_type.clone(), job_id.clone(), command.clone(), log_path.clone());
        Self {
            scanned,
            new: DeltaCounts::default(),
            changed: DeltaCounts::default(),
            deleted: DeltaCounts::default(),
            renamed: DeltaCounts::default(),
            error_stats: ErrorStats::default(),
            command,
            job_id,
            job_type,
            log_path,
            start_time: Instant::now(),
        }
    }

    pub fn update_from_message(&mut self, message: &StorageEntryMessage) {
        match message {
            StorageEntryMessage::Scanned(entry) | StorageEntryMessage::Packaged(entry) => {
                self.scanned.update(entry.as_ref());
            }
            StorageEntryMessage::New(entry) => self.new.add(entry.as_ref()),
            StorageEntryMessage::Changed { entry, .. } => self.changed.add(entry.as_ref()),
            StorageEntryMessage::Deleted(entry) => self.deleted.add(entry.as_ref()),
            StorageEntryMessage::Renamed((_, to_entry)) => self.renamed.add(to_entry.as_ref()),
            StorageEntryMessage::Error { event, .. } => self.error_stats.record(*event),
            _ => {}
        }
    }
}

impl fmt::Display for IncrementalStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_delta(f: &mut fmt::Formatter<'_>, label: &str, d: &DeltaCounts) -> fmt::Result {
            writeln!(
                f,
                "   ├─ {:8} {:>8} total | dirs {:>6} ({}) | files {:>6} ({}) | symlinks {:>4}",
                label,
                d.total_count(),
                d.dir_count,
                format_bytes(d.dir_size as f64, true),
                d.regular_file_count,
                format_bytes(d.regular_file_size as f64, true),
                d.symlink_count
            )
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();

        writeln!(f, "{:=<80}", "")?;
        writeln!(f, "{:^80}", "Job Completion Summary")?;
        writeln!(f, "{:=<80}", "")?;
        writeln!(f, "  Job Information:")?;
        writeln!(f, "   ├─ Command:    {}", self.command)?;
        writeln!(f, "   ├─ Job ID:     {}", self.job_id)?;
        writeln!(f, "   ├─ Log Path:   {}", self.log_path)?;
        writeln!(f, "   └─ Total Time: {elapsed:.1}s")?;
        writeln!(f)?;

        // Scanned 基础统计（复用 FullStats 的 Basic Statistics 部分）
        let s = &self.scanned;
        writeln!(f, "  Scanned Statistics:")?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Dirs:          {:>12}  ({})", s.dir_count, format_bytes(s.dir_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Regular Files: {:>12}  ({})", s.regular_file_count, format_bytes(s.regular_file_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   ├─ Symlinks:      {:>12}  ({})", s.symlink_count, format_bytes(s.symlink_size as f64, true))?;
        #[rustfmt::skip]
        writeln!(f, "   └─ Total:         {:>12}  ({})", s.total_count(), format_bytes(s.total_size() as f64, true))?;
        writeln!(f)?;

        // 增量统计
        writeln!(f, "  Incremental Statistics:")?;
        fmt_delta(f, "New:", &self.new)?;
        fmt_delta(f, "Changed:", &self.changed)?;
        fmt_delta(f, "Renamed:", &self.renamed)?;
        writeln!(
            f,
            "   └─ {:8} {:>8} total | dirs {:>6} ({}) | files {:>6} ({}) | symlinks {:>4}",
            "Deleted:",
            self.deleted.total_count(),
            self.deleted.dir_count,
            format_bytes(self.deleted.dir_size as f64, true),
            self.deleted.regular_file_count,
            format_bytes(self.deleted.regular_file_size as f64, true),
            self.deleted.symlink_count
        )?;
        writeln!(f)?;

        fmt_error_stats(f, &self.error_stats)?;

        // Scanned 的分布统计
        let avg_file_size = if s.regular_file_count > 0 {
            s.regular_file_size as f64 / s.regular_file_count as f64
        } else {
            0.0
        };
        let avg_name_len = if s.total_count() > 0 {
            s.total_name_length as f64 / s.total_count() as f64
        } else {
            0.0
        };
        let avg_dir_depth = if s.dir_count > 0 {
            s.total_dir_depth as f64 / s.dir_count as f64
        } else {
            0.0
        };
        writeln!(f, "  Extended Statistics (Scanned):")?;
        writeln!(f, "   ├─ Avg File Size: {:>19}", format_bytes(avg_file_size, true))?;
        writeln!(f, "   ├─ Max Name Len:  {:>15}", s.max_name_length)?;
        writeln!(f, "   ├─ Avg Name Len:  {avg_name_len:>15.1}")?;
        writeln!(f, "   ├─ Max Dir Depth: {:>15}", s.max_dir_depth)?;
        writeln!(f, "   └─ Avg Dir Depth: {avg_dir_depth:>15.1}")?;
        writeln!(f)?;

        fmt_time_range_table(f, &s.time_range_stats, s.total_count(), s.total_size())?;
        fmt_file_size_range_table(f, &s.file_size_range_stats, s.regular_file_count, s.regular_file_size)?;
        fmt_top_extensions(f, s.get_top_extensions(), s.regular_file_count, s.regular_file_size)?;

        Ok(())
    }
}

// ─────────────────────────────────────────────────
// StatsKind 枚举
// ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum StatsKind {
    Full(FullStats),
    Incremental(IncrementalStats),
}

impl StatsKind {
    pub fn update_from_message(&mut self, message: &StorageEntryMessage) {
        match self {
            StatsKind::Full(s) => s.update_from_message(message),
            StatsKind::Incremental(s) => s.update_from_message(message),
        }
    }

    pub fn job_type(&self) -> &JobType {
        match self {
            StatsKind::Full(s) => &s.job_type,
            StatsKind::Incremental(s) => &s.job_type,
        }
    }

    pub fn job_id(&self) -> &str {
        match self {
            StatsKind::Full(s) => &s.job_id,
            StatsKind::Incremental(s) => &s.job_id,
        }
    }
}

impl fmt::Display for StatsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatsKind::Full(s) => fmt::Display::fmt(s, f),
            StatsKind::Incremental(s) => fmt::Display::fmt(s, f),
        }
    }
}

// ─────────────────────────────────────────────────
// 表格格式化辅助函数
// ─────────────────────────────────────────────────

pub(crate) fn fmt_elapsed(elapsed: f64) -> String {
    let seconds = elapsed as u64;
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    if hours > 0 {
        format!("{hours}h{minutes}m{secs}s")
    } else if minutes > 0 {
        format!("{minutes}m{secs}s")
    } else {
        format!("{secs}s")
    }
}

fn fmt_time_range_table(
    f: &mut fmt::Formatter<'_>, stats: &HashMap<TimeRange, (i64, usize)>, total_count: usize, total_size: i64,
) -> fmt::Result {
    const C1: usize = 20;
    const C2: usize = 14;
    const C3: usize = 9;
    const C4: usize = 14;
    const C5: usize = 10;
    const C6: usize = 9;

    writeln!(f, "  MODIFICATION TIME DISTRIBUTION:")?;
    writeln!(
        f,
        "    ┌{}┬{}┬{}┬{}┬{}┬{}┐",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(
        f,
        "    │ {:^18} │ {:^12} │ {:^7} │ {:^12} │ {:^8} │ {:^7} │",
        "Time Range", "Total Size", "Size %", "Avg Size", "Count", "Count %"
    )?;
    writeln!(
        f,
        "    ├{}┼{}┼{}┼{}┼{}┼{}┤",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;

    let ranges = [
        (TimeRange::WithinDay, "<= 1 day"),
        (TimeRange::DayToWeek, "1 day ~ 1 week"),
        (TimeRange::WeekToMonth, "1 week ~ 1 month"),
        (TimeRange::MonthToHalfYear, "1 month ~ 6 months"),
        (TimeRange::HalfYearToYear, "6 months ~ 1 year"),
        (TimeRange::OverYear, ">= 1 year"),
    ];

    for (range, name) in ranges {
        let (sz, cnt) = stats.get(&range).unwrap_or(&(0, 0));
        let avg = if *cnt > 0 { *sz as f64 / *cnt as f64 } else { 0.0 };
        let cnt_pct = if total_count > 0 {
            *cnt as f64 / total_count as f64 * 100.0
        } else {
            0.0
        };
        let sz_pct = if total_size > 0 {
            *sz as f64 / total_size as f64 * 100.0
        } else {
            0.0
        };
        writeln!(
            f,
            "    │ {:^18} │ {:>12} │ {:>7} │ {:>12} │ {:^8} │ {:>7} │",
            name,
            format_bytes(*sz as f64, true),
            fmt_pct(sz_pct),
            format_bytes(avg, true),
            cnt,
            fmt_pct(cnt_pct)
        )?;
    }
    writeln!(
        f,
        "    └{}┴{}┴{}┴{}┴{}┴{}┘",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(f)
}

fn fmt_file_size_range_table(
    f: &mut fmt::Formatter<'_>, stats: &HashMap<FileSizeRange, (i64, usize)>, total_regular_files: usize,
    total_files_size: i64,
) -> fmt::Result {
    const C1: usize = 25;
    const C2: usize = 14;
    const C3: usize = 9;
    const C4: usize = 14;
    const C5: usize = 10;
    const C6: usize = 9;

    writeln!(f, "  FILE SIZE DISTRIBUTION (regular files only):")?;
    writeln!(
        f,
        "    ┌{}┬{}┬{}┬{}┬{}┬{}┐",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(
        f,
        "    │ {:^23} │ {:^12} │ {:^7} │ {:^12} │ {:^8} │ {:^7} │",
        "Size Range", "Total Size", "Size %", "Avg Size", "Count", "Count %"
    )?;
    writeln!(
        f,
        "    ├{}┼{}┼{}┼{}┼{}┼{}┤",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;

    let ranges = [
        (FileSizeRange::LessThan4K, "< 4 KiB"),
        (FileSizeRange::Between4KAnd64K, "4 KiB ~ 64 KiB"),
        (FileSizeRange::Between64KAnd128K, "64 KiB ~ 128 KiB"),
        (FileSizeRange::Between128KAnd256K, "128 KiB ~ 256 KiB"),
        (FileSizeRange::Between256KAnd512K, "256 KiB ~ 512 KiB"),
        (FileSizeRange::Between512KAnd1M, "512 KiB ~ 1 MiB"),
        (FileSizeRange::Between1MAnd2M, "1 MiB ~ 2 MiB"),
        (FileSizeRange::Between2MAnd16M, "2 MiB ~ 16 MiB"),
        (FileSizeRange::Between16MAnd64M, "16 MiB ~ 64 MiB"),
        (FileSizeRange::Between64MAnd128M, "64 MiB ~ 128 MiB"),
        (FileSizeRange::Between128MAnd512M, "128 MiB ~ 512 MiB"),
        (FileSizeRange::Over512M, ">= 512 MiB"),
    ];

    for (range, name) in ranges {
        let (sz, cnt) = stats.get(&range).unwrap_or(&(0, 0));
        let avg = if *cnt > 0 { *sz as f64 / *cnt as f64 } else { 0.0 };
        let cnt_pct = if total_regular_files > 0 {
            *cnt as f64 / total_regular_files as f64 * 100.0
        } else {
            0.0
        };
        let sz_pct = if total_files_size > 0 {
            *sz as f64 / total_files_size as f64 * 100.0
        } else {
            0.0
        };
        writeln!(
            f,
            "    │ {:^23} │ {:>12} │ {:>7} │ {:>12} │ {:^8} │ {:>7} │",
            name,
            format_bytes(*sz as f64, true),
            fmt_pct(sz_pct),
            format_bytes(avg, true),
            cnt,
            fmt_pct(cnt_pct)
        )?;
    }
    writeln!(
        f,
        "    └{}┴{}┴{}┴{}┴{}┴{}┘",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(f)
}

fn fmt_top_extensions(
    f: &mut fmt::Formatter<'_>, top: Vec<(&String, &(i64, usize))>, total_regular_files: usize, total_files_size: i64,
) -> fmt::Result {
    const C1: usize = 14;
    const C2: usize = 14;
    const C3: usize = 9;
    const C4: usize = 14;
    const C5: usize = 11;
    const C6: usize = 9;

    if top.is_empty() {
        return Ok(());
    }

    writeln!(f, "  TOP 5 FILE EXTENSIONS:")?;
    writeln!(
        f,
        "    ┌{}┬{}┬{}┬{}┬{}┬{}┐",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(
        f,
        "    │ {:^12} │ {:^12} │ {:^7} │ {:^12} │ {:^9} │ {:^7} │",
        "Extension", "Total Size", "Size %", "Avg Size", "Count", "Count %"
    )?;
    writeln!(
        f,
        "    ├{}┼{}┼{}┼{}┼{}┼{}┤",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;

    for (ext, (sz, cnt)) in top {
        let avg = if *cnt > 0 { *sz as f64 / *cnt as f64 } else { 0.0 };
        let cnt_pct = if total_regular_files > 0 {
            *cnt as f64 / total_regular_files as f64 * 100.0
        } else {
            0.0
        };
        let sz_pct = if total_files_size > 0 {
            *sz as f64 / total_files_size as f64 * 100.0
        } else {
            0.0
        };
        writeln!(
            f,
            "    │ .{:<11} │ {:>12} │ {:>7} │ {:>12} │ {:^9} │ {:>7} │",
            ext,
            format_bytes(*sz as f64, true),
            fmt_pct(sz_pct),
            format_bytes(avg, true),
            cnt,
            fmt_pct(cnt_pct)
        )?;
    }
    writeln!(
        f,
        "    └{}┴{}┴{}┴{}┴{}┴{}┘",
        "─".repeat(C1),
        "─".repeat(C2),
        "─".repeat(C3),
        "─".repeat(C4),
        "─".repeat(C5),
        "─".repeat(C6)
    )?;
    writeln!(f)
}

fn fmt_pct(pct: f64) -> String {
    if pct >= 100.0 {
        "100%".to_string()
    } else {
        format!("{pct:>4.1}%")
    }
}

fn fmt_error_stats(f: &mut fmt::Formatter<'_>, error_stats: &ErrorStats) -> fmt::Result {
    const C1: usize = 14;
    const C2: usize = 10;

    if error_stats.is_empty() {
        return Ok(());
    }

    let total = error_stats.total();

    writeln!(f, "  ERROR STATISTICS:")?;
    writeln!(f, "    ┌{}┬{}┐", "─".repeat(C1), "─".repeat(C2))?;
    writeln!(f, "    │ {:^12} │ {:^8} │", "Type", "Count")?;
    writeln!(f, "    ├{}┼{}┤", "─".repeat(C1), "─".repeat(C2))?;

    for (label, count) in [
        ("scan", error_stats.scan),
        ("copy", error_stats.copy),
        ("copy_acl", error_stats.copy_acl),
        ("copy_xattr", error_stats.copy_xattr),
        ("delete", error_stats.delete),
        ("rename", error_stats.rename),
        ("symlink_op", error_stats.symlink_op),
        ("pack", error_stats.pack),
        ("integrity_check", error_stats.integrity_check),
    ] {
        if count > 0 {
            writeln!(f, "    │ {label:<12} │ {count:>8} │")?;
        }
    }

    writeln!(f, "    ├{}┼{}┤", "─".repeat(C1), "─".repeat(C2))?;
    writeln!(f, "    │ {:^12} │ {:>8} │", "total", total)?;
    writeln!(f, "    └{}┴{}┘", "─".repeat(C1), "─".repeat(C2))?;
    writeln!(f)
}

// ─────────────────────────────────────────────────
// Integrity Check 结果格式化
// ─────────────────────────────────────────────────

/// 打印 integrity check 校验结果（结果汇总 + Missing 表格 + Mismatch 表格）
pub fn print_integrity_check_result(
    issues: &[IntegrityIssue], total_checked: usize, _checked_files: usize, _checked_dirs: usize,
    _checked_symlinks: usize, quick: bool, auto_fix: bool,
) {
    let mode = if quick { "Quick" } else { "Full" };
    let fix_label = if auto_fix { "On" } else { "Off" };

    let missing: Vec<_> = issues.iter().filter(|i| matches!(i.kind, IssueKind::Missing)).collect();
    let mismatch: Vec<_> = issues
        .iter()
        .filter(|i| matches!(i.kind, IssueKind::Mismatch))
        .collect();
    let errors: Vec<_> = issues.iter().filter(|i| matches!(i.kind, IssueKind::Error)).collect();

    let total_issues = issues.len();
    let total_passed = total_checked.saturating_sub(total_issues);

    // 结果汇总
    println!("  Integrity Check Results:               Mode: {mode}, Auto-Fix: {fix_label}");
    if total_issues == 0 {
        println!("   ├─ Checked:       {total_checked:>12}");
        println!("   └─ All Passed ✓");
    } else {
        let pass_pct = if total_checked > 0 {
            total_passed as f64 / total_checked as f64 * 100.0
        } else {
            0.0
        };
        let fail_pct = if total_checked > 0 {
            total_issues as f64 / total_checked as f64 * 100.0
        } else {
            0.0
        };

        println!("   ├─ Checked:       {total_checked:>12}");
        println!("   ├─ Passed:        {:>12}  ({})", total_passed, fmt_pct(pass_pct));
        println!("   └─ Issues:        {:>12}  ({})", total_issues, fmt_pct(fail_pct));

        // 分类明细：连接符根据后续是否还有非空分组动态选择
        let has_mismatch = !mismatch.is_empty();
        let has_errors = !errors.is_empty();
        let has_auto_fix_line = auto_fix;

        if !missing.is_empty() {
            let m_files = missing.iter().filter(|i| i.entry_type == "file").count();
            let m_dirs = missing.iter().filter(|i| i.entry_type == "dir").count();
            let m_symlinks = missing.iter().filter(|i| i.entry_type == "symlink").count();
            let connector = if has_mismatch || has_errors || has_auto_fix_line {
                "├"
            } else {
                "└"
            };
            println!(
                "      {}─ Missing:    {:>8}  (files: {}, dirs: {}, symlinks: {})",
                connector,
                missing.len(),
                m_files,
                m_dirs,
                m_symlinks
            );
        }
        if has_mismatch {
            let mm_files = mismatch.iter().filter(|i| i.entry_type == "file").count();
            let mm_dirs = mismatch.iter().filter(|i| i.entry_type == "dir").count();
            let mm_symlinks = mismatch.iter().filter(|i| i.entry_type == "symlink").count();
            let connector = if has_errors || has_auto_fix_line { "├" } else { "└" };
            println!(
                "      {}─ Mismatch:  {:>8}  (files: {}, dirs: {}, symlinks: {})",
                connector,
                mismatch.len(),
                mm_files,
                mm_dirs,
                mm_symlinks
            );
        }
        if has_errors {
            let e_files = errors.iter().filter(|i| i.entry_type == "file").count();
            let e_dirs = errors.iter().filter(|i| i.entry_type == "dir").count();
            let e_symlinks = errors.iter().filter(|i| i.entry_type == "symlink").count();
            let connector = if has_auto_fix_line { "├" } else { "└" };
            println!(
                "      {}─ Errors:    {:>8}  (files: {}, dirs: {}, symlinks: {}) — transient NFS failures",
                connector,
                errors.len(),
                e_files,
                e_dirs,
                e_symlinks
            );
        }
        if auto_fix {
            let fixed = mismatch
                .iter()
                .filter(|i| matches!(i.fix_status, FixStatus::Fixed))
                .count();
            let unfixed = mismatch.len() - fixed;
            println!("      └─ Auto-Fixed: {fixed:>8}  (unfixed: {unfixed})");
        }

        println!();

        // Missing 详情表格
        if !missing.is_empty() {
            print_missing_table(&missing);
        }

        // Mismatch 详情表格
        if !mismatch.is_empty() {
            print_mismatch_table(&mismatch, auto_fix);
        }

        // Errors 详情表格
        if !errors.is_empty() {
            print_errors_table(&errors);
        }
    }

    println!();
}

/// 打印 Errors 条目列表（瞬时 NFS 错误导致无法验证的条目）。
///
/// 与 [`print_missing_table`] 区分：Missing 是确认 ENOENT 的真缺失；
/// Errors 是 LOOKUP 因服务端瞬时故障（`NFS4ERR_DELAY` 重试耗尽、连接断开等）失败，
/// 文件可能存在，仅本次未能验证。建议人工复核或重跑 integrity-check。
fn print_errors_table(errors: &[&IntegrityIssue]) {
    println!("  TRANSIENT ERRORS — UNABLE TO VERIFY ({}):", errors.len());
    println!(
        "   (Files may exist; LOOKUP failed due to NFS server busy or connection issues. Re-run integrity-check to confirm.)"
    );

    for (idx, &entry_type) in ["file", "dir", "symlink"].iter().enumerate() {
        let group: Vec<_> = errors.iter().filter(|i| i.entry_type == entry_type).collect();
        if group.is_empty() {
            continue;
        }

        let remaining_types = ["file", "dir", "symlink"][idx + 1..]
            .iter()
            .any(|t| errors.iter().any(|i| i.entry_type == *t));
        let group_connector = if remaining_types { "├" } else { "└" };
        let child_prefix = if remaining_types { "│" } else { " " };

        println!(
            "   {}─ [{}] ({})",
            group_connector,
            entry_type.to_uppercase(),
            group.len()
        );
        for (i, issue) in group.iter().enumerate() {
            let is_last = i == group.len() - 1;
            let item_connector = if is_last { "└" } else { "├" };
            println!("   {}  {}─ {}", child_prefix, item_connector, issue.path);
        }
    }
    println!();
}

/// 打印 Missing 条目列表（tree 结构，按类型分组）
fn print_missing_table(missing: &[&IntegrityIssue]) {
    println!("  MISSING IN DESTINATION ({}):", missing.len());

    for (idx, &entry_type) in ["file", "dir", "symlink"].iter().enumerate() {
        let group: Vec<_> = missing.iter().filter(|i| i.entry_type == entry_type).collect();
        if group.is_empty() {
            continue;
        }

        // 判断当前类型是否是最后一个非空分组
        let remaining_types = ["file", "dir", "symlink"][idx + 1..]
            .iter()
            .any(|t| missing.iter().any(|i| i.entry_type == *t));
        let group_connector = if remaining_types { "├" } else { "└" };
        let child_prefix = if remaining_types { "│" } else { " " };

        println!(
            "   {}─ [{}] ({})",
            group_connector,
            entry_type.to_uppercase(),
            group.len()
        );
        for (i, issue) in group.iter().enumerate() {
            let is_last = i == group.len() - 1;
            let item_connector = if is_last { "└" } else { "├" };
            println!("   {}  {}─ {}", child_prefix, item_connector, issue.path);
        }
    }
    println!();
}

/// 打印 Mismatch 条目列表（tree 结构，按类型分组，显示 mismatch 详情和修复状态）
fn print_mismatch_table(mismatch: &[&IntegrityIssue], auto_fix: bool) {
    println!("  METADATA/CONTENT MISMATCH ({}):", mismatch.len());

    for (idx, &entry_type) in ["file", "dir", "symlink"].iter().enumerate() {
        let group: Vec<_> = mismatch.iter().filter(|i| i.entry_type == entry_type).collect();
        if group.is_empty() {
            continue;
        }

        let remaining_types = ["file", "dir", "symlink"][idx + 1..]
            .iter()
            .any(|t| mismatch.iter().any(|i| i.entry_type == *t));
        let group_connector = if remaining_types { "├" } else { "└" };
        let child_prefix = if remaining_types { "│" } else { " " };

        if auto_fix {
            let fixed = group
                .iter()
                .filter(|i| matches!(i.fix_status, FixStatus::Fixed))
                .count();
            let unfixed = group.len() - fixed;
            println!(
                "   {}─ [{}] ({}, fixed: {}, unfixed: {})",
                group_connector,
                entry_type.to_uppercase(),
                group.len(),
                fixed,
                unfixed
            );
        } else {
            println!(
                "   {}─ [{}] ({})",
                group_connector,
                entry_type.to_uppercase(),
                group.len()
            );
        }
        for (i, issue) in group.iter().enumerate() {
            let is_last = i == group.len() - 1;
            let item_connector = if is_last { "└" } else { "├" };

            let mut detail = issue.mismatches.join(", ");
            if auto_fix {
                match &issue.fix_status {
                    FixStatus::Fixed => detail.push_str("  [FIXED]"),
                    FixStatus::PartiallyFixed | FixStatus::FixFailed => detail.push_str("  [UNFIXED]"),
                    FixStatus::NotAttempted => {}
                }
            }

            println!("   {}  {}─ {}  ({})", child_prefix, item_connector, issue.path, detail);
        }
    }
    println!();
}
