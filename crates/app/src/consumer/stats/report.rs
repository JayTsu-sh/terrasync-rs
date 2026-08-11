//! 可序列化的报告/输出类型
//!
//! 核心输出类型，职责正交：
//! - `ProgressReport` — 实时进度 + HTTP 回调 envelope（统一快照与回调）
//! - `JobResult` — 任务完成后的最终返回值（scan/sync/ic 统一）
//! - `FinalStats` — 最终分布统计（附加到 `ProgressReport` 的 final 回调）

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::accumulator::{DeltaCounts, ErrorStats, FileSizeRange, StatsKind, TimeRange};
use crate::config::JobType;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// (1) ProgressDetail — Full / Incremental 进度明细
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ProgressDetail {
    Full {
        scanned_files: u64,
        scanned_dirs: u64,
        total_bytes: u64,
        transferred_bytes: u64,
        error_count: u64,
    },
    Incremental {
        scanned_files: u64,
        scanned_dirs: u64,
        scanned_symlinks: u64,
        scanned_size: u64,
        transferred_bytes: u64,
        error_count: u64,
        new_count: u64,
        new_size: u64,
        changed_count: u64,
        changed_size: u64,
        deleted_count: u64,
        deleted_size: u64,
        renamed_count: u64,
        renamed_size: u64,
    },
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// (2) ProgressReport — 实时进度 + HTTP 回调 envelope
//     合并原 ProgressSnapshot + ProgressReport
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressReport {
    pub job_id: String,
    pub job_type: String,
    pub elapsed_secs: f64,
    pub speed_bytes_per_sec: f64,
    pub detail: ProgressDetail,
    pub is_final: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_committed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_stats: Option<FinalStats>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// (3) JobResult — 任务完成后的最终返回值
//     替代: StatisticJobStats
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub job_type: String,
    pub duration_secs: f64,
    pub summary: JobSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum JobSummary {
    Full {
        dirs: usize,
        regular_files: usize,
        symlinks: usize,
        total_size: i64,
        transferred_bytes: u64,
        errors: ErrorStats,
    },
    Incremental {
        scanned_dirs: usize,
        scanned_regular_files: usize,
        scanned_symlinks: usize,
        scanned_size: i64,
        transferred_bytes: u64,
        errors: ErrorStats,
        new: DeltaCounts,
        changed: DeltaCounts,
        deleted: DeltaCounts,
        renamed: DeltaCounts,
    },
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// (4) FinalStats — 最终分布统计
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalStats {
    pub dirs: usize,
    pub dir_size: i64,
    pub regular_files: usize,
    pub regular_file_size: i64,
    pub symlinks: usize,
    pub symlink_size: i64,
    pub errors: ErrorStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension_stats: Option<Vec<ExtensionStatEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_stats: Option<Vec<TimeRangeStatEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size_range_stats: Option<Vec<FileSizeRangeStatEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental: Option<IncrementalDeltaStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalDeltaStats {
    pub new: DeltaCounts,
    pub changed: DeltaCounts,
    pub deleted: DeltaCounts,
    pub renamed: DeltaCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionStatEntry {
    pub extension: String,
    pub total_size: i64,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRangeStatEntry {
    pub range: String,
    pub total_size: i64,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSizeRangeStatEntry {
    pub range: String,
    pub total_size: i64,
    pub count: usize,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// StatsKind → report 类型的转换方法
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

impl StatsKind {
    /// 生成最终结果报告（替代 `StatisticJobStats`）
    pub fn to_job_result(&self) -> JobResult {
        match self {
            StatsKind::Full(s) => JobResult {
                job_id: s.job_id.clone(),
                job_type: s.job_type.to_callback_name().to_string(),
                duration_secs: s.start_time.elapsed().as_secs_f64(),
                summary: JobSummary::Full {
                    dirs: s.dir_count,
                    regular_files: s.regular_file_count,
                    symlinks: s.symlink_count,
                    total_size: s.total_size(),
                    transferred_bytes: 0, // scan 不传输
                    errors: s.error_stats.clone(),
                },
            },
            StatsKind::Incremental(s) => JobResult {
                job_id: s.job_id.clone(),
                job_type: s.job_type.to_callback_name().to_string(),
                duration_secs: s.start_time.elapsed().as_secs_f64(),
                summary: JobSummary::Incremental {
                    scanned_dirs: s.scanned.dir_count,
                    scanned_regular_files: s.scanned.regular_file_count,
                    scanned_symlinks: s.scanned.symlink_count,
                    scanned_size: s.scanned.total_size(),
                    transferred_bytes: 0,
                    errors: s.error_stats.clone(),
                    new: s.new.clone(),
                    changed: s.changed.clone(),
                    deleted: s.deleted.clone(),
                    renamed: s.renamed.clone(),
                },
            },
        }
    }

    /// 生成最终分布统计
    pub fn to_final_stats(&self) -> FinalStats {
        match self {
            StatsKind::Full(s) => {
                let is_ic = matches!(s.job_type, JobType::IntegrityCheck);
                FinalStats {
                    dirs: s.dir_count,
                    dir_size: s.dir_size,
                    regular_files: s.regular_file_count,
                    regular_file_size: s.regular_file_size,
                    symlinks: s.symlink_count,
                    symlink_size: s.symlink_size,
                    errors: s.error_stats.clone(),
                    extension_stats: if is_ic {
                        None
                    } else {
                        Some(convert_extension_stats(&s.extension_stats))
                    },
                    time_range_stats: if is_ic {
                        None
                    } else {
                        Some(convert_time_range_stats(&s.time_range_stats))
                    },
                    file_size_range_stats: if is_ic {
                        None
                    } else {
                        Some(convert_file_size_range_stats(&s.file_size_range_stats))
                    },
                    incremental: None,
                }
            }
            StatsKind::Incremental(s) => FinalStats {
                dirs: s.scanned.dir_count,
                dir_size: s.scanned.dir_size,
                regular_files: s.scanned.regular_file_count,
                regular_file_size: s.scanned.regular_file_size,
                symlinks: s.scanned.symlink_count,
                symlink_size: s.scanned.symlink_size,
                errors: s.error_stats.clone(),
                extension_stats: Some(convert_extension_stats(&s.scanned.extension_stats)),
                time_range_stats: Some(convert_time_range_stats(&s.scanned.time_range_stats)),
                file_size_range_stats: Some(convert_file_size_range_stats(&s.scanned.file_size_range_stats)),
                incremental: Some(IncrementalDeltaStats {
                    new: s.new.clone(),
                    changed: s.changed.clone(),
                    deleted: s.deleted.clone(),
                    renamed: s.renamed.clone(),
                }),
            },
        }
    }
}

fn convert_extension_stats(stats: &HashMap<String, (i64, usize)>) -> Vec<ExtensionStatEntry> {
    let mut v: Vec<_> = stats
        .iter()
        .map(|(ext, (size, count))| ExtensionStatEntry {
            extension: ext.clone(),
            total_size: *size,
            count: *count,
        })
        .collect();
    v.sort_by(|a, b| b.total_size.cmp(&a.total_size));
    v
}

fn convert_time_range_stats(stats: &HashMap<TimeRange, (i64, usize)>) -> Vec<TimeRangeStatEntry> {
    stats
        .iter()
        .map(|(range, (size, count))| TimeRangeStatEntry {
            range: range.to_string(),
            total_size: *size,
            count: *count,
        })
        .collect()
}

fn convert_file_size_range_stats(stats: &HashMap<FileSizeRange, (i64, usize)>) -> Vec<FileSizeRangeStatEntry> {
    stats
        .iter()
        .map(|(range, (size, count))| FileSizeRangeStatEntry {
            range: range.to_string(),
            total_size: *size,
            count: *count,
        })
        .collect()
}
