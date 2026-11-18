//! 将任务累计值投影为对外报告。
use std::collections::HashMap;

use super::accumulator::{FileSizeRange, StatsKind, TimeRange};
use crate::config::JobType;
use crate::report::{
    ExtensionStatEntry, FileSizeRangeStatEntry, FinalStats, IncrementalDeltaStats, JobResult, JobSummary,
    TimeRangeStatEntry,
};

impl StatsKind {
    /// 生成最终结果报告（替代 `StatisticJobStats`）
    pub fn to_job_result(&self) -> JobResult {
        match self {
            StatsKind::Full(s) => JobResult {
                skipped_count: s.skipped_count,
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
                skipped_count: s.skipped_count,
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
                    skipped_count: s.skipped_count,
                    dirs: s.dir_count,
                    dir_size: Some(s.dir_size),
                    regular_files: s.regular_file_count,
                    regular_file_size: s.regular_file_size,
                    symlinks: s.symlink_count,
                    symlink_size: Some(s.symlink_size),
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
                skipped_count: s.skipped_count,
                dirs: s.scanned.dir_count,
                dir_size: Some(s.scanned.dir_size),
                regular_files: s.scanned.regular_file_count,
                regular_file_size: s.scanned.regular_file_size,
                symlinks: s.scanned.symlink_count,
                symlink_size: Some(s.scanned.symlink_size),
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
