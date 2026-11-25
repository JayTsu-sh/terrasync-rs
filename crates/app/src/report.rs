//! 可序列化的报告/输出类型
//!
//! 核心输出类型，职责正交：
//! - `ProgressReport` — 实时进度 + HTTP 回调 envelope（统一快照与回调）
//! - `JobResult` — 任务完成后的最终返回值（scan/sync/ic 统一）
//! - `FinalStats` — 最终分布统计（附加到 `ProgressReport` 的 final 回调）

use serde::{Deserialize, Serialize};
use task_model::migration::MigrationReport;

pub use crate::consumer::stats::{DeltaCounts, ErrorStats};
pub use crate::scan::statistics::ScanSnapshot;
use task_model::diagnostics::{CallbackIdentity, DiagnosticBatch};

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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanRuntimePhase {
    #[default]
    Running,
    Recovering,
    Failed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ScanRuntimeState {
    pub state: ScanRuntimePhase,
    pub network_faults: u64,
    pub recovery_attempts: u64,
    pub successful_recoveries: u64,
    pub current_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressReport {
    pub target_preserved_count: u64,
    #[serde(default)]
    pub skipped_count: u64,
    pub job_id: String,
    pub job_type: String,
    pub elapsed_secs: f64,
    pub speed_bytes_per_sec: f64,
    pub detail: ProgressDetail,
    /// 当前文件尚未发布的本轮确认写入字节。
    #[serde(default)]
    pub in_flight_bytes: u64,
    pub is_final: bool,
    #[serde(default)]
    pub diagnostics_complete: bool,
    #[serde(default)]
    pub task_error_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<ScanSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_identity: Option<CallbackIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<DiagnosticBatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_runtime: Option<ScanRuntimeState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_committed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<MigrationReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_stats: Option<FinalStats>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// (3) JobResult — 任务完成后的最终返回值
//     替代: StatisticJobStats
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub skipped_count: u64,
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
    #[serde(default)]
    pub skipped_count: u64,
    pub dirs: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir_size: Option<i64>,
    pub regular_files: usize,
    pub regular_file_size: i64,
    pub symlinks: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symlink_size: Option<i64>,
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
