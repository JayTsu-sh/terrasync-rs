//! 统计信息子模块
//!
//! 职责拆分：
//! - `accumulator` — 累计统计（FullStats / IncrementalStats / StatsKind / ErrorStats / DeltaCounts）
//! - `report` — 可序列化输出类型（ProgressReport / JobResult / FinalStats）
//! - `progress_bar` — 终端进度条 + 原子计数器
//! - `consumer` — StatisticConsumer（聚合以上模块 + HTTP 回调）

pub mod accumulator;
pub mod consumer;
pub mod progress_bar;
pub mod report;

// 重新导出：上层只需 `use crate::consumer::stats::*` 即可拿到常用类型
pub use accumulator::{
    DeltaCounts, ErrorStats, FileSizeRange, FullStats, IncrementalStats, StatsKind, TimeRange,
    calculate_file_size_range, calculate_time_range, format_bytes, print_integrity_check_result,
};
pub use consumer::StatisticConsumer;
pub use progress_bar::{DirectoryMetadataProgressBar, ProgressBar};
pub use report::{FinalStats, JobResult, JobSummary, ProgressDetail, ProgressReport};
