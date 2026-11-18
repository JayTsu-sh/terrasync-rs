//! 统计信息子模块
//!
//! 职责拆分：
//! - `accumulator` — 累计统计（FullStats / IncrementalStats / StatsKind / ErrorStats / DeltaCounts）
//! - `projection` 生成 `app::report` 中定义的可序列化结果
//! - `progress_bar` — 终端进度条 + 原子计数器
//! - `consumer` — StatisticConsumer（聚合以上模块 + HTTP 回调）

pub mod accumulator;
pub(crate) mod callback_state;
pub mod consumer;
pub(crate) mod delivery;
pub mod diagnostics;
pub(crate) mod journal;
pub mod progress_bar;
mod projection;
pub(crate) mod recovery_progress;

#[cfg(test)]
mod entry_count_tests;

// 重新导出：上层只需 `use crate::consumer::stats::*` 即可拿到常用类型
#[cfg(test)]
pub(crate) use crate::report::{JobSummary, ProgressDetail, ProgressReport};
pub use accumulator::{
    DeltaCounts, ErrorStats, FileSizeRange, FullStats, IncrementalStats, StatsKind, TimeRange,
    calculate_file_size_range, calculate_time_range, format_bytes, print_integrity_check_result,
};
pub use consumer::StatisticConsumer;
pub use progress_bar::{DirectoryMetadataProgressBar, ProgressBar};
