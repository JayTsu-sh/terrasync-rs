use std::sync::Arc;

use app::consumer::stats::report::{FinalStats, ProgressReport};
use serde::Serialize;
use tracing::{debug, warn};

use crate::domain::repository::ProgressRepository;
use crate::error::Result;

/// 分析数据聚合结果
#[derive(Debug, Serialize)]
pub struct AnalyticsData {
    pub file_type_distribution: Vec<FileTypeEntry>,
    pub size_distribution: Vec<SizeEntry>,
    pub time_distribution: Vec<TimeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FileTypeEntry {
    pub ext: String,
    pub count: u64,
    pub total_size: u64,
}

#[derive(Debug, Serialize)]
pub struct SizeEntry {
    pub range: String,
    pub count: u64,
    pub total_size: u64,
}

#[derive(Debug, Serialize)]
pub struct TimeEntry {
    pub bucket: String,
    pub count: u64,
}

/// 分析查询服务 — 从 SQLite 中 final callback 存储的 FinalStats 提取图表数据
pub struct AnalyticsService {
    progress_repo: Arc<dyn ProgressRepository>,
}

impl AnalyticsService {
    pub fn new(progress_repo: Arc<dyn ProgressRepository>) -> Self {
        Self { progress_repo }
    }

    /// 获取指定任务的分析数据（文件类型分布、大小分布、时间分布）
    ///
    /// 数据来源：`task_progress.report_json` 中 `is_final=true` 的 `FinalStats`，
    /// 由 app 层 StatisticConsumer 通过 HTTP callback 写入。
    pub async fn get_analytics(&self, task_id: &str) -> Result<AnalyticsData> {
        let progress = self.progress_repo.find_by_task_id(task_id).await?;

        let Some(progress) = progress else {
            debug!("[analytics] no progress data for task {task_id}");
            return Ok(empty_analytics(Some("任务尚未产生统计数据".to_string())));
        };

        if !progress.is_final {
            debug!("[analytics] progress for task {task_id} is not final yet");
            return Ok(empty_analytics(Some(
                "任务尚未完成，统计数据待最终回调写入".to_string(),
            )));
        }

        let report: ProgressReport = match serde_json::from_str(&progress.report_json) {
            Ok(r) => r,
            Err(e) => {
                warn!("[analytics] failed to parse report_json for task {task_id}: {e}");
                return Ok(empty_analytics(Some(format!("解析进度报告失败: {e}"))));
            }
        };

        let Some(final_stats) = report.final_stats else {
            debug!("[analytics] final callback has no final_stats for task {task_id}");
            return Ok(empty_analytics(Some("最终回调中未包含统计分布数据".to_string())));
        };

        Ok(convert_final_stats(&final_stats))
    }
}

fn empty_analytics(db_error: Option<String>) -> AnalyticsData {
    AnalyticsData {
        file_type_distribution: Vec::new(),
        size_distribution: Vec::new(),
        time_distribution: Vec::new(),
        db_error,
    }
}

/// 将 app 层的 `FinalStats` 转换为前端期望的 `AnalyticsData`
fn convert_final_stats(stats: &FinalStats) -> AnalyticsData {
    let file_type_distribution = stats
        .extension_stats
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|e| FileTypeEntry {
                    ext: if e.extension.is_empty() {
                        "(无扩展名)".to_string()
                    } else {
                        e.extension.clone()
                    },
                    count: e.count as u64,
                    total_size: e.total_size.max(0) as u64,
                })
                .collect()
        })
        .unwrap_or_default();

    let size_distribution = stats
        .file_size_range_stats
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|e| SizeEntry {
                    range: e.range.clone(),
                    count: e.count as u64,
                    total_size: e.total_size.max(0) as u64,
                })
                .collect()
        })
        .unwrap_or_default();

    let time_distribution = stats
        .time_range_stats
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|e| TimeEntry {
                    bucket: e.range.clone(),
                    count: e.count as u64,
                })
                .collect()
        })
        .unwrap_or_default();

    AnalyticsData {
        file_type_distribution,
        size_distribution,
        time_distribution,
        db_error: None,
    }
}
