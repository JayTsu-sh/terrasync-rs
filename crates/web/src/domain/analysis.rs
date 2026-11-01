use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, WebError};

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisMetric {
    #[default]
    Size,
    Count,
}

#[derive(Debug, Default, Deserialize)]
pub struct AnalysisTreeQuery {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub metric: AnalysisMetric,
    pub limit: Option<u16>,
}

impl AnalysisTreeQuery {
    pub fn validate(&self) -> Result<(String, u16)> {
        let path = self.path.trim_end_matches('/');
        if path.len() > 4096
            || path.starts_with('/')
            || path.chars().any(char::is_control)
            || path.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(WebError::ValidationError(
                "分析路径必须是扫描根目录内的相对路径".to_string(),
            ));
        }
        let limit = self.limit.unwrap_or(120);
        if !(1..=500).contains(&limit) {
            return Err(WebError::ValidationError(
                "分析条目上限必须在 1 到 500 之间".to_string(),
            ));
        }
        Ok((path.to_string(), limit))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisNode {
    pub path: String,
    pub name: String,
    pub kind: String,
    pub bytes: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub modified_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StorageAnalysis {
    pub task_id: String,
    pub path: String,
    pub source_path: String,
    pub scanned_at: Option<String>,
    pub is_partial: bool,
    pub total_bytes: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub children: Vec<AnalysisNode>,
}

/// 查询层只返回当前目录的有界摘要，原始扫描明细保留在数据库中。
#[async_trait]
pub trait AnalysisRepository: Send + Sync {
    async fn children(
        &self, task_id: &str, path: &str, metric: AnalysisMetric, limit: u16,
    ) -> Result<Vec<AnalysisNode>>;
}

#[cfg(test)]
mod tests {
    use super::AnalysisTreeQuery;

    #[test]
    fn validates_relative_boundaries_and_query_budget() {
        for path in ["/etc", "../etc", "a/../b", "a/./b", "a\nb"] {
            assert!(
                AnalysisTreeQuery {
                    path: path.into(),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            AnalysisTreeQuery {
                limit: Some(501),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            AnalysisTreeQuery {
                limit: Some(0),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        let query = AnalysisTreeQuery {
            path: "reports/2026/".into(),
            ..Default::default()
        };
        assert!(matches!(query.validate(), Ok((path, 120)) if path == "reports/2026"));
    }
}
