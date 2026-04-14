use serde::{Deserialize, Serialize};

use super::task::TaskStatus;

/// 执行统计数据（值对象）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub scanned_files: u64,
    pub scanned_dirs: u64,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub error_count: u64,
    pub duration_secs: f64,
    pub avg_speed_bytes_per_sec: f64,
}

/// 执行类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionType {
    Full,
    Incremental,
}

impl std::fmt::Display for ExecutionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Incremental => write!(f, "incremental"),
        }
    }
}

impl std::str::FromStr for ExecutionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full" => Ok(Self::Full),
            "incremental" => Ok(Self::Incremental),
            other => Err(format!("unknown execution type: {other}")),
        }
    }
}

/// TaskExecution 实体 — 任务执行历史记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    pub id: String,
    pub task_id: String,
    pub execution_type: ExecutionType,
    pub status: TaskStatus,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<ExecutionStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// 日志级别
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warn => write!(f, "warn"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(format!("unknown log level: {other}")),
        }
    }
}

/// TaskLog 值对象 — 任务执行日志
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLog {
    pub id: String,
    pub execution_id: String,
    pub level: LogLevel,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
