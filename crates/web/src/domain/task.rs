use serde::{Deserialize, Serialize};

/// 任务类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    Scan,
    Sync,
    IntegrityCheck,
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan => write!(f, "scan"),
            Self::Sync => write!(f, "sync"),
            Self::IntegrityCheck => write!(f, "integrity_check"),
        }
    }
}

impl std::str::FromStr for TaskType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scan" => Ok(Self::Scan),
            "sync" => Ok(Self::Sync),
            "integrity_check" => Ok(Self::IntegrityCheck),
            other => Err(format!("unknown task type: {other}")),
        }
    }
}

/// 任务状态枚举（值对象）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("unknown task status: {other}")),
        }
    }
}

impl TaskStatus {
    /// 该状态下是否允许启动任务
    pub fn can_start(&self) -> bool {
        !matches!(self, Self::Running)
    }

    /// 该状态下是否允许取消任务
    pub fn can_cancel(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// 该状态下是否允许删除任务
    pub fn can_delete(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// 任务配置（值对象）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_expr: Option<String>,
    #[serde(default)]
    pub depth: u32,
    #[serde(default)]
    pub enable_integrity_check: bool,
    #[serde(default)]
    pub enable_acl: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<String>,
    #[serde(default = "default_peak_qos_rate")]
    pub peak_qos_rate: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iops: Option<u32>,
}

fn default_peak_qos_rate() -> f32 {
    2.0
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            match_expr: None,
            exclude_expr: None,
            depth: 0,
            enable_integrity_check: false,
            enable_acl: false,
            qos: None,
            peak_qos_rate: 2.0,
            block_size: None,
            iops: None,
        }
    }
}

/// MigrationTask 聚合根
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTask {
    pub id: String,
    pub name: String,
    pub task_type: TaskType,
    pub source_endpoint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest_endpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest_path_id: Option<String>,
    pub config: TaskConfig,
    pub status: TaskStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// 任务列表过滤条件
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub status: Option<TaskStatus>,
    pub task_type: Option<TaskType>,
}
