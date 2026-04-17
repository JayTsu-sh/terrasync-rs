use serde::{Deserialize, Serialize};

/// `TaskProgress` 值对象 — 实时任务进度
///
/// `snapshot_json` 存储 `app::consumer::stats::ProgressReport` 的 JSON 序列化，
/// 避免逐列拆解 + 22 列的 mirror type 问题。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgress {
    pub task_id: String,
    /// `ProgressReport` 的 JSON 序列化（包含 snapshot + `is_final` + `final_stats`）
    pub report_json: String,
    pub is_final: bool,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// WebSocket 推送的进度事件
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgressEvent {
    Progress { task_id: String, data: serde_json::Value },
    StatusChange { task_id: String, data: StatusChangeData },
}

/// 状态变更数据
#[derive(Debug, Clone, Serialize)]
pub struct StatusChangeData {
    pub old_status: String,
    pub new_status: String,
}
