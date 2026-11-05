use app::consumer::stats::diagnostics::{CallbackAck, CallbackIdentity, DiagnosticBatch, DiagnosticEvent};
use serde::{Deserialize, Serialize};

use super::execution::ExecutionStats;

/// 一次回调的原子持久化单元；明细 ACK 与快照接受与否相互独立。
pub struct ProgressSubmission<'a> {
    pub progress: &'a TaskProgress,
    pub identity: Option<&'a CallbackIdentity>,
    pub diagnostics: Option<&'a DiagnosticBatch>,
    pub execution_id: Option<&'a str>,
    pub final_stats: Option<&'a ExecutionStats>,
}

/// 事务提交后的结果；旧回调可确认明细，但不能覆盖新快照。
pub struct ProgressReceipt {
    pub accepted: bool,
    pub ack: CallbackAck,
}

/// 有界明细页面；省略数字按已持久化的唯一批次累计。
#[derive(Debug, Serialize)]
pub struct DiagnosticPage {
    pub events: Vec<DiagnosticEvent>,
    pub omitted_count: u64,
    pub next_offset: Option<u32>,
}

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
