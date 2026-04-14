use app::consumer::ProgressReport;
use app::consumer::stats::ProgressDetail;
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use tracing::warn;

use crate::api::state::AppState;
use crate::application::task_service::{CreateTaskRequest, UpdateTaskRequest};
use crate::domain::execution::{LogLevel, TaskExecution, TaskLog};
use crate::domain::task::{MigrationTask, TaskFilter, TaskStatus, TaskType};
use crate::error::Result;

#[derive(Debug, Deserialize, Default)]
pub struct TaskListQuery {
    pub status: Option<String>,
    pub task_type: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct LogQuery {
    pub level: Option<String>,
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

pub async fn list_tasks(
    State(state): State<AppState>, Query(query): Query<TaskListQuery>,
) -> Result<Json<Vec<MigrationTask>>> {
    let filter = TaskFilter {
        status: query.status.and_then(|s| s.parse::<TaskStatus>().ok()),
        task_type: query.task_type.and_then(|t| t.parse::<TaskType>().ok()),
    };
    let tasks = state.task_service.list_tasks(filter).await?;
    Ok(Json(tasks))
}

pub async fn create_task(
    State(state): State<AppState>, req: std::result::Result<Json<CreateTaskRequest>, JsonRejection>,
) -> Result<Json<MigrationTask>> {
    let Json(req) = req.map_err(crate::error::WebError::from)?;
    let task = state.task_service.create_task(req).await?;
    Ok(Json(task))
}

pub async fn update_task(
    State(state): State<AppState>, Path(id): Path<String>,
    req: std::result::Result<Json<UpdateTaskRequest>, JsonRejection>,
) -> Result<Json<MigrationTask>> {
    let Json(req) = req.map_err(crate::error::WebError::from)?;
    let task = state.task_service.update_task(&id, req).await?;
    Ok(Json(task))
}

pub async fn get_task(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<MigrationTask>> {
    let task = state.task_service.get_task(&id).await?;
    Ok(Json(task))
}

pub async fn delete_task(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<()>> {
    state.task_service.delete_task(&id).await?;
    Ok(Json(()))
}

pub async fn start_task(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<String>> {
    let execution_id = state.task_service.start_task(&id).await?;
    Ok(Json(execution_id))
}

pub async fn cancel_task(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<()>> {
    state.task_service.cancel_task(&id).await?;
    Ok(Json(()))
}

pub async fn list_executions(
    State(state): State<AppState>, Path(task_id): Path<String>,
) -> Result<Json<Vec<TaskExecution>>> {
    let executions = state.task_service.get_executions(&task_id).await?;
    Ok(Json(executions))
}

pub async fn get_execution(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<TaskExecution>> {
    let execution = state.task_service.get_execution(&id).await?;
    Ok(Json(execution))
}

pub async fn get_execution_logs(
    State(state): State<AppState>, Path(id): Path<String>, Query(query): Query<LogQuery>,
) -> Result<Json<Vec<TaskLog>>> {
    let level = query.level.and_then(|l| l.parse::<LogLevel>().ok());
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(100);
    let logs = state.task_service.get_execution_logs(&id, level, offset, limit).await?;
    Ok(Json(logs))
}

pub async fn delete_execution(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<()>> {
    state.task_service.delete_execution(&id).await?;
    Ok(Json(()))
}

pub async fn get_progress(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<serde_json::Value>> {
    // 从 SQLite 读取（callback 上报的数据），转换为前端期望的扁平结构
    if let Some(progress) = state.task_service.get_progress(&id).await? {
        match serde_json::from_str::<ProgressReport>(&progress.report_json) {
            Ok(report) => return Ok(Json(progress_report_to_snapshot(&report))),
            Err(e) => warn!("[Progress] Failed to parse report_json for task {id}: {e}"),
        }
    }
    Ok(Json(serde_json::json!(null)))
}

/// 将 ProgressReport 转换为前端 TaskProgressSnapshot 期望的扁平结构
fn progress_report_to_snapshot(report: &ProgressReport) -> serde_json::Value {
    match &report.detail {
        ProgressDetail::Full {
            scanned_files,
            scanned_dirs,
            total_bytes,
            error_count,
            ..
        } => serde_json::json!({
            "scanned_files": scanned_files,
            "scanned_dirs": scanned_dirs,
            "total_bytes": total_bytes,
            "error_count": error_count,
            "elapsed_secs": report.elapsed_secs,
            "speed_bytes_per_sec": report.speed_bytes_per_sec,
        }),
        ProgressDetail::Incremental {
            scanned_files,
            scanned_dirs,
            scanned_size,
            error_count,
            ..
        } => serde_json::json!({
            "scanned_files": scanned_files,
            "scanned_dirs": scanned_dirs,
            "total_bytes": scanned_size,
            "error_count": error_count,
            "elapsed_secs": report.elapsed_secs,
            "speed_bytes_per_sec": report.speed_bytes_per_sec,
        }),
    }
}

/// 接收来自 app 层 StatisticConsumer 的进度回调
pub async fn update_progress(
    State(state): State<AppState>, Path(id): Path<String>, Json(report): Json<ProgressReport>,
) -> Result<Json<()>> {
    state.task_service.update_progress(&id, report).await?;
    Ok(Json(()))
}
