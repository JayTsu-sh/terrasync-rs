use std::net::SocketAddr;

use app::consumer::ProgressReport;
use app::consumer::stats::ProgressDetail;
use axum::Json;
use axum::extract::ConnectInfo;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use tracing::warn;

use crate::api::state::AppState;
use crate::application::task_service::{CreateTaskRequest, UpdateTaskRequest};
use crate::domain::execution::{LogLevel, TaskExecution, TaskLog};
use crate::domain::progress::DiagnosticPage;
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

#[derive(Debug, Deserialize, Default)]
pub struct DiagnosticQuery {
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

pub async fn get_diagnostics(
    State(state): State<AppState>, Path(id): Path<String>, Query(query): Query<DiagnosticQuery>,
) -> Result<Json<DiagnosticPage>> {
    Ok(Json(
        state
            .task_service
            .get_diagnostics(&id, query.offset.unwrap_or(0), query.limit.unwrap_or(50))
            .await?,
    ))
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
    // 最大返回 10,000 条，防止大查询导致内存耗尽
    let limit = query.limit.unwrap_or(100).min(10_000);
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
            Ok(report) => return Ok(Json(progress_report_to_snapshot(&report)?)),
            Err(e) => warn!("[Progress] Failed to parse report_json for task {id}: {e}"),
        }
    }
    Ok(Json(serde_json::json!(null)))
}

/// 将 `ProgressReport` 转换为前端 `TaskProgressSnapshot` 期望的扁平结构
fn progress_report_to_snapshot(report: &ProgressReport) -> Result<serde_json::Value> {
    let mut snapshot = match &report.detail {
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
            scanned_symlinks,
            scanned_dirs,
            scanned_size,
            error_count,
            ..
        } => serde_json::json!({
            "scanned_files": scanned_files.saturating_add(*scanned_symlinks),
            "scanned_dirs": scanned_dirs,
            "total_bytes": scanned_size,
            "error_count": error_count,
            "elapsed_secs": report.elapsed_secs,
            "speed_bytes_per_sec": report.speed_bytes_per_sec,
        }),
    };
    if let Some(scan) = &report.scan {
        snapshot["scan"] = serde_json::to_value(scan)?;
    }
    if let Some(identity) = &report.callback_identity {
        snapshot["callback_identity"] = serde_json::to_value(identity)?;
    }
    if let Some(runtime) = &report.scan_runtime {
        snapshot["scan_runtime"] = serde_json::to_value(runtime)?;
    }
    Ok(snapshot)
}

#[derive(Debug, Deserialize, Default)]
pub struct CallbackQuery {
    pub execution_id: Option<String>,
}

/// 接收本机回调，仅在持久化成功后返回批次 ACK。
pub async fn update_progress(
    ConnectInfo(peer): ConnectInfo<SocketAddr>, State(state): State<AppState>, Path(id): Path<String>,
    Query(query): Query<CallbackQuery>, Json(report): Json<ProgressReport>,
) -> impl IntoResponse {
    let ip = peer.ip();
    if !ip.is_loopback() {
        warn!("拒绝来自非本机地址的 update_progress 请求: {}", peer);
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    match state
        .task_service
        .update_progress(&id, report, query.execution_id.as_deref())
        .await
    {
        Ok(ack) => Json(ack).into_response(),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn incremental_snapshot_counts_links_like_the_full_snapshot() -> Result<()> {
        let report: ProgressReport = serde_json::from_value(serde_json::json!({
            "job_id":"links", "job_type":"IncrementalScan", "elapsed_secs":1.0,
            "speed_bytes_per_sec":0.0, "is_final":true,
            "detail":{"kind":"Incremental", "scanned_files":10, "scanned_dirs":2,
                "scanned_symlinks":4, "scanned_size":0, "transferred_bytes":0,
                "error_count":0, "new_count":0, "new_size":0, "changed_count":0,
                "changed_size":0, "deleted_count":0, "deleted_size":0,
                "renamed_count":0, "renamed_size":0}
        }))?;
        let snapshot = progress_report_to_snapshot(&report)?;
        assert_eq!(snapshot["scanned_files"], 14);
        assert_eq!(snapshot["scanned_dirs"], 2);
        Ok(())
    }

    #[test]
    fn progress_snapshot_preserves_scan_counts_capabilities_and_current_recovery_state() -> Result<()> {
        let kinds = serde_json::json!({"directories":1,"regular_files":2,"symlinks":0,"block_devices":0,
            "character_devices":0,"fifos":0,"sockets":0,"unknown":0});
        let empty = serde_json::json!({"directories":0,"regular_files":0,"symlinks":0,"block_devices":0,
            "character_devices":0,"fifos":0,"sockets":0,"unknown":0});
        let report: ProgressReport = serde_json::from_value(serde_json::json!({
            "job_id":"scan", "job_type":"FullScan", "elapsed_secs":1.0, "speed_bytes_per_sec":0.0,
            "is_final":false, "detail":{"kind":"Full","scanned_files":2,"scanned_dirs":1,
                "total_bytes":10,"transferred_bytes":0,"error_count":3},
            "scan":{"scanned_entries":3,"filtered_entries":0,"retained_entries":3,"complete":false,
                "replayed_directory_listings":7,"distributions_reconciled":true,
                "counts":{"scanned":kinds,"filtered":empty,"retained":kinds,"failed":empty,"vanished":empty,
                    "pruned_directories":0,"truncated_directories":0,"regular_file_bytes":10,
                    "unknown_file_sizes":0,"versions":2,"delete_markers":0,"all_versions_bytes":10,
                    "completed_listings":1,"interrupted_listings":0,"unreadable_listings":0,"vanished_listings":0},
                "capabilities":{"backend":"s3","symlinks":"unsupported","special_files":"unsupported",
                    "directory_objects":false,"metadata":[]}},
            "scan_runtime":{"state":"recovering","network_faults":3,"recovery_attempts":2,
                "successful_recoveries":1,"current_error":"connection reset"}
        }))?;
        let snapshot = progress_report_to_snapshot(&report)?;
        assert_eq!(snapshot["scanned_files"], 2);
        assert_eq!(snapshot["error_count"], 3);
        assert_eq!(snapshot["scan"]["retained_entries"], 3);
        assert_eq!(snapshot["scan"]["replayed_directory_listings"], 7);
        assert_eq!(snapshot["scan"]["distributions_reconciled"], true);
        assert_eq!(snapshot["scan"]["capabilities"]["symlinks"], "unsupported");
        assert_eq!(snapshot["scan_runtime"]["current_error"], "connection reset");
        Ok(())
    }
}
