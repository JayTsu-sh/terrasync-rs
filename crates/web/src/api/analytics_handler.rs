use axum::Json;
use axum::extract::{Path, Query, State};
use tracing::info;

use crate::api::state::AppState;
use crate::application::analytics_service::AnalyticsData;
use crate::domain::analysis::{AnalysisTreeQuery, StorageAnalysis};
use crate::error::Result;

/// GET /api/v1/tasks/{id}/analytics
pub async fn get_analytics(State(state): State<AppState>, Path(task_id): Path<String>) -> Result<Json<AnalyticsData>> {
    // 验证任务存在
    let task = state.task_service.get_task(&task_id).await?;
    info!("[analytics] request for task {}, status: {:?}", task_id, task.status);

    let data = state.analytics_service.get_analytics(&task_id).await?;
    Ok(Json(data))
}

/// GET /api/v1/tasks/{id}/analysis/tree
pub async fn get_tree(
    State(state): State<AppState>, Path(task_id): Path<String>, Query(query): Query<AnalysisTreeQuery>,
) -> Result<Json<StorageAnalysis>> {
    Ok(Json(state.storage_analysis_service.get_tree(&task_id, &query).await?))
}
