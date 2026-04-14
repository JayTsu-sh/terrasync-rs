use axum::Json;
use axum::extract::{Path, State};

use crate::api::state::AppState;
use crate::application::path_service::CreatePathRequest;
use crate::domain::path::MigrationPath;
use crate::error::Result;

pub async fn list_paths(
    State(state): State<AppState>, Path(endpoint_id): Path<String>,
) -> Result<Json<Vec<MigrationPath>>> {
    let paths = state.path_service.list_paths(&endpoint_id).await?;
    Ok(Json(paths))
}

pub async fn create_path(
    State(state): State<AppState>, Path(endpoint_id): Path<String>, Json(req): Json<CreatePathRequest>,
) -> Result<Json<MigrationPath>> {
    let path = state.path_service.add_path(&endpoint_id, req).await?;
    Ok(Json(path))
}

pub async fn delete_path(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<()>> {
    state.path_service.delete_path(&id).await?;
    Ok(Json(()))
}
