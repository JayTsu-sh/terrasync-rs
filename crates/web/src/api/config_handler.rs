use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::application::config_service::{ConfigUpdate, ConfigView};
use crate::error::Result;

#[derive(Debug, Deserialize)]
pub struct ConfigUpdateRequest {
    pub updates: Vec<ConfigUpdate>,
}

pub async fn get_config(State(state): State<AppState>) -> Result<Json<ConfigView>> {
    let view = state.config_service.get_config().await?;
    Ok(Json(view))
}

pub async fn update_config(State(state): State<AppState>, Json(req): Json<ConfigUpdateRequest>) -> Result<Json<()>> {
    state.config_service.update_config(&req.updates).await?;
    Ok(Json(()))
}

#[derive(Debug, Deserialize)]
pub struct TestClickHouseRequest {
    pub dsn: String,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Serialize)]
pub struct TestClickHouseResponse {
    pub success: bool,
    pub message: String,
}

pub async fn test_clickhouse(
    State(state): State<AppState>, Json(req): Json<TestClickHouseRequest>,
) -> Result<Json<TestClickHouseResponse>> {
    state
        .config_service
        .test_clickhouse(&req.dsn, &req.username, &req.password, &req.database)
        .await?;

    Ok(Json(TestClickHouseResponse {
        success: true,
        message: "连接成功".to_string(),
    }))
}
