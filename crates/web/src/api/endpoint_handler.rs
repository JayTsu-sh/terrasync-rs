use axum::Json;
use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};
use storage_v2::S3BucketInfo;

use crate::api::state::AppState;
use crate::application::endpoint_service::{CreateEndpointRequest, UpdateEndpointRequest, UpdateEndpointResponse};
use crate::domain::endpoint::Endpoint;
use crate::error::Result;

pub async fn list_endpoints(State(state): State<AppState>) -> Result<Json<Vec<Endpoint>>> {
    let endpoints = state.endpoint_service.list_endpoints().await?;
    Ok(Json(endpoints))
}

pub async fn create_endpoint(
    State(state): State<AppState>, Json(req): Json<CreateEndpointRequest>,
) -> Result<Json<Endpoint>> {
    let endpoint = state.endpoint_service.create_endpoint(req).await?;
    Ok(Json(endpoint))
}

pub async fn get_endpoint(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Endpoint>> {
    let endpoint = state.endpoint_service.get_endpoint(&id).await?;
    Ok(Json(endpoint))
}

pub async fn update_endpoint(
    State(state): State<AppState>, Path(id): Path<String>, Json(req): Json<UpdateEndpointRequest>,
) -> Result<Json<UpdateEndpointResponse>> {
    let force = req.force;
    let resp = state.endpoint_service.update_endpoint(&id, req, force).await?;
    Ok(Json(resp))
}

pub async fn delete_endpoint(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<()>> {
    state.endpoint_service.delete_endpoint(&id).await?;
    Ok(Json(()))
}

pub async fn test_connection(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<String>> {
    let msg = state.endpoint_service.test_connection(&id).await?;
    Ok(Json(msg))
}

// ── DTO 类型 ──

#[derive(Debug, Deserialize)]
pub struct ListDirsRequest {
    pub path: String,
    pub storage_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DirEntryItem {
    pub name: String,
    pub full_path: String,
}

#[derive(Debug, Serialize)]
pub struct ListDirsResponse {
    pub parent: String,
    pub entries: Vec<DirEntryItem>,
    pub sep: String,
}

#[derive(Debug, Deserialize)]
pub struct ListNfsExportsRequest {
    pub server: String,
    pub port: Option<u16>,
}

#[derive(Debug, Serialize)]
pub struct NfsExportEntry {
    pub path: String,
    pub groups: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListS3BucketsRequest {
    pub server: String,
    pub access_key: String,
    pub secret_key: String,
    pub use_https: Option<bool>,
}

// ── Handler 函数 ──

pub async fn list_dirs(
    State(state): State<AppState>, Json(req): Json<ListDirsRequest>,
) -> Result<Json<ListDirsResponse>> {
    let result = state
        .storage_browser_service
        .list_dirs(&req.path, req.storage_url.as_deref())
        .await?;

    let entries = result
        .entries
        .into_iter()
        .map(|e| DirEntryItem {
            name: e.name,
            full_path: e.full_path,
        })
        .collect();

    Ok(Json(ListDirsResponse {
        parent: result.parent,
        entries,
        sep: result.sep,
    }))
}

pub async fn list_nfs_exports(
    State(state): State<AppState>, Json(req): Json<ListNfsExportsRequest>,
) -> Result<Json<Vec<NfsExportEntry>>> {
    let exports = state
        .storage_browser_service
        .list_nfs_exports(&req.server, req.port)
        .await?;

    let entries = exports
        .into_iter()
        .map(|e| NfsExportEntry {
            path: e.path,
            groups: e.groups,
        })
        .collect();

    Ok(Json(entries))
}

pub async fn list_s3_buckets(
    State(state): State<AppState>, Json(req): Json<ListS3BucketsRequest>,
) -> Result<Json<Vec<S3BucketInfo>>> {
    let buckets = state
        .storage_browser_service
        .list_s3_buckets(
            &req.server,
            &req.access_key,
            &req.secret_key,
            req.use_https.unwrap_or(false),
        )
        .await?;

    Ok(Json(buckets))
}
