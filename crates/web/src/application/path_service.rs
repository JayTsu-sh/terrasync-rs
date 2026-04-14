use std::sync::Arc;

use serde::Deserialize;

use crate::domain::path::MigrationPath;
use crate::domain::repository::{EndpointRepository, PathRepository};
use crate::error::{Result, WebError};

pub struct PathService {
    path_repo: Arc<dyn PathRepository>,
    endpoint_repo: Arc<dyn EndpointRepository>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePathRequest {
    pub sub_path: String,
    pub description: Option<String>,
}

impl PathService {
    pub fn new(path_repo: Arc<dyn PathRepository>, endpoint_repo: Arc<dyn EndpointRepository>) -> Self {
        Self {
            path_repo,
            endpoint_repo,
        }
    }

    pub async fn add_path(&self, endpoint_id: &str, req: CreatePathRequest) -> Result<MigrationPath> {
        if req.sub_path.trim().is_empty() {
            return Err(WebError::ValidationError("Sub path cannot be empty".to_string()));
        }

        // 确认端点存在
        self.endpoint_repo
            .find_by_id(endpoint_id)
            .await?
            .ok_or_else(|| WebError::EndpointNotFound(endpoint_id.to_string()))?;

        let path = MigrationPath {
            id: uuid::Uuid::new_v4().to_string(),
            endpoint_id: endpoint_id.to_string(),
            sub_path: req.sub_path,
            description: req.description,
            created_at: chrono::Utc::now(),
        };

        self.path_repo.save(&path).await?;
        Ok(path)
    }

    pub async fn list_paths(&self, endpoint_id: &str) -> Result<Vec<MigrationPath>> {
        self.path_repo.find_by_endpoint_id(endpoint_id).await
    }

    pub async fn delete_path(&self, id: &str) -> Result<()> {
        self.path_repo.delete(id).await
    }
}
