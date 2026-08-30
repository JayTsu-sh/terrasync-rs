use std::sync::Arc;

use serde::{Deserialize, Serialize};

use app::storage_factory::{StorageRole, create_storage_for_role};

use crate::domain::endpoint::{Endpoint, EndpointConfig, EndpointType};
use crate::domain::repository::{EndpointRepository, TaskRepository};
use crate::domain::task::TaskStatus;
use crate::error::{Result, WebError};

pub struct EndpointService {
    endpoint_repo: Arc<dyn EndpointRepository>,
    task_repo: Arc<dyn TaskRepository>,
}

/// 更新端点的响应，支持两阶段确认
#[derive(Debug, Serialize)]
pub struct UpdateEndpointResponse {
    /// 更新成功时返回端点信息
    pub endpoint: Option<Endpoint>,
    /// 是否需要用户确认（有非运行中的任务引用该端点）
    pub needs_confirm: bool,
    /// 关联的任务名称列表
    pub conflicting_tasks: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEndpointRequest {
    pub name: String,
    pub endpoint_type: EndpointType,
    pub config: EndpointConfig,
}

#[derive(Debug, Deserialize)]
pub struct UpdateEndpointRequest {
    pub name: Option<String>,
    pub config: Option<EndpointConfig>,
    #[serde(default)]
    pub force: bool,
}

impl EndpointService {
    pub fn new(endpoint_repo: Arc<dyn EndpointRepository>, task_repo: Arc<dyn TaskRepository>) -> Self {
        Self {
            endpoint_repo,
            task_repo,
        }
    }

    pub async fn create_endpoint(&self, req: CreateEndpointRequest) -> Result<Endpoint> {
        if req.name.trim().is_empty() {
            return Err(WebError::ValidationError("Endpoint name cannot be empty".to_string()));
        }

        let now = chrono::Utc::now();
        let endpoint = Endpoint {
            id: uuid::Uuid::new_v4().to_string(),
            name: req.name,
            endpoint_type: req.endpoint_type,
            config: req.config,
            created_at: now,
            updated_at: now,
        };

        self.endpoint_repo.save(&endpoint).await?;
        Ok(endpoint)
    }

    pub async fn get_endpoint(&self, id: &str) -> Result<Endpoint> {
        self.endpoint_repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| WebError::EndpointNotFound(id.to_string()))
    }

    pub async fn list_endpoints(&self) -> Result<Vec<Endpoint>> {
        self.endpoint_repo.find_all().await
    }

    pub async fn update_endpoint(
        &self, id: &str, req: UpdateEndpointRequest, force: bool,
    ) -> Result<UpdateEndpointResponse> {
        let mut endpoint = self.get_endpoint(id).await?;

        // 检查是否有任务引用该端点
        let related_tasks = self.task_repo.find_by_endpoint_id(id).await?;
        if !related_tasks.is_empty() {
            // 有 Running 状态的任务 → 直接拒绝
            let running_names: Vec<String> = related_tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Running)
                .map(|t| t.name.clone())
                .collect();
            if !running_names.is_empty() {
                return Err(WebError::EndpointInUseByRunningTask(running_names.join(", ")));
            }

            // 有非 Running 任务且未确认 → 返回需确认
            if !force {
                let task_names: Vec<String> = related_tasks.iter().map(|t| t.name.clone()).collect();
                return Ok(UpdateEndpointResponse {
                    endpoint: None,
                    needs_confirm: true,
                    conflicting_tasks: task_names,
                });
            }
        }

        if let Some(name) = req.name {
            if name.trim().is_empty() {
                return Err(WebError::ValidationError("Endpoint name cannot be empty".to_string()));
            }
            endpoint.name = name;
        }
        if let Some(config) = req.config {
            // 更新 endpoint_type 与 config 保持一致
            endpoint.endpoint_type = match &config {
                EndpointConfig::Local { .. } => EndpointType::Local,
                EndpointConfig::Nfs { .. } => EndpointType::NfsV3,
                EndpointConfig::S3 { .. } => EndpointType::S3,
                EndpointConfig::Cifs { .. } => EndpointType::Cifs,
            };
            endpoint.config = config;
        }

        // local 类型保存前校验路径是否存在
        if matches!(endpoint.config, EndpointConfig::Local { .. }) {
            Self::test_endpoint_connectivity(&endpoint).await?;
        }

        endpoint.updated_at = chrono::Utc::now();

        self.endpoint_repo.update(&endpoint).await?;
        Ok(UpdateEndpointResponse {
            endpoint: Some(endpoint),
            needs_confirm: false,
            conflicting_tasks: vec![],
        })
    }

    pub async fn delete_endpoint(&self, id: &str) -> Result<()> {
        // 检查是否有正在运行的任务，有则拒绝删除
        let related_tasks = self.task_repo.find_by_endpoint_id(id).await?;
        let running_names: Vec<String> = related_tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Running)
            .map(|t| t.name.clone())
            .collect();
        if !running_names.is_empty() {
            return Err(WebError::EndpointInUseByRunningTask(running_names.join(", ")));
        }

        // 先删除关联的任务（task_executions/task_progress/task_logs 有 ON DELETE CASCADE）
        for task in &related_tasks {
            self.task_repo.delete(&task.id).await?;
        }

        // 外键级联删除会自动清理关联的 paths
        self.endpoint_repo.delete(id).await
    }

    pub async fn test_connection(&self, id: &str) -> Result<String> {
        let endpoint = self.get_endpoint(id).await?;
        Self::test_endpoint_connectivity(&endpoint).await
    }

    /// 测试端点连通性（复用于 `test_connection` 和 `update_endpoint` 保存前校验）
    async fn test_endpoint_connectivity(endpoint: &Endpoint) -> Result<String> {
        let url = endpoint.to_storage_url(None);

        let storage = create_storage_for_role(&url, None, false, StorageRole::Source)
            .await
            .map_err(WebError::EndpointAppConnectionFailed)?;
        storage
            .check_connectivity()
            .await
            .map_err(WebError::EndpointStorageConnectionFailed)?;

        Ok("Connection successful".to_string())
    }
}
