use super::storage_failure::StorageFailureDetail;
use async_trait::async_trait;

use super::endpoint::Endpoint;
use super::execution::{ExecutionStats, LogLevel, TaskExecution, TaskLog};
use super::path::MigrationPath;
use super::progress::{DiagnosticPage, ProgressReceipt, ProgressSubmission, TaskProgress};
use super::task::{MigrationTask, TaskFilter, TaskStatus};
use crate::error::Result;

#[async_trait]
pub trait EndpointRepository: Send + Sync {
    async fn save(&self, endpoint: &Endpoint) -> Result<()>;
    async fn find_by_id(&self, id: &str) -> Result<Option<Endpoint>>;
    async fn find_all(&self) -> Result<Vec<Endpoint>>;
    async fn update(&self, endpoint: &Endpoint) -> Result<()>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait PathRepository: Send + Sync {
    async fn save(&self, path: &MigrationPath) -> Result<()>;
    async fn find_by_endpoint_id(&self, endpoint_id: &str) -> Result<Vec<MigrationPath>>;
    async fn find_by_id(&self, id: &str) -> Result<Option<MigrationPath>>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn save(&self, task: &MigrationTask) -> Result<()>;
    async fn find_by_id(&self, id: &str) -> Result<Option<MigrationTask>>;
    async fn find_all(&self, filter: &TaskFilter) -> Result<Vec<MigrationTask>>;
    async fn find_by_endpoint_id(&self, endpoint_id: &str) -> Result<Vec<MigrationTask>>;
    async fn update(&self, task: &MigrationTask) -> Result<()>;
    async fn update_status(
        &self, id: &str, status: TaskStatus, error_msg: Option<&str>, storage_error: Option<&StorageFailureDetail>,
    ) -> Result<()>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ProgressRepository: Send + Sync {
    async fn receive(&self, submission: ProgressSubmission<'_>) -> Result<ProgressReceipt>;
    async fn diagnostics(&self, task_id: &str, offset: u32, limit: u32) -> Result<DiagnosticPage>;
    async fn find_by_task_id(&self, task_id: &str) -> Result<Option<TaskProgress>>;
    async fn delete_by_task_id(&self, task_id: &str) -> Result<()>;
}

#[async_trait]
pub trait ExecutionRepository: Send + Sync {
    async fn save(&self, execution: &TaskExecution) -> Result<()>;
    async fn find_by_task_id(&self, task_id: &str) -> Result<Vec<TaskExecution>>;
    async fn find_by_id(&self, id: &str) -> Result<Option<TaskExecution>>;
    async fn update_status(
        &self, id: &str, status: TaskStatus, stats: Option<&ExecutionStats>, error_msg: Option<&str>,
        storage_error: Option<&StorageFailureDetail>,
    ) -> Result<()>;
    async fn save_log(&self, log: &TaskLog) -> Result<()>;
    async fn find_logs(
        &self, execution_id: &str, level: Option<LogLevel>, offset: u32, limit: u32,
    ) -> Result<Vec<TaskLog>>;
    async fn delete_by_task_id(&self, task_id: &str) -> Result<()>;
    async fn delete_by_id(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ConfigRepository: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn get_all(&self) -> Result<Vec<(String, String)>>;
    /// 同一批设置必须原子可见，避免部署模式与集群名称出现中间状态。
    async fn set_many(&self, values: &[(String, String)]) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
}
