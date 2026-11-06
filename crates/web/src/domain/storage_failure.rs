use data_mover::traversal::RemoteScanFailure;
use serde::{Deserialize, Serialize};

/// 可持久化且不含凭据的扫描故障；调用方无需解析 `error_message`。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageFailureDetail {
    pub code: String,
    pub backend: String,
    pub operation: String,
    pub path: String,
    pub retryable: bool,
    pub operation_timeout_secs: u64,
}
impl From<&RemoteScanFailure> for StorageFailureDetail {
    fn from(failure: &RemoteScanFailure) -> Self {
        Self {
            code: failure.code().into(),
            backend: failure.backend.as_str().into(),
            operation: format!("{:?}", failure.operation).to_ascii_lowercase(),
            path: failure.path.as_str().into(),
            retryable: true,
            operation_timeout_secs: failure.operation_timeout.as_secs(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::{
        domain::{
            repository::{ExecutionRepository, TaskRepository},
            task::TaskStatus,
        },
        infrastructure::{execution_repo::SqliteExecutionRepo, task_repo::SqliteTaskRepo},
    };
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn typed_storage_failure_survives_sqlite_and_is_cleared_when_task_restarts() -> Result<()> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await?;
        sqlx::raw_sql("INSERT INTO endpoints(id,name,endpoint_type,config_json) VALUES('e','nfs','nfs_v3','{}');
          INSERT INTO migration_tasks(id,name,task_type,source_endpoint_id,config_json,created_at) VALUES('t','scan','scan','e','{}','2026-11-02T00:00:00Z');
          INSERT INTO task_executions(id,task_id,execution_type,started_at) VALUES('x','t','full','2026-11-02T00:00:00Z');").execute(&pool).await?;
        let detail = StorageFailureDetail {
            code: "storage_connection_lost".into(),
            backend: "nfs".into(),
            operation: "traverse".into(),
            path: "dir".into(),
            retryable: true,
            operation_timeout_secs: 3,
        };
        let tasks = SqliteTaskRepo::new(pool.clone());
        let executions = SqliteExecutionRepo::new(pool);
        tasks
            .update_status("t", TaskStatus::Failed, Some("connection lost"), Some(&detail))
            .await?;
        executions
            .update_status("x", TaskStatus::Failed, None, Some("connection lost"), Some(&detail))
            .await?;
        let task = tasks.find_by_id("t").await?.unwrap();
        assert_eq!(task.storage_error.as_ref(), Some(&detail));
        assert_eq!(
            executions.find_by_id("x").await?.unwrap().storage_error.as_ref(),
            Some(&detail)
        );
        assert_eq!(
            serde_json::to_value(task)?["storage_error"]["code"],
            "storage_connection_lost"
        );
        tasks.update_status("t", TaskStatus::Running, None, None).await?;
        assert!(tasks.find_by_id("t").await?.unwrap().storage_error.is_none());
        Ok(())
    }
}
