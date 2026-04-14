use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::repository::TaskRepository;
use crate::domain::task::{MigrationTask, TaskConfig, TaskFilter, TaskStatus, TaskType};
use crate::error::{Result, WebError};
use crate::infrastructure::db::{map_unique_violation, parse_rfc3339};

pub struct SqliteTaskRepo {
    pool: SqlitePool,
}

impl SqliteTaskRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TaskRepository for SqliteTaskRepo {
    async fn save(&self, task: &MigrationTask) -> Result<()> {
        let task_type = task.task_type.to_string();
        let config_json = serde_json::to_string(&task.config)?;
        let status = task.status.to_string();
        let created_at = task.created_at.to_rfc3339();
        let started_at = task.started_at.map(|t| t.to_rfc3339());
        let finished_at = task.finished_at.map(|t| t.to_rfc3339());

        sqlx::query(
            "INSERT INTO migration_tasks (id, name, task_type, source_endpoint_id, source_path_id, dest_endpoint_id, dest_path_id, config_json, status, created_at, started_at, finished_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.id)
        .bind(&task.name)
        .bind(&task_type)
        .bind(&task.source_endpoint_id)
        .bind(&task.source_path_id)
        .bind(&task.dest_endpoint_id)
        .bind(&task.dest_path_id)
        .bind(&config_json)
        .bind(&status)
        .bind(&created_at)
        .bind(&started_at)
        .bind(&finished_at)
        .bind(&task.error_message)
        .execute(&self.pool)
        .await
        .map_err(|e| map_unique_violation(e, "Task", &task.name))?;

        Ok(())
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<MigrationTask>> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT id, name, task_type, source_endpoint_id, source_path_id, dest_endpoint_id, dest_path_id, config_json, status, created_at, started_at, finished_at, error_message FROM migration_tasks WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.try_into()).transpose()
    }

    async fn find_by_endpoint_id(&self, endpoint_id: &str) -> Result<Vec<MigrationTask>> {
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT id, name, task_type, source_endpoint_id, source_path_id, dest_endpoint_id, dest_path_id, config_json, status, created_at, started_at, finished_at, error_message FROM migration_tasks WHERE source_endpoint_id = ? OR dest_endpoint_id = ?",
        )
        .bind(endpoint_id)
        .bind(endpoint_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|r| r.try_into()).collect()
    }

    async fn find_all(&self, filter: &TaskFilter) -> Result<Vec<MigrationTask>> {
        let base = "SELECT id, name, task_type, source_endpoint_id, source_path_id, dest_endpoint_id, dest_path_id, config_json, status, created_at, started_at, finished_at, error_message FROM migration_tasks";

        let (sql, status_val, task_type_val) = match (&filter.status, &filter.task_type) {
            (Some(s), Some(t)) => (
                format!("{base} WHERE status = ? AND task_type = ? ORDER BY created_at DESC"),
                Some(s.to_string()),
                Some(t.to_string()),
            ),
            (Some(s), None) => (
                format!("{base} WHERE status = ? ORDER BY created_at DESC"),
                Some(s.to_string()),
                None,
            ),
            (None, Some(t)) => (
                format!("{base} WHERE task_type = ? ORDER BY created_at DESC"),
                None,
                Some(t.to_string()),
            ),
            (None, None) => (format!("{base} ORDER BY created_at DESC"), None, None),
        };

        let mut query = sqlx::query_as::<_, TaskRow>(&sql);
        if let Some(ref s) = status_val {
            query = query.bind(s);
        }
        if let Some(ref t) = task_type_val {
            query = query.bind(t);
        }

        let rows = query.fetch_all(&self.pool).await?;
        rows.into_iter().map(|r| r.try_into()).collect()
    }

    async fn update(&self, task: &MigrationTask) -> Result<()> {
        let config_json = serde_json::to_string(&task.config)?;

        let result = sqlx::query(
            "UPDATE migration_tasks SET name = ?, source_endpoint_id = ?, source_path_id = ?, config_json = ? WHERE id = ?",
        )
        .bind(&task.name)
        .bind(&task.source_endpoint_id)
        .bind(&task.source_path_id)
        .bind(&config_json)
        .bind(&task.id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::TaskNotFound(task.id.clone()));
        }
        Ok(())
    }

    async fn update_status(&self, id: &str, status: TaskStatus, error_msg: Option<&str>) -> Result<()> {
        let status_str = status.to_string();
        let now = chrono::Utc::now().to_rfc3339();

        let (started_at_clause, finished_at_clause) = match status {
            TaskStatus::Running => (", started_at = ?", ""),
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => ("", ", finished_at = ?"),
            _ => ("", ""),
        };

        let sql = format!(
            "UPDATE migration_tasks SET status = ?, error_message = ?{started_at_clause}{finished_at_clause} WHERE id = ?"
        );

        let mut query = sqlx::query(&sql).bind(&status_str).bind(error_msg);

        match status {
            TaskStatus::Running => {
                query = query.bind(&now);
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => {
                query = query.bind(&now);
            }
            _ => {}
        }

        let result = query.bind(id).execute(&self.pool).await?;

        if result.rows_affected() == 0 {
            return Err(WebError::TaskNotFound(id.to_string()));
        }
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM migration_tasks WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::TaskNotFound(id.to_string()));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct TaskRow {
    id: String,
    name: String,
    task_type: String,
    source_endpoint_id: String,
    source_path_id: Option<String>,
    dest_endpoint_id: Option<String>,
    dest_path_id: Option<String>,
    config_json: String,
    status: String,
    created_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    error_message: Option<String>,
}

impl TryFrom<TaskRow> for MigrationTask {
    type Error = WebError;

    fn try_from(row: TaskRow) -> Result<Self> {
        let task_type: TaskType = row
            .task_type
            .parse()
            .map_err(|e: String| WebError::ValidationError(e))?;
        let status: TaskStatus = row.status.parse().map_err(|e: String| WebError::ValidationError(e))?;
        let config: TaskConfig = serde_json::from_str(&row.config_json)?;

        let created_at = parse_rfc3339(&row.created_at)?;
        let started_at = row.started_at.as_deref().map(parse_rfc3339).transpose()?;
        let finished_at = row.finished_at.as_deref().map(parse_rfc3339).transpose()?;

        Ok(MigrationTask {
            id: row.id,
            name: row.name,
            task_type,
            source_endpoint_id: row.source_endpoint_id,
            source_path_id: row.source_path_id,
            dest_endpoint_id: row.dest_endpoint_id,
            dest_path_id: row.dest_path_id,
            config,
            status,
            created_at,
            started_at,
            finished_at,
            error_message: row.error_message,
        })
    }
}
