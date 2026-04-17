use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::execution::{ExecutionStats, ExecutionType, LogLevel, TaskExecution, TaskLog};
use crate::domain::repository::ExecutionRepository;
use crate::domain::task::TaskStatus;
use crate::error::{Result, WebError};
use crate::infrastructure::db::parse_rfc3339;

pub struct SqliteExecutionRepo {
    pool: SqlitePool,
}

impl SqliteExecutionRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ExecutionRepository for SqliteExecutionRepo {
    async fn save(&self, execution: &TaskExecution) -> Result<()> {
        let execution_type = execution.execution_type.to_string();
        let status = execution.status.to_string();
        let started_at = execution.started_at.to_rfc3339();
        let finished_at = execution.finished_at.map(|t| t.to_rfc3339());
        let stats_json = execution.stats.as_ref().map(serde_json::to_string).transpose()?;

        sqlx::query(
            "INSERT INTO task_executions (id, task_id, execution_type, status, started_at, finished_at, stats_json, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&execution.id)
        .bind(&execution.task_id)
        .bind(&execution_type)
        .bind(&status)
        .bind(&started_at)
        .bind(&finished_at)
        .bind(&stats_json)
        .bind(&execution.error_message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn find_by_task_id(&self, task_id: &str) -> Result<Vec<TaskExecution>> {
        let rows = sqlx::query_as::<_, ExecutionRow>(
            "SELECT id, task_id, execution_type, status, started_at, finished_at, stats_json, error_message FROM task_executions WHERE task_id = ? ORDER BY started_at DESC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<TaskExecution>> {
        let row = sqlx::query_as::<_, ExecutionRow>(
            "SELECT id, task_id, execution_type, status, started_at, finished_at, stats_json, error_message FROM task_executions WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    #[allow(clippy::similar_names)]
    async fn update_status(
        &self, id: &str, status: TaskStatus, stats: Option<&ExecutionStats>, error_msg: Option<&str>,
    ) -> Result<()> {
        let status_str = status.to_string();
        let finished_at = match status {
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => Some(chrono::Utc::now().to_rfc3339()),
            _ => None,
        };
        let stats_json = stats.map(serde_json::to_string).transpose()?;

        let result = sqlx::query(
            "UPDATE task_executions SET status = ?, finished_at = COALESCE(?, finished_at), stats_json = COALESCE(?, stats_json), error_message = COALESCE(?, error_message) WHERE id = ?",
        )
        .bind(&status_str)
        .bind(&finished_at)
        .bind(&stats_json)
        .bind(error_msg)
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::ExecutionNotFound(id.to_string()));
        }
        Ok(())
    }

    async fn save_log(&self, log: &TaskLog) -> Result<()> {
        let level = log.level.to_string();
        let created_at = log.created_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO task_logs (id, execution_id, level, message, file_path, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&log.id)
        .bind(&log.execution_id)
        .bind(&level)
        .bind(&log.message)
        .bind(&log.file_path)
        .bind(&created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete_by_task_id(&self, task_id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // 先删除关联的日志（通过 execution_id 关联）
        sqlx::query("DELETE FROM task_logs WHERE execution_id IN (SELECT id FROM task_executions WHERE task_id = ?)")
            .bind(task_id)
            .execute(&mut *tx)
            .await?;

        // 再删除执行记录
        sqlx::query("DELETE FROM task_executions WHERE task_id = ?")
            .bind(task_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn delete_by_id(&self, id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // 先删除关联的日志
        sqlx::query("DELETE FROM task_logs WHERE execution_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        // 再删除执行记录
        let result = sqlx::query("DELETE FROM task_executions WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::ExecutionNotFound(id.to_string()));
        }

        tx.commit().await?;
        Ok(())
    }

    async fn find_logs(
        &self, execution_id: &str, level: Option<LogLevel>, offset: u32, limit: u32,
    ) -> Result<Vec<TaskLog>> {
        let level_str = level.map(|l| l.to_string());

        let sql = if level_str.is_some() {
            "SELECT id, execution_id, level, message, file_path, created_at FROM task_logs WHERE execution_id = ? AND level = ? ORDER BY created_at ASC LIMIT ? OFFSET ?"
        } else {
            "SELECT id, execution_id, level, message, file_path, created_at FROM task_logs WHERE execution_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?"
        };

        let rows = if let Some(ref lvl) = level_str {
            sqlx::query_as::<_, LogRow>(sql)
                .bind(execution_id)
                .bind(lvl)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query_as::<_, LogRow>(sql)
                .bind(execution_id)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await?
        };

        rows.into_iter().map(TryInto::try_into).collect()
    }
}

#[derive(sqlx::FromRow)]
struct ExecutionRow {
    id: String,
    task_id: String,
    execution_type: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    stats_json: Option<String>,
    error_message: Option<String>,
}

impl TryFrom<ExecutionRow> for TaskExecution {
    type Error = WebError;

    #[allow(clippy::similar_names)]
    fn try_from(row: ExecutionRow) -> Result<Self> {
        let execution_type: ExecutionType = row.execution_type.parse().map_err(WebError::ValidationError)?;
        let status: TaskStatus = row.status.parse().map_err(WebError::ValidationError)?;
        let started_at = parse_rfc3339(&row.started_at)?;
        let finished_at = row.finished_at.as_deref().map(parse_rfc3339).transpose()?;
        let stats: Option<ExecutionStats> = row.stats_json.as_deref().map(serde_json::from_str).transpose()?;

        Ok(TaskExecution {
            id: row.id,
            task_id: row.task_id,
            execution_type,
            status,
            started_at,
            finished_at,
            stats,
            error_message: row.error_message,
        })
    }
}

#[derive(sqlx::FromRow)]
struct LogRow {
    id: String,
    execution_id: String,
    level: String,
    message: String,
    file_path: Option<String>,
    created_at: String,
}

impl TryFrom<LogRow> for TaskLog {
    type Error = WebError;

    fn try_from(row: LogRow) -> Result<Self> {
        let level: LogLevel = row.level.parse().map_err(WebError::ValidationError)?;
        let created_at = parse_rfc3339(&row.created_at)?;

        Ok(TaskLog {
            id: row.id,
            execution_id: row.execution_id,
            level,
            message: row.message,
            file_path: row.file_path,
            created_at,
        })
    }
}
