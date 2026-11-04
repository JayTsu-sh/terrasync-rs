use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use super::callback_receipt;
use crate::domain::progress::{DiagnosticPage, ProgressReceipt, ProgressSubmission, TaskProgress};
use crate::domain::repository::ProgressRepository;
use crate::error::Result;
use crate::infrastructure::db::parse_rfc3339;

pub struct SqliteProgressRepo {
    pool: SqlitePool,
}

impl SqliteProgressRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProgressRepository for SqliteProgressRepo {
    async fn receive(&self, submission: ProgressSubmission<'_>) -> Result<ProgressReceipt> {
        let mut transaction = self.pool.begin().await?;
        let receipt = callback_receipt::receive(&mut transaction, submission).await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    async fn diagnostics(&self, task_id: &str, offset: u32, limit: u32) -> Result<DiagnosticPage> {
        let limit = limit.clamp(1, 100);
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT event_json FROM task_diagnostic_events WHERE task_id=? ORDER BY rowid LIMIT ? OFFSET ?",
        )
        .bind(task_id)
        .bind(limit + 1)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let next_offset = (rows.len() > limit as usize).then(|| offset.saturating_add(limit));
        let events = rows
            .into_iter()
            .take(limit as usize)
            .map(|json| serde_json::from_str(&json))
            .collect::<std::result::Result<_, _>>()?;
        let omitted: Option<String> =
            sqlx::query_scalar("SELECT omitted_count FROM task_diagnostic_summary WHERE task_id=?")
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(DiagnosticPage {
            events,
            omitted_count: callback_receipt::parse_omitted(omitted.as_deref())?,
            next_offset,
        })
    }

    async fn find_by_task_id(&self, task_id: &str) -> Result<Option<TaskProgress>> {
        let row = sqlx::query("SELECT task_id, report_json, is_final, updated_at FROM task_progress WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|r| {
            let updated_at_str: String = r.get("updated_at");
            Ok(TaskProgress {
                task_id: r.get("task_id"),
                report_json: r.get("report_json"),
                is_final: r.get("is_final"),
                updated_at: parse_rfc3339(&updated_at_str)?,
            })
        })
        .transpose()
    }

    async fn delete_by_task_id(&self, task_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM task_progress WHERE task_id = ?")
            .bind(task_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
