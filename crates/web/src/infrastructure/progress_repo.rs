use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::domain::progress::TaskProgress;
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
    async fn upsert(&self, p: &TaskProgress) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO task_progress (task_id, report_json, is_final, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(task_id) DO UPDATE SET
                report_json = excluded.report_json,
                is_final = excluded.is_final,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&p.task_id)
        .bind(&p.report_json)
        .bind(p.is_final)
        .bind(p.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
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
