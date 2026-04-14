use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::repository::ConfigRepository;
use crate::error::Result;

pub struct SqliteConfigRepo {
    pool: SqlitePool,
}

impl SqliteConfigRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConfigRepository for SqliteConfigRepo {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as("SELECT value_json FROM system_config WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.0))
    }

    async fn get_all(&self) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT key, value_json FROM system_config ORDER BY key")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn set(&self, key: &str, value_json: &str) -> Result<()> {
        let updated_at = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO system_config (key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value_json)
        .bind(&updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM system_config WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
