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

    async fn set_many(&self, values: &[(String, String)]) -> Result<()> {
        let updated_at = chrono::Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        for (key, value_json) in values {
            sqlx::query(
                "INSERT INTO system_config (key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
            ).bind(key).bind(value_json).bind(&updated_at).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::clickhouse_config::parse_ch_config;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;

    fn deployment(mode: &str, cluster: &str) -> Vec<(String, String)> {
        [("clickhouse_mode", mode), ("clickhouse_cluster", cluster)]
            .into_iter()
            .map(|(key, value)| (key.into(), format!("\"{value}\"")))
            .collect()
    }

    #[tokio::test]
    async fn deployment_updates_are_atomic_for_readers_and_roll_back_on_partial_failure() -> Result<()> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await?;
        let repository = Arc::new(SqliteConfigRepo::new(pool.clone()));
        repository
            .set_many(&[("clickhouse_host".into(), "\"localhost\"".into())])
            .await?;
        repository.set_many(&deployment("single", "")).await?;
        let reader = repository.clone();
        let reads = async move {
            for _ in 0..100 {
                assert!(parse_ch_config(&reader.get_all().await?).is_ok());
            }
            Result::Ok(())
        };
        let writes = async {
            for _ in 0..50 {
                repository.set_many(&deployment("cluster", "six_nodes")).await?;
                repository.set_many(&deployment("single", "")).await?;
            }
            Result::Ok(())
        };
        let (read, write) = tokio::join!(reads, writes);
        read?;
        write?;
        sqlx::query("CREATE TRIGGER reject_fixture BEFORE INSERT ON system_config WHEN NEW.key='clickhouse_cluster' AND NEW.value_json='\"bad\"' BEGIN SELECT RAISE(FAIL,'fixture failure'); END")
            .execute(&pool).await?;
        assert!(repository.set_many(&deployment("cluster", "bad")).await.is_err());
        assert_eq!(repository.get("clickhouse_mode").await?.as_deref(), Some("\"single\""));
        assert_eq!(repository.get("clickhouse_cluster").await?.as_deref(), Some("\"\""));
        Ok(())
    }
}
