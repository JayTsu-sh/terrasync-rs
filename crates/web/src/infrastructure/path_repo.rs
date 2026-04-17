use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::path::MigrationPath;
use crate::domain::repository::PathRepository;
use crate::error::{Result, WebError};
use crate::infrastructure::db::{map_unique_violation, parse_rfc3339};

pub struct SqlitePathRepo {
    pool: SqlitePool,
}

impl SqlitePathRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PathRepository for SqlitePathRepo {
    async fn save(&self, path: &MigrationPath) -> Result<()> {
        let created_at = path.created_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO migration_paths (id, endpoint_id, sub_path, description, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&path.id)
        .bind(&path.endpoint_id)
        .bind(&path.sub_path)
        .bind(&path.description)
        .bind(&created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| map_unique_violation(e, "Path", &path.sub_path))?;

        Ok(())
    }

    async fn find_by_endpoint_id(&self, endpoint_id: &str) -> Result<Vec<MigrationPath>> {
        let rows = sqlx::query_as::<_, PathRow>(
            "SELECT id, endpoint_id, sub_path, description, created_at FROM migration_paths WHERE endpoint_id = ? ORDER BY created_at DESC",
        )
        .bind(endpoint_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<MigrationPath>> {
        let row = sqlx::query_as::<_, PathRow>(
            "SELECT id, endpoint_id, sub_path, description, created_at FROM migration_paths WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM migration_paths WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::PathNotFound(id.to_string()));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PathRow {
    id: String,
    endpoint_id: String,
    sub_path: String,
    description: Option<String>,
    created_at: String,
}

impl TryFrom<PathRow> for MigrationPath {
    type Error = WebError;

    fn try_from(row: PathRow) -> Result<Self> {
        let created_at = parse_rfc3339(&row.created_at)?;

        Ok(MigrationPath {
            id: row.id,
            endpoint_id: row.endpoint_id,
            sub_path: row.sub_path,
            description: row.description,
            created_at,
        })
    }
}
