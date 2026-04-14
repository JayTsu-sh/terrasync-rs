use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::endpoint::{Endpoint, EndpointConfig, EndpointType};
use crate::domain::repository::EndpointRepository;
use crate::error::{Result, WebError};
use crate::infrastructure::db::{map_unique_violation, parse_rfc3339};

pub struct SqliteEndpointRepo {
    pool: SqlitePool,
}

impl SqliteEndpointRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EndpointRepository for SqliteEndpointRepo {
    async fn save(&self, endpoint: &Endpoint) -> Result<()> {
        let config_json = serde_json::to_string(&endpoint.config)?;
        let endpoint_type = endpoint.endpoint_type.to_string();
        let created_at = endpoint.created_at.to_rfc3339();
        let updated_at = endpoint.updated_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO endpoints (id, name, endpoint_type, config_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&endpoint.id)
        .bind(&endpoint.name)
        .bind(&endpoint_type)
        .bind(&config_json)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| map_unique_violation(e, "Endpoint", &endpoint.name))?;

        Ok(())
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Endpoint>> {
        let row = sqlx::query_as::<_, EndpointRow>(
            "SELECT id, name, endpoint_type, config_json, created_at, updated_at FROM endpoints WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| r.try_into()).transpose()
    }

    async fn find_all(&self) -> Result<Vec<Endpoint>> {
        let rows = sqlx::query_as::<_, EndpointRow>(
            "SELECT id, name, endpoint_type, config_json, created_at, updated_at FROM endpoints ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|r| r.try_into()).collect()
    }

    async fn update(&self, endpoint: &Endpoint) -> Result<()> {
        let config_json = serde_json::to_string(&endpoint.config)?;
        let endpoint_type = endpoint.endpoint_type.to_string();
        let updated_at = endpoint.updated_at.to_rfc3339();

        let result = sqlx::query(
            "UPDATE endpoints SET name = ?, endpoint_type = ?, config_json = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&endpoint.name)
        .bind(&endpoint_type)
        .bind(&config_json)
        .bind(&updated_at)
        .bind(&endpoint.id)
        .execute(&self.pool)
        .await
        .map_err(|e| map_unique_violation(e, "Endpoint", &endpoint.name))?;

        if result.rows_affected() == 0 {
            return Err(WebError::EndpointNotFound(endpoint.id.clone()));
        }
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM endpoints WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(WebError::EndpointNotFound(id.to_string()));
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct EndpointRow {
    id: String,
    name: String,
    endpoint_type: String,
    config_json: String,
    created_at: String,
    updated_at: String,
}

impl TryFrom<EndpointRow> for Endpoint {
    type Error = WebError;

    fn try_from(row: EndpointRow) -> Result<Self> {
        let endpoint_type: EndpointType = row
            .endpoint_type
            .parse()
            .map_err(|e: String| WebError::ValidationError(e))?;
        let config: EndpointConfig = serde_json::from_str(&row.config_json)?;
        let created_at = parse_rfc3339(&row.created_at)?;
        let updated_at = parse_rfc3339(&row.updated_at)?;

        Ok(Endpoint {
            id: row.id,
            name: row.name,
            endpoint_type,
            config,
            created_at,
            updated_at,
        })
    }
}
