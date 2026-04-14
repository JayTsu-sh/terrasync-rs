use serde::{Deserialize, Serialize};

/// MigrationPath 实体 — 属于 Endpoint 的子路径
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPath {
    pub id: String,
    pub endpoint_id: String,
    pub sub_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
