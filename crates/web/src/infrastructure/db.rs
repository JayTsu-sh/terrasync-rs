use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use tracing::info;
/// 重导出 `utils::sanitize_job_id`
pub use utils::sanitize_job_id;

use crate::error::{Result, WebError};

/// 数据库文件默认路径
const DEFAULT_DB_PATH: &str = "data/terrasync_web.db";

/// 初始化 `SQLite` 数据库连接池并执行迁移
pub async fn init_database() -> Result<SqlitePool> {
    // 确保数据目录存在
    if let Some(parent) = std::path::Path::new(DEFAULT_DB_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let database_url = format!("sqlite:{DEFAULT_DB_PATH}?mode=rwc");

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // 启用 WAL 模式以支持并发读写
    sqlx::query("PRAGMA journal_mode=WAL").execute(&pool).await?;

    // 启用外键约束
    sqlx::query("PRAGMA foreign_keys=ON").execute(&pool).await?;

    // 执行初始化迁移（001_init.sql 使用 CREATE TABLE IF NOT EXISTS，幂等安全）
    let init_sql = include_str!("../migrations/001_init.sql");
    sqlx::raw_sql(init_sql).execute(&pool).await?;

    info!("SQLite database initialized at {}", DEFAULT_DB_PATH);
    Ok(pool)
}

/// 解析 RFC3339 格式的日期时间字符串为 `DateTime<Utc>`
pub fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| WebError::ValidationError(format!("Invalid datetime: {e}")))
        .map(|dt| dt.with_timezone(&Utc))
}

/// 将 sqlx 错误转换为 `WebError`，对 UNIQUE 约束冲突返回 `DuplicateName`
pub fn map_unique_violation(e: sqlx::Error, entity: &str, name: &str) -> WebError {
    match &e {
        sqlx::Error::Database(db_err) if db_err.message().contains("UNIQUE") => WebError::DuplicateName {
            entity: entity.to_string(),
            name: name.to_string(),
        },
        _ => WebError::DatabaseError(e),
    }
}
