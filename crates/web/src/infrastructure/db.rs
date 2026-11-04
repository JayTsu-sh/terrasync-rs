use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use std::fs::{File, OpenOptions};
use tracing::info;
/// 重导出 `utils::sanitize_job_id`
pub use utils::sanitize_job_id;

use super::clickhouse_config::migrate_mode;
use crate::error::{Result, WebError};

/// 数据库文件默认路径
const DEFAULT_DB_PATH: &str = "data/terrasync_web.db";

/// 单一进程持有数据库租约，避免第二个 GUI 把仍在执行的任务当作崩溃恢复。
pub fn lock_database() -> Result<File> {
    std::fs::create_dir_all("data")?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open("data/terrasync_web.lock")?;
    file.try_lock()?;
    Ok(file)
}

/// 重启只恢复可操作状态；扫描保留 ClickHouse 游标，等待用户继续。
pub async fn recover_interrupted_tasks(pool: &SqlitePool) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("UPDATE task_executions SET status = CASE WHEN task_id IN (SELECT id FROM migration_tasks WHERE task_type='scan') THEN 'cancelled' ELSE 'failed' END, finished_at = ?, error_message = '服务重启，执行已中断；扫描可继续' WHERE status='running'")
        .bind(Utc::now().to_rfc3339()).execute(&mut *transaction).await?;
    sqlx::query("UPDATE migration_tasks SET status = CASE WHEN task_type='scan' THEN 'cancelled' ELSE 'failed' END, finished_at = ?, error_message = '服务重启，执行已中断；扫描可继续' WHERE status='running'")
        .bind(Utc::now().to_rfc3339()).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(())
}

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

    ensure_storage_failure_columns(&pool).await?;
    migrate_mode(&pool).await?;
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

#[cfg(test)]
mod resume_tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn startup_recovers_interrupted_scans_without_restarting_finished_tasks() -> Result<()> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql("CREATE TABLE migration_tasks (id TEXT, task_type TEXT, status TEXT, finished_at TEXT, error_message TEXT); CREATE TABLE task_executions (id TEXT, task_id TEXT, status TEXT, finished_at TEXT, error_message TEXT);
          INSERT INTO migration_tasks VALUES ('scan','scan','running',NULL,NULL),('done','scan','completed','old',NULL),('sync','sync','running',NULL,NULL);
          INSERT INTO task_executions VALUES ('scan-run','scan','running',NULL,NULL),('done-run','done','completed','old',NULL),('sync-run','sync','running',NULL,NULL);").execute(&pool).await?;
        recover_interrupted_tasks(&pool).await?;
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT id,status FROM migration_tasks ORDER BY id")
            .fetch_all(&pool)
            .await?;
        assert_eq!(
            rows,
            vec![
                ("done".into(), "completed".into()),
                ("scan".into(), "cancelled".into()),
                ("sync".into(), "failed".into())
            ]
        );
        let status: String = sqlx::query_scalar("SELECT status FROM task_executions WHERE id='scan-run'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(status, "cancelled");
        Ok(())
    }
}

/// 旧任务没有结构化错误时保持 NULL；重启后新增失败仍可按类型查询。
async fn ensure_storage_failure_columns(pool: &SqlitePool) -> Result<()> {
    for table in ["migration_tasks", "task_executions"] {
        let columns: Vec<String> = sqlx::query_scalar(&format!("SELECT name FROM pragma_table_info('{table}')"))
            .fetch_all(pool)
            .await?;
        if !columns.iter().any(|column| column == "storage_error_json") {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN storage_error_json TEXT"))
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}
