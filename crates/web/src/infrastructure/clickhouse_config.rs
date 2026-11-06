//! Web 配置到统一 ClickHouse 配置的转换；业务模块不推断部署模式。
use crate::{
    domain::repository::ConfigRepository,
    error::{Result, WebError},
};
use serde_json::Value;
use sqlx::SqlitePool;
use utils::app_config::{AppConfig, ClickHouseMode, ClickhouseConfig, DEFAULT_DATABASE_BATCH_SIZE};

pub(crate) struct ChConfig {
    pub connection: ClickhouseConfig,
    pub batch_size: u32,
}

fn value(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.to_owned())
}

pub(crate) fn parse_ch_config(all: &[(String, String)]) -> Result<Option<ChConfig>> {
    let get = |key: &str| all.iter().find(|(k, _)| k == key).map(|(_, raw)| value(raw));
    let Some(host) = get("clickhouse_host").filter(|host| !host.trim().is_empty()) else {
        return Ok(None);
    };
    if host.contains(['/', '?', '#', '@', '\n', '\r', '\0']) {
        return Err(WebError::ValidationError("clickhouse_host 包含非法字符".into()));
    }
    let port: u16 = get("clickhouse_port")
        .unwrap_or_else(|| "8123".into())
        .parse()
        .map_err(|_| WebError::ValidationError("clickhouse_port 必须是有效端口号".into()))?;
    if port == 0 {
        return Err(WebError::ValidationError("clickhouse_port 必须大于 0".into()));
    }
    let batch_size = get("clickhouse_batch_size")
        .map_or(Ok(DEFAULT_DATABASE_BATCH_SIZE), |size| size.parse::<u32>())
        .map_err(|_| WebError::ValidationError("clickhouse_batch_size 必须是正整数".into()))?;
    if batch_size == 0 {
        return Err(WebError::ValidationError("clickhouse_batch_size 必须大于 0".into()));
    }
    let mode: ClickHouseMode =
        serde_json::from_value(Value::String(get("clickhouse_mode").unwrap_or_else(|| "single".into())))
            .map_err(|_| WebError::ValidationError("clickhouse_mode 必须是 single 或 cluster".into()))?;
    let connection = ClickhouseConfig {
        mode,
        cluster: get("clickhouse_cluster")
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty()),
        dsn: format!("http://{host}:{port}"),
        username: get("clickhouse_user").unwrap_or_else(|| "default".into()),
        password: get("clickhouse_password").filter(|password| !password.is_empty()),
        database: get("clickhouse_database").unwrap_or_else(|| "default".into()),
        dial_timeout: 10,
        read_timeout: 30,
    };
    for text in [&connection.username, &connection.database] {
        if text.contains(['\n', '\r', '\0']) {
            return Err(WebError::ValidationError(
                "ClickHouse 用户名或数据库包含非法字符".into(),
            ));
        }
    }
    connection.cluster_name().map_err(WebError::ClickHouseConfigInvalid)?;
    Ok(Some(ChConfig { connection, batch_size }))
}

pub(crate) async fn load_ch_config(repository: &dyn ConfigRepository) -> Result<Option<ChConfig>> {
    parse_ch_config(&repository.get_all().await?)
}

pub(crate) async fn apply_db_config_from_sqlite(repository: &dyn ConfigRepository) -> Result<Option<ChConfig>> {
    let Some(config) = load_ch_config(repository).await? else {
        return Ok(None);
    };
    apply_ch_config(&config)?;
    Ok(Some(config))
}

pub(crate) fn apply_ch_config(config: &ChConfig) -> Result<()> {
    AppConfig::override_with(|app| {
        app.database.enabled = true;
        app.database.batch_size = config.batch_size;
        app.database.clickhouse = config.connection.clone();
    })?;
    Ok(())
}

/// 仅迁移旧 SQLite 配置一次；新配置始终由显式 mode 决定部署方式。
pub(crate) async fn migrate_mode(pool: &SqlitePool) -> Result<()> {
    let cluster: Option<String> =
        sqlx::query_scalar("SELECT value_json FROM system_config WHERE key='clickhouse_cluster'")
            .fetch_optional(pool)
            .await?;
    let mode = if cluster.is_some_and(|raw| !value(&raw).trim().is_empty()) {
        "cluster"
    } else {
        "single"
    };
    sqlx::query("INSERT OR IGNORE INTO system_config (key,value_json) VALUES ('clickhouse_mode',?)")
        .bind(serde_json::to_string(mode)?)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::error::Error as StdError;

    fn settings(mode: &str, cluster: &str) -> Vec<(String, String)> {
        [
            ("clickhouse_host", "localhost"),
            ("clickhouse_mode", mode),
            ("clickhouse_cluster", cluster),
        ]
        .into_iter()
        .map(|(key, text)| (key.into(), text.into()))
        .collect()
    }

    #[test]
    fn mode_and_batch_are_validated_before_applying_settings() -> Result<()> {
        let single = parse_ch_config(&settings("single", ""))?
            .ok_or_else(|| WebError::ValidationError("missing fixture".into()))?;
        assert_eq!(single.batch_size, 100_000);
        assert_eq!(single.connection.cluster_name()?, None);
        let cluster = parse_ch_config(&settings("cluster", "test_cluster"))?
            .ok_or_else(|| WebError::ValidationError("missing fixture".into()))?;
        assert_eq!(cluster.connection.cluster_name()?, Some("test_cluster"));
        for (mode, name) in [
            ("single", "cluster"),
            ("cluster", ""),
            ("cluster", "x;DROP"),
            ("typo", ""),
        ] {
            assert!(parse_ch_config(&settings(mode, name)).is_err());
        }
        for size in ["0", "-1", "wrong"] {
            let mut all = settings("single", "");
            all.push(("clickhouse_batch_size".into(), size.into()));
            assert!(parse_ch_config(&all).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_cluster_name_is_an_actionable_bad_request()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Err(error) = parse_ch_config(&settings("cluster", "")) else {
            panic!("invalid fixture accepted")
        };
        assert!(StdError::source(&error).is_some());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 4096).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|message| message.contains("cluster") && message.contains("名称"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_mode_migration_runs_once_and_preserves_explicit_selection() -> Result<()> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::query("CREATE TABLE system_config (key TEXT PRIMARY KEY, value_json TEXT NOT NULL)")
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO system_config VALUES ('clickhouse_cluster','\"old_cluster\"')")
            .execute(&pool)
            .await?;
        migrate_mode(&pool).await?;
        let first: String = sqlx::query_scalar("SELECT value_json FROM system_config WHERE key='clickhouse_mode'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(first, "\"cluster\"");
        sqlx::query("UPDATE system_config SET value_json='\"single\"' WHERE key='clickhouse_mode'")
            .execute(&pool)
            .await?;
        migrate_mode(&pool).await?;
        let second: String = sqlx::query_scalar("SELECT value_json FROM system_config WHERE key='clickhouse_mode'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(second, "\"single\"");
        Ok(())
    }
}
