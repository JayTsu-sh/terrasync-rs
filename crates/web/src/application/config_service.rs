use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::domain::repository::ConfigRepository;
use crate::error::Result;
use crate::infrastructure::clickhouse_client;
use crate::infrastructure::task_runner::apply_db_config_from_sqlite;

#[derive(Debug, Serialize)]
pub struct ConfigView {
    pub items: Vec<ConfigItem>,
}

#[derive(Debug, Serialize)]
pub struct ConfigItem {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ConfigUpdate {
    pub key: String,
    pub value: serde_json::Value,
}

/// 配置管理服务
pub struct ConfigService {
    config_repo: Arc<dyn ConfigRepository>,
}

impl ConfigService {
    pub fn new(config_repo: Arc<dyn ConfigRepository>) -> Self {
        Self { config_repo }
    }

    /// 获取所有配置项
    pub async fn get_config(&self) -> Result<ConfigView> {
        let all: Vec<(String, String)> = self.config_repo.get_all().await?;
        let items = all
            .into_iter()
            .map(|(key, value_json)| {
                let value = serde_json::from_str(&value_json).unwrap_or_else(|_| serde_json::Value::String(value_json));
                ConfigItem { key, value }
            })
            .collect();

        Ok(ConfigView { items })
    }

    /// 批量更新配置项，并处理副作用（日志级别重载、数据库配置应用）
    pub async fn update_config(&self, updates: &[ConfigUpdate]) -> Result<()> {
        for update in updates {
            let value_json = serde_json::to_string(&update.value)?;
            self.config_repo.set(&update.key, &value_json).await?;
        }

        // 如果更新了日志级别，动态应用到运行中的日志系统
        if let Some(log_update) = updates.iter().find(|u| u.key == "log.level") {
            if let Some(level) = log_update.value.as_str() {
                if let Err(e) = utils::logger::reload_log_level(level) {
                    warn!("Failed to reload log level: {e}");
                }
            }
        }

        // 如果更新了 ClickHouse 相关配置，重新加载到 AppConfig
        let has_db_update = updates.iter().any(|u| u.key.starts_with("clickhouse_"));
        if has_db_update {
            if let Err(e) = apply_db_config_from_sqlite(self.config_repo.as_ref()).await {
                warn!("Failed to apply ClickHouse config: {e}");
            }
        }

        Ok(())
    }

    /// 测试 ClickHouse 连通性
    pub async fn test_clickhouse(&self, dsn: &str, username: &str, password: &str, database: &str) -> Result<()> {
        clickhouse_client::test_connectivity(dsn, username, password, database).await
    }

    /// 启动时从 SQLite 加载已保存的配置并应用（日志级别 + ClickHouse 配置）
    pub async fn apply_saved_config(&self) {
        // 应用日志级别
        if let Ok(Some(level_json)) = self.config_repo.get("log.level").await {
            let level = serde_json::from_str::<String>(&level_json).unwrap_or(level_json);
            if !level.is_empty() {
                match utils::logger::reload_log_level(&level) {
                    Ok(()) => tracing::info!("Applied saved log level from SQLite: {}", level),
                    Err(e) => warn!("Failed to apply saved log level '{}': {}", level, e),
                }
            }
        }

        // 应用 ClickHouse 配置
        if let Err(e) = apply_db_config_from_sqlite(self.config_repo.as_ref()).await {
            warn!("Failed to apply saved ClickHouse config: {}", e);
        }
    }
}
