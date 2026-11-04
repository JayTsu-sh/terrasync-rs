//! 数据库配置模块
//!
//! 该模块定义了数据库的配置结构和类型，包括：
//! 1. 数据库类型枚举
//! 2. 通用数据库配置
//! 3. `ClickHouse` 特定配置
//! 4. 配置转换功能

// 标准库
// 无

// 外部crate
use serde::{Deserialize, Serialize};
use utils::app_config::DatabaseConfig as AppDatabaseConfig;

// 内部模块
/// 数据库类型枚举
/// 支持的数据库后端类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DatabaseType {
    #[serde(rename = "clickhouse")]
    ClickHouse,
}

/// 数据库配置结构体
/// 包含数据库通用配置和特定数据库的配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// 是否启用数据库
    pub enabled: bool,
    /// 数据库类型名称
    pub db_type: String,
    /// 批处理大小
    pub batch_size: u32,
    /// `ClickHouse` 特定配置
    pub clickhouse: Option<ClickHouseConfig>,
}

/// 从 `utils::app_config::DatabaseConfig` 转换为 `db::config::DatabaseConfig`
impl From<&AppDatabaseConfig> for DatabaseConfig {
    fn from(app_config: &AppDatabaseConfig) -> Self {
        Self {
            enabled: app_config.enabled,
            db_type: app_config.r#type.clone(),
            batch_size: app_config.batch_size,
            clickhouse: Some(app_config.clickhouse.clone()),
        }
    }
}

/// 提供显式方法进行类型转换
impl DatabaseConfig {
    pub fn from_app_config(app_config: &AppDatabaseConfig) -> Self {
        Self::from(app_config)
    }
}

// 使用同一连接及部署配置，避免转换时丢失模式或集群名称。
pub use utils::app_config::ClickhouseConfig as ClickHouseConfig;
