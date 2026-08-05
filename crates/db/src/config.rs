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
            clickhouse: Some(ClickHouseConfig {
                dsn: app_config.clickhouse.dsn.clone(),
                dial_timeout: app_config.clickhouse.dial_timeout,
                read_timeout: app_config.clickhouse.read_timeout,
                database: app_config.clickhouse.database.clone(),
                username: app_config.clickhouse.username.clone(),
                password: app_config.clickhouse.password.clone(),
            }),
        }
    }
}

/// 提供显式方法进行类型转换
impl DatabaseConfig {
    pub fn from_app_config(app_config: &AppDatabaseConfig) -> Self {
        Self::from(app_config)
    }
}

/// `ClickHouse` 数据库配置结构体
/// 包含连接 `ClickHouse` 所需的所有参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// `ClickHouse` 连接字符串
    pub dsn: String,
    /// 连接超时时间（秒）
    pub dial_timeout: u32,
    /// 读取超时时间（秒）
    pub read_timeout: u32,
    /// 数据库名称
    pub database: String,
    /// 用户名
    pub username: String,
    /// 密码（可选）
    pub password: Option<String>,
}
