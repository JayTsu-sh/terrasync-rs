//! 数据库配置模块
//!
//! 该模块定义了数据库的配置结构和类型，包括：
//! 1. 数据库类型枚举
//! 2. 通用数据库配置
//! 3. ClickHouse特定配置
//! 4. DuckDB特定配置（可选）
//! 5. 配置转换功能

// 标准库
// 无

// 外部crate
use serde::{Deserialize, Serialize};
use utils::app_config::DatabaseConfig as AppDatabaseConfig;

// 内部模块
#[cfg(feature = "duckdb")]
use crate::error::Result;

/// 数据库类型枚举
/// 支持的数据库后端类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DatabaseType {
    #[serde(rename = "clickhouse")]
    ClickHouse,
    #[cfg(feature = "duckdb")]
    #[serde(rename = "duckdb")]
    DuckDB,
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
    /// ClickHouse特定配置
    pub clickhouse: Option<ClickHouseConfig>,
    /// DuckDB特定配置
    #[cfg(feature = "duckdb")]
    pub duckdb: Option<DuckDBConfig>,
}

/// 从utils::app_config::DatabaseConfig转换为db::config::DatabaseConfig
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
            #[cfg(feature = "duckdb")]
            duckdb: Some(DuckDBConfig {
                in_memory: app_config.duckdb.in_memory,
                pool_size: app_config.duckdb.pool_size,
                job_dir: "".to_string(),
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

/// ClickHouse数据库配置结构体
/// 包含连接ClickHouse所需的所有参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// ClickHouse连接字符串
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

#[cfg(feature = "duckdb")]
/// DuckDB数据库配置结构体
/// 包含配置DuckDB所需的所有参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDBConfig {
    /// 是否使用内存模式
    pub in_memory: bool,
    /// 连接池大小
    pub pool_size: u32,
    /// 任务目录路径（用于文件模式）
    pub job_dir: String,
}

#[cfg(feature = "duckdb")]
impl DuckDBConfig {
    /// 获取DuckDB数据库文件路径
    ///
    /// # 返回值
    /// - 内存模式返回":memory:"
    /// - 文件模式返回格式化的文件路径
    pub fn get_path(&self) -> Result<String> {
        if self.in_memory {
            Ok(":memory:".to_string())
        } else {
            // 直接使用job_dir作为路径，因为它已经包含了必要的目录结构
            Ok(format!("{}/duckdb.db", self.job_dir))
        }
    }
}
