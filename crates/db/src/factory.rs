//! 数据库工厂模块
//!
//! 该模块提供了数据库实例的创建和管理功能，实现了工厂模式：
//! 1. 数据库类型注册机制
//! 2. 基于配置动态创建数据库实例
//! 3. 支持多种数据库后端
//! 4. 线程安全的设计

// 标准库
use std::sync::Arc;
use std::sync::LazyLock;

// 外部crate
use dashmap::DashMap;
use tracing::error;

// 内部模块
use crate::clickhouse::ClickHouseDatabase;
use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::Database;

/// 数据库创建函数类型定义
/// 用于创建特定类型的数据库实例
pub type DatabaseCreator = fn(config: &DatabaseConfig, job_id: String) -> Result<Arc<dyn Database>>;

/// 数据库注册表 - 线程安全的全局映射
/// 用于存储数据库类型名称和对应的创建函数
static DATABASE_REGISTRY: LazyLock<DashMap<String, DatabaseCreator>> = LazyLock::new(|| {
    let registry = DashMap::new();
    // 自动注册内置数据库类型
    register_builtin_types(&registry);
    registry
});

/// 数据库工厂类
/// 提供创建和注册数据库实例的功能
pub struct DatabaseFactory;

impl DatabaseFactory {
    /// 注册新的数据库类型
    ///
    /// # 参数
    /// - `db_type`: 数据库类型名称
    /// - `creator`: 创建该类型数据库实例的函数
    ///
    /// # 返回值
    /// - 成功返回Ok(()), 失败返回错误信息
    pub fn register_database_type(db_type: &str, creator: DatabaseCreator) -> Result<()> {
        DATABASE_REGISTRY.insert(db_type.to_string(), creator);
        Ok(())
    }

    /// 根据配置创建数据库实例
    ///
    /// # 参数
    /// - `config`: 数据库配置信息
    /// - `job_id`: 任务ID，用于表命名隔离
    ///
    /// # 返回值
    /// - 成功返回数据库实例的Arc指针
    /// - 失败返回包含错误信息的Result
    pub async fn new_database(config: &DatabaseConfig, job_id: &str) -> Result<Box<dyn Database>> {
        // 检查数据库是否启用
        if !config.enabled {
            return Err(DatabaseError::DatabaseDisabledError());
        }

        // 根据数据库类型创建实例
        let db_instance = match config.db_type.as_str() {
            "clickhouse" => {
                // 提取ClickHouse配置
                let clickhouse_config = config
                    .clickhouse
                    .as_ref()
                    .ok_or_else(|| DatabaseError::ConfigError("ClickHouse configuration missing".to_owned()))?;

                // 创建ClickHouse数据库实例
                let db = ClickHouseDatabase::new(clickhouse_config, job_id);
                Box::new(db) as Box<dyn Database>
            }
            _ => {
                // 不支持的数据库类型
                return Err(DatabaseError::UnsupportedType(format!(
                    "Database type '{}' is not supported",
                    config.db_type
                )));
            }
        };

        // 初始化数据库连接
        if let Err(e) = db_instance.ping().await {
            error!("[DatabaseFactory] Failed to connect to database: {:?}", e);
            return Err(DatabaseError::ConnectionError(format!(
                "Failed to connect to database: {e:?}"
            )));
        }

        Ok(db_instance)
    }
}

// 内置类型注册函数
fn register_builtin_types(registry: &DashMap<String, DatabaseCreator>) {
    // Register ClickHouse
    registry.insert("clickhouse".to_string(), |config, job_id| {
        let clickhouse_config = config
            .clickhouse
            .as_ref()
            .ok_or_else(|| DatabaseError::ConfigError("ClickHouse configuration missing".to_string()))?;

        let db = ClickHouseDatabase::new(clickhouse_config, &job_id);
        Ok(Arc::new(db) as Arc<dyn Database>)
    });
}
