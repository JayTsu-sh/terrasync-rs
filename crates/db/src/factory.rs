//! 创建 ClickHouse 目录连接，并阻止未完成扫描的 working 数据被普通目录入口读取。

use tracing::error;

// 内部模块
use crate::catalogue::ClickHouseDatabase;
use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::scan::ScanStore;
use crate::traits::Database;

/// ClickHouse 目录连接的创建入口。
pub struct DatabaseFactory;

impl DatabaseFactory {
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
                let db = ClickHouseDatabase::new(clickhouse_config, job_id)?;
                ScanStore::ensure_metadata_available(clickhouse_config, job_id).await?;
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
