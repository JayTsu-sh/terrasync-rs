#![recursion_limit = "256"]

//! 数据库访问模块
//!
//! 提供统一的数据库接口抽象和 `ClickHouse` 数据库实现
//! 主要功能包括表创建、数据插入、查询、差异比较等操作

// 标准库
// 无

// 外部crate
use utils::sanitize_job_id;
use uuid::Uuid;

// 内部模块
// 公共模块导出
pub mod clickhouse; // ClickHouse数据库实现
pub mod common; // Common types and utilities
pub mod config; // 数据库配置定义
pub mod error; // 错误处理
pub mod factory; // 数据库工厂，用于创建不同类型的数据库实例
pub mod observation_record;
mod recovery_state;
pub mod traits; // 数据库接口定义

// 共享的表名常量
/// 扫描基础表的基本名称
pub const SCAN_BASE_TABLE_BASE_NAME: &str = "base";
/// 扫描基础表的基本名称
pub const INCREMENTAL_SCAN_TABLE_BASE_NAME: &str = "incremental";
/// 临时扫描表的基本名称
pub const SCAN_TEMP_TABLE_BASE_NAME: &str = "temp";
/// 扫描状态表的基本名称
pub const SCAN_STATE_TABLE_BASE_NAME: &str = "state";
/// Tar manifest 表的基本名称
pub const TAR_MANIFEST_TABLE_BASE_NAME: &str = "tar_manifest";

// 类型重导出，方便外部使用
pub use clickhouse::ClickHouseDatabase; // ClickHouse数据库具体实现
pub use common::DeletionStatus; // Common deletion status enum
pub use config::{ClickHouseConfig, DatabaseConfig, DatabaseType}; // 配置相关类型
pub use error::{DatabaseError, Result}; // 错误处理相关类型
pub use factory::DatabaseFactory; // 数据库工厂类型
pub use observation_record::{OBSERVATION_IDENTITY_JOIN_CLAUSE, ObservedEntryRecord};
pub use recovery_state::{RecoveryAttemptId, RecoveryAttemptRegistration};
pub use traits::Database; // 数据库接口类型

/// 公共 API 的 prelude 模块
///
/// 用户可以通过 `use db::prelude::*` 来导入最常用的类型，
/// 简化应用开发过程中的导入语句
pub mod prelude {
    /// 数据库具体实现
    pub use super::ClickHouseDatabase;
    /// 数据库接口类型
    pub use super::Database;
    /// 数据库工厂类型
    pub use super::DatabaseFactory;
    /// Common deletion status enum
    pub use super::DeletionStatus;
    /// 配置相关类型
    pub use super::{ClickHouseConfig, DatabaseConfig, DatabaseType};
    /// 错误处理相关类型
    pub use super::{DatabaseError, Result};
    pub use super::{OBSERVATION_IDENTITY_JOIN_CLAUSE, ObservedEntryRecord};
    pub use super::{RecoveryAttemptId, RecoveryAttemptRegistration};
}

/// 根据 `job_id` 生成扫描基础表名
///
/// # 参数
/// - `job_id`: 任务ID，用于隔离不同任务的数据
///
/// # 返回值
/// - 包含任务ID的完整扫描基础表名
pub(crate) fn get_scan_base_table_name(job_id: &str) -> String {
    format!("{}_{}", SCAN_BASE_TABLE_BASE_NAME, sanitize_job_id(job_id))
}

/// 根据 `job_id` 生成增量扫描基础表名
///
/// # 参数
/// - `job_id`: 任务ID，用于隔离不同任务的数据
///
/// # 返回值
/// - 包含任务ID的完整增量扫描基础表名
pub(crate) fn get_incremental_scan_base_table_name(job_id: &str) -> String {
    format!("{}_{}", INCREMENTAL_SCAN_TABLE_BASE_NAME, sanitize_job_id(job_id))
}

/// 根据 `job_id` 生成扫描状态表名
///
/// # 参数
/// - `job_id`: 任务ID，用于隔离不同任务的数据
///
/// # 返回值
/// - 包含任务ID的完整扫描状态表名
pub(crate) fn get_scan_state_table_name(job_id: &str) -> String {
    format!("{}_{}", SCAN_STATE_TABLE_BASE_NAME, sanitize_job_id(job_id))
}

/// 根据 `job_id` 生成 tar manifest 表名
pub(crate) fn get_tar_manifest_table_name(job_id: &str) -> String {
    format!("{}_{}", TAR_MANIFEST_TABLE_BASE_NAME, sanitize_job_id(job_id))
}

/// 根据 `job_id` 生成 base 表的完整表名（供外部判定增量模式使用）
///
/// 内部调用 `sanitize_job_id` 处理特殊字符
pub fn base_table_name_for_job(job_id: &str) -> String {
    get_scan_base_table_name(job_id)
}

/// 生成唯一的临时扫描表名
/// 使用UUID生成唯一标识符，确保临时表名不会冲突
///
/// # 返回值
/// - 唯一的临时扫描表名
pub(crate) fn generate_scan_temp_table_name() -> String {
    let uuid = Uuid::new_v4().to_string().replace('-', "_");
    format!("{SCAN_TEMP_TABLE_BASE_NAME}_{uuid}")
}
