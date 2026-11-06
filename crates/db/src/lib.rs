#![recursion_limit = "256"]

//! 任务拥有的 ClickHouse 目录及扫描持久化。
//!
//! 直接使用具体目录类型；Single／Cluster 布局与 SQL 行编码封装在 db 内。

// 标准库
// 无

// 外部crate
use utils::sanitize_job_id;
use uuid::Uuid;

// 内部模块
// 公共模块导出
pub mod catalogue; // ClickHouse数据库实现
pub mod config; // 数据库配置定义
pub mod error; // 错误处理
mod execution;
pub mod scan;
mod schema;

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
pub use catalogue::{ClickHouseDatabase, CopyGenerationStatus, DeletionStatus};
pub use config::ClickHouseConfig; // 配置相关类型
pub use error::{DatabaseError, Result}; // 错误处理相关类型

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

/// 生成唯一的临时扫描表名
/// 使用UUID生成唯一标识符，确保临时表名不会冲突
///
/// # 返回值
/// - 唯一的临时扫描表名
pub(crate) fn generate_scan_temp_table_name() -> String {
    let uuid = Uuid::new_v4().to_string().replace('-', "_");
    format!("{SCAN_TEMP_TABLE_BASE_NAME}_{uuid}")
}

#[cfg(test)]
#[path = "../../../tests/support/observed.rs"]
mod observed_fixtures;

pub use schema::entry::EntrySummary;
