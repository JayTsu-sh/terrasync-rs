//! Task-owned ClickHouse catalogue. Single/Cluster placement is private to layout.
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use clickhouse::Client;

use self::layout::ClusterCatalogue;
use self::lifecycle::CatalogueLifecycle;

mod comparison;
mod connection;
mod copy_generation;
mod deletion;
mod generation;
mod layout;
mod lifecycle;
mod native;
mod read;
mod scan_resume;
mod scan_resume_statistics;
mod tables;
mod write;

pub use comparison::{CatalogueChange, ComparisonMode};
pub use copy_generation::CopyGenerationStatus;
pub use deletion::DeletionStatus;

#[cfg(test)]
mod baseline_tests;
#[cfg(test)]
mod catalogue_tests;
#[cfg(test)]
mod record_tests;

/// `ClickHouse`数据库实现
///
/// 提供与ClickHouse数据库交互的所有操作功能，包括表的创建、数据的插入、查询和删除等。
pub struct ClickHouseDatabase {
    /// ClickHouse同步客户端，用于执行同步操作如查询和简单插入
    sync_client: Client,
    catalogue_cluster: Option<ClusterCatalogue>,
    /// ClickHouse异步客户端，配置了异步插入功能，用于大批量数据插入
    async_client: Client,
    /// 任务ID，用于在多任务环境下表命名的隔离和区分
    job_id: String,
    /// 当前使用的临时扫描表名，可选值，创建临时表时设置，删除时清空
    scan_temp_table_name: Option<String>,
    /// 缓存的 `scan_state` 值（255 = 未缓存），避免每次 `batch_insert` 都查询数据库
    cached_scan_state: AtomicU8,
    /// 每次扫描实例只检查一次旧基线的身份格式，避免每批全表统计。
    resume_base_has_typed_handles: Option<bool>,
    /// 由本任务显式克隆的目录实例共享的代次与写入协调状态
    generation_lifecycle: Arc<CatalogueLifecycle>,
}
