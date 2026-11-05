//! 任务使用的目录操作契约；行编码与 SQL 投影由 schema 模块负责。
use crate::catalogue::{CatalogueChange, CatalogueDifference, ComparisonMode, compare_catalogue_batch};
use crate::common::DeletionStatus;
use crate::error::Result;
use async_trait::async_trait;
use std::sync::Arc;
use storage_adapter::observed_scan::ObservedItem;
use storage_adapter::{ChangeKind, ObservedEntry, StorageEntryMessage};
use tokio::sync::mpsc;

/// 复制目录轮次是否需要重放未确认的目标操作。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyGenerationStatus {
    /// 首轮或上一轮全部确认成功，沿用正常增量比较。
    Fresh,
    /// 上轮复制没有确认完成，必须重放源项或由调用方拒绝不安全的恢复。
    Recovering,
}

/// 任务使用的 ClickHouse 目录操作；单机与集群布局由实现内部处理。
#[async_trait]
pub trait Database: Send + Sync {
    /// 创建当前对象的Box克隆
    /// 用于支持Box<dyn Database>的Clone操作
    fn clone_box(&self) -> Box<dyn Database>;

    /// 初始化数据库
    ///
    /// 创建必要的表结构。默认创建 base 表 + state 表。
    /// 可通过 `initialize_tables` 指定需要创建的表。
    async fn initialize(&self) -> Result<()>;

    /// 按需初始化指定的表
    ///
    /// # 参数
    /// - `tables`: 需要创建的表名列表（使用 `SCAN_BASE_TABLE_BASE_NAME`、
    ///   `INCREMENTAL_SCAN_TABLE_BASE_NAME` 等常量）
    async fn initialize_tables(&self, tables: &[&str]) -> Result<()> {
        for table in tables {
            self.create_table(table).await?;
        }
        Ok(())
    }

    /// 测试数据库连接
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回连接错误
    async fn ping(&self) -> Result<()>;

    /// 根据表名创建表
    ///
    /// # 参数
    /// - `table_name`: 表名
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn create_table(&self, table_name: &str) -> Result<()>;

    /// 创建临时扫描表
    /// 用于存储最新的扫描结果
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn create_scan_temporary_table(&mut self) -> Result<()>;

    /// 删除当前临时表
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn drop_scan_temporary_table(&mut self) -> Result<()>;

    /// 删除指定表（IF EXISTS）
    async fn drop_table_by_name(&self, table_name: &str) -> Result<()>;

    /// 批量插入数据到主表
    ///
    /// # 参数
    /// - `records`: `ObservedEntry` 切片
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_base_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()>;

    /// 更新单个主表记录
    ///
    /// # 参数
    /// - `record`: `ObservedEntry` 引用
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    /// 仅写入实际完成的版本，借用共享观察，不扩大到同一对象的其他历史。
    async fn batch_insert_completed_records(&self, records: &[ObservedItem]) -> Result<()>;

    async fn update_base_record(&self, record: &Arc<ObservedEntry>) -> Result<()>;

    /// 批量删除基础表记录
    async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()>;

    /// 批量插入临时表记录
    ///
    /// # 参数
    /// - `records`: `ObservedEntry` 切片
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_temp_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()>;

    /// 批量插入增量记录
    ///
    /// # 参数
    /// - `records`: `StorageEntryMessage` 切片（来自 `data_mover`）
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_incremental_record(&self, records: &[StorageEntryMessage]) -> Result<()>;

    /// 开始复制元数据轮次，绑定不含凭据的恢复计划摘要。
    ///
    /// # 返回值
    /// - Fresh 表示正常比较；Recovering 要求调用方确认目标允许安全重放
    /// - 失败返回错误信息
    async fn begin_copy_generation(&self, request_key: &str) -> Result<CopyGenerationStatus>;

    /// 从持久 generation 开始一轮扫描，但不持久化 working generation
    async fn begin_scan_generation(&self) -> Result<u8>;

    /// 提交本轮 begin 得到的 working generation
    async fn commit_scan_generation(&self) -> Result<()>;

    /// 新扫描必须使同一 job 的旧游标失效。
    async fn abandon_scan_resume(&self) -> Result<()> {
        Ok(())
    }

    /// 在数据库层完成整个比对会话；返回的每项共享输入的原生观察及版本选择。
    async fn compare_catalogue_batch(
        &self, records: &[Arc<ObservedEntry>], mode: ComparisonMode,
    ) -> Result<Vec<CatalogueChange>> {
        compare_catalogue_batch(self.clone_box(), records, mode).await
    }
    async fn classify_catalogue_items(&self) -> Result<Vec<CatalogueDifference>>;

    /// 查询在最新扫描中新增的文件
    /// 即在临时表中存在但在主表中不存在的文件
    ///
    /// # 返回值
    /// - 成功返回包含 `ObservedEntry` 迭代器的 `Ok`
    /// - 失败返回错误信息
    async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = ObservedEntry> + Send>>;

    /// 查询在最新扫描中发生变更的文件
    /// 区分三种变更：
    /// - `ChangeKind::DataOnly`：size 或 mtime 变了（内容变了），mode/uid/gid 未变
    /// - `ChangeKind::MetadataOnly`：size + mtime 未变，但 mode/uid/gid 至少一项变了（chmod/chown）
    /// - `ChangeKind::Both`：内容和属性都变了
    ///
    /// # 返回值
    /// - 成功返回 `(ObservedEntry, ChangeKind)` 迭代器
    /// - 失败返回错误信息
    async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (ObservedEntry, ChangeKind)> + Send>>;

    /// 查询在上一次扫描中存在但在最新扫描中缺失的文件
    /// 识别出待删除的文件后，立即在数据库中批量删除这些记录
    ///
    /// # 返回值
    /// - 成功返回包含 `DeletionStatus` 迭代器的 `Ok`
    /// - 失败返回错误信息
    async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>>;

    /// 将临时表中的记录合并到主表中，排除指定的路径
    ///
    /// # 参数
    /// - `excluded_paths`: 需要排除的 (`relative_path`, `version_id`) 列表
    ///   - 增量扫描（`keep_item=true`）：传空切片，全量插入
    ///   - 增量拷贝（`keep_item=false`）：传 new+changed 的路径，仅插入 unchanged
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn insert_temp_to_base_table(&self, excluded_paths: &[(String, String)]) -> Result<()>;

    /// 获取指定表中所有记录的数量
    ///
    /// # 参数
    /// - `table_name`: 表名
    ///
    /// # 返回值
    /// - 成功返回Ok(u64)，包含表中所有记录的数量
    /// - 失败返回错误信息
    async fn get_count(&self, table_name: &str) -> Result<u64>;

    /// 查询存储条目
    ///
    /// 根据提供的条件查询存储条目，并将结果发送到指定通道
    ///
    /// # 参数
    /// - `is_dir`: 是否为目录，None表示不限制
    /// - `is_symlink`: 是否为符号链接，None表示不限制
    /// - `extension`: 文件扩展名，None表示不限制
    /// - `tx`: 用于发送查询结果的通道发送器
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn query_storage_entry(
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>,
        tx: mpsc::Sender<ObservedEntry>,
    ) -> Result<()>;

    /// 创建 tar manifest 表
    async fn create_tar_manifest_table(&self) -> Result<()>;

    /// 批量插入 tar manifest 记录
    async fn batch_insert_tar_manifest(&self, tar_path: &str, entries: &[Arc<ObservedEntry>]) -> Result<()>;

    /// 检查指定表名是否存在
    ///
    /// # 参数
    /// - `table_name`: 完整的表名（如 `base_my_job`）
    ///
    /// # 返回值
    /// - `Ok(true)` 表存在
    /// - `Ok(false)` 表不存在
    /// - `Err(...)` 查询失败
    async fn table_exists(&self, table_name: &str) -> Result<bool>;
}

/// 为Box<dyn Database>实现Clone trait
/// 这样就可以直接克隆Box<dyn Database>对象
impl Clone for Box<dyn Database> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}
