//! 数据库接口和数据结构定义模块
//!
//! 该模块定义了数据库访问的核心接口和数据结构，包括：
//! 1. 数据库访问接口（Database trait）
//! 2. 存储条目消息类型
//! 3. 查询结果结构体
//! 4. 存储条目记录结构体
//! 5. 辅助函数和转换方法

// 标准库
use std::sync::Arc;
use std::time::SystemTime;
use storage_adapter::ObservedEntryExt;

// 外部crate
use crate::catalogue::{CatalogueChange, CatalogueDifference, ComparisonMode, compare_catalogue_batch};
use crate::error::DatabaseError;
use crate::scan::rows::{capture_summary, incremental_record};
use async_trait::async_trait;
use data_mover::model::{BackendIdentity, BackendKind};
use serde::{Deserialize, Serialize};
use storage_adapter::{ChangeKind, ObservedEntry, StorageEntryMessage};
use tokio::sync::mpsc;

// 内部模块
use crate::common::DeletionStatus;
use crate::error::Result;

/// 查询结果结构体
/// 包含查询返回的行数据、受影响的行数和最后插入的ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueryResult {
    /// 查询返回的行数据
    pub rows: Vec<serde_json::Value>,
    /// 受影响的行数
    pub affected_rows: u64,
    /// 最后插入的ID（可选）
    pub last_insert_id: Option<u64>,
}

/// 文件扫描记录结构体
/// 统一的数据结构，用于存储文件系统扫描结果
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct StorageEntryRecord {
    /// 文件或目录的名称
    pub name: String,
    /// 相对路径(主键)
    pub relative_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 创建时间（时间戳）
    pub ctime: i64,
    /// 修改时间（时间戳）
    pub mtime: i64,
    /// 访问时间（时间戳）
    pub atime: i64,
    /// Unix权限模式（可选，如0o755）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为普通文件
    pub is_regular_file: bool,
    /// 硬链接数量
    pub hard_links: u32,
    /// 当前扫描状态
    pub current_state: u8,
    /// 用户ID
    pub uid: Option<u32>,
    /// 组ID
    pub gid: Option<u32>,
    /// 索引节点号
    pub ino: Option<u64>,
    /// 文件句柄（NFS file handle / CIFS file ID 等）
    pub file_handle: Option<String>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: String,
    /// 文件标签
    pub tags: Option<String>,
    /// 版本计数
    pub version_count: Option<u32>,
    pub marker_state: Option<String>,
    pub backend_kind: String,
    pub backend_id: String,
    pub entry_snapshot: Vec<u8>,
}

impl StorageEntryRecord {
    #[cfg(test)]
    pub fn capture(entry: &ObservedEntry, generation: u8) -> Result<Self> {
        let fields = capture_summary(
            storage_adapter::observed_scan::ObservedView {
                entry,
                version_index: entry.history().map(|_| 0),
            },
            generation,
        )?;
        Ok(Self {
            name: fields.name,
            relative_path: fields.relative_path,
            size: fields.size,
            ext: fields.ext,
            ctime: fields.ctime,
            mtime: fields.mtime,
            atime: fields.atime,
            mode: fields.mode,
            storage_type: fields.storage_type,
            is_symlink: fields.is_symlink,
            is_dir: fields.is_dir,
            is_regular_file: fields.is_regular_file,
            hard_links: fields.hard_links,
            current_state: fields.current_state,
            uid: fields.uid,
            gid: fields.gid,
            ino: fields.ino,
            file_handle: fields.file_handle,
            version_id: fields.version_id,
            tags: fields.tags,
            version_count: fields.version_count,
            marker_state: fields.marker_state,
            backend_kind: entry.backend_kind().as_str().into(),
            backend_id: entry.source_identity().backend().stable_id().into(),
            entry_snapshot: entry.try_encode_snapshot()?.as_bytes().to_vec(),
        })
    }
    /// 目录持久化读取只接受完整原生 snapshot，不从查询列拼造观察。
    pub fn to_observation(&self) -> Result<ObservedEntry> {
        let backend = BackendIdentity::new(self.backend_kind.parse()?, self.backend_id.clone())?;
        let entry = ObservedEntry::decode_snapshot(&self.entry_snapshot, &backend)?;
        if entry.get_relative_path() != std::path::Path::new(&self.relative_path)
            && entry
                .history_source_path()?
                .as_ref()
                .is_none_or(|path| path.as_str() != self.relative_path)
        {
            return Err(DatabaseError::ObservationProjectionMismatch { field: "relative_path" });
        }
        Ok(entry)
    }
}

/// 文件增量扫描记录结构体
/// 对应 `FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION` 中的列定义
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct IncrementalStorageEntryRecord {
    /// 操作类型：new, changed, deleted, rename
    pub operation_type: String,
    /// 文件或目录的名称
    pub name: String,
    /// 相对路径
    pub relative_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 创建时间（纳秒时间戳）
    pub ctime: i64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,
    /// 访问时间（纳秒时间戳）
    pub atime: i64,
    /// Unix权限模式（可选，如0o755）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为普通文件
    pub is_regular_file: bool,
    /// 硬链接数量
    pub hard_links: u32,
    /// 当前扫描状态
    pub current_state: u8,
    /// 用户ID
    pub uid: Option<u32>,
    /// 组ID
    pub gid: Option<u32>,
    /// 索引节点号
    pub ino: Option<u64>,
    /// 文件句柄（hex 字符串格式）
    pub file_handle: Option<String>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: String,
    /// 文件标签
    pub tags: Option<String>,
    /// 版本计数
    pub version_count: Option<u32>,
    pub marker_state: Option<String>,
    /// 记录创建时间
    pub create_at: i64,
    /// 备注信息（如重命名时的源路径）
    pub comment: Option<String>,
}

impl IncrementalStorageEntryRecord {
    pub fn from_message(message: &StorageEntryMessage) -> Result<Vec<Self>> {
        let now = i64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos())?;
        let (entry, operation, comment) = match message {
            StorageEntryMessage::Scanned(entry) => (entry, "scanned", None),
            StorageEntryMessage::Packaged(entry) => (entry, "packaged", None),
            StorageEntryMessage::New(item) => {
                let row = capture_summary(item.view(), 0)?;
                return Ok(vec![incremental_record(row, "new", None, now)]);
            }
            StorageEntryMessage::Changed { entry, kind } => {
                let row = capture_summary(entry.view(), 0)?;
                return Ok(vec![incremental_record(row, kind.as_state_str(), None, now)]);
            }
            StorageEntryMessage::Deleted(entry) => (entry, "deleted", None),
            StorageEntryMessage::IntegrityChecked(entry) => (entry, "integrity_checked", None),
            StorageEntryMessage::Renamed((from, to)) => (to, "rename", Some(from.path().as_str().to_owned())),
            StorageEntryMessage::Error {
                event,
                path,
                entry,
                reason,
            } => {
                let comment = Some(format!("[{event}] {reason}"));
                if let Some(entry) = entry {
                    return Self::observed_rows(entry, "error", comment, now);
                }
                let row = crate::schema::entry::EntrySummary {
                    name: path
                        .file_name()
                        .map_or_else(String::new, |value| value.to_string_lossy().into_owned()),
                    relative_path: path.to_string_lossy().into_owned(),
                    ..Default::default()
                };
                return Ok(vec![incremental_record(row, "error", comment, now)]);
            }
            StorageEntryMessage::TarManifest { .. } => {
                return Err(DatabaseError::ConversionError(
                    "tar manifest must use the manifest writer".into(),
                ));
            }
        };
        Self::observed_rows(entry, operation, comment, now)
    }

    fn observed_rows(entry: &ObservedEntry, operation: &str, comment: Option<String>, now: i64) -> Result<Vec<Self>> {
        storage_adapter::observed_scan::ObservedView::all(entry)
            .map(|view| {
                let row = capture_summary(view, 0)?;
                Ok(incremental_record(row, operation, comment.clone(), now))
            })
            .collect()
    }
}

/// Tar 打包 manifest 记录
/// 记录每个 .tar 文件内部包含的条目信息，兼容 NAS 和 S3 条目
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct TarManifestRecord {
    /// .tar 文件的 `relative_path`
    pub tar_path: String,
    /// tar 内条目的相对路径
    pub entry_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,
    /// Unix 权限模式（NAS 有，S3 为 None）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 用户 ID（NAS 有，S3 为 None）
    pub uid: Option<u32>,
    /// 组 ID（NAS 有，S3 为 None）
    pub gid: Option<u32>,
    /// 对象版本 ID（S3 多版本使用，NAS 为空字符串）
    pub version_id: String,
    /// 文件标签（S3 使用，JSON 字符串）
    pub tags: Option<String>,
}

impl TarManifestRecord {
    /// 从 `ObservedEntry` 和 tar 路径创建 `TarManifestRecord`
    pub fn from_entry(entry: &ObservedEntry, tar_path: &str) -> Self {
        let storage_type = match entry.backend_kind() {
            BackendKind::S3 => "s3",
            BackendKind::Hdfs => "hdfs",
            _ => "nas",
        };
        let tags = entry.get_tags().and_then(|tags| {
            serde_json::to_string(
                &tags
                    .iter()
                    .map(|tag| (tag.key(), tag.value()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
            .ok()
        });

        Self {
            tar_path: tar_path.to_string(),
            entry_path: entry.get_relative_path().to_string_lossy().into_owned(),
            size: entry.get_size(),
            ext: entry.get_extension().map(str::to_string),
            mtime: entry.get_mtime(),
            mode: entry.get_mode(),
            storage_type: storage_type.to_string(),
            is_dir: entry.get_is_dir(),
            is_symlink: entry.get_is_symlink(),
            uid: entry.get_uid(),
            gid: entry.get_gid(),
            version_id: entry.get_version_id().unwrap_or_default().to_string(),
            tags,
        }
    }
}

/// 复制目录轮次是否需要重放未确认的目标操作。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyGenerationStatus {
    /// 首轮或上一轮全部确认成功，沿用正常增量比较。
    Fresh,
    /// 上轮复制没有确认完成，必须重放源项或由调用方拒绝不安全的恢复。
    Recovering,
}

/// 数据库通用接口
/// 定义所有数据库后端需要实现的方法
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
    async fn batch_insert_completed_records(
        &self, records: &[storage_adapter::observed_scan::ObservedItem],
    ) -> Result<()>;

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
    async fn batch_insert_tar_manifest(&self, records: &[TarManifestRecord]) -> Result<()>;

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

impl storage_adapter::observed_scan::EntryStatistics for StorageEntryRecord {
    fn get_size(&self) -> u64 {
        self.size
    }
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_relative_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.relative_path)
    }
    fn get_extension(&self) -> Option<&str> {
        self.ext.as_deref()
    }
    fn get_is_dir(&self) -> bool {
        self.is_dir
    }
    fn get_is_symlink(&self) -> bool {
        self.is_symlink
    }
    fn get_is_regular_file(&self) -> bool {
        self.is_regular_file
    }
    fn get_mtime(&self) -> i64 {
        self.mtime
    }
}
