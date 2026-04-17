//! 数据库接口和数据结构定义模块
//!
//! 该模块定义了数据库访问的核心接口和数据结构，包括：
//! 1. 数据库访问接口（Database trait）
//! 2. 存储条目消息类型
//! 3. 查询结果结构体
//! 4. 存储条目记录结构体
//! 5. 辅助函数和转换方法

// 标准库
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

// 外部crate
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use storage_v2::{ChangeKind, EntryEnum, NASEntry, S3Entry, StorageEntryMessage};
use tokio::sync::mpsc;
use tracing::{trace, warn};

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
}

impl StorageEntryRecord {
    /// 将 `EntryEnum` 转换为 `StorageEntryRecord`
    ///
    /// # 参数
    /// - `entry`: 存储条目对象（NAS或S3）
    /// - `current_state`: 当前扫描状态
    ///
    /// # 返回值
    /// - 转换后的 `StorageEntryRecord` 对象
    pub fn from_entry_enum(entry: &EntryEnum, current_state: u8) -> Self {
        let ext = entry.get_extension().map(str::to_string);
        trace!("{:?}", entry.get_file_handle());

        // 将Bytes转换为hex字符串
        let file_handle = entry.get_file_handle().map(hex::encode);
        trace!("{:?}", file_handle);

        // 将Vec<Tag>转换为JSON字符串
        let tags = entry.get_tags().and_then(|tags| serde_json::to_string(tags).ok());

        let storage_type = match entry {
            EntryEnum::NAS(_) => "nas",
            EntryEnum::S3(_) => "s3",
        };

        StorageEntryRecord {
            name: entry.get_name().to_string(),
            relative_path: entry.get_relative_path().to_string_lossy().into_owned(),
            size: entry.get_size(),
            ext,
            ctime: entry.get_ctime(),
            mtime: entry.get_mtime(),
            atime: entry.get_atime(),
            mode: entry.get_mode(),
            storage_type: storage_type.to_string(),
            is_symlink: entry.get_is_symlink(),
            is_dir: entry.get_is_dir(),
            is_regular_file: entry.get_is_regular_file(),
            hard_links: entry.get_hard_links().unwrap_or(1),
            current_state,
            uid: entry.get_uid(),
            gid: entry.get_gid(),
            ino: entry.get_ino(),
            file_handle,
            version_id: entry.get_version_id().unwrap_or_default().to_string(),
            tags,
            version_count: entry.get_version_count(),
        }
    }

    /// 将 `StorageEntryRecord` 转换为 `EntryEnum`
    ///
    /// # 返回值
    /// - 转换后的 `EntryEnum` 对象（根据 `storage_type` 选择 NAS 或 S3）
    pub fn to_entry_enum(&self) -> EntryEnum {
        // 将十六进制字符串解码为Bytes
        let file_handle = self
            .file_handle
            .as_ref()
            .and_then(|s| hex::decode(s).ok())
            .map(Bytes::from);

        // 将JSON字符串转换为Vec<Tag>
        let tags = self
            .tags
            .as_ref()
            .and_then(|tags_str| serde_json::from_str::<Vec<storage_v2::Tag>>(tags_str).ok());

        if self.storage_type == "s3" {
            EntryEnum::S3(S3Entry {
                name: self.name.clone(),
                relative_path: self.relative_path.clone(),
                extension: self.ext.clone(),
                size: self.size,
                mtime: self.mtime,
                tags,
                version_id: if self.version_id.is_empty() {
                    None
                } else {
                    Some(self.version_id.clone())
                },
                is_latest: false,
                is_delete_marker: false,
                version_count: self.version_count,
                is_dir: self.is_dir,
            })
        } else {
            EntryEnum::NAS(NASEntry {
                name: self.name.clone(),
                relative_path: PathBuf::from(&self.relative_path),
                extension: self.ext.clone(),
                is_dir: self.is_dir,
                size: self.size,
                atime: self.atime,
                ctime: self.ctime,
                mtime: self.mtime,
                mode: self.mode.unwrap_or(0),
                is_symlink: self.is_symlink,
                hard_links: Some(self.hard_links),
                file_handle,
                uid: self.uid,
                gid: self.gid,
                ino: self.ino,
                acl: None,
                owner: None,
                owner_group: None,
                xattrs: None,
            })
        }
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
    /// 记录创建时间
    pub create_at: i64,
    /// 备注信息（如重命名时的源路径）
    pub comment: Option<String>,
}

impl IncrementalStorageEntryRecord {
    /// 从 `StorageEntryMessage` 创建 `IncrementalStorageEntryRecord`
    ///
    /// # 参数
    /// - `message`: 存储条目消息对象（来自 `storage_v2`）
    ///
    /// # 返回值
    /// - 转换后的 `IncrementalStorageEntryRecord` 对象
    pub fn from_message(message: &StorageEntryMessage) -> Self {
        // 计算当前时间戳，避免在每个分支重复计算
        let create_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                warn!("系统时钟早于 Unix epoch，create_at 将记为 0");
                Duration::ZERO
            })
            .as_nanos() as i64;

        match message {
            // 处理普通操作（new、changed、deleted）的公共逻辑
            StorageEntryMessage::Scanned(entry) => Self::from_entry(entry.as_ref(), "scanned", None, create_at),
            StorageEntryMessage::Packaged(entry) => Self::from_entry(entry.as_ref(), "packaged", None, create_at),
            StorageEntryMessage::New(entry) => Self::from_entry(entry.as_ref(), "new", None, create_at),
            StorageEntryMessage::Changed { entry, kind } => {
                // state 列区分三种变更：data_changed / metadata_changed / both_changed
                Self::from_entry(entry.as_ref(), kind.as_state_str(), None, create_at)
            }
            StorageEntryMessage::Deleted(entry) => Self::from_entry(entry.as_ref(), "deleted", None, create_at),
            StorageEntryMessage::IntegrityChecked(entry) => {
                Self::from_entry(entry.as_ref(), "integrity_checked", None, create_at)
            }

            // 处理重命名操作的特殊逻辑
            StorageEntryMessage::Renamed((from_entry, to_entry)) => {
                let comment = Some(from_entry.get_relative_path().to_string_lossy().into_owned());
                Self::from_entry(to_entry.as_ref(), "rename", comment, create_at)
            }

            // TarManifest 由 DatabaseConsumer 单独处理，此分支仅为满足 match 穷举
            StorageEntryMessage::TarManifest { tar_path, .. } => Self {
                operation_type: "tar_manifest".to_string(),
                name: String::new(),
                relative_path: tar_path.clone(),
                size: 0,
                ext: None,
                ctime: 0,
                mtime: 0,
                atime: 0,
                mode: None,
                storage_type: String::new(),
                is_symlink: false,
                is_dir: false,
                is_regular_file: false,
                hard_links: 0,
                current_state: 0,
                uid: None,
                gid: None,
                ino: None,
                file_handle: None,
                version_id: String::new(),
                create_at,
                comment: None,
                tags: None,
                version_count: None,
            },

            // 错误消息记录为 error 操作类型
            StorageEntryMessage::Error { event, path, reason } => Self {
                operation_type: "error".to_string(),
                name: path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                relative_path: path.to_string_lossy().into_owned(),
                size: 0,
                ext: None,
                ctime: 0,
                mtime: 0,
                atime: 0,
                mode: None,
                storage_type: String::new(),
                is_symlink: false,
                is_dir: false,
                is_regular_file: false,
                hard_links: 0,
                current_state: 0,
                uid: None,
                gid: None,
                ino: None,
                file_handle: None,
                version_id: String::new(),
                create_at,
                comment: Some(format!("[{event}] {reason}")),
                tags: None,
                version_count: None,
            },
        }
    }

    /// 从 `EntryEnum` 和操作类型创建 `IncrementalStorageEntryRecord` 的辅助方法
    ///
    /// # 参数
    /// - `entry`: 存储条目对象（NAS或S3）
    /// - `operation_type`: 操作类型字符串
    /// - `comment`: 可选的备注信息
    /// - `create_at`: 创建时间戳
    ///
    /// # 返回值
    /// - 构建的 `IncrementalStorageEntryRecord` 对象
    fn from_entry(entry: &EntryEnum, operation_type: &str, comment: Option<String>, create_at: i64) -> Self {
        // 将Bytes转换为hex字符串
        let file_handle = entry.get_file_handle().map(hex::encode);

        // 将Vec<Tag>转换为JSON字符串
        let tags = entry.get_tags().and_then(|tags| serde_json::to_string(tags).ok());

        let storage_type = match entry {
            EntryEnum::NAS(_) => "nas",
            EntryEnum::S3(_) => "s3",
        };

        Self {
            operation_type: operation_type.to_string(),
            name: entry.get_name().to_string(),
            relative_path: entry.get_relative_path().to_string_lossy().into_owned(),
            size: entry.get_size(),
            ext: entry.get_extension().map(str::to_string),
            ctime: entry.get_ctime(),
            mtime: entry.get_mtime(),
            atime: entry.get_atime(),
            mode: entry.get_mode(),
            storage_type: storage_type.to_string(),
            is_symlink: entry.get_is_symlink(),
            is_dir: entry.get_is_dir(),
            is_regular_file: entry.get_is_regular_file(),
            hard_links: entry.get_hard_links().unwrap_or(1),
            current_state: 0, // 默认值，增量记录不需要current_state
            uid: entry.get_uid(),
            gid: entry.get_gid(),
            ino: entry.get_ino(),
            file_handle,
            version_id: entry.get_version_id().unwrap_or_default().to_string(),
            create_at,
            comment,
            tags,
            version_count: entry.get_version_count(),
        }
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
    /// 从 `EntryEnum` 和 tar 路径创建 `TarManifestRecord`
    pub fn from_entry(entry: &EntryEnum, tar_path: &str) -> Self {
        let storage_type = match entry {
            EntryEnum::NAS(_) => "nas",
            EntryEnum::S3(_) => "s3",
        };
        let tags = entry.get_tags().and_then(|tags| serde_json::to_string(tags).ok());

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
    /// - `records`: `EntryEnum` 切片
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_base_record(&self, records: &[Arc<EntryEnum>]) -> Result<()>;

    /// 更新单个主表记录
    ///
    /// # 参数
    /// - `record`: `EntryEnum` 引用
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn update_base_record(&self, record: &Arc<EntryEnum>) -> Result<()>;

    /// 批量删除基础表记录
    async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()>;

    /// 批量插入临时表记录
    ///
    /// # 参数
    /// - `records`: `EntryEnum` 切片
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_temp_record(&self, records: &[Arc<EntryEnum>]) -> Result<()>;

    /// 批量插入增量记录
    ///
    /// # 参数
    /// - `records`: `StorageEntryMessage` 切片（来自 `storage_v2`）
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn batch_insert_incremental_record(&self, records: &[StorageEntryMessage]) -> Result<()>;

    /// 切换扫描状态
    /// 在增量扫描时使用，用于标记新旧数据
    ///
    /// # 返回值
    /// - 成功返回Ok(())
    /// - 失败返回错误信息
    async fn switch_scan_state(&self) -> Result<()>;

    /// 查询在最新扫描中新增的文件
    /// 即在临时表中存在但在主表中不存在的文件
    ///
    /// # 返回值
    /// - 成功返回包含 `EntryEnum` 迭代器的 `Ok`
    /// - 失败返回错误信息
    async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = EntryEnum> + Send>>;

    /// 查询在最新扫描中发生变更的文件
    /// 区分三种变更：
    /// - `ChangeKind::DataOnly`：size 或 mtime 变了（内容变了），mode/uid/gid 未变
    /// - `ChangeKind::MetadataOnly`：size + mtime 未变，但 mode/uid/gid 至少一项变了（chmod/chown）
    /// - `ChangeKind::Both`：内容和属性都变了
    ///
    /// # 返回值
    /// - 成功返回 `(EntryEnum, ChangeKind)` 迭代器
    /// - 失败返回错误信息
    async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (EntryEnum, ChangeKind)> + Send>>;

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
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>, tx: mpsc::Sender<EntryEnum>,
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
