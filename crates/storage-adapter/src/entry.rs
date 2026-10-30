//! TerraSync 数据库及 wire 使用的数据投影；不承担存储 IO。
// 数据结构源自 data-mover d76ff5c（Apache-2.0）。
use bytes::Bytes;
use data_mover::model::AclMetadata;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};
#[must_use]
pub fn datetime_to_string(time: i64) -> String {
    let magnitude = std::time::Duration::from_nanos(time.unsigned_abs());
    let system_time = if time.is_negative() {
        std::time::SystemTime::UNIX_EPOCH.checked_sub(magnitude)
    } else {
        std::time::SystemTime::UNIX_EPOCH.checked_add(magnitude)
    }
    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    let datetime: chrono::DateTime<chrono::Utc> = system_time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
}

/// 数据块结构体 - 用于表示文件传输过程中的数据片段
///
/// 该结构体封装了文件数据的一个数据块及其在原始文件中的偏移量，
/// 主要用于在文件复制过程中进行数据分块传输和处理。
#[derive(Debug, Serialize, Deserialize)]
pub struct DataChunk {
    /// 文件中的偏移量，表示该数据块在原文件中的起始位置
    pub offset: u64,
    /// 数据块的实际内容
    pub data: Bytes,
}

/// 字节级断点续传的进度回调：参数 `(offset, len)` 表示 `[offset, offset+len)`
/// 已由目标端确认写入；不代表 crash durability。回调内不得阻塞写管道。
pub type CommitCallback = std::sync::Arc<dyn Fn(u64, u64) + Send + Sync>;

/// 旧产品进度记录。新引擎忽略这里的路径和区间，独立发现目标端恢复状态。
pub struct ResumeContext {
    /// 兼容保留的旧 `.part` 路径，不用于驱动新存储写入。
    pub part_relative_path: std::path::PathBuf,
    /// 兼容保留的本地缺失区间，不作为目标端恢复权威。
    pub missing_intervals: Vec<(u64, u64)>,
    /// 每个 chunk 确认落盘后触发的进度回调
    pub on_committed: CommitCallback,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryEnum {
    NAS(NASEntry),
    S3(S3Entry),
    HDFS(HDFSEntry),
}

impl EntryEnum {
    pub fn get_size(&self) -> u64 {
        match self {
            EntryEnum::NAS(entry) => entry.size,
            EntryEnum::S3(entry) => entry.size,
            EntryEnum::HDFS(entry) => entry.size,
        }
    }

    pub fn get_relative_path(&self) -> &Path {
        match self {
            EntryEnum::NAS(entry) => entry.relative_path.as_path(),
            EntryEnum::S3(entry) => Path::new(&entry.relative_path),
            EntryEnum::HDFS(entry) => entry.relative_path.as_path(),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            EntryEnum::NAS(entry) => &entry.name,
            EntryEnum::S3(entry) => &entry.name,
            EntryEnum::HDFS(entry) => &entry.name,
        }
    }

    pub fn get_is_dir(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => entry.is_dir,
            EntryEnum::S3(entry) => entry.is_dir,
            EntryEnum::HDFS(entry) => entry.is_dir,
        }
    }

    pub fn get_is_symlink(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => entry.is_symlink,
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => false,
        }
    }

    pub fn get_is_regular_file(&self) -> bool {
        match self {
            EntryEnum::NAS(entry) => !entry.is_dir && !entry.is_symlink,
            EntryEnum::S3(entry) => !entry.is_dir,
            EntryEnum::HDFS(entry) => !entry.is_dir,
        }
    }

    pub fn get_mtime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.mtime,
            EntryEnum::S3(entry) => entry.mtime,
            EntryEnum::HDFS(entry) => entry.mtime,
        }
    }

    pub fn get_atime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.atime,
            EntryEnum::S3(entry) => entry.mtime,
            EntryEnum::HDFS(entry) => entry.atime,
        }
    }

    pub fn get_ctime(&self) -> i64 {
        match self {
            EntryEnum::NAS(entry) => entry.ctime,
            EntryEnum::S3(entry) => entry.mtime,
            EntryEnum::HDFS(entry) => entry.mtime,
        }
    }

    pub fn get_mode(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.mode,
            EntryEnum::S3(_) => None,
            EntryEnum::HDFS(entry) => entry.mode,
        }
    }

    pub fn get_extension(&self) -> Option<&str> {
        match self {
            EntryEnum::NAS(entry) => entry.extension.as_deref(),
            EntryEnum::S3(entry) => entry.extension.as_deref(),
            EntryEnum::HDFS(entry) => entry.extension.as_deref(),
        }
    }

    pub fn get_hard_links(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.hard_links,
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => None,
        }
    }

    pub fn get_uid(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.uid,
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => None,
        }
    }

    pub fn get_gid(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(entry) => entry.gid,
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => None,
        }
    }

    pub fn get_ino(&self) -> Option<u64> {
        match self {
            EntryEnum::NAS(entry) => entry.ino,
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => None,
        }
    }

    pub fn get_file_handle(&self) -> Option<&Bytes> {
        match self {
            EntryEnum::NAS(entry) => entry.file_handle.as_ref(),
            EntryEnum::S3(_) | EntryEnum::HDFS(_) => None,
        }
    }

    pub fn get_version_id(&self) -> Option<&str> {
        match self {
            EntryEnum::NAS(_) | EntryEnum::HDFS(_) => None,
            EntryEnum::S3(entry) => entry.version_id.as_deref(),
        }
    }

    pub fn get_tags(&self) -> Option<&Vec<Tag>> {
        match self {
            EntryEnum::NAS(_) | EntryEnum::HDFS(_) => None,
            EntryEnum::S3(entry) => entry.tags.as_ref(),
        }
    }

    pub fn get_version_count(&self) -> Option<u32> {
        match self {
            EntryEnum::NAS(_) | EntryEnum::HDFS(_) => None,
            EntryEnum::S3(entry) => entry.version_count,
        }
    }

    pub fn get_is_latest(&self) -> bool {
        match self {
            EntryEnum::NAS(_) | EntryEnum::HDFS(_) => true,
            EntryEnum::S3(entry) => entry.is_latest,
        }
    }

    pub fn get_is_delete_marker(&self) -> bool {
        match self {
            EntryEnum::NAS(_) | EntryEnum::HDFS(_) => false,
            EntryEnum::S3(entry) => entry.is_delete_marker,
        }
    }

    pub fn set_version_count(&mut self, count: u32) {
        if let EntryEnum::S3(entry) = self {
            entry.version_count = Some(count);
        }
    }
}

/// HDFS 原生元数据，不附加 NAS 或对象存储语义。
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HDFSEntry {
    pub name: String,
    pub relative_path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
    /// 自 Unix epoch 起经过的纳秒数。
    pub mtime: i64,
    /// 自 Unix epoch 起经过的纳秒数。
    pub atime: i64,
    pub mode: Option<u32>,
    pub owner: String,
    pub group: String,
    pub replication: Option<u32>,
    pub block_size: Option<u64>,
    pub extension: Option<String>,
}

/// 失败事件类型，用于区分不同场景下的错误
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorEvent {
    /// 扫描阶段失败（元数据获取、目录遍历、路径处理等）
    Scan,
    /// 文件内容复制失败（含 mkdir、目录结构建立）
    Copy,
    /// ACL/权限复制失败
    CopyAcl,
    /// xattr 复制失败
    CopyXattr,
    /// 删除操作失败
    Delete,
    /// 重命名操作失败
    Rename,
    /// 软链接操作失败（创建/删除 symlink）
    SymlinkOp,
    /// 打包失败
    Pack,
    /// 独立完整性检查失败
    IntegrityCheck,
}

impl fmt::Display for ErrorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorEvent::Scan => write!(f, "scan"),
            ErrorEvent::Copy => write!(f, "copy"),
            ErrorEvent::CopyAcl => write!(f, "copy_acl"),
            ErrorEvent::CopyXattr => write!(f, "copy_xattr"),
            ErrorEvent::Delete => write!(f, "delete"),
            ErrorEvent::Rename => write!(f, "rename"),
            ErrorEvent::SymlinkOp => write!(f, "symlink_op"),
            ErrorEvent::Pack => write!(f, "pack"),
            ErrorEvent::IntegrityCheck => write!(f, "integrity_check"),
        }
    }
}

/// 变更的维度：用于区分内容变更、元数据变更、或两者同时变更
///
/// - `DataOnly`：size 或 mtime 不同（内容变了），属性 mode/uid/gid 未变 → 需 `copy_file` + `set_metadata`
/// - `MetadataOnly`：size 和 mtime 相同，但 mode/uid/gid 至少一项不同（chmod/chown）→ 只需 `set_metadata，跳过` `copy_file`
/// - `Both`：内容和属性都变了 → 需 `copy_file` + `set_metadata`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeKind {
    DataOnly,
    MetadataOnly,
    Both,
}

impl ChangeKind {
    /// 对应数据库 state 列的取值，与 `db/src/traits.rs` 中 `Changed` 分支保持一致
    #[must_use]
    pub fn as_state_str(&self) -> &'static str {
        match self {
            ChangeKind::DataOnly => "data_changed",
            ChangeKind::MetadataOnly => "metadata_changed",
            ChangeKind::Both => "both_changed",
        }
    }

    /// 根据两个 entry 的属性差异推断变更类型；无变更时返回 `None`。
    #[must_use]
    pub fn from_entry_diff(from: &EntryEnum, to: &EntryEnum) -> Option<Self> {
        let data_changed = from.get_size() != to.get_size() || from.get_mtime() != to.get_mtime();
        let meta_changed =
            from.get_mode() != to.get_mode() || from.get_uid() != to.get_uid() || from.get_gid() != to.get_gid();
        match (data_changed, meta_changed) {
            (true, true) => Some(Self::Both),
            (true, false) => Some(Self::DataOnly),
            (false, true) => Some(Self::MetadataOnly),
            (false, false) => None,
        }
    }
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_state_str())
    }
}

/// 文件操作消息枚举，用于在扫描/同步过程中传递文件状态变化
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageEntryMessage {
    /// 文件被扫描到
    Scanned(Arc<EntryEnum>),
    /// 新增文件
    New(Arc<EntryEnum>),
    /// 文件已变更（kind 区分 data/metadata/both）
    Changed { entry: Arc<EntryEnum>, kind: ChangeKind },
    /// 文件已删除
    Deleted(Arc<EntryEnum>),
    /// 文件被重命名，(from, to)
    Renamed((Arc<EntryEnum>, Arc<EntryEnum>)),
    /// 文件完整性检查完成
    IntegrityChecked(Arc<EntryEnum>),
    /// 目录需要打包为 tar（仅在 --packaged 模式下由 walkdir 发射）
    Packaged(Arc<EntryEnum>),
    /// 打包完成后的 manifest 数据，由 sync worker 广播，DatabaseConsumer 消费写入数据库
    TarManifest {
        /// `.tar` 文件的 `relative_path`
        tar_path: String,
        /// tar 内包含的所有条目
        entries: Vec<Arc<EntryEnum>>,
    },
    /// 扫描/同步过程中遇到的 per-entry 错误
    Error {
        /// 出错的事件类型
        event: ErrorEvent,
        /// 出错的文件或目录路径
        path: PathBuf,
        /// 条目上下文；早期扫描错误尚未构造完整条目时为 `None`。
        entry: Option<Arc<EntryEnum>>,
        /// 错误原因
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NASEntry {
    /// 通用属性
    /// 文件或目录的名称
    pub name: String,
    /// 相对于扫描根目录的相对路径
    pub relative_path: PathBuf,
    /// 文件扩展名（如果有）
    pub extension: Option<String>,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小（字节），目录大小通常为0
    pub size: u64,
    /// 访问时间（纳秒时间戳）
    pub atime: i64,
    /// 创建时间（纳秒时间戳）
    pub ctime: i64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,
    /// 实际观测到的 POSIX mode；None 表示未知，Some(0) 表示真实的 000 权限。
    pub mode: Option<u32>,

    /// 是否为符号链接（Unix和NFS客户端使用）
    pub is_symlink: bool,
    /// 硬链接数（Unix和NFS客户端使用）
    pub hard_links: Option<u32>,
    /// 用户ID（Unix和NFS客户端使用）
    pub uid: Option<u32>,
    /// 组ID（Unix和NFS客户端使用）
    pub gid: Option<u32>,
    /// inode编号（Unix和NFS客户端使用）
    pub ino: Option<u64>,

    /// 文件句柄（NFS file handle / CIFS file ID 等，用于跨协议唯一标识文件）
    #[serde(skip)]
    pub file_handle: Option<Bytes>,

    /// `NFSv4` ACL（仅 `NFSv4`+ 填充）
    #[serde(skip)]
    pub acl: Option<AclMetadata>,

    /// `NFSv4` owner 字符串，如 "root@localdomain"
    pub owner: Option<String>,

    /// `NFSv4` `owner_group` 字符串
    pub owner_group: Option<String>,

    /// Extended attributes (xattr) key-value pairs（仅 `NFSv4`+ 扫描时填充）
    #[serde(skip)]
    pub xattrs: Option<Vec<(String, Vec<u8>)>>,
}

impl fmt::Display for NASEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NASEntry {{")?;
        write!(f, "name: {:?}, ", self.name)?;
        write!(f, "relative_path: {}, ", self.relative_path.display())?;
        write!(f, "extension: {:?}, ", self.extension)?;
        write!(f, "is_dir: {}, ", self.is_dir)?;
        write!(f, "size: {}, ", self.size)?;
        write!(f, "atime: {}, ", datetime_to_string(self.atime))?;
        write!(f, "ctime: {}, ", datetime_to_string(self.ctime))?;
        write!(f, "mtime: {}, ", datetime_to_string(self.mtime))?;
        write!(f, "mode: {:?}, ", self.mode)?; // 八进制格式化
        write!(f, "is_symlink: {}, ", self.is_symlink)?;
        write!(f, "hard_links: {:?}, ", self.hard_links)?;
        write!(f, "uid: {:?}, ", self.uid)?;
        write!(f, "gid: {:?}, ", self.gid)?;
        write!(f, "ino: {:?}, ", self.ino)?;
        write!(f, "file_handle: {:?}, ", self.file_handle)?;
        if let Some(ref owner) = self.owner {
            write!(f, "owner: {owner:?}, ")?;
        }
        if let Some(ref owner_group) = self.owner_group {
            write!(f, "owner_group: {owner_group:?}, ")?;
        }
        if let Some(ref acl) = self.acl {
            write!(f, "acl: {} aces, ", acl.access().map_or(0, <[u8]>::len))?;
        }
        if let Some(ref xattrs) = self.xattrs {
            write!(f, "xattrs: {} entries, ", xattrs.len())?;
        }
        write!(f, "}}")
    }
}

/// 文件标签结构，用于存储文件的元数据标签
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// 标签键
    pub key: String,
    /// 标签值
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Entry {
    /// 通用属性
    /// 文件或目录的名称
    pub name: String,
    /// 相对于扫描根目录的相对路径
    pub relative_path: String,
    /// 文件扩展名（如果有）
    pub extension: Option<String>,
    /// 文件大小（字节），目录大小通常为0
    pub size: u64,
    /// 修改时间（纳秒时间戳）
    pub mtime: i64,

    /// 对象特有属性
    /// 文件标签（S3使用）
    pub tags: Option<Vec<Tag>>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: Option<String>,
    /// 是否为最新版本（S3多版本使用）
    pub is_latest: bool,
    /// 是否为删除标记（S3多版本使用）
    pub is_delete_marker: bool,
    /// 对象版本数量（S3多版本使用）
    pub version_count: Option<u32>,
    /// 是否为目录（S3 prefix）
    pub is_dir: bool,
}

impl fmt::Display for S3Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3Entry {{")?;
        write!(f, "name: {:?}, ", self.name)?;
        write!(f, "relative_path: {:?}, ", self.relative_path)?;
        write!(f, "extension: {:?}, ", self.extension)?;
        write!(f, "size: {}, ", self.size)?;
        write!(f, "mtime: {}, ", datetime_to_string(self.mtime))?;
        write!(f, "tags: {:?}, ", self.tags)?;
        write!(f, "version_id: {:?}, ", self.version_id)?;
        write!(f, "is_latest: {}, ", self.is_latest)?;
        write!(f, "is_delete_marker: {}, ", self.is_delete_marker)?;
        write!(f, "version_count: {:?}, ", self.version_count)?;
        write!(f, "is_dir: {}", self.is_dir)?;
        write!(f, "}}")
    }
}
