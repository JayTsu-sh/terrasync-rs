//! TerraSync 数据库及 wire 使用的数据投影；不承担存储 IO。
// 数据结构源自 data-mover d76ff5c（Apache-2.0）。
use crate::ObservedEntryExt;
use crate::observed_scan::ObservedItem;
use bytes::Bytes;
use data_mover::model::ObservedEntry;
use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf, sync::Arc};
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
    pub fn from_entry_diff(from: &ObservedEntry, to: &ObservedEntry) -> Option<Self> {
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

/// 数据库分类与复制完成共用的变更语义；条目和版本选择由 ObservedItem 携带。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryChange {
    New,
    Changed(ChangeKind),
}
impl EntryChange {
    pub fn message(self, item: ObservedItem) -> StorageEntryMessage {
        match self {
            Self::New => StorageEntryMessage::New(item),
            Self::Changed(kind) => StorageEntryMessage::Changed { entry: item, kind },
        }
    }
}

/// 文件操作消息枚举，用于在扫描/同步过程中传递文件状态变化
#[derive(Debug, Clone)]
pub enum StorageEntryMessage {
    /// 文件被扫描到
    Scanned(Arc<ObservedEntry>),
    /// 新增文件
    New(ObservedItem),
    /// 文件已变更（kind 区分 data/metadata/both）
    Changed { entry: ObservedItem, kind: ChangeKind },
    /// 文件已删除
    Deleted(Arc<ObservedEntry>),
    /// 文件被重命名，(from, to)
    Renamed((Arc<ObservedEntry>, Arc<ObservedEntry>)),
    /// 文件完整性检查完成
    IntegrityChecked(Arc<ObservedEntry>),
    /// 目录需要打包为 tar（仅在 --packaged 模式下由 walkdir 发射）
    Packaged(Arc<ObservedEntry>),
    /// 打包完成后的 manifest 数据，由 sync worker 广播，DatabaseConsumer 消费写入数据库
    TarManifest {
        /// `.tar` 文件的 `relative_path`
        tar_path: String,
        /// tar 内包含的所有条目
        entries: Vec<Arc<ObservedEntry>>,
    },
    /// 扫描/同步过程中遇到的 per-entry 错误
    Error {
        /// 出错的事件类型
        event: ErrorEvent,
        /// 出错的文件或目录路径
        path: PathBuf,
        /// 条目上下文；早期扫描错误尚未构造完整条目时为 `None`。
        entry: Option<Arc<ObservedEntry>>,
        /// 错误原因
        reason: String,
    },
}

/// 文件标签结构，用于存储文件的元数据标签
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// 标签键
    pub key: String,
    /// 标签值
    pub value: String,
}
