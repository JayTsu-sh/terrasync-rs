//! 任务分类与观察事件；不承担数据库、网络和存储 I/O。
use crate::{ObservedEntryExt, observation::ObservedItem};
use data_mover_model::ObservedEntry;
use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf, sync::Arc};

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
    /// 已确认的源目录标记对象消失，保留目录及后代。
    DirectoryMarkerRemoved,
    DataOnly,
    MetadataOnly,
    Both,
}

impl ChangeKind {
    /// 对应数据库 state 列的取值，与 `db/src/traits.rs` 中 `Changed` 分支保持一致
    #[must_use]
    pub fn as_state_str(&self) -> &'static str {
        match self {
            ChangeKind::DirectoryMarkerRemoved => "directory_marker_removed",
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
