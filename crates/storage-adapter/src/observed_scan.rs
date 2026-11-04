//! 扫描保留原生观察对象；逐版本视图只用于统计、比较和数据库行投影。
use crate::{ChangeKind, EntryEnum};
use data_mover::model::{BackendKind, EntryKind, MetadataObservations, ObservedEntry, StorageTimestamp, VersionRecord};
use std::{
    borrow::Cow,
    path::Path,
    sync::{Arc, OnceLock},
};

/// 统计所需的公共事实，兼容迁移旧消息和原生扫描，均不转换条目。
pub trait EntryStatistics {
    fn get_size(&self) -> u64;
    fn get_name(&self) -> &str;
    fn get_relative_path(&self) -> &Path;
    fn get_extension(&self) -> Option<&str>;
    fn get_is_dir(&self) -> bool;
    fn get_is_symlink(&self) -> bool;
    fn get_is_regular_file(&self) -> bool;
    fn get_mtime(&self) -> i64;
}
impl EntryStatistics for EntryEnum {
    fn get_size(&self) -> u64 {
        self.get_size()
    }
    fn get_name(&self) -> &str {
        self.get_name()
    }
    fn get_relative_path(&self) -> &Path {
        self.get_relative_path()
    }
    fn get_extension(&self) -> Option<&str> {
        self.get_extension()
    }
    fn get_is_dir(&self) -> bool {
        self.get_is_dir()
    }
    fn get_is_symlink(&self) -> bool {
        self.get_is_symlink()
    }
    fn get_is_regular_file(&self) -> bool {
        self.get_is_regular_file()
    }
    fn get_mtime(&self) -> i64 {
        self.get_mtime()
    }
}

/// 增量事件选择某个历史版本；多个事件共享原始 ObservedEntry。
#[derive(Clone, Debug)]
pub struct ObservedItem {
    pub entry: Arc<ObservedEntry>,
    pub version_index: Option<usize>,
}
impl ObservedItem {
    pub fn view(&self) -> ObservedView<'_> {
        ObservedView {
            entry: &self.entry,
            version_index: self.version_index,
        }
    }
    pub fn all(entry: &Arc<ObservedEntry>) -> impl Iterator<Item = Self> + '_ {
        let count = entry.history().map_or(1, |history| history.records().len());
        (0..count).map(move |index| Self {
            entry: Arc::clone(entry),
            version_index: entry.history().map(|_| index),
        })
    }
}

#[derive(Clone, Copy)]
pub struct ObservedView<'a> {
    pub entry: &'a ObservedEntry,
    pub version_index: Option<usize>,
}
impl<'a> ObservedView<'a> {
    pub fn all(entry: &'a ObservedEntry) -> impl Iterator<Item = Self> {
        let count = entry.history().map_or(1, |history| history.records().len());
        (0..count).map(move |index| Self {
            entry,
            version_index: entry.history().map(|_| index),
        })
    }
    pub fn version(&self) -> Option<&'a VersionRecord> {
        self.version_index
            .and_then(|index| self.entry.history()?.records().get(index))
    }
    pub fn version_id(&self) -> &'a str {
        self.version()
            .map_or("", |record| record.version_id().unwrap_or("null"))
    }
    pub fn path(&self) -> String {
        self.sql_path().into_owned()
    }
    /// 普通路径借用原生观察；历史目录对象的 SQL 键保留尾部斜线。
    pub fn sql_path(&self) -> Cow<'a, str> {
        if self.version().is_some() && self.entry.kind() == EntryKind::Directory {
            Cow::Owned(format!("{}/", self.entry.path().as_str()))
        } else {
            Cow::Borrowed(self.entry.path().as_str())
        }
    }
    pub fn file_handle(&self) -> Option<String> {
        if self.entry.backend_kind() == BackendKind::Local {
            return None;
        }
        self.entry.source_identity().stable_bytes().map(|_| {
            let mut value = String::with_capacity(72);
            value.push_str("54534934");
            for byte in self.entry.identity_key().as_bytes() {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                value.push(HEX[(byte >> 4) as usize] as char);
                value.push(HEX[(byte & 15) as usize] as char);
            }
            value
        })
    }
    pub fn metadata(&self) -> &'a MetadataObservations {
        if let Some(version) = self.version() {
            static EMPTY: OnceLock<MetadataObservations> = OnceLock::new();
            version
                .metadata()
                .unwrap_or_else(|| EMPTY.get_or_init(MetadataObservations::default))
        } else {
            self.entry.metadata()
        }
    }
}
impl EntryStatistics for ObservedView<'_> {
    fn get_size(&self) -> u64 {
        self.version().map_or_else(
            || self.entry.size().unwrap_or_default(),
            |record| record.size().unwrap_or_default(),
        )
    }
    fn get_name(&self) -> &str {
        self.get_relative_path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
    }
    fn get_relative_path(&self) -> &Path {
        Path::new(self.entry.path().as_str())
    }
    fn get_extension(&self) -> Option<&str> {
        self.get_relative_path().extension().and_then(|ext| ext.to_str())
    }
    fn get_is_dir(&self) -> bool {
        self.entry.kind() == EntryKind::Directory
    }
    fn get_is_symlink(&self) -> bool {
        self.entry.kind() == EntryKind::Symlink
    }
    fn get_is_regular_file(&self) -> bool {
        self.entry.kind() == EntryKind::File
    }
    fn get_mtime(&self) -> i64 {
        let modified = self.version().map_or_else(
            || {
                self.entry
                    .metadata()
                    .timestamps()
                    .value()
                    .and_then(|times| times.modified)
                    .or(self.entry.modified())
            },
            VersionRecord::modified,
        );
        timestamp_nanos(modified)
    }
}
pub fn timestamp_nanos(time: Option<StorageTimestamp>) -> i64 {
    time.map_or(0, |value| {
        value.unix_nanos().clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    })
}

#[derive(Clone, Debug)]
pub enum ObservedScanEvent {
    New(ObservedItem),
    /// 带身份的路径未确认匹配；整轮结束后再区分新增、重命名和新增硬链接。
    Candidate(ObservedItem),
    Changed {
        item: ObservedItem,
        kind: ChangeKind,
    },
    Deleted(ObservedItem),
    Renamed {
        from: ObservedItem,
        to: ObservedItem,
    },
}
impl ObservedScanEvent {
    pub fn item(&self) -> &ObservedItem {
        match self {
            Self::Candidate(item) | Self::New(item) | Self::Changed { item, .. } | Self::Deleted(item) => item,
            Self::Renamed { to, .. } => to,
        }
    }
    pub fn operation(&self) -> &'static str {
        match self {
            Self::New(_) => "new",
            Self::Candidate(_) => "candidate_new",
            Self::Changed { kind, .. } => kind.as_state_str(),
            Self::Deleted(_) => "deleted",
            Self::Renamed { .. } => "rename",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod path_tests {
    use super::*;
    use data_mover::model::{
        BackendIdentity, HistoryOrder, IdentityStrength, ObjectHistory, RecordKind, SourceIdentity, StoragePath,
    };

    #[test]
    fn borrowed_projection_keeps_regular_and_versioned_directory_sql_keys() {
        let backend = BackendIdentity::new(BackendKind::S3, "sql-path-fixture").unwrap();
        let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, b"folder").unwrap();
        let history = ObjectHistory::whole(
            vec![
                VersionRecord::from_listing(
                    "v1",
                    true,
                    RecordKind::Object {
                        size: 0,
                        etag: "etag".into(),
                        modified: None,
                    },
                    identity.clone(),
                )
                .unwrap(),
            ],
            HistoryOrder::Server,
        )
        .unwrap();
        let directory = ObservedEntry::new(
            StoragePath::new("folder").unwrap(),
            EntryKind::Directory,
            None,
            None,
            identity,
        )
        .unwrap()
        .with_history(Some(Box::new(history)))
        .unwrap();
        let current = ObservedView {
            entry: &directory,
            version_index: None,
        };
        let historical = ObservedView {
            entry: &directory,
            version_index: Some(0),
        };
        assert_eq!(current.sql_path(), "folder");
        assert_eq!(current.path(), "folder");
        assert_eq!(historical.sql_path(), "folder/");
        assert_eq!(historical.path(), "folder/");
        assert_eq!(historical.version_id(), "v1");
    }
}
