//! 扫描逻辑统计：按目录替换贡献，回调与检查点共用同一快照。
use data_mover::{
    model::{BackendKind, MetadataFamily, ObservationSupport},
    traversal::{DirectoryStatistics, EntryKindCounts, ListingOutcome},
};
use serde::{Deserialize, Serialize};
use std::fmt;
use storage_adapter::StorageEnum;

/// 每个目录的直属孩子贡献；历史版本与逻辑 entry 分开计数。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanCounts {
    pub scanned: EntryKindCounts,
    pub filtered: EntryKindCounts,
    pub retained: EntryKindCounts,
    pub failed: EntryKindCounts,
    pub vanished: EntryKindCounts,
    pub pruned_directories: u64,
    pub truncated_directories: u64,
    pub regular_file_bytes: u64,
    pub unknown_file_sizes: u64,
    pub versions: u64,
    pub delete_markers: u64,
    pub all_versions_bytes: u64,
    pub completed_listings: u64,
    pub interrupted_listings: u64,
    pub unreadable_listings: u64,
    pub vanished_listings: u64,
}

impl ScanCounts {
    pub(crate) fn from_directory(value: &DirectoryStatistics) -> Self {
        Self {
            scanned: value.scanned,
            filtered: value.filtered,
            retained: value.retained,
            failed: value.failed,
            vanished: value.vanished,
            pruned_directories: value.pruned_dirs,
            truncated_directories: value.truncated_dirs,
            regular_file_bytes: value.regular_file_bytes,
            unknown_file_sizes: value.unknown_file_sizes,
            versions: value.versions,
            delete_markers: value.delete_markers,
            all_versions_bytes: value.all_versions_bytes,
            completed_listings: u64::from(value.outcome == Some(ListingOutcome::Complete)),
            interrupted_listings: u64::from(value.outcome == Some(ListingOutcome::Interrupted)),
            unreadable_listings: u64::from(value.outcome == Some(ListingOutcome::Unreadable)),
            vanished_listings: u64::from(value.vanished_listing),
        }
    }

    /// 去掉该目录旧贡献再替换；重放本身不能增加累计计数。
    pub(crate) fn replace(&mut self, old: &Self, new: &Self) {
        macro_rules! kinds { ($($field:ident),*) => { $(self.$field = self.$field.saturating_sub(old.$field).saturating_add(new.$field);)* }; }
        kinds!(scanned, filtered, retained, failed, vanished);
        macro_rules! scalars { ($($field:ident),*) => { $(self.$field = self.$field.saturating_sub(old.$field).saturating_add(new.$field);)* }; }
        scalars!(
            pruned_directories,
            truncated_directories,
            regular_file_bytes,
            unknown_file_sizes,
            versions,
            delete_markers,
            all_versions_bytes,
            completed_listings,
            interrupted_listings,
            unreadable_listings,
            vanished_listings
        );
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticSupport {
    Supported,
    Unsupported,
    NotApplicable,
    Unknown,
}

impl From<ObservationSupport> for StatisticSupport {
    fn from(value: ObservationSupport) -> Self {
        match value {
            ObservationSupport::Supported => Self::Supported,
            ObservationSupport::Unsupported => Self::Unsupported,
            ObservationSupport::NotApplicable => Self::NotApplicable,
            ObservationSupport::Unknown => Self::Unknown,
        }
    }
}

/// 只读取连接已经声明的能力；不为统计增加逐文件 RPC。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanCapabilities {
    pub backend: String,
    pub symlinks: StatisticSupport,
    pub special_files: StatisticSupport,
    pub directory_objects: bool,
    pub metadata: Vec<MetadataStatisticCapability>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetadataStatisticCapability {
    pub family: String,
    pub support: StatisticSupport,
    pub requested: bool,
}

impl ScanCapabilities {
    pub(crate) fn from_storage(storage: &StorageEnum, tags: bool) -> Self {
        let roles = storage.storage();
        let namespace = roles.namespace().ok().map(|role| role.capabilities());
        let metadata = roles.metadata().ok().map(|role| role.observation_capabilities());
        Self {
            backend: roles.kind().as_str().into(),
            symlinks: if namespace.is_some_and(|value| value.symlinks) {
                StatisticSupport::Supported
            } else if roles.kind() == BackendKind::S3 {
                StatisticSupport::NotApplicable
            } else {
                StatisticSupport::Unsupported
            },
            special_files: match roles.kind() {
                BackendKind::Local | BackendKind::Nfs => StatisticSupport::Supported,
                BackendKind::S3 => StatisticSupport::NotApplicable,
                _ => StatisticSupport::Unsupported,
            },
            directory_objects: namespace.is_some_and(|value| value.directory_objects),
            metadata: MetadataFamily::ALL
                .into_iter()
                .map(|family| MetadataStatisticCapability {
                    family: match family {
                        MetadataFamily::Acl => "acl",
                        MetadataFamily::Xattrs => "xattrs",
                        MetadataFamily::Tags => "tags",
                        MetadataFamily::OwnershipMode => "ownership_mode",
                        MetadataFamily::Timestamps => "timestamps",
                    }
                    .into(),
                    support: metadata
                        .map_or(ObservationSupport::Unsupported, |value| value.support(family))
                        .into(),
                    requested: match family {
                        MetadataFamily::Tags => tags,
                        MetadataFamily::Timestamps | MetadataFamily::OwnershipMode => true,
                        _ => false,
                    },
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanSnapshot {
    pub scanned_entries: u64,
    pub filtered_entries: u64,
    pub retained_entries: u64,
    pub counts: ScanCounts,
    pub complete: bool,
    #[serde(default)]
    pub restarts: u64,
    #[serde(default)]
    pub distributions_complete: bool,
    #[serde(default)]
    pub distributions_reconciled: bool,
    pub capabilities: ScanCapabilities,
}

impl ScanSnapshot {
    pub(crate) fn valid_checkpoint(&self, observed: u64) -> bool {
        let accounted = self
            .counts
            .filtered
            .saturating_add(self.counts.retained)
            .saturating_add(self.counts.failed)
            .saturating_add(self.counts.vanished);
        self.counts.scanned == accounted
            && self.scanned_entries == self.counts.scanned.total()
            && self.scanned_entries == observed
            && self.filtered_entries == self.counts.filtered.total()
            && self.retained_entries == self.counts.retained.total()
    }
    pub(crate) fn new(capabilities: ScanCapabilities) -> Self {
        Self {
            scanned_entries: 0,
            filtered_entries: 0,
            retained_entries: 0,
            counts: ScanCounts::default(),
            complete: false,
            restarts: 0,
            distributions_complete: true,
            distributions_reconciled: false,
            capabilities,
        }
    }

    pub(crate) fn replace(&mut self, old: &ScanCounts, new: &ScanCounts) {
        self.counts.replace(old, new);
        self.scanned_entries = self.counts.scanned.total();
        self.filtered_entries = self.counts.filtered.total();
        self.retained_entries = self.counts.retained.total();
    }
}

impl fmt::Display for ScanSnapshot {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            output,
            "Scanned Entries: {}\nFiltered Entries: {}\nRetained Entries: {}",
            self.scanned_entries, self.filtered_entries, self.retained_entries
        )?;
        writeln!(
            output,
            "{:<20} {:>12} {:>12} {:>12}",
            "Entry type", "Scanned", "Filtered", "Retained"
        )?;
        macro_rules! rows { ($(($name:literal,$field:ident)),*) => { $(writeln!(output,"{:<20} {:>12} {:>12} {:>12}",$name,self.counts.scanned.$field,self.counts.filtered.$field,self.counts.retained.$field)?;)* }; }
        rows!(
            ("Directories", directories),
            ("Regular files", regular_files),
            ("Symlinks", symlinks),
            ("Block devices", block_devices),
            ("Character devices", character_devices),
            ("FIFOs", fifos),
            ("Sockets", sockets),
            ("Unknown", unknown)
        );
        writeln!(
            output,
            "Regular file bytes: {} (unknown sizes: {})",
            self.counts.regular_file_bytes, self.counts.unknown_file_sizes
        )?;
        writeln!(
            output,
            "Versions: {}; delete markers: {}; all version bytes: {}",
            self.counts.versions, self.counts.delete_markers, self.counts.all_versions_bytes
        )?;
        write!(
            output,
            "Complete: {}; symlink support: {:?}; special-file support: {:?}",
            self.complete, self.capabilities.symlinks, self.capabilities.special_files
        )
    }
}
