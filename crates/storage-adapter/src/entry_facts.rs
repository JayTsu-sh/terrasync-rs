//! 原生观察的借用查询，不保存重复的条目字段，也不展开历史。
use crate::observed_scan::ObservedView;
use data_mover::model::{BackendKind, EntryKind, ObjectTag, ObservedEntry};
use std::path::Path;

use crate::observed_scan::timestamp_nanos;

pub trait ObservedEntryExt {
    fn get_size(&self) -> u64;
    fn get_relative_path(&self) -> &Path;
    fn get_name(&self) -> &str;
    fn get_extension(&self) -> Option<&str>;
    fn get_is_dir(&self) -> bool;
    fn get_is_symlink(&self) -> bool;
    fn get_is_regular_file(&self) -> bool;
    fn get_mtime(&self) -> i64;
    fn get_atime(&self) -> i64;
    fn get_ctime(&self) -> i64;
    fn get_mode(&self) -> Option<u32>;
    fn get_uid(&self) -> Option<u32>;
    fn get_gid(&self) -> Option<u32>;
    fn get_tags(&self) -> Option<&[ObjectTag]>;
    fn get_file_handle(&self) -> Option<[u8; 36]>;
    fn get_version_id(&self) -> Option<&str>;
    fn get_is_latest(&self) -> bool;
    fn get_is_delete_marker(&self) -> bool;
}

impl ObservedEntryExt for ObservedEntry {
    fn get_size(&self) -> u64 {
        self.size().unwrap_or_default()
    }
    fn get_relative_path(&self) -> &Path {
        Path::new(self.path().as_str())
    }
    fn get_name(&self) -> &str {
        self.get_relative_path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
    }
    fn get_extension(&self) -> Option<&str> {
        self.get_relative_path().extension().and_then(|ext| ext.to_str())
    }
    fn get_is_dir(&self) -> bool {
        self.kind() == EntryKind::Directory
    }
    fn get_is_symlink(&self) -> bool {
        self.kind() == EntryKind::Symlink
    }
    fn get_is_regular_file(&self) -> bool {
        self.kind() == EntryKind::File
    }
    fn get_mtime(&self) -> i64 {
        timestamp_nanos(
            self.metadata()
                .timestamps()
                .value()
                .and_then(|times| times.modified)
                .or(self.modified()),
        )
    }
    fn get_atime(&self) -> i64 {
        timestamp_nanos(self.metadata().timestamps().value().and_then(|times| times.accessed))
    }
    fn get_ctime(&self) -> i64 {
        timestamp_nanos(self.metadata().timestamps().value().and_then(|times| times.created))
    }
    fn get_mode(&self) -> Option<u32> {
        self.metadata()
            .ownership_mode()
            .value()
            .map(|value| value.mode)
            .or_else(|| self.permissions()?.mode)
    }
    fn get_uid(&self) -> Option<u32> {
        ObservedView {
            entry: self,
            version_index: None,
        }
        .uid()
    }
    fn get_gid(&self) -> Option<u32> {
        ObservedView {
            entry: self,
            version_index: None,
        }
        .gid()
    }
    fn get_tags(&self) -> Option<&[ObjectTag]> {
        self.metadata().tags().value().map(Vec::as_slice)
    }
    fn get_file_handle(&self) -> Option<[u8; 36]> {
        if self.backend_kind() == BackendKind::Local || self.source_identity().stable_bytes().is_none() {
            return None;
        }
        let mut key = [0; 36];
        key[..4].copy_from_slice(b"TSI4");
        key[4..].copy_from_slice(self.identity_key().as_bytes());
        Some(key)
    }
    fn get_version_id(&self) -> Option<&str> {
        let history = self.history()?;
        let record = if history.records().len() == 1 {
            history.records().first()?
        } else {
            history.latest()?
        };
        Some(record.version_id().unwrap_or("null"))
    }
    fn get_is_latest(&self) -> bool {
        self.history()
            .and_then(|history| history.records().first())
            .is_none_or(|record| record.is_latest())
    }
    fn get_is_delete_marker(&self) -> bool {
        self.history()
            .and_then(|history| history.records().first())
            .is_some_and(|record| record.is_delete_marker())
    }
}

impl ObservedEntryExt for crate::observed_scan::ObservedItem {
    fn get_size(&self) -> u64 {
        crate::observed_scan::EntryStatistics::get_size(&self.view())
    }
    fn get_relative_path(&self) -> &Path {
        Path::new(self.entry.path().as_str())
    }
    fn get_name(&self) -> &str {
        self.get_relative_path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
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
        crate::observed_scan::EntryStatistics::get_mtime(&self.view())
    }
    fn get_atime(&self) -> i64 {
        timestamp_nanos(
            self.view()
                .metadata()
                .timestamps()
                .value()
                .and_then(|times| times.accessed),
        )
    }
    fn get_ctime(&self) -> i64 {
        timestamp_nanos(
            self.view()
                .metadata()
                .timestamps()
                .value()
                .and_then(|times| times.created),
        )
    }
    fn get_mode(&self) -> Option<u32> {
        self.view().mode()
    }
    fn get_uid(&self) -> Option<u32> {
        self.view().uid()
    }
    fn get_gid(&self) -> Option<u32> {
        self.view().gid()
    }
    fn get_tags(&self) -> Option<&[ObjectTag]> {
        self.view().metadata().tags().value().map(Vec::as_slice)
    }
    fn get_file_handle(&self) -> Option<[u8; 36]> {
        self.entry.get_file_handle()
    }
    fn get_version_id(&self) -> Option<&str> {
        self.view()
            .version()
            .map(|record| record.version_id().unwrap_or("null"))
    }
    fn get_is_latest(&self) -> bool {
        self.view().version().is_none_or(|record| record.is_latest())
    }
    fn get_is_delete_marker(&self) -> bool {
        self.view().version().is_some_and(|record| record.is_delete_marker())
    }
}
