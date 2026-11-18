//! 目标端目录页的匹配状态与传输分类。

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use data_mover::model::{EntryKind, ObservedEntry};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedView;
use transport::message::TransferDecision;

/// 索引键借用共享观察的路径，不复制路径或整段历史。
#[derive(Debug)]
struct ObservedPath(Arc<ObservedEntry>);
impl Borrow<str> for ObservedPath {
    fn borrow(&self) -> &str {
        self.0.path().as_str()
    }
}
impl PartialEq for ObservedPath {
    fn eq(&self, other: &Self) -> bool {
        self.0.path() == other.0.path()
    }
}
impl Eq for ObservedPath {}
impl Hash for ObservedPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.path().as_str().hash(state);
    }
}
fn index_slot(entry: &ObservedEntry) -> usize {
    usize::from(entry.kind() == EntryKind::Directory)
}

/// 目标端目录索引；文件和逻辑目录可以同路径共存，对象键 p/ 另行保留。
#[derive(Debug, Default)]
pub(crate) struct DestIndex {
    // slot 0 存非目录条目，slot 1 存逻辑目录；bool 表示本页已匹配。
    entries: [HashMap<ObservedPath, bool>; 2],
}
impl DestIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn insert(&mut self, entry: Arc<ObservedEntry>) {
        let entries = &mut self.entries[index_slot(&entry)];
        // HashMap 替换相等键时保留旧键；这里必须同时更新键所借用的观察事实。
        entries.remove(entry.path().as_str());
        entries.insert(ObservedPath(entry), false);
    }
    pub(crate) fn check(&mut self, source: &ObservedEntry) -> TransferDecision {
        let entries = &mut self.entries[index_slot(source)];
        let Some((key, _)) = entries.get_key_value(source.path().as_str()) else {
            return TransferDecision::FullTransfer;
        };
        let decision = match (data_check(source, &key.0), metadata_check(source, &key.0)) {
            (true, true) => TransferDecision::Skip,
            (true, false) => TransferDecision::MetadataOnly,
            (false, _) => TransferDecision::DeltaTransfer,
        };
        if let Some(matched) = entries.get_mut(source.path().as_str()) {
            *matched = true;
        }
        decision
    }
    /// 零字节对象数据相同；只比较本次复制契约要求且源端适用的 tags。
    pub(crate) fn check_directory_marker(&self, source: &ObservedEntry, copy_tags: bool) -> TransferDecision {
        let Some(marker) = source.directory_marker_object() else {
            return TransferDecision::Skip;
        };
        let destination = self.current_directory_marker(source);
        if destination.is_some()
            && (!copy_tags
                || marker.metadata().tags().value().is_none()
                || marker.metadata().tags().value() == destination.and_then(|view| view.metadata().tags().value()))
        {
            TransferDecision::Skip
        } else {
            TransferDecision::FullTransfer
        }
    }
    pub(crate) fn has_directory_marker(&self, entry: &ObservedEntry) -> bool {
        self.current_directory_marker(entry).is_some()
    }
    fn current_directory_marker(&self, source: &ObservedEntry) -> Option<ObservedView<'_>> {
        if let Some((key, _)) = self.entries[1].get_key_value(source.path().as_str()) {
            let entry = &key.0;
            if let Some(marker) = entry.directory_marker_object() {
                return Some(ObservedView {
                    entry: marker,
                    version_index: None,
                });
            }
            if let Some(view) = current_zero_object(entry) {
                return Some(view);
            }
        }
        let path = source
            .directory_marker_object()
            .map(|marker| Cow::Borrowed(marker.path().as_str()))
            .unwrap_or_else(|| Cow::Owned(format!("{}/", source.path().as_str())));
        let (key, _) = self.entries[0].get_key_value(path.as_ref())?;
        current_zero_object(&key.0)
    }
    pub(crate) fn mark_matched(&mut self, source: &ObservedEntry) {
        let already = self.entries[index_slot(source)]
            .get_mut(source.path().as_str())
            .is_some_and(|matched| {
                let old = *matched;
                *matched = true;
                old
            });
        if !already
            && source.kind() == EntryKind::Directory
            && (source.directory_marker_object().is_some() || source.history().is_some())
        {
            self.mark_directory_object_matched(source);
        }
    }
    /// 已确认的独立对象操作不再由本页 orphan 清理重复执行。
    pub(crate) fn mark_directory_object_matched(&mut self, directory: &ObservedEntry) {
        let path = directory
            .directory_marker_object()
            .map(|marker| Cow::Borrowed(marker.path().as_str()))
            .unwrap_or_else(|| Cow::Owned(format!("{}/", directory.path().as_str())));
        if let Some(matched) = self.entries[0].get_mut(path.as_ref()) {
            *matched = true;
        }
    }
    pub(crate) fn orphaned_entries(&self) -> Vec<&Arc<ObservedEntry>> {
        self.entries
            .iter()
            .flat_map(|entries| entries.iter())
            .filter(|(_, matched)| !**matched)
            .map(|(key, _)| &key.0)
            .collect()
    }
}
fn current_zero_object(entry: &ObservedEntry) -> Option<ObservedView<'_>> {
    if entry.kind() != EntryKind::Directory && !entry.path().names_directory_object() {
        return None;
    }
    let history = entry.history()?;
    let index = history
        .records()
        .iter()
        .position(|record| record.is_latest() && !record.is_delete_marker() && record.size() == Some(0))?;
    Some(ObservedView {
        entry,
        version_index: Some(index),
    })
}

/// 数据比较：mtime + size
fn data_check(src: &ObservedEntry, dest: &ObservedEntry) -> bool {
    src.get_mtime() == dest.get_mtime() && src.get_size() == dest.get_size()
}

/// 元数据比较：按 `ObservedEntry` 类型比较不同字段
fn metadata_check(src: &ObservedEntry, dest: &ObservedEntry) -> bool {
    src.backend_kind() == dest.backend_kind()
        && src.metadata().ownership_mode() == dest.metadata().ownership_mode()
        && src.metadata().tags() == dest.metadata().tags()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::observed_fixtures::{directory_marker, observation};
    use data_mover::model::BackendKind;

    #[test]
    fn marker_comparison_requires_the_actual_object_and_its_metadata() {
        let source = directory_marker("folder", vec![("tag".into(), "new".into())]);
        let virtual_directory = source.clone().with_directory_marker_object(None).unwrap();
        let mut index = DestIndex::new();
        index.insert(Arc::new(virtual_directory));
        assert_eq!(
            index.check_directory_marker(&source, true),
            TransferDecision::FullTransfer
        );
        assert_eq!(
            index.check_directory_marker(&source, false),
            TransferDecision::FullTransfer
        );
        index.insert(Arc::new(directory_marker("folder", vec![("tag".into(), "old".into())])));
        assert_eq!(
            index.check_directory_marker(&source, true),
            TransferDecision::FullTransfer
        );
        assert_eq!(index.check_directory_marker(&source, false), TransferDecision::Skip);
        index.insert(Arc::new(source.clone()));
        assert_eq!(index.check_directory_marker(&source, true), TransferDecision::Skip);
        let target = observation(
            BackendKind::S3,
            "folder".into(),
            EntryKind::Directory,
            0,
            [999; 3],
            None,
            None,
            Some(vec![("tag".into(), "new".into())]),
            Some((Some("target-version".into()), true, false)),
        );
        let ordinary_file = observation(
            BackendKind::S3,
            "folder".into(),
            EntryKind::File,
            0,
            [999; 3],
            None,
            None,
            Some(vec![("tag".into(), "new".into())]),
            Some((Some("file-version".into()), true, false)),
        );
        let mut ordinary_index = DestIndex::new();
        ordinary_index.insert(Arc::new(ordinary_file));
        assert_eq!(
            ordinary_index.check_directory_marker(&source, true),
            TransferDecision::FullTransfer
        );
        index.insert(Arc::new(target));
        assert!(index.has_directory_marker(&source));
        assert_eq!(index.check_directory_marker(&source, true), TransferDecision::Skip);
    }

    #[test]
    fn destination_index_keeps_same_path_file_and_directory_independent() {
        let directory = Arc::new(directory_marker("folder", vec![("tag".into(), "new".into())]));
        let file = Arc::new(observation(
            BackendKind::S3,
            "folder".into(),
            EntryKind::File,
            0,
            [123; 3],
            None,
            None,
            Some(Vec::new()),
            None,
        ));
        let mut index = DestIndex::new();
        index.insert(Arc::clone(&directory));
        index.insert(Arc::clone(&file));
        assert_eq!(index.check_directory_marker(&directory, true), TransferDecision::Skip);
        assert_eq!(index.check(&file), TransferDecision::Skip);
        assert_eq!(index.orphaned_entries().len(), 1);
        assert!(index.orphaned_entries()[0].get_is_dir());
        index.mark_matched(&directory);
        assert!(index.orphaned_entries().is_empty());
    }
}
