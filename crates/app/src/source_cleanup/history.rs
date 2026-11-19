//! 逐行核验历史目标，仅在完整分段及所有所选版本均就绪后永久清理源 key。
use std::{path::Path, sync::Arc};

use data_mover::model::{EntryKind, ObservedEntry, SourceVersion, StoragePath, VersionRecord};
use data_mover::storage::SourceDescriptor;
use db::catalogue::CopyCatalogueEntry;

use super::{SourceCleanupCheck, content, verification};
use crate::error::{AppError, CopyStep, Result, StorageError};
use crate::file_copy::metadata::captured_metadata_plan;
use crate::storage_factory::StorageContext;

#[derive(Default)]
pub(super) struct HistoryCleanup {
    group: Option<Group>,
}

struct Group {
    path: StoragePath,
    segment: u32,
    records: u32,
    next_index: u32,
    complete: bool,
    anchor: Option<Arc<ObservedEntry>>,
}

impl Group {
    fn unconfirmed(&self, reason: &'static str) -> AppError {
        StorageError::HistoryCleanupUnconfirmed {
            path: self.path.clone(),
            reason,
        }
        .into()
    }

    fn start(row: &CopyCatalogueEntry) -> Result<Self> {
        let path = row.completion.item.entry.path().clone();
        if row.history_segment != 0 || row.history_index != 0 {
            return Err(StorageError::HistoryCleanupUnconfirmed {
                path,
                reason: "历史不是从首段首版本开始",
            }
            .into());
        }
        Ok(Self {
            path,
            segment: 0,
            records: row.history_records,
            next_index: 0,
            complete: row.history_complete,
            anchor: None,
        })
    }

    fn anchor(&self) -> Result<&Arc<ObservedEntry>> {
        self.anchor
            .as_ref()
            .filter(|_| self.complete && self.next_index == self.records)
            .ok_or_else(|| self.unconfirmed("历史未到完整当前版本，不授权删除源 key"))
    }

    fn observe(&mut self, row: &CopyCatalogueEntry) -> Result<()> {
        if self.anchor.is_some() {
            return Err(self.unconfirmed("当前版本之后仍有历史事实"));
        }
        if row.history_segment != self.segment {
            if self.segment.checked_add(1) != Some(row.history_segment)
                || self.next_index != self.records
                || self.complete
                || row.history_index != 0
            {
                return Err(self.unconfirmed("历史分段缺失、乱序或前段未完成"));
            }
            self.segment = row.history_segment;
            self.records = row.history_records;
            self.next_index = 0;
            self.complete = row.history_complete;
        }
        if row.history_index != self.next_index
            || row.history_records != self.records
            || row.history_complete != self.complete
            || self.next_index >= self.records
        {
            return Err(self.unconfirmed("历史段内事实缺失、重复或边界不一致"));
        }
        self.next_index += 1;
        let entry = &row.completion.item.entry;
        let record = row
            .completion
            .item
            .view()
            .version()
            .ok_or_else(|| self.unconfirmed("历史事实缺少版本"))?;
        if record.is_latest() {
            if !self.complete || self.next_index != self.records {
                return Err(self.unconfirmed("当前版本不在完整末段的最后"));
            }
            self.anchor = Some(entry.clone());
        }
        Ok(())
    }
}

impl HistoryCleanup {
    pub(super) fn has_other_path(&self, path: &StoragePath) -> bool {
        self.group.as_ref().is_some_and(|group| &group.path != path)
    }

    pub(super) async fn observe(
        &mut self, row: CopyCatalogueEntry, source: &StorageContext, target: &StorageContext,
        check: &SourceCleanupCheck,
    ) -> Result<()> {
        if self.group.is_none() {
            self.group = Some(Group::start(&row)?);
        }
        let group = self
            .group
            .as_mut()
            .ok_or_else(|| AppError::ConfigError("历史核验状态缺失".into()))?;
        group.observe(&row)?;
        verify_version(&row, source, target, check).await
    }

    pub(super) fn query_failed(&self, error: AppError) -> AppError {
        match &self.group {
            Some(group) => AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(group.path.as_str()), error),
            None => error,
        }
    }

    pub(super) async fn finish(&mut self, source: &StorageContext, check: &SourceCleanupCheck) -> Result<()> {
        let Some(group) = self.group.take() else {
            return Ok(());
        };
        let result: Result<()> = async {
            let anchor = group.anchor()?;
            // 当前删除标记隐藏普通路径；其精确身份已逐版本确认，purge 会再次复核当前锚点。
            let latest_marker = anchor
                .history()
                .and_then(|history| history.latest())
                .is_some_and(VersionRecord::is_delete_marker);
            if !latest_marker && !verification::verify_source_metadata(source, anchor, check).await? {
                return Err(StorageError::SourceHistoryCurrentUnconfirmed {
                    path: group.path.clone(),
                }
                .into());
            }
            source
                .storage()
                .namespace()?
                .purge_observed(anchor, check.cancel.clone())
                .await
                .map_err(|source| StorageError::SourcePurgeRefused {
                    path: group.path.clone(),
                    source,
                })?;
            Ok(())
        }
        .await;
        result.map_err(|error| {
            AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(group.path.as_str()), error)
        })
    }
}

fn verification_failure(row: &CopyCatalogueEntry, phase: &'static str, error: AppError) -> AppError {
    match error {
        AppError::StorageError(source) => StorageError::HistoryCleanupVerification {
            path: row.completion.item.entry.path().clone(),
            version: row.completion.item.view().version_id().into(),
            target_version: row.completion.target_version.clone(),
            phase,
            source: Box::new(source),
        }
        .into(),
        other => other,
    }
}

async fn verify_version(
    row: &CopyCatalogueEntry, source: &StorageContext, target: &StorageContext, check: &SourceCleanupCheck,
) -> Result<()> {
    let view = row.completion.item.view();
    let descriptor = verify_target(row, target, check)
        .await
        .map_err(|error| verification_failure(row, "target_ready", error))?;
    verification::verify_source_version(source, view, check)
        .await
        .map_err(|error| verification_failure(row, "source_metadata", error))?;
    if check.verify_content && view.version().is_some_and(|record| !record.is_delete_marker()) {
        content::verify(
            source,
            target,
            view.entry,
            &descriptor,
            check,
            view.version().is_some_and(|record| !record.is_latest()),
        )
        .await
        .map_err(|error| verification_failure(row, "content", error))?;
    }
    Ok(())
}

async fn verify_target(
    row: &CopyCatalogueEntry, target: &StorageContext, check: &SourceCleanupCheck,
) -> Result<SourceDescriptor> {
    let view = row.completion.item.view();
    if view.entry.kind() != EntryKind::File || view.version().is_none() {
        return Err(StorageError::PermanentVersionCleanupUnavailable {
            path: view.entry.path().clone(),
        }
        .into());
    }
    let version = row
        .completion
        .target_version
        .as_deref()
        .ok_or_else(|| StorageError::TargetVersionUnconfirmed {
            path: view.entry.path().clone(),
            source_version: view.version_id().into(),
        })?;
    let target_version = SourceVersion::Id(version.into());
    let marker = view.version().is_some_and(VersionRecord::is_delete_marker);
    let mut descriptor = captured_metadata_plan(target.storage(), view)?
        .verify_published(
            target.storage(),
            view.entry.path(),
            &target_version,
            view.version()
                .filter(|record| record.is_latest() && !record.is_delete_marker())
                .map(|_| version),
            None,
            check.cancel.clone(),
        )
        .await
        .map_err(StorageError::from)?;
    if descriptor.kind != EntryKind::File
        || descriptor.size != view.version().and_then(VersionRecord::size)
        || (marker && verification::selected_marker(&descriptor).is_none())
    {
        return Err(StorageError::TargetPublicationChanged {
            path: descriptor.path,
            source: None,
        }
        .into());
    }
    if marker && view.version().is_some_and(VersionRecord::is_latest) {
        // 保留目标实测身份，只收紧 selector；当前绑定的协议检查归 namespace 所有。
        descriptor.version = SourceVersion::Current;
    }
    target
        .storage()
        .namespace()?
        .persist_observed(&descriptor, check.cancel.clone())
        .await
        .map_err(|source| StorageError::TargetPersistenceUnconfirmed {
            path: descriptor.path.clone(),
            source,
        })?;
    Ok(descriptor)
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
