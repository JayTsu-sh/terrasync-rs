//! 按逻辑 entry 管理历史版本的交付、失败阻断和一次计数；不持有完整历史或已完成 entry 集合。
use std::{collections::HashMap, iter::once, path::PathBuf, sync::Arc};

use data_mover::model::{ObjectHistory, ObservedEntry};
use task_model::observation::{EntryStatistics, ObservedItem, ObservedView};
use task_model::{ChangeKind, EntryChange, ErrorEvent, StorageEntryMessage};
use tracing::error;

use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result};
use crate::file_copy::selection::skip_unrepresentable;
use crate::file_copy::{CopyOptions, VersionCopyScope, VersionFailureContext, copy_versions, retain_version};
use crate::storage_factory::StorageContext;

#[derive(Clone, Copy)]
pub(crate) enum HistoryCompletion {
    Full,
    Incremental,
}

#[derive(Clone, Copy)]
pub(crate) struct HistoryCopyPolicy {
    pub active_entry_limit: usize,
    pub target_history: Option<bool>,
    pub completion: HistoryCompletion,
}

pub(crate) struct HistoryCopyContext {
    pub source: Arc<StorageContext>,
    pub destination: Arc<StorageContext>,
    pub broadcaster: BroadcastForwarder<StorageEntryMessage>,
    pub options: CopyOptions,
    pub job_id: String,
}

#[derive(Default)]
struct EntryDelivery {
    next_segment: u32,
    last_version: Option<String>,
    last_version_index: Option<usize>,
    change: Option<EntryChange>,
    logical_bytes: u64,
    failed: bool,
    skipped: bool,
    blocked: Option<String>,
}

pub(crate) struct HistoryCopyWorker {
    context: HistoryCopyContext,
    policy: HistoryCopyPolicy,
    entries: HashMap<String, EntryDelivery>,
    failure: Option<AppError>,
    data_failed: bool,
}

impl HistoryCopyWorker {
    pub(crate) fn new(context: HistoryCopyContext, policy: HistoryCopyPolicy) -> Self {
        let policy = HistoryCopyPolicy {
            active_entry_limit: policy.active_entry_limit.max(1),
            ..policy
        };
        Self {
            context,
            policy,
            entries: HashMap::new(),
            failure: None,
            data_failed: false,
        }
    }

    fn entry(&mut self, path: &str, segment: u32) -> Result<&mut EntryDelivery> {
        if !self.entries.contains_key(path) {
            if self.entries.len() >= self.policy.active_entry_limit.max(1) {
                return Err(AppError::CopyHistoryCapacity {
                    path: path.into(),
                    limit: self.policy.active_entry_limit,
                });
            }
            self.entries.insert(path.into(), EntryDelivery::default());
        }
        let state = self
            .entries
            .get_mut(path)
            .ok_or_else(|| AppError::CopyHistoryIncomplete {
                path: path.into(),
                last_version: None,
            })?;
        if state.next_segment != segment {
            return Err(AppError::CopyHistoryOrder {
                path: path.into(),
                expected: state.next_segment,
                actual: segment,
            });
        }
        Ok(state)
    }

    async fn target_history(&mut self) -> Result<bool> {
        if let Some(value) = self.policy.target_history {
            return Ok(value);
        }
        let value = self
            .context
            .destination
            .storage()
            .staged_destination()?
            .keeps_history()
            .await?;
        self.policy.target_history = Some(value);
        Ok(value)
    }

    pub(crate) async fn process(&mut self, message: &StorageEntryMessage) -> Result<bool> {
        match message {
            StorageEntryMessage::New(item) if item.entry.history().is_some() => {
                self.version(item, EntryChange::New).await?;
            }
            StorageEntryMessage::Changed { entry, kind }
                if entry.entry.history().is_some() && *kind != ChangeKind::DirectoryMarkerRemoved =>
            {
                self.version(entry, EntryChange::Changed(*kind)).await?;
            }
            StorageEntryMessage::CopyEntryBoundary(entry) => self.end_segment(entry).await?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub(crate) async fn copy_segment(&mut self, entry: &Arc<ObservedEntry>) -> Result<()> {
        for item in ObservedItem::all(entry) {
            self.version(&item, EntryChange::New).await?;
        }
        self.end_segment(entry).await
    }

    async fn version(&mut self, item: &ObservedItem, change: EntryChange) -> Result<()> {
        let path = item.view().source_path()?.as_str().to_owned();
        let index = item
            .version_index
            .filter(|_| item.view().version().is_some())
            .ok_or_else(|| AppError::CopyHistorySelection {
                path: path.clone().into(),
                version_index: item.version_index,
            })?;
        let target_history = self.target_history().await?;
        let segment = item.entry.history().map_or(0, ObjectHistory::segment);
        let state = self.entry(&path, segment)?;
        if let Some(previous) = state.last_version_index
            && previous >= index
        {
            return Err(AppError::CopyHistoryVersionOrder {
                path: path.into(),
                segment,
                previous,
                actual: index,
            });
        }
        state.last_version_index = Some(index);
        state.last_version = Some(item.view().version_id().into());
        if state.skipped {
            return Ok(());
        }
        if let Some(reason) = state.blocked.clone() {
            self.context
                .broadcaster
                .broadcast(StorageEntryMessage::CopyVersionFailed {
                    entry: item.clone(),
                    phase: "blocked_by_prior_version".into(),
                    backend: self.context.destination.storage().kind(),
                    reason: format!("此前版本的数据复制失败，此版本未执行：{reason}"),
                    first_in_entry: false,
                })
                .await?;
            return Ok(());
        }
        if skip_unrepresentable(&item.entry, &self.context.destination, &self.context.broadcaster).await? {
            self.entry(&path, segment)?.skipped = true;
            return Ok(());
        }
        let result = copy_versions(
            once(item.clone()),
            &self.context.source,
            &self.context.destination,
            self.context.options.clone(),
            &self.context.job_id,
            &self.context.broadcaster,
            VersionCopyScope::SelectedVersion {
                change,
                target_history: Some(target_history),
            },
        )
        .await;
        match result {
            Ok(()) if retain_version(item, target_history) => {
                let state = self.entry(&path, segment)?;
                state.logical_bytes = state
                    .logical_bytes
                    .checked_add(item.view().payload().get_size())
                    .ok_or_else(|| AppError::CopyHistoryBytesOverflow { path: path.into() })?;
                state.change = Some(merge_change(state.change, change));
            }
            Ok(()) => {}
            Err(error) if error.stops_event_pipeline() => return Err(error),
            Err(error) => {
                let context = error.storage_error().map_or_else(
                    || VersionFailureContext {
                        phase: "copy_version".into(),
                        backend: self.context.destination.storage().kind(),
                        metadata_only: false,
                    },
                    |error| {
                        VersionFailureContext::for_error(
                            error,
                            self.context.source.storage().kind(),
                            self.context.destination.storage().kind(),
                        )
                    },
                );
                let reason = error.to_string();
                error!(%path,version=item.view().version_id(),phase=%context.phase,backend=?context.backend,%error,"版本复制最终失败");
                let state = self.entry(&path, segment)?;
                let first_in_entry = !state.failed;
                state.failed = true;
                if !context.metadata_only {
                    state.blocked = Some(format!(
                        "version={} phase={} {reason}",
                        item.view().version_id(),
                        context.phase
                    ));
                    self.data_failed = true;
                }
                self.context
                    .broadcaster
                    .broadcast(StorageEntryMessage::CopyVersionFailed {
                        entry: item.clone(),
                        phase: context.phase,
                        backend: context.backend,
                        reason,
                        first_in_entry,
                    })
                    .await?;
                self.failure.get_or_insert(error);
            }
        }
        Ok(())
    }

    async fn end_segment(&mut self, entry: &Arc<ObservedEntry>) -> Result<()> {
        let path = ObservedView {
            entry,
            version_index: None,
        }
        .source_path()?
        .as_str()
        .to_owned();
        let history = entry.history().ok_or_else(|| AppError::CopyHistoryIncomplete {
            path: path.clone().into(),
            last_version: None,
        })?;
        let state = self.entry(&path, history.segment())?;
        state.last_version = history
            .records()
            .last()
            .map(|version| version.version_id().unwrap_or("null").into());
        if !history.is_complete() {
            state.last_version_index = None;
            state.next_segment = state
                .next_segment
                .checked_add(1)
                .ok_or_else(|| AppError::CopyHistorySegmentOverflow { path: path.into() })?;
            return Ok(());
        }
        let state = self
            .entries
            .remove(&path)
            .ok_or_else(|| AppError::CopyHistoryIncomplete {
                path: path.into(),
                last_version: None,
            })?;
        if !state.failed
            && !state.skipped
            && let Some(change) = state.change
        {
            let message = match self.policy.completion {
                HistoryCompletion::Full => StorageEntryMessage::CopyEntryCompleted {
                    entry: entry.clone(),
                    logical_bytes: state.logical_bytes,
                },
                HistoryCompletion::Incremental => StorageEntryMessage::CopyEntryDeltaCompleted {
                    entry: entry.clone(),
                    change,
                    logical_bytes: state.logical_bytes,
                },
            };
            self.context.broadcaster.broadcast(message).await?;
        }
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<()> {
        for (path, state) in self.entries.drain() {
            let error = AppError::CopyHistoryIncomplete {
                path: PathBuf::from(&path),
                last_version: state.last_version,
            };
            let message = if state.failed {
                StorageEntryMessage::Diagnostic {
                    event: ErrorEvent::Copy,
                    path: path.into(),
                    reason: error.to_string(),
                }
            } else {
                StorageEntryMessage::Error {
                    event: ErrorEvent::Copy,
                    path: path.into(),
                    entry: None,
                    reason: error.to_string(),
                }
            };
            self.context.broadcaster.broadcast(message).await?;
            self.failure.get_or_insert(error);
            self.data_failed = true;
        }
        match self.failure {
            Some(source) => Err(AppError::ReportedCopyFailure {
                source: Box::new(source),
                data_failed: self.data_failed,
            }),
            None => Ok(()),
        }
    }
}

fn merge_change(previous: Option<EntryChange>, next: EntryChange) -> EntryChange {
    match (previous, next) {
        (None, next) => next,
        (Some(EntryChange::New), EntryChange::New) => EntryChange::New,
        (Some(EntryChange::Changed(left)), EntryChange::Changed(right)) if left == right => EntryChange::Changed(left),
        _ => EntryChange::Changed(ChangeKind::Both),
    }
}

#[cfg(test)]
#[path = "copy_history_tests.rs"]
mod tests;
