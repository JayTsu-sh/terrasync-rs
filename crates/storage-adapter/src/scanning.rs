use crate::connected::path;
use crate::filter::FilterExpression;
use crate::projection::{HistoryProjection, observation_plan};
use crate::{dir_tree::*, error::operation, *};
use chrono::NaiveDate;
use data_mover::{
    DslTraversalFilter,
    model::{EntryKind, EntryOperationFailure, MetadataObservations, MetadataRequirements, ObservedEntry},
    storage::{
        DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeTerminalFailure, StorageRoleFailure,
        delete_tree,
    },
    traversal::{
        Checkpoint, DirectoryStatistics, Resumable, ScanFailureSignal, TraversalCandidate, TraversalDecision,
        TraversalFilter, TraversalItem, TraversalOrder, TraversalOutcome, TraversalRecoveryStatus, TraversalRequest,
        TraversalSession, TraversalSource,
    },
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

impl ConnectedStorage {
    fn traversal_request(&self, sub: Option<&Path>, options: &WalkOptions, blocks: bool) -> Result<TraversalRequest> {
        let root = path(sub.unwrap_or(Path::new("")))?;
        let mut request = TraversalRequest::new(root)
            .observation_plan(observation_plan(options.include_tags))
            .max_inflight_operations(NonZeroUsize::new(options.concurrency).unwrap_or(NonZeroUsize::MIN))
            .max_depth(options.depth.and_then(NonZeroUsize::new));
        if blocks {
            request = request.order(TraversalOrder::Blocks);
        }
        if options.packaged {
            request = request.filter(Arc::new(PackageFilter::new(options)));
        } else if options.match_expressions.is_some() || options.exclude_expressions.is_some() {
            request = request.filter(Arc::new(DslTraversalFilter::new(
                options.match_expressions.clone(),
                options.exclude_expressions.clone(),
            )));
        }
        Ok(request)
    }
    fn traversal(&self, sub: Option<&Path>, options: &WalkOptions, blocks: bool) -> Result<TraversalSession> {
        let (source, _) = ScanFailureSignal::source(&self.roles, options.operation_timeout)?;
        Ok(source.traverse(self.traversal_request(sub, options, blocks)?))
    }

    /// 保存检查点前先持久化条目；恢复时可能重放一个目录。
    pub fn resumable_scan(
        &self, options: &WalkOptions, checkpoint: Option<Vec<u8>>, cancel: CancellationToken,
        every: std::time::Duration,
    ) -> Result<ResumableScan> {
        let cancel = cancel.child_token();
        let mut request = self
            .traversal_request(None, options, false)?
            .directory_statistics(true)
            .cancel(cancel.clone())
            .resumable(Resumable::On { every });
        if let Some(bytes) = checkpoint {
            request = request.resume_from(Checkpoint::from_bytes(bytes.into()));
        }
        let scan = self.start_traversal(request, options)?;
        Ok(ResumableScan {
            session: scan.session,
            guard: scan.failure,
            recovery: scan.recovery,
            history: None,
            cancel,
        })
    }
    pub async fn walkdir(&self, sub: Option<&Path>, options: WalkOptions) -> Result<WalkDirAsyncIterator> {
        let scan = self.start_traversal(self.traversal_request(sub, &options, false)?, &options)?;
        let mut session = scan.session;
        let guard = scan.failure;
        let recovery = scan.recovery;
        let terminal_error = Arc::new(std::sync::Mutex::new(None));
        let stored_error = terminal_error.clone();
        let package = PackageFilter::new(&options);
        let root = sub.unwrap_or(Path::new("")).to_path_buf();
        let (tx, rx) = async_channel::bounded(128);
        let connected = self.clone();
        tokio::spawn(async move {
            let mut histories = HistoryProjection::default();
            while let Some(item) = session.next_item().await {
                let message = match item {
                    TraversalItem::Entry(entry) => {
                        let projected = match connected.project_history(&entry, &mut histories).await {
                            Ok(entries) => entries,
                            Err(error) => {
                                let reason = error.to_string();
                                stored_error
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .get_or_insert(error);
                                let _ = tx
                                    .send(StorageEntryMessage::Error {
                                        event: ErrorEvent::Scan,
                                        path: entry.path().as_str().into(),
                                        entry: None,
                                        reason,
                                    })
                                    .await;
                                return;
                            }
                        };
                        for projected in projected {
                            let relative = projected
                                .get_relative_path()
                                .strip_prefix(&root)
                                .unwrap_or(projected.get_relative_path());
                            let message =
                                if projected.get_is_dir() && package.packaged_path(&relative.to_string_lossy()) {
                                    StorageEntryMessage::Packaged(Arc::new(projected))
                                } else {
                                    StorageEntryMessage::Scanned(Arc::new(projected))
                                };
                            if tx.send(message).await.is_err() {
                                return;
                            }
                        }
                        continue;
                    }
                    TraversalItem::Failure(failure) => StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(failure.path.as_str()),
                        entry: None,
                        reason: {
                            let error = StorageError::TraversalEntry {
                                path: failure.path.clone(),
                                scope: failure.scope,
                                delivered: failure.delivered,
                                source: failure.failure,
                            };
                            let reason = error.to_string();
                            stored_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get_or_insert(error);
                            reason
                        },
                    },
                    TraversalItem::ObservationFailure(failure) => StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::from(failure.path.as_str()),
                        entry: None,
                        reason: {
                            let error = StorageError::TraversalObservation(failure);
                            let reason = error.to_string();
                            stored_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get_or_insert(error);
                            reason
                        },
                    },
                    _ => continue,
                };
                if tx.send(message).await.is_err() {
                    return;
                }
            }
            if !histories.is_complete() {
                stored_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert_with(|| operation("incomplete object history"));
                let _ = tx
                    .send(StorageEntryMessage::Error {
                        event: ErrorEvent::Scan,
                        path: PathBuf::new(),
                        entry: None,
                        reason: "incomplete object history".into(),
                    })
                    .await;
                return;
            }
            match session.finish().await {
                Ok(TraversalOutcome::Completed(_)) => {}
                result => {
                    let reason = format!("traversal did not complete: {result:?}");
                    let error = match result {
                        Err(error) => StorageError::from(error),
                        _ => StorageError::Cancelled,
                    };
                    stored_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert(error);
                    let _ = tx
                        .send(StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: PathBuf::new(),
                            entry: None,
                            reason,
                        })
                        .await;
                }
            }
        });
        Ok(AsyncReceiver::new(rx)
            .with_scan_failure(terminal_error, guard)
            .with_recovery(recovery))
    }
    pub async fn walkdir_2(
        &self, sub: Option<&Path>, depth: Option<usize>, match_expressions: Option<FilterExpression>,
        exclude_expressions: Option<FilterExpression>, concurrency: usize, include_tags: bool,
    ) -> Result<WalkDirAsyncIterator2> {
        let mut session = self.traversal(
            sub,
            &WalkOptions {
                depth,
                match_expressions,
                exclude_expressions,
                concurrency,
                include_tags,
                ..Default::default()
            },
            true,
        )?;
        let (tx, rx) = async_channel::bounded(2);
        let connected = self.clone();
        tokio::spawn(async move {
            let mut histories = HistoryProjection::default();
            let mut files = Vec::new();
            let mut subdirs = Vec::new();
            let mut next = 0i32;
            while let Some(item) = session.next_item().await {
                match item {
                    TraversalItem::Entry(entry) => {
                        let projected = match connected.project_history(&entry, &mut histories).await {
                            Ok(entries) => entries,
                            Err(error) => {
                                let _ = tx
                                    .send(NdxEvent::Error {
                                        path: entry.path().as_str().into(),
                                        reason: error.to_string(),
                                    })
                                    .await;
                                return;
                            }
                        };
                        for entry in projected {
                            let entry = Arc::new(entry);
                            if entry.get_is_dir() {
                                subdirs.push(entry);
                            } else {
                                files.push(entry);
                            }
                        }
                    }
                    TraversalItem::DirectoryListed(listed) => {
                        files.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                        subdirs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
                        let start = next;
                        let mut indexed = |entries: &mut Vec<Arc<EntryEnum>>| -> Vec<NdxEntry> {
                            entries
                                .drain(..)
                                .map(|entry| {
                                    let ndx = next;
                                    next = next.saturating_add(1);
                                    NdxEntry { ndx, entry }
                                })
                                .collect()
                        };
                        let files = indexed(&mut files);
                        let subdirs = indexed(&mut subdirs);
                        let gap_ndx = next;
                        next = next.saturating_add(1);
                        if tx
                            .send(NdxEvent::Page(DirPageResult {
                                dir_path: listed.path.as_str().into(),
                                ndx_start: start,
                                files,
                                subdirs,
                                gap_ndx,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    TraversalItem::Failure(f) => {
                        if tx
                            .send(NdxEvent::Error {
                                path: f.path.as_str().into(),
                                reason: format!("{:?}: {:?}", f.scope, f.failure.class()),
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    TraversalItem::ObservationFailure(f) => {
                        if tx
                            .send(NdxEvent::Error {
                                path: f.path.as_str().into(),
                                reason: format!("{:?}: {:?}", f.family, f.class),
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    _ => {}
                }
            }
            if !histories.is_complete() {
                let _ = tx
                    .send(NdxEvent::Error {
                        path: String::new(),
                        reason: "incomplete object history".into(),
                    })
                    .await;
                return;
            }
            match session.finish().await {
                Ok(TraversalOutcome::Completed(_)) => {
                    let _ = tx.send(NdxEvent::Done).await;
                }
                result => {
                    let _ = tx
                        .send(NdxEvent::Error {
                            path: String::new(),
                            reason: format!("traversal did not complete: {result:?}"),
                        })
                        .await;
                }
            }
        });
        Ok(AsyncReceiver::new(rx))
    }
    pub async fn delete_dir_all_with_progress(
        &self, sub: Option<&Path>, concurrency: usize,
    ) -> Result<AsyncReceiver<DeleteEvent>> {
        let mut session = delete_tree(
            &self.roles,
            DeleteTreeRequest {
                root: path(sub.unwrap_or(Path::new("")))?,
                delete_root: true,
                max_inflight_operations: NonZeroUsize::new(concurrency).unwrap_or(NonZeroUsize::MIN),
                max_buffered_items: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
                cancel: CancellationToken::new(),
            },
        )
        .map_err(StorageError::from)?;
        let (tx, rx) = async_channel::bounded(128);
        tokio::spawn(async move {
            while let Some(item) = session.next_item().await {
                let event = match item {
                    DeleteTreeItem::Deleted { path, kind } => DeleteEvent {
                        relative_path: path.as_str().into(),
                        is_dir: kind == EntryKind::Directory,
                        error: None,
                        failure: None,
                    },
                    DeleteTreeItem::EntryFailure(f) => DeleteEvent {
                        relative_path: f.path().as_str().into(),
                        is_dir: false,
                        error: Some(f.to_string()),
                        failure: Some(DeleteEventFailure::Entry(f)),
                    },
                };
                if tx.send(event).await.is_err() {
                    return;
                }
            }
            let result = session.finish().await;
            if !matches!(&result, Ok(DeleteTreeOutcome::Completed(_))) {
                let _ = tx
                    .send(DeleteEvent {
                        relative_path: PathBuf::new(),
                        is_dir: false,
                        error: Some("recursive deletion did not complete".into()),
                        failure: result.err().map(DeleteEventFailure::Terminal),
                    })
                    .await;
            }
        });
        Ok(AsyncReceiver::new(rx))
    }
    pub async fn delete_dir_all(&self, entry: &EntryEnum) -> Result<()> {
        let events = self
            .delete_dir_all_with_progress(Some(entry.get_relative_path()), 4)
            .await?;
        while let Some(event) = events.next().await {
            if let Some(error) = event.error {
                return Err(match event.failure {
                    Some(DeleteEventFailure::Entry(source)) => StorageRoleFailure::Entry(source).into(),
                    Some(DeleteEventFailure::Terminal(source)) => source.into(),
                    None => operation(error),
                });
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DeleteEvent {
    pub relative_path: PathBuf,
    pub is_dir: bool,
    pub error: Option<String>,
    // 仅在适配层内保留原始来源；对外进度消息继续使用原有文本。
    failure: Option<DeleteEventFailure>,
}

#[derive(Debug, Clone)]
enum DeleteEventFailure {
    Entry(EntryOperationFailure),
    Terminal(DeleteTreeTerminalFailure),
}

/// 按交付顺序返回产品条目及其 opaque 遍历检查点。
pub enum ResumableScanItem {
    Entry(Box<ObservedEntry>),
    Statistics(Box<DirectoryStatistics>),
    Checkpoint(Vec<u8>),
}

pub struct ResumableScan {
    session: TraversalSession,
    recovery: Option<TraversalRecoveryStatus>,
    guard: ScanFailureSignal,
    history: Option<(String, u32)>,
    cancel: CancellationToken,
}

impl ResumableScan {
    /// 当前恢复活动；临时断连不会触发最终失败信号。
    pub fn recovery_status(&self) -> Option<&TraversalRecoveryStatus> {
        self.recovery.as_ref()
    }
    /// 旁路通知让调用方立即停止预读缓冲的消费，并保留原始类型化错误。
    pub fn failure_signal(&self) -> ScanFailureSignal {
        self.guard.clone()
    }
    /// 放弃未确认的尾部条目，等待遍历真正停止；不把取消时的部分历史当成完成证据。
    pub async fn stop(mut self) -> Result<TraversalOutcome> {
        self.cancel.cancel();
        while self.session.next_item().await.is_some() {}
        self.session.finish().await.map_err(StorageError::from)
    }
    pub async fn next(&mut self) -> Option<Result<ResumableScanItem>> {
        loop {
            let item = tokio::select! {
                biased;
                () = self.guard.failed() => return self.guard.failure().map(|failure| Err(failure.into())),
                item = self.session.next_item() => item,
            };
            let Some(item) = item else { break };
            match item {
                TraversalItem::DirectoryStatistics(statistics) if self.history.is_none() => {
                    return Some(Ok(ResumableScanItem::Statistics(statistics)));
                }
                TraversalItem::Entry(entry) => {
                    if let Some(history) = entry.history() {
                        let expected = self.history.get_or_insert_with(|| (entry.path().as_str().into(), 0));
                        if expected.0 != entry.path().as_str() || expected.1 != history.segment() {
                            return Some(Err(operation("out-of-order object history")));
                        }
                        expected.1 = expected.1.saturating_add(1);
                        if history.is_complete() {
                            self.history = None;
                        }
                    } else if self.history.is_some() {
                        return Some(Err(operation("interrupted object history")));
                    }
                    return Some(Ok(ResumableScanItem::Entry(entry)));
                }
                TraversalItem::Checkpoint(checkpoint) if self.history.is_none() => {
                    return Some(Ok(ResumableScanItem::Checkpoint(checkpoint.as_bytes().to_vec())));
                }
                TraversalItem::Failure(failure) => {
                    return Some(Err(StorageError::TraversalEntry {
                        path: failure.path,
                        scope: failure.scope,
                        delivered: failure.delivered,
                        source: failure.failure,
                    }));
                }
                TraversalItem::ObservationFailure(failure) => {
                    return Some(Err(StorageError::TraversalObservation(failure)));
                }
                _ => {}
            }
        }
        None
    }

    pub async fn finish(self) -> Result<TraversalOutcome> {
        let partial = self.history.is_some();
        let result = self.session.finish().await.map_err(StorageError::from);
        if let Some(failure) = self.guard.failure() {
            return Err(failure.into());
        }
        let outcome = result?;
        if partial {
            return Err(operation("scan ended with an incomplete version history"));
        }
        Ok(outcome)
    }
}
pub type DeleteDirIterator = AsyncReceiver<DeleteEvent>;

/// 打包选择属于产品层；遍历仍由上游按页执行，在选中目录处停止下钻。
#[derive(Debug)]
struct PackageFilter {
    dsl: DslTraversalFilter,
    enabled: bool,
    extra_depth: usize,
    maximum: Option<usize>,
}
impl PackageFilter {
    fn new(options: &WalkOptions) -> Self {
        Self {
            dsl: DslTraversalFilter::new(options.match_expressions.clone(), options.exclude_expressions.clone()),
            enabled: options.packaged && options.match_expressions.is_some(),
            extra_depth: options.package_depth,
            maximum: options.depth.filter(|v| *v > 0),
        }
    }
    fn target_depth(&self, value: &str, is_dir: bool) -> Option<usize> {
        if !self.enabled {
            return None;
        }
        let parts: Vec<_> = value.split('/').filter(|v| !v.is_empty()).collect();
        let directories = parts.len().saturating_sub(usize::from(!is_dir));
        for index in 0..directories {
            if !date_name(parts[index]) {
                continue;
            }
            let ancestor = parts[..=index].join("/");
            let metadata = MetadataObservations::default();
            let candidate = TraversalCandidate {
                path: &ancestor,
                name: parts[index],
                kind: EntryKind::Directory,
                size: None,
                modified: None,
                metadata: &metadata,
            };
            if self.dsl.decide(&candidate).emit {
                let target = index + 1 + self.extra_depth;
                return Some(target);
            }
        }
        None
    }
    fn packaged_path(&self, value: &str) -> bool {
        self.target_depth(value, true) == Some(value.split('/').filter(|v| !v.is_empty()).count())
    }
}
impl TraversalFilter for PackageFilter {
    fn resume_key(&self) -> Option<Vec<u8>> {
        let mut key = b"terrasync-package-filter-v1\0".to_vec();
        key.extend(self.dsl.resume_key()?);
        key.push(u8::from(self.enabled));
        key.extend_from_slice(&(self.extra_depth as u64).to_le_bytes());
        key.push(u8::from(self.maximum.is_some()));
        key.extend_from_slice(&(self.maximum.unwrap_or_default() as u64).to_le_bytes());
        Some(key)
    }
    fn required_metadata(&self) -> MetadataRequirements {
        self.dsl.required_metadata()
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let mut decision = self.dsl.decide(candidate);
        let is_dir = candidate.kind == EntryKind::Directory;
        if let Some(target) = self.target_depth(candidate.path, is_dir) {
            let depth = candidate.path.split('/').filter(|v| !v.is_empty()).count();
            if self.maximum.is_some_and(|maximum| target > maximum) {
                decision.emit = false;
                decision.descend = false;
            } else if depth == target && is_dir {
                decision.descend = false;
            } else {
                decision.emit = false;
            }
        }
        // 必须在每层评估打包深度；普通 DSL 可以提前放行整个子树。
        decision.filter_children = true;
        decision
    }
}
fn date_name(name: &str) -> bool {
    name.char_indices().any(|(start, _)| {
        [(10, "%Y-%m-%d"), (8, "%Y%m%d"), (6, "%y%m%d")]
            .into_iter()
            .any(|(length, format)| {
                name.get(start..start + length)
                    .is_some_and(|value| NaiveDate::parse_from_str(value, format).is_ok())
            })
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod stopping_tests {
    use super::*;
    use data_mover::traversal::TraversalTerminalFailure;
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn stopping_partial_history_waits_for_the_source_without_cancelling_its_parent() {
        let parent = CancellationToken::new();
        let cancel = parent.child_token();
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).unwrap(), cancel.clone());
        let directory = tempfile::tempdir().unwrap();
        let storage = create_storage(&directory.path().to_string_lossy(), CreateStorageOptions::default())
            .await
            .unwrap();
        let scan = ResumableScan {
            recovery: None,
            guard: ScanFailureSignal::source(&storage.roles, None).unwrap().1,
            session,
            history: Some(("partial-history".into(), 1)),
            cancel: cancel.clone(),
        };
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let source = tokio::spawn(async move {
            cancel.cancelled().await;
            sleep(Duration::from_millis(10)).await;
            flag.store(true, Ordering::SeqCst);
            producer.finish(Err(TraversalTerminalFailure::Internal));
        });
        assert!(scan.stop().await.is_err());
        assert!(finished.load(Ordering::SeqCst));
        assert!(!parent.is_cancelled());
        source.await.unwrap();
    }
}
