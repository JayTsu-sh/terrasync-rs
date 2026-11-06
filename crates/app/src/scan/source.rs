use super::observation::{HistorySequence, observation_plan, tags_required, validate_required_tags};
use crate::error::storage::{Result, StorageError, operation};
use crate::storage_factory::{StorageContext, path};
use task_model::ObservedEntryExt;

use super::packaging::PackageFilter;
use crate::task_stream::AsyncReceiver;
use data_mover::{
    DslTraversalFilter,
    model::ObservedEntry,
    traversal::{
        Checkpoint, DirectoryStatistics, Resumable, ScanFailureSignal, TraversalItem, TraversalOrder, TraversalOutcome,
        TraversalRecoveryResume, TraversalRecoveryStatus, TraversalRequest, TraversalResume, TraversalSession,
        TraversalSource, TraversalTerminalFailure,
    },
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use task_model::{ErrorEvent, StorageEntryMessage};
use tokio_util::sync::CancellationToken;

pub enum ScanSourceMode {
    Resumable {
        checkpoint: Option<Vec<u8>>,
        every: std::time::Duration,
    },
    Streaming {
        order: TraversalOrder,
    },
}

impl StorageContext {
    fn traversal_request(&self, sub: Option<&Path>, options: &WalkOptions, blocks: bool) -> Result<TraversalRequest> {
        let root = path(sub.unwrap_or(Path::new("")))?;
        let mut request = TraversalRequest::new(root)
            .observation_plan(observation_plan(tags_required(&self.roles, options.include_tags)?))
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

    /// 产品显式选择扫描顺序及恢复语义；不由 backend 或数据库开关推断。
    pub fn scan_source(
        &self, options: &WalkOptions, mode: ScanSourceMode, cancel: CancellationToken,
    ) -> Result<ResumableScan> {
        let cancel = cancel.child_token();
        let mut request = self
            .traversal_request(None, options, false)?
            .directory_statistics(true)
            .cancel(cancel.clone());
        let recovery_resume = match mode {
            ScanSourceMode::Resumable { checkpoint, every } => {
                request = request.order(TraversalOrder::Blocks).resumable(Resumable::On { every });
                if let Some(bytes) = checkpoint {
                    request = request.resume_from(Checkpoint::from_bytes(bytes.into()));
                }
                TraversalRecoveryResume::Checkpoint
            }
            ScanSourceMode::Streaming { order } => {
                request = request.order(order);
                TraversalRecoveryResume::RestartRoot
            }
        };
        let scan = self.start_traversal_with_resume(request, options, recovery_resume)?;
        Ok(ResumableScan {
            session: scan.session,
            guard: scan.failure,
            recovery: scan.recovery,
            history: None,
            include_tags: tags_required(&self.roles, options.include_tags)?,
            cancel,
        })
    }
    pub async fn walkdir(&self, sub: Option<&Path>, options: WalkOptions) -> Result<WalkDirAsyncIterator> {
        self.walkdir_with_root_policy(sub, options, true).await
    }

    /// 枚举已由父页处理其自身对象的目录；只返回直接子条目。
    pub async fn read_directory(&self, directory: &Path, include_tags: bool) -> Result<WalkDirAsyncIterator> {
        self.walkdir_with_root_policy(
            Some(directory),
            WalkOptions {
                depth: Some(1),
                include_tags,
                ..Default::default()
            },
            false,
        )
        .await
    }

    async fn walkdir_with_root_policy(
        &self, sub: Option<&Path>, options: WalkOptions, report_root: bool,
    ) -> Result<WalkDirAsyncIterator> {
        let required_tags = tags_required(&self.roles, options.include_tags)?;
        let request = self
            .traversal_request(sub, &options, false)?
            .report_root_directory_object(report_root);
        let scan = self.start_traversal(request, &options)?;
        let mut session = scan.session;
        let guard = scan.failure;
        let recovery = scan.recovery;
        let terminal_error = Arc::new(std::sync::Mutex::new(None));
        let stored_error = terminal_error.clone();
        let package = PackageFilter::new(&options);
        let root = sub.unwrap_or(Path::new("")).to_path_buf();
        let (tx, rx) = async_channel::bounded(128);
        tokio::spawn(async move {
            let mut histories = HistorySequence::default();
            while let Some(item) = session.next_item().await {
                let message = match item {
                    TraversalItem::Resumed(_) => {
                        histories = HistorySequence::default();
                        continue;
                    }
                    TraversalItem::Entry(entry) => {
                        if let Err(error) = histories.check(&entry, required_tags) {
                            let reason = error.to_string();
                            stored_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get_or_insert(error);
                            let _ = tx
                                .send(StorageEntryMessage::Error {
                                    event: ErrorEvent::Scan,
                                    path: root.clone(),
                                    entry: None,
                                    reason,
                                })
                                .await;
                            return;
                        }
                        let projected = *entry;
                        let relative = projected
                            .get_relative_path()
                            .strip_prefix(&root)
                            .unwrap_or(projected.get_relative_path());
                        let message = if projected.get_is_dir() && package.packaged_path(&relative.to_string_lossy()) {
                            StorageEntryMessage::Packaged(Arc::new(projected))
                        } else {
                            StorageEntryMessage::Scanned(Arc::new(projected))
                        };
                        if tx.send(message).await.is_err() {
                            return;
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
                                source: Box::new(failure.failure),
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
    /// 交付原生目录块；历史顺序和必需事实在编号／传输前完成准入。
    pub fn directory_inventory(&self, options: &WalkOptions) -> Result<DirectoryInventory> {
        Ok(DirectoryInventory {
            required_tags: tags_required(&self.roles, options.include_tags)?,
            session: self.traversal(None, options, true)?,
            histories: HistorySequence::default(),
            ended: false,
        })
    }
}
/// 有界原生目录流，不持有 NDX、wire 页或整棵目录的观察。
pub struct DirectoryInventory {
    required_tags: bool,
    session: TraversalSession,
    histories: HistorySequence,
    ended: bool,
}

impl DirectoryInventory {
    pub async fn next(&mut self) -> Option<Result<TraversalItem>> {
        if self.ended {
            return None;
        }
        match self.session.next_item().await {
            Some(TraversalItem::Resumed(event)) => {
                self.histories = HistorySequence::default();
                Some(Ok(TraversalItem::Resumed(event)))
            }
            Some(TraversalItem::Entry(entry)) => match self.histories.check(&entry, self.required_tags) {
                Ok(()) => Some(Ok(TraversalItem::Entry(entry))),
                Err(error) => {
                    self.ended = true;
                    Some(Err(error))
                }
            },
            Some(item) => Some(Ok(item)),
            None => {
                self.ended = true;
                if self.histories.is_complete() {
                    None
                } else {
                    Some(Err(StorageError::IncompleteHistory))
                }
            }
        }
    }

    pub async fn finish(self) -> std::result::Result<TraversalOutcome, TraversalTerminalFailure> {
        self.session.finish().await
    }
}

/// 按交付顺序返回产品条目及其 opaque 遍历检查点。
pub enum ResumableScanItem {
    Resumed(Box<TraversalResume>),
    Entry(Box<ObservedEntry>),
    Statistics(Box<DirectoryStatistics>),
    Checkpoint(Vec<u8>),
}

pub struct ResumableScan {
    include_tags: bool,
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
                TraversalItem::Resumed(event) => {
                    self.history = None;
                    return Some(Ok(ResumableScanItem::Resumed(event)));
                }
                TraversalItem::DirectoryStatistics(statistics) if self.history.is_none() => {
                    return Some(Ok(ResumableScanItem::Statistics(statistics)));
                }
                TraversalItem::Entry(entry) => {
                    if self.include_tags {
                        if let Err(error) = validate_required_tags(&entry) {
                            return Some(Err(error));
                        }
                    }
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
                        source: Box::new(failure.failure),
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod stopping_tests {
    use super::*;
    use crate::storage_factory::{CreateStorageOptions, create_storage};
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
            include_tags: false,
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

pub type WalkDirAsyncIterator = AsyncReceiver<StorageEntryMessage>;

#[derive(Debug, Clone)]
pub struct WalkOptions {
    /// 单个远程存储操作的等待上限；独立于自动恢复，不包含消费者背压。
    pub operation_timeout: Option<std::time::Duration>,
    /// 调用方显式选择的故障恢复窗口；None 直接报告失败，不自动重连。
    pub recovery_window: Option<std::time::Duration>,
    pub depth: Option<usize>,
    pub match_expressions: Option<FilterExpression>,
    pub exclude_expressions: Option<FilterExpression>,
    pub concurrency: usize,
    pub include_tags: bool,
    pub packaged: bool,
    pub package_depth: usize,
}

impl Default for WalkOptions {
    fn default() -> Self {
        Self {
            operation_timeout: None,
            recovery_window: None,
            depth: None,
            match_expressions: None,
            exclude_expressions: None,
            concurrency: 1,
            include_tags: false,
            packaged: false,
            package_depth: 0,
        }
    }
}

use data_mover::filter::FilterExpression;
