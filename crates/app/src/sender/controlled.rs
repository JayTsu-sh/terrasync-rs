//! `NoDB` 的消费者完成驱动分派；复制和终态发布仍由进程内 `Receiver` 唯一负责。
use std::{
    collections::{HashMap, VecDeque},
    mem::{replace, size_of, take},
    path::PathBuf,
    result::Result as StdResult,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use data_mover::traversal::ScanFailureSignal;
use data_mover::{
    model::{EntryIdentityKey, EntryKind, MetadataObservationFailure, ObservedEntry, StoragePath},
    traversal::{ObservationFailure, TraversalItem, TraversalOutcome, TraversalSession, TraversalTerminalFailure},
};
use task_model::{ErrorEvent, StorageEntryMessage};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    error::{AppError, Result, StorageError},
    file_copy::selection::skip_unrepresentable,
    receiver::{
        LocalCopyCompletion, LocalCopyExecutor, LocalCopyOutcome, LocalCopyPhase, LocalCopyPhaseProof, LocalCopyWork,
    },
};

pub(crate) struct ControlledCopyCounters {
    pub(crate) entries: Arc<AtomicUsize>,
    pub(crate) bytes: Arc<AtomicU64>,
    pub(crate) active: Arc<AtomicUsize>,
}

/// 原生节点预算与应用目标身份预算分别记账；W 不限制跨阶段目录的数量。
pub(crate) struct DirectoryBudget {
    pub(crate) count: usize,
    pub(crate) bytes: usize,
}

struct PreparedDirectory {
    identity: EntryIdentityKey,
    source_failure: Option<StorageError>,
    charge: usize,
}

struct DirectoryStates {
    values: HashMap<StoragePath, PreparedDirectory>,
    used: usize,
    limit: DirectoryBudget,
}

impl DirectoryStates {
    fn insert(&mut self, entry: &ObservedEntry, identity: EntryIdentityKey) -> Result<()> {
        let source_failure = captured_failure(entry);
        let failure_bytes = entry
            .metadata()
            .failures()
            .count()
            .saturating_mul(size_of::<MetadataObservationFailure>());
        let charge = size_of::<(StoragePath, PreparedDirectory)>()
            .saturating_add(entry.path().as_str().len().saturating_mul(2))
            .saturating_add(failure_bytes);
        if self.values.len() >= self.limit.count || self.used.saturating_add(charge) > self.limit.bytes {
            return Err(AppError::CopyDirectoryBudget {
                path: PathBuf::from(entry.path().as_str()),
                count_limit: self.limit.count,
                byte_limit: self.limit.bytes,
            });
        }
        if self.values.contains_key(entry.path()) {
            return Err(AppError::CopyWorkProtocol {
                reason: "directory prepared twice",
            });
        }
        self.used += charge;
        self.values.insert(
            entry.path().clone(),
            PreparedDirectory {
                identity,
                source_failure,
                charge,
            },
        );
        Ok(())
    }

    fn take(&mut self, path: &StoragePath) -> Result<PreparedDirectory> {
        let directory = self.values.remove(path).ok_or(AppError::CopyWorkProtocol {
            reason: "directory finalize without successful prepare",
        })?;
        self.used -= directory.charge;
        Ok(directory)
    }
}

struct RunningWork {
    work: LocalCopyWork,
    executor: LocalCopyExecutor,
}

/// 连续的诊断前缀最多保留一个路径及其有限元数据族，不积累整棵树的 path 历史。
#[derive(Default)]
struct ObservationPrefix {
    path: Option<StoragePath>,
    failures: Vec<MetadataObservationFailure>,
}

impl ObservationPrefix {
    fn binds(&self, entry: &ObservedEntry) -> bool {
        self.path.as_ref() == Some(entry.path())
            && self
                .failures
                .iter()
                .all(|failure| entry.metadata().failures().any(|value| value == *failure))
    }

    fn take(&mut self) -> Option<StorageError> {
        self.path
            .take()
            .map(|path| StorageError::SourceMetadataAcquisitionFailed {
                path,
                version: None,
                failures: take(&mut self.failures),
            })
    }
}

struct Coordinator {
    available: Vec<LocalCopyExecutor>,
    tasks: JoinSet<LocalCopyCompletion>,
    running: HashMap<Id, RunningWork>,
    pending: VecDeque<LocalCopyWork>,
    directories: DirectoryStates,
    observations: ObservationPrefix,
    feedback: Vec<([u8; 32], bool)>,
    counters: ControlledCopyCounters,
    reporter: LocalCopyExecutor,
    cancel: CancellationToken,
    failure: Option<AppError>,
    source_failure: Option<AppError>,
}

/// 并发上限就是可用 executor 数量；不创建等待 permit 的无限 task。
pub(crate) async fn run(
    scan: (TraversalSession, ScanFailureSignal), executors: Vec<LocalCopyExecutor>, reporter: LocalCopyExecutor,
    counters: ControlledCopyCounters, budget: DirectoryBudget, cancel: CancellationToken,
) -> Result<()> {
    if budget.count == 0 || budget.bytes == 0 {
        return Err(AppError::CopyWorkProtocol {
            reason: "directory identity budget must be positive",
        });
    }
    let mut coordinator = Coordinator {
        available: executors,
        tasks: JoinSet::new(),
        running: HashMap::new(),
        pending: VecDeque::new(),
        directories: DirectoryStates {
            values: HashMap::new(),
            used: 0,
            limit: budget,
        },
        observations: ObservationPrefix::default(),
        feedback: Vec::new(),
        counters,
        reporter,
        cancel,
        failure: None,
        source_failure: None,
    };
    let (session, source_guard) = scan;
    let mut session = Some(session);
    let mut terminal = None;
    let mut ended = false;
    let mut cancellation_seen = false;
    let mut source_seen = false;
    while !ended || !coordinator.tasks.is_empty() || !coordinator.pending.is_empty() {
        if coordinator.cancel.is_cancelled() {
            coordinator.discard_pending().await;
        } else {
            coordinator.dispatch();
        }
        if ended && coordinator.tasks.is_empty() && coordinator.pending.is_empty() {
            break;
        }
        tokio::select! {
            biased;
            result = coordinator.tasks.join_next_with_id(), if !coordinator.tasks.is_empty() => {
                if let Some(result) = result {
                    coordinator.completed(result).await;
                }
            }
            () = source_guard.failed(), if !source_seen => {
                source_seen = true;
                coordinator.cancel.cancel();
            }
            () = coordinator.cancel.cancelled(), if !cancellation_seen => {
                cancellation_seen = true;
            }
            item = async {
                match session.as_mut() {
                    Some(session) => session.next_item(&mut coordinator.feedback).await,
                    None => None,
                }
            }, if !ended => {
                if let Some(item) = item {
                    coordinator.item(item).await;
                } else {
                        ended = true;
                        if let Some(session) = session.take() {
                            let result = session.finish().await;
                            if matches!(&result, Err(error) if !matches!(error, TraversalTerminalFailure::ConsumerFailed))
                                || matches!(&result, Ok(TraversalOutcome::Cancelled))
                            { coordinator.cancel.cancel(); }
                            terminal = Some(result);
                        }
                }
            }
        }
    }
    coordinator.flush_observations().await;
    // next_item 已排空已知诊断；终态源原因优先于它派生的取消。
    let terminal = terminal.unwrap_or(Err(TraversalTerminalFailure::Internal));
    coordinator.flush_prepared_failures().await;
    if let Some(failure) = source_guard.failure() {
        return Err(StorageError::from(failure).into());
    }
    if let Err(error) = &terminal {
        match error {
            TraversalTerminalFailure::ConsumerFailed => {}
            TraversalTerminalFailure::ConsumerSource(_) if coordinator.source_failure.is_some() => {}
            _ => return Err(error.clone().into()),
        }
    }
    if matches!(&terminal, Err(TraversalTerminalFailure::ConsumerSource(_)))
        && let Some(error) = coordinator.source_failure.take()
    {
        return Err(error);
    }
    if let Some(error) = coordinator.failure {
        return Err(error);
    }
    if let Some(error) = coordinator.source_failure {
        return Err(error);
    }
    match terminal {
        Ok(TraversalOutcome::Completed(_)) if !coordinator.cancel.is_cancelled() => Ok(()),
        Ok(_) => Err(StorageError::Cancelled.into()),
        Err(error) => Err(error.into()),
    }
}

impl Coordinator {
    fn fail(&mut self, error: AppError) {
        self.failure.get_or_insert(error);
        self.cancel.cancel();
    }

    fn dispatch(&mut self) {
        while !self.available.is_empty() && !self.pending.is_empty() {
            let Some(executor) = self.available.pop() else { break };
            let Some(work) = self.pending.pop_front() else {
                self.available.push(executor);
                break;
            };
            let running = RunningWork {
                work: work.clone(),
                executor: executor.clone(),
            };
            self.counters.active.fetch_add(1, Ordering::Relaxed);
            let handle = self.tasks.spawn(async move { executor.execute(work).await });
            self.running.insert(handle.id(), running);
        }
    }

    async fn item(&mut self, item: TraversalItem) {
        if let TraversalItem::ObservationFailure(failure) = item {
            self.observation(*failure).await;
            return;
        }
        let scan_metadata = match &item {
            TraversalItem::Failure(failure) if self.observations.path.as_ref() == Some(&failure.path) => {
                self.observations.take()
            }
            _ => None,
        };
        let binds = match &item {
            TraversalItem::ConsumerEntry(work) => self.observations.binds(&work.1),
            _ => false,
        };
        if binds {
            self.observations.take();
        } else {
            self.flush_observations().await;
        }
        match item {
            TraversalItem::ConsumerEntry(work) => {
                let (id, entry) = *work;
                let entry = Arc::new(entry);
                if self.cancel.is_cancelled() {
                    self.report_captured(&entry).await;
                    return;
                }
                match skip_unrepresentable(&entry, &self.reporter.destination, &self.reporter.broadcaster).await {
                    Ok(true) => {
                        self.feedback.push((id, true));
                        return;
                    }
                    Err(error) => {
                        self.fail(error);
                        return;
                    }
                    Ok(false) => {}
                }
                self.counters.entries.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .bytes
                    .fetch_add(entry.size().unwrap_or_default(), Ordering::Relaxed);
                let phase = if entry.kind() == EntryKind::Directory {
                    LocalCopyPhase::Prepare
                } else {
                    LocalCopyPhase::Copy
                };
                self.pending.push_back(LocalCopyWork {
                    entry,
                    control: Some((id, phase)),
                });
            }
            TraversalItem::SubtreeReady(work) => {
                let (id, entry) = *work;
                let entry = Arc::new(entry);
                if self.cancel.is_cancelled() {
                    // 已准备目录的失败证据统一由最终排空交付，避免双计。
                    return;
                }
                match self.directories.take(entry.path()) {
                    Ok(directory) => self.pending.push_back(LocalCopyWork {
                        entry,
                        control: Some((
                            id,
                            LocalCopyPhase::Finalize {
                                identity: directory.identity,
                            },
                        )),
                    }),
                    Err(error) => self.fail(error),
                }
            }
            TraversalItem::Failure(failure) => {
                let error = StorageError::TraversalEntry {
                    path: failure.path.clone(),
                    scope: failure.scope,
                    delivered: failure.delivered,
                    source: Box::new(failure.failure),
                };
                let error = match scan_metadata {
                    Some(metadata) => StorageError::TransferAndMetadata {
                        data: Box::new(error),
                        metadata: Box::new(metadata),
                    },
                    None => error,
                };
                self.report_source(error).await;
                self.cancel.cancel();
            }
            TraversalItem::DirectoryStatistics(_) | TraversalItem::DirectoryListed(_) => {}
            _ => self.fail(AppError::CopyWorkProtocol {
                reason: "ordinary entry or checkpoint in controlled traversal",
            }),
        }
    }

    async fn completed(&mut self, result: StdResult<(Id, LocalCopyCompletion), JoinError>) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let Some(running) = self.running.remove(&id) else {
            self.fail(AppError::CopyWorkProtocol {
                reason: "completion has no issued worker identity",
            });
            return;
        };
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
        let mut completion = match result {
            Ok((_, completion)) => completion,
            Err(source) => {
                let phase = phase_name(&running.work);
                let error = AppError::CopyWorkerFailed {
                    path: PathBuf::from(running.work.entry.path().as_str()),
                    phase,
                    source,
                };
                self.cancel.cancel();
                LocalCopyCompletion {
                    work: running.work.clone(),
                    result: Err(error),
                    phase_proof: LocalCopyPhaseProof::default(),
                }
            }
        };
        if completion.work.control != running.work.control || !Arc::ptr_eq(&completion.work.entry, &running.work.entry)
        {
            self.fail(AppError::CopyWorkProtocol {
                reason: "worker result does not match its issued phase",
            });
            return;
        }
        let Some((work_id, _phase)) = completion.work.control else {
            self.fail(AppError::CopyWorkProtocol {
                reason: "controlled completion lost its phase",
            });
            return;
        };
        // 中断数据操作不能抹去先前已获得的源元数据失败。
        if completion
            .result
            .as_ref()
            .is_err_and(|error| error.storage_error().is_some_and(StorageError::is_cancelled))
            && let Some(failure) = captured_failure(&completion.work.entry)
        {
            completion.result = Err(failure.into());
        }
        if let Err(error) = &completion.result
            && !contains_metadata_failure(error)
            && let Some(metadata) = captured_failure(&completion.work.entry)
        {
            let Err(source) = replace(&mut completion.result, Ok(LocalCopyOutcome::Copied(None))) else {
                return;
            };
            completion.result = Err(AppError::CopyAndSourceMetadataFailed {
                source: Box::new(source),
                metadata: Box::new(metadata),
            });
        }
        // entry错误仍发布失败；这里只消费执行点建立的独立命名空间阶段证据。
        let mut phase_ready = completion.phase_proof.is_ready();
        if let Ok(LocalCopyOutcome::Prepared(identity)) = &completion.result
            && let Err(error) = self.directories.insert(&completion.work.entry, *identity)
        {
            phase_ready = false;
            self.fail(error);
            self.report_captured(&completion.work.entry).await;
        }
        let cancelled_only = completion
            .result
            .as_ref()
            .is_err_and(|error| error.storage_error().is_some_and(StorageError::is_cancelled));
        if !cancelled_only && let Err(error) = running.executor.publish(completion).await {
            if error.is_reported_copy_failure() {
                self.failure.get_or_insert(error);
            } else {
                self.fail(error);
            }
        }
        if !self.cancel.is_cancelled() {
            // 严格按工作 ID + phase 反馈；普通 New 广播不参与 ACK 推断。
            self.feedback.push((work_id, phase_ready));
        }
        self.available.push(running.executor);
    }

    async fn observation(&mut self, failure: ObservationFailure) {
        if self
            .observations
            .path
            .as_ref()
            .is_some_and(|path| path != &failure.path)
        {
            self.flush_observations().await;
        }
        self.observations.path.get_or_insert(failure.path);
        let value = MetadataObservationFailure {
            family: failure.family,
            class: failure.class,
            transience: failure.transience,
        };
        if !self.observations.failures.contains(&value) {
            self.observations.failures.push(value);
        }
    }

    async fn flush_observations(&mut self) {
        if let Some(failure) = self.observations.take() {
            self.report_source(failure).await;
        }
    }

    async fn report_source(&mut self, error: StorageError) {
        let path = diagnostic_path(&error)
            .map(|path| PathBuf::from(path.as_str()))
            .unwrap_or_default();
        let delivered = self
            .reporter
            .broadcaster
            .broadcast(StorageEntryMessage::Error {
                event: ErrorEvent::Scan,
                path,
                entry: None,
                reason: error.to_string(),
            })
            .await;
        let fatal = matches!(
            &error,
            StorageError::TraversalEntry { .. } | StorageError::TransferAndMetadata { .. }
        );
        let reported = AppError::ReportedCopyFailure {
            source: Box::new(error.into()),
            data_failed: false,
        };
        if fatal {
            self.source_failure = Some(reported);
        } else {
            self.source_failure.get_or_insert(reported);
        }
        if let Err(error) = delivered {
            self.fail(error.into());
        }
    }

    async fn report_captured(&mut self, entry: &ObservedEntry) {
        if let Some(failure) = captured_failure(entry) {
            self.report_source(failure).await;
        }
    }

    async fn discard_pending(&mut self) {
        while let Some(work) = self.pending.pop_front() {
            self.report_captured(&work.entry).await;
        }
    }

    async fn flush_prepared_failures(&mut self) {
        let pending = take(&mut self.directories.values);
        for directory in pending.into_values() {
            if let Some(error) = directory.source_failure {
                self.report_source(error).await;
            }
        }
    }
}

fn captured_failure(entry: &ObservedEntry) -> Option<StorageError> {
    let failures: Vec<_> = entry.metadata().failures().collect();
    (!failures.is_empty()).then(|| StorageError::SourceMetadataAcquisitionFailed {
        path: entry.path().clone(),
        version: None,
        failures,
    })
}

fn contains_metadata_failure(error: &AppError) -> bool {
    fn stored(error: &StorageError) -> bool {
        match error {
            StorageError::SourceMetadataAcquisitionFailed { .. } => true,
            StorageError::PublishedDataMetadata { source, .. } => stored(source),
            StorageError::TransferAndMetadata { data, metadata } => stored(data) || stored(metadata),
            _ => false,
        }
    }
    matches!(error, AppError::CopyAndSourceMetadataFailed { .. }) || error.storage_error().is_some_and(stored)
}

fn diagnostic_path(error: &StorageError) -> Option<&StoragePath> {
    match error {
        StorageError::TraversalEntry { path, .. } | StorageError::SourceMetadataAcquisitionFailed { path, .. } => {
            Some(path)
        }
        StorageError::TransferAndMetadata { data, .. } => diagnostic_path(data),
        _ => None,
    }
}

fn phase_name(work: &LocalCopyWork) -> &'static str {
    match work.control.map(|(_, phase)| phase) {
        Some(LocalCopyPhase::Prepare) => "directory_prepare",
        Some(LocalCopyPhase::Finalize { .. }) => "directory_finalize",
        _ => "copy_entry",
    }
}

#[cfg(all(test, unix))]
#[path = "controlled_tests.rs"]
mod tests;
