//! Protocol-neutral, bounded in-process transfer orchestration.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use data_mover::model::{EntryKind, EntryOperationFailure, ObservedEntry};
use data_mover::storage::{ExistingDestinationPolicy, RecoveryIdentity, Storage};
use data_mover::transfer::{
    InflightLimits, RecoveryContext, RecoveryProvider, RecoveryRegistrar, RecoveryRegistrationFailure, Resumability,
    TransferFailure, TransferIdentity, TransferOutcome, TransferRequest, transfer,
};
use data_mover::traversal::{TraversalItem, TraversalOutcome, TraversalSession};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result};
use db::{ClickHouseDatabase, RecoveryAttemptId};

/// Inputs shared by all files admitted from one traversal.
#[derive(Clone)]
pub struct LocalTransferConfig {
    pub job_identity: String,
    pub source: Storage,
    pub destination: Storage,
    pub inflight: InflightLimits,
    pub max_concurrent_files: NonZeroUsize,
    pub existing_destination: ExistingDestinationPolicy,
    pub resumability: Resumability,
    pub recovery: Option<Arc<dyn EntryRecoveryState>>,
    pub cancel: CancellationToken,
}

/// Caller-owned recovery state for one observed entry and one transfer attempt.
pub struct RecoveryRegistration {
    identity: Option<RecoveryIdentity>,
    claim: [u8; 32],
    registrar: Arc<dyn RecoveryRegistrar>,
}

impl RecoveryRegistration {
    #[must_use]
    pub fn new(identity: Option<RecoveryIdentity>, claim: [u8; 32], registrar: Arc<dyn RecoveryRegistrar>) -> Self {
        Self {
            identity,
            claim,
            registrar,
        }
    }

    fn into_context(self) -> RecoveryContext {
        RecoveryContext::new(self.identity, self.claim, self.registrar)
    }
}

struct LazyEntryRecoveryProvider {
    store: Arc<dyn EntryRecoveryState>,
    entry: ObservedEntry,
    opened: Arc<AtomicBool>,
}

pub(crate) fn lazy_entry_recovery_provider(
    store: Arc<dyn EntryRecoveryState>, entry: ObservedEntry,
) -> (Arc<dyn RecoveryProvider>, Arc<AtomicBool>) {
    let opened = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(LazyEntryRecoveryProvider {
        store,
        entry,
        opened: Arc::clone(&opened),
    });
    (provider as Arc<dyn RecoveryProvider>, opened)
}

#[async_trait]
impl RecoveryProvider for LazyEntryRecoveryProvider {
    async fn open(&self) -> std::result::Result<RecoveryContext, RecoveryRegistrationFailure> {
        let registration = self.store.open(&self.entry).await.map_err(|error| match error {
            AppError::DatabaseError(
                db::DatabaseError::ConcurrencyError(_) | db::DatabaseError::RecoveryAttemptCompleted,
            ) => RecoveryRegistrationFailure::Rejected,
            _ => RecoveryRegistrationFailure::Unavailable,
        })?;
        self.opened.store(true, Ordering::Release);
        Ok(registration.into_context())
    }
}

/// Entry-scoped recovery lifecycle owned by the application persistence layer.
#[async_trait]
pub trait EntryRecoveryState: Send + Sync {
    async fn open(&self, entry: &ObservedEntry) -> Result<RecoveryRegistration>;
    async fn completed(&self, entry: &ObservedEntry) -> Result<()>;
}

/// `ClickHouse` adapter that keeps recovery state in the job's observation big table.
pub struct ClickHouseEntryRecovery {
    database: Arc<ClickHouseDatabase>,
    attempt: RecoveryAttemptId,
}

impl ClickHouseEntryRecovery {
    #[must_use]
    pub fn new(database: Arc<ClickHouseDatabase>, attempt: RecoveryAttemptId) -> Self {
        Self { database, attempt }
    }
}

#[async_trait]
impl EntryRecoveryState for ClickHouseEntryRecovery {
    async fn open(&self, entry: &ObservedEntry) -> Result<RecoveryRegistration> {
        let durable = self
            .database
            .open_recovery_attempt(entry.identity_key(), self.attempt.clone())
            .await?;
        Ok(RecoveryRegistration::new(
            durable.identity().cloned(),
            durable.claim(),
            durable.registrar(),
        ))
    }

    async fn completed(&self, entry: &ObservedEntry) -> Result<()> {
        self.database
            .complete_recovery_attempt(entry.identity_key(), &self.attempt)
            .await?;
        Ok(())
    }
}

/// Transfer results remain typed and owned until the application records a recovery decision.
#[async_trait]
pub trait LocalTransferSink: Send {
    async fn completed(&mut self, entry: ObservedEntry, outcome: TransferOutcome) -> Result<()>;
    async fn entry_failed(&mut self, failure: EntryOperationFailure) -> Result<()>;
    async fn transfer_failed(&mut self, entry: ObservedEntry, failure: TransferFailure) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalTransferReport {
    pub observed_entries: u64,
    pub completed_files: u64,
    pub transferred_bytes: u64,
    pub entry_failures: u64,
    pub transfer_failures: u64,
    pub skipped_non_files: u64,
    pub peak_inflight_files: usize,
    pub cancelled: bool,
}

type Attempt = (
    ObservedEntry,
    Arc<AtomicBool>,
    std::result::Result<TransferOutcome, TransferFailure>,
);

/// Drains a bounded traversal into the unified data-mover transfer API.
///
/// Source and destination are independent `Storage` values. This layer neither inspects their
/// protocols nor dispatches on backend pairs. Namespace and metadata work for non-files belongs
/// to their dedicated workflow and is reported as skipped here.
pub async fn run_local_transfer(
    mut session: TraversalSession, config: LocalTransferConfig, sink: &mut dyn LocalTransferSink,
) -> Result<LocalTransferReport> {
    validate_job_identity(&config.job_identity)?;
    validate_recovery_configuration(&config)?;
    let mut report = LocalTransferReport::default();
    let mut attempts = JoinSet::<Attempt>::new();

    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                report.observed_entries += 1;
                let entry = *entry;
                if entry.kind() != EntryKind::File {
                    report.skipped_non_files += 1;
                    continue;
                }
                while attempts.len() >= config.max_concurrent_files.get() {
                    if let Err(error) = settle_one(&mut attempts, &config, sink, &mut report).await {
                        return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                    }
                }
                let (request, recovery_opened) = match request_for(&config, &entry) {
                    Ok(request) => request,
                    Err(error) => {
                        return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                    }
                };
                attempts.spawn(async move {
                    let result = transfer(request).await;
                    (entry, recovery_opened, result)
                });
                report.peak_inflight_files = report.peak_inflight_files.max(attempts.len());
            }
            TraversalItem::EntryFailure(failure) => {
                report.entry_failures += 1;
                if let Err(error) = sink.entry_failed(failure).await {
                    return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                }
            }
        }
    }

    while !attempts.is_empty() {
        if let Err(error) = settle_one(&mut attempts, &config, sink, &mut report).await {
            return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
        }
    }
    match session.finish().await? {
        TraversalOutcome::Cancelled => report.cancelled = true,
        TraversalOutcome::Completed(completion) => {
            if completion.observed_entries != report.observed_entries
                || completion.entry_failures != report.entry_failures
            {
                return Err(AppError::TraversalCountMismatch {
                    consumed_entries: report.observed_entries,
                    reported_entries: completion.observed_entries,
                    consumed_failures: report.entry_failures,
                    reported_failures: completion.entry_failures,
                });
            }
        }
    }
    Ok(report)
}

fn request_for(config: &LocalTransferConfig, entry: &ObservedEntry) -> Result<(TransferRequest, Arc<AtomicBool>)> {
    let identity = TransferIdentity::new(format!(
        "{}:{}",
        config.job_identity,
        encode_identity(entry.identity_key().as_bytes())
    ))
    .map_err(|error| AppError::ConfigError(error.to_string()))?;
    let request = TransferRequest::new(
        identity,
        config.source.clone(),
        entry.path().clone(),
        config.destination.clone(),
        entry.path().clone(),
        config.inflight,
        config.cancel.clone(),
    )
    .with_existing_destination_policy(config.existing_destination);
    let (provider, opened) = config.recovery.as_ref().map_or_else(
        || (None, Arc::new(AtomicBool::new(false))),
        |store| {
            let (provider, opened) = lazy_entry_recovery_provider(Arc::clone(store), entry.clone());
            (Some(provider), opened)
        },
    );
    Ok((request.with_recovery(config.resumability, provider), opened))
}

fn validate_job_identity(identity: &str) -> Result<()> {
    TransferIdentity::new(format!("{identity}:{}", "0".repeat(64)))
        .map(|_| ())
        .map_err(|error| AppError::ConfigError(error.to_string()))
}

fn validate_recovery_configuration(config: &LocalTransferConfig) -> Result<()> {
    match (config.resumability, config.recovery.is_some()) {
        (Resumability::Enabled, false) => Err(AppError::ConfigError(
            "enabled resumability requires entry recovery persistence".to_string(),
        )),
        (Resumability::Disabled, true) => Err(AppError::ConfigError(
            "disabled resumability cannot accept entry recovery persistence".to_string(),
        )),
        _ => Ok(()),
    }
}

async fn drain_after_error(
    primary: AppError, config: &LocalTransferConfig, attempts: &mut JoinSet<Attempt>, sink: &mut dyn LocalTransferSink,
    report: &mut LocalTransferReport,
) -> Result<LocalTransferReport> {
    config.cancel.cancel();
    while !attempts.is_empty() {
        let _ = settle_one(attempts, config, sink, report).await;
    }
    Err(primary)
}

fn encode_identity(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

async fn settle_one(
    attempts: &mut JoinSet<Attempt>, config: &LocalTransferConfig, sink: &mut dyn LocalTransferSink,
    report: &mut LocalTransferReport,
) -> Result<()> {
    let Some(attempt) = attempts.join_next().await else {
        return Ok(());
    };
    let (entry, recovery_opened, outcome) = attempt?;
    match outcome {
        Ok(outcome) => {
            report.completed_files += 1;
            report.transferred_bytes += outcome.transferred_bytes;
            if recovery_opened.load(Ordering::Acquire)
                && let Some(recovery) = &config.recovery
            {
                recovery.completed(&entry).await?;
            }
            sink.completed(entry, outcome).await?;
        }
        Err(failure) => {
            report.transfer_failures += 1;
            sink.transfer_failed(entry, failure).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use data_mover::model::{
        BackendIdentity, BackendKind, FailureClass, IdentityStrength, Operation, SourceIdentity, StoragePath,
        Transience,
    };
    use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
    use data_mover::transfer::RecoveryRegistrationFailure;
    use data_mover::traversal::{TraversalCompletion, TraversalProducer};
    use tempfile::TempDir;

    use super::*;

    #[derive(Default)]
    struct Sink {
        completed: Vec<String>,
        entry_failures: usize,
        transfer_failures: Vec<TransferFailure>,
        fail_completion: bool,
    }

    #[derive(Default)]
    struct Recovery {
        lookups: AtomicUsize,
        completions: AtomicUsize,
        identity: Mutex<Option<RecoveryIdentity>>,
        claim: [u8; 32],
        registrations: Arc<Mutex<Vec<RecoveryIdentity>>>,
    }

    struct SinkRecoveryRegistrar {
        registrations: Arc<Mutex<Vec<RecoveryIdentity>>>,
    }

    #[async_trait]
    impl RecoveryRegistrar for SinkRecoveryRegistrar {
        async fn register(&self, identity: RecoveryIdentity) -> std::result::Result<(), RecoveryRegistrationFailure> {
            self.registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(identity);
            Ok(())
        }
    }

    #[async_trait]
    impl EntryRecoveryState for Recovery {
        async fn open(&self, _entry: &ObservedEntry) -> Result<RecoveryRegistration> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            Ok(RecoveryRegistration::new(
                self.identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
                self.claim,
                Arc::new(SinkRecoveryRegistrar {
                    registrations: Arc::clone(&self.registrations),
                }),
            ))
        }

        async fn completed(&self, _entry: &ObservedEntry) -> Result<()> {
            self.completions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[async_trait]
    impl LocalTransferSink for Sink {
        async fn completed(&mut self, entry: ObservedEntry, _outcome: TransferOutcome) -> Result<()> {
            self.completed.push(entry.path().as_str().to_owned());
            if self.fail_completion {
                return Err(AppError::ConfigError("durable completion failed".to_string()));
            }
            Ok(())
        }

        async fn entry_failed(&mut self, _failure: EntryOperationFailure) -> Result<()> {
            self.entry_failures += 1;
            Ok(())
        }

        async fn transfer_failed(&mut self, _entry: ObservedEntry, failure: TransferFailure) -> Result<()> {
            self.transfer_failures.push(failure);
            Ok(())
        }
    }

    async fn storage(root: &Path, name: &str) -> Storage {
        connect_backend(BackendConfig::Local(LocalBackendConfig {
            root: root.to_path_buf(),
            identity: BackendIdentity::new(BackendKind::Local, name).unwrap(),
            read_concurrency: NonZeroUsize::new(2).unwrap(),
            write_concurrency: NonZeroUsize::new(2).unwrap(),
        }))
        .await
        .unwrap()
    }

    fn entry(path: &str, size: u64) -> TraversalItem {
        let backend = BackendIdentity::new(BackendKind::Local, "observed-source").unwrap();
        let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, path).unwrap();
        TraversalItem::Entry(Box::new(
            ObservedEntry::new(
                StoragePath::new(path).unwrap(),
                EntryKind::File,
                Some(size),
                None,
                identity,
            )
            .unwrap(),
        ))
    }

    fn entry_failure(path: &str) -> TraversalItem {
        TraversalItem::EntryFailure(
            EntryOperationFailure::new(
                StoragePath::new(path).unwrap(),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            )
            .unwrap(),
        )
    }

    fn session(items: Vec<TraversalItem>, outcome: TraversalOutcome) -> TraversalSession {
        let cancel = CancellationToken::new();
        let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(2).unwrap(), cancel);
        tokio::spawn(publish(producer, items, outcome));
        session
    }

    async fn publish(producer: TraversalProducer, items: Vec<TraversalItem>, outcome: TraversalOutcome) {
        for item in items {
            producer.send(item).await.unwrap();
        }
        producer.finish(Ok(outcome));
    }

    async fn config(source: &Path, destination: &Path, maximum: usize) -> (LocalTransferConfig, Arc<Recovery>) {
        let recovery = Arc::new(Recovery::default());
        (
            LocalTransferConfig {
                job_identity: "job-147".to_owned(),
                source: storage(source, "source").await,
                destination: storage(destination, "destination").await,
                inflight: InflightLimits::new(2, 128 * 1024, 2).unwrap(),
                max_concurrent_files: NonZeroUsize::new(maximum).unwrap(),
                existing_destination: ExistingDestinationPolicy::default(),
                resumability: Resumability::Enabled,
                recovery: Some(recovery.clone()),
                cancel: CancellationToken::new(),
            },
            recovery,
        )
    }

    #[tokio::test]
    async fn copies_files_through_bounded_unified_transfer() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        std::fs::write(source.path().join("a"), b"alpha").unwrap();
        std::fs::write(source.path().join("b"), b"beta").unwrap();
        let session = session(
            vec![entry("a", 5), entry("b", 4)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 2,
                entry_failures: 0,
            }),
        );
        let mut sink = Sink::default();

        let (config, recovery) = config(source.path(), destination.path(), 1).await;
        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.completed_files, 2);
        assert_eq!(report.transferred_bytes, 9);
        assert_eq!(report.peak_inflight_files, 1);
        assert_eq!(std::fs::read(destination.path().join("a")).unwrap(), b"alpha");
        assert_eq!(std::fs::read(destination.path().join("b")).unwrap(), b"beta");
        assert_eq!(recovery.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(recovery.completions.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn durably_registers_recoverable_stage_before_large_file_transfer() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let payload = vec![0x5a; 128 * 1024 + 1];
        std::fs::write(source.path().join("large"), &payload).unwrap();
        let session = session(
            vec![entry("large", payload.len() as u64)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 0,
            }),
        );
        let mut sink = Sink::default();

        let (config, recovery) = config(source.path(), destination.path(), 1).await;
        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.completed_files, 1);
        assert_eq!(recovery.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(
            recovery
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
        assert_eq!(recovery.completions.load(Ordering::Relaxed), 1);
        assert_eq!(std::fs::read(destination.path().join("large")).unwrap(), payload);
    }

    #[tokio::test]
    async fn keeps_entry_and_transfer_failures_in_separate_channels() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let session = session(
            vec![entry_failure("denied"), entry("missing", 1)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 1,
            }),
        );
        let mut sink = Sink::default();

        let report = run_local_transfer(session, config(source.path(), destination.path(), 2).await.0, &mut sink)
            .await
            .unwrap();

        assert_eq!(report.entry_failures, 1);
        assert_eq!(report.transfer_failures, 1);
        assert_eq!(sink.entry_failures, 1);
        assert_eq!(sink.transfer_failures.len(), 1);
    }

    #[tokio::test]
    async fn returns_cancelled_as_a_normal_terminal_outcome() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let mut sink = Sink::default();
        let report = run_local_transfer(
            session(Vec::new(), TraversalOutcome::Cancelled),
            config(source.path(), destination.path(), 1).await.0,
            &mut sink,
        )
        .await
        .unwrap();
        assert!(report.cancelled);
        assert_eq!(report.completed_files, 0);
    }

    #[tokio::test]
    async fn cancellation_reaches_an_admitted_transfer_and_is_settled() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        std::fs::write(source.path().join("cancelled"), b"payload").unwrap();
        let session = session(
            vec![entry("cancelled", 7)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 0,
            }),
        );
        let (config, recovery) = config(source.path(), destination.path(), 1).await;
        config.cancel.cancel();
        let mut sink = Sink::default();

        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.transfer_failures, 1);
        assert_eq!(sink.transfer_failures.len(), 1);
        assert_eq!(recovery.lookups.load(Ordering::Relaxed), 0);
        assert!(!destination.path().join("cancelled").exists());
    }

    #[tokio::test]
    async fn forwards_persisted_recovery_identity_to_unified_transfer() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let payload = vec![0x6d; 128 * 1024 + 1];
        std::fs::write(source.path().join("recover"), &payload).unwrap();
        let session = session(
            vec![entry("recover", payload.len() as u64)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 0,
            }),
        );
        let (mut config, _) = config(source.path(), destination.path(), 1).await;
        config.resumability = Resumability::Enabled;
        let recovery = Arc::new(Recovery {
            identity: Mutex::new(Some(RecoveryIdentity::from_bytes(&b"invalid-stage"[..]).unwrap())),
            claim: [0x5a; 32],
            ..Recovery::default()
        });
        config.recovery = Some(recovery.clone());
        let mut sink = Sink::default();

        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(recovery.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(report.transfer_failures, 1);
        assert_eq!(sink.transfer_failures.len(), 1);
        assert!(!destination.path().join("recover").exists());
    }

    #[tokio::test]
    async fn rejects_inconsistent_traversal_completion_evidence() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let mut sink = Sink::default();
        let error = run_local_transfer(
            session(
                Vec::new(),
                TraversalOutcome::Completed(TraversalCompletion {
                    observed_entries: 1,
                    entry_failures: 0,
                }),
            ),
            config(source.path(), destination.path(), 1).await.0,
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AppError::TraversalCountMismatch { .. }));
    }

    #[tokio::test]
    async fn returns_durable_completion_failure_after_payload_commit() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        std::fs::write(source.path().join("committed"), b"payload").unwrap();
        let session = session(
            vec![entry("committed", 7)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 0,
            }),
        );
        let mut sink = Sink {
            fail_completion: true,
            ..Sink::default()
        };

        let error = run_local_transfer(session, config(source.path(), destination.path(), 1).await.0, &mut sink)
            .await
            .unwrap_err();

        assert!(matches!(error, AppError::ConfigError(message) if message == "durable completion failed"));
        assert_eq!(std::fs::read(destination.path().join("committed")).unwrap(), b"payload");
    }

    #[test]
    fn production_queue_has_no_backend_pair_or_transport_dispatch() {
        let production = include_str!("local_transfer.rs").split("#[cfg(test)]").next().unwrap();
        for forbidden in ["BackendKind", "StorageEnum", "Ndx", "NDX", "Quic", "QUIC"] {
            assert!(!production.contains(forbidden), "production queue contains {forbidden}");
        }
    }
}
