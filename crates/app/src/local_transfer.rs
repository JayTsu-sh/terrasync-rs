//! Protocol-neutral, bounded in-process transfer orchestration.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use data_mover::model::{EntryKind, EntryOperationFailure, ObservedEntry};
use data_mover::storage::{ExistingDestinationPolicy, RecoveryIdentity, Storage};
use data_mover::transfer::{
    InflightLimits, RecoveryPolicy, TransferFailure, TransferIdentity, TransferOutcome, TransferRequest, transfer,
};
use data_mover::traversal::{TraversalItem, TraversalOutcome, TraversalSession};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result};

/// Inputs shared by all files admitted from one traversal.
#[derive(Clone)]
pub struct LocalTransferConfig {
    pub job_identity: String,
    pub source: Storage,
    pub destination: Storage,
    pub inflight: InflightLimits,
    pub max_concurrent_files: NonZeroUsize,
    pub existing_destination: ExistingDestinationPolicy,
    pub recovery_policy: RecoveryPolicy,
    pub cancel: CancellationToken,
}

/// Transfer results remain typed and owned until the application records a recovery decision.
#[async_trait]
pub trait LocalTransferSink: Send {
    /// Loads caller-persisted opaque recovery state without exposing backend facts here.
    async fn recovery_identity(&mut self, _entry: &ObservedEntry) -> Option<RecoveryIdentity> {
        None
    }
    async fn completed(&mut self, entry: ObservedEntry, outcome: TransferOutcome);
    async fn entry_failed(&mut self, failure: EntryOperationFailure);
    async fn transfer_failed(&mut self, entry: ObservedEntry, failure: TransferFailure);
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

type Attempt = (ObservedEntry, std::result::Result<TransferOutcome, TransferFailure>);

/// Drains a bounded traversal into the unified data-mover transfer API.
///
/// Source and destination are independent `Storage` values. This layer neither inspects their
/// protocols nor dispatches on backend pairs. Namespace and metadata work for non-files belongs
/// to their dedicated workflow and is reported as skipped here.
pub async fn run_local_transfer(
    mut session: TraversalSession, config: LocalTransferConfig, sink: &mut dyn LocalTransferSink,
) -> Result<LocalTransferReport> {
    validate_job_identity(&config.job_identity)?;
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
                    if let Err(error) = settle_one(&mut attempts, sink, &mut report).await {
                        return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                    }
                }
                let recovery = sink.recovery_identity(&entry).await;
                let request = match request_for(&config, &entry, recovery) {
                    Ok(request) => request,
                    Err(error) => {
                        return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                    }
                };
                attempts.spawn(async move {
                    let result = transfer(request).await;
                    (entry, result)
                });
                report.peak_inflight_files = report.peak_inflight_files.max(attempts.len());
            }
            TraversalItem::EntryFailure(failure) => {
                report.entry_failures += 1;
                sink.entry_failed(failure).await;
            }
        }
    }

    while !attempts.is_empty() {
        if let Err(error) = settle_one(&mut attempts, sink, &mut report).await {
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

fn request_for(
    config: &LocalTransferConfig, entry: &ObservedEntry, recovery: Option<RecoveryIdentity>,
) -> Result<TransferRequest> {
    let identity = TransferIdentity::new(format!(
        "{}:{}",
        config.job_identity,
        encode_identity(entry.identity_key().as_bytes())
    ))
    .map_err(|error| AppError::ConfigError(error.to_string()))?;
    Ok(TransferRequest::new(
        identity,
        config.source.clone(),
        entry.path().clone(),
        config.destination.clone(),
        entry.path().clone(),
        config.inflight,
        config.cancel.clone(),
    )
    .with_existing_destination_policy(config.existing_destination)
    .with_recovery(config.recovery_policy, recovery))
}

fn validate_job_identity(identity: &str) -> Result<()> {
    TransferIdentity::new(format!("{identity}:{}", "0".repeat(64)))
        .map(|_| ())
        .map_err(|error| AppError::ConfigError(error.to_string()))
}

async fn drain_after_error(
    primary: AppError, config: &LocalTransferConfig, attempts: &mut JoinSet<Attempt>, sink: &mut dyn LocalTransferSink,
    report: &mut LocalTransferReport,
) -> Result<LocalTransferReport> {
    config.cancel.cancel();
    while !attempts.is_empty() {
        let _ = settle_one(attempts, sink, report).await;
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
    attempts: &mut JoinSet<Attempt>, sink: &mut dyn LocalTransferSink, report: &mut LocalTransferReport,
) -> Result<()> {
    let Some(attempt) = attempts.join_next().await else {
        return Ok(());
    };
    let (entry, outcome) = attempt?;
    match outcome {
        Ok(outcome) => {
            report.completed_files += 1;
            report.transferred_bytes += outcome.transferred_bytes;
            sink.completed(entry, outcome).await;
        }
        Err(failure) => {
            report.transfer_failures += 1;
            sink.transfer_failed(entry, failure).await;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;

    use data_mover::model::{
        BackendIdentity, BackendKind, FailureClass, IdentityStrength, Operation, SourceIdentity, StoragePath,
        Transience,
    };
    use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
    use data_mover::traversal::{TraversalCompletion, TraversalProducer};
    use tempfile::TempDir;

    use super::*;

    #[derive(Default)]
    struct Sink {
        completed: Vec<String>,
        entry_failures: usize,
        transfer_failures: Vec<TransferFailure>,
        recovery_lookups: usize,
        recovery: Option<RecoveryIdentity>,
    }

    #[async_trait]
    impl LocalTransferSink for Sink {
        async fn recovery_identity(&mut self, _entry: &ObservedEntry) -> Option<RecoveryIdentity> {
            self.recovery_lookups += 1;
            self.recovery.clone()
        }

        async fn completed(&mut self, entry: ObservedEntry, _outcome: TransferOutcome) {
            self.completed.push(entry.path().as_str().to_owned());
        }

        async fn entry_failed(&mut self, _failure: EntryOperationFailure) {
            self.entry_failures += 1;
        }

        async fn transfer_failed(&mut self, _entry: ObservedEntry, failure: TransferFailure) {
            self.transfer_failures.push(failure);
        }
    }

    async fn storage(root: &Path, name: &str) -> Storage {
        connect_backend(BackendConfig::Local(LocalBackendConfig {
            root: root.to_path_buf(),
            identity: BackendIdentity::new(BackendKind::Local, name).unwrap(),
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

    async fn config(source: &Path, destination: &Path, maximum: usize) -> LocalTransferConfig {
        LocalTransferConfig {
            job_identity: "job-147".to_owned(),
            source: storage(source, "source").await,
            destination: storage(destination, "destination").await,
            inflight: InflightLimits::new(2, 128 * 1024, 2).unwrap(),
            max_concurrent_files: NonZeroUsize::new(maximum).unwrap(),
            existing_destination: ExistingDestinationPolicy::default(),
            recovery_policy: RecoveryPolicy::ResumeOrRestart,
            cancel: CancellationToken::new(),
        }
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

        let report = run_local_transfer(session, config(source.path(), destination.path(), 1).await, &mut sink)
            .await
            .unwrap();

        assert_eq!(report.completed_files, 2);
        assert_eq!(report.transferred_bytes, 9);
        assert_eq!(report.peak_inflight_files, 1);
        assert_eq!(std::fs::read(destination.path().join("a")).unwrap(), b"alpha");
        assert_eq!(std::fs::read(destination.path().join("b")).unwrap(), b"beta");
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

        let report = run_local_transfer(session, config(source.path(), destination.path(), 2).await, &mut sink)
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
            config(source.path(), destination.path(), 1).await,
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
        let config = config(source.path(), destination.path(), 1).await;
        config.cancel.cancel();
        let mut sink = Sink::default();

        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.transfer_failures, 1);
        assert_eq!(sink.transfer_failures.len(), 1);
        assert_eq!(sink.recovery_lookups, 1);
        assert!(!destination.path().join("cancelled").exists());
    }

    #[tokio::test]
    async fn forwards_persisted_recovery_identity_to_unified_transfer() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        std::fs::write(source.path().join("recover"), b"payload").unwrap();
        let session = session(
            vec![entry("recover", 7)],
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 1,
                entry_failures: 0,
            }),
        );
        let mut config = config(source.path(), destination.path(), 1).await;
        config.recovery_policy = RecoveryPolicy::RequireResume;
        let mut sink = Sink {
            recovery: Some(RecoveryIdentity::from_bytes(&b"invalid-stage"[..]).unwrap()),
            ..Sink::default()
        };

        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(sink.recovery_lookups, 1);
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
            config(source.path(), destination.path(), 1).await,
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, AppError::TraversalCountMismatch { .. }));
    }

    #[test]
    fn production_queue_has_no_backend_pair_or_transport_dispatch() {
        let production = include_str!("local_transfer.rs").split("#[cfg(test)]").next().unwrap();
        for forbidden in ["BackendKind", "StorageEnum", "Ndx", "NDX", "Quic", "QUIC"] {
            assert!(!production.contains(forbidden), "production queue contains {forbidden}");
        }
    }
}
