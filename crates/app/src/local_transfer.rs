//! 与协议无关的有界进程内传输编排。

use async_trait::async_trait;
use data_mover::model::{EntryKind, EntryOperationFailure, ObservedEntry};
use data_mover::storage::Storage;
use data_mover::transfer::{
    TransferFailure, TransferIdentity, TransferOutcome, TransferPolicy, TransferRequest, transfer,
};
use data_mover::traversal::{TraversalItem, TraversalOutcome, TraversalSession};
use std::num::NonZeroUsize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result};

/// 同一次遍历中所有接收文件共用的输入。
#[derive(Clone)]
pub struct LocalTransferConfig {
    pub job_identity: String,
    pub source: Storage,
    pub destination: Storage,

    pub max_concurrent_files: NonZeroUsize,

    pub transfer_policy: TransferPolicy,
    pub cancel: CancellationToken,
}

/// 应用记录恢复决定前，传输结果保留类型及所有权。
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

type Attempt = (ObservedEntry, std::result::Result<TransferOutcome, TransferFailure>);

/// 排空有界遍历，并通过统一 data-mover transfer API 执行传输。
///
/// 源和目标是独立的 `Storage` 值；本层不检查协议，也不按后端组合分派。
/// 非文件条目的命名空间和元数据操作由专属流程处理，此处统计为跳过。
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
                    if let Err(error) = settle_one(&mut attempts, &config, sink, &mut report).await {
                        return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                    }
                }
                let request = match request_for(&config, &entry) {
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
            TraversalItem::Failure(failure) => {
                report.entry_failures += 1;
                if let Err(error) = sink.entry_failed(failure.failure).await {
                    return drain_after_error(error, &config, &mut attempts, sink, &mut report).await;
                }
            }
            TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_) => {}
            item => {
                return drain_after_error(
                    AppError::ConfigError(format!("unsupported traversal event: {item:?}")),
                    &config,
                    &mut attempts,
                    sink,
                    &mut report,
                )
                .await;
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
            if completion.totals.emitted != report.observed_entries
                || (completion.totals.failed_children + completion.totals.listing_failures) != report.entry_failures
            {
                return Err(AppError::TraversalCountMismatch {
                    consumed_entries: report.observed_entries,
                    reported_entries: completion.totals.emitted,
                    consumed_failures: report.entry_failures,
                    reported_failures: (completion.totals.failed_children + completion.totals.listing_failures),
                });
            }
        }
    }
    Ok(report)
}

fn request_for(config: &LocalTransferConfig, entry: &ObservedEntry) -> Result<TransferRequest> {
    let identity = TransferIdentity::from_label(format!(
        "{}:{}",
        config.job_identity,
        encode_identity(entry.identity_key().as_bytes())
    ))?;
    let request = TransferRequest::from_observation(
        config.source.clone(),
        entry.clone(),
        config.destination.clone(),
        config.cancel.clone(),
    )?
    .with_identity_override(identity);
    Ok(request.with_transfer_policy(config.transfer_policy))
}

fn validate_job_identity(identity: &str) -> Result<()> {
    TransferIdentity::from_label(format!("{identity}:{}", "0".repeat(64)))
        .map(|_| ())
        .map_err(AppError::from)
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
    attempts: &mut JoinSet<Attempt>, _config: &LocalTransferConfig, sink: &mut dyn LocalTransferSink,
    report: &mut LocalTransferReport,
) -> Result<()> {
    let Some(attempt) = attempts.join_next().await else {
        return Ok(());
    };
    let (entry, outcome) = attempt?;
    match outcome {
        Ok(outcome) => {
            report.completed_files += 1;
            report.transferred_bytes += outcome.transferred_bytes;
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
    use data_mover::traversal::{ScanFailure, ScanTotals, TraversalCompletion};

    fn completion(entries: u64, failures: u64) -> TraversalCompletion {
        let mut totals = ScanTotals::default();
        totals.emitted = entries;
        totals.failed_children = failures;
        TraversalCompletion::new(totals)
    }

    use std::path::Path;

    use data_mover::model::{FailureClass, IdentityStrength, Operation, SourceIdentity, StoragePath, Transience};
    use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
    use data_mover::traversal::TraversalProducer;
    use tempfile::TempDir;

    use super::*;

    #[derive(Default)]
    struct Sink {
        completed: Vec<String>,
        entry_failures: usize,
        transfer_failures: Vec<TransferFailure>,
        fail_completion: bool,
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

    async fn storage(root: &Path, _name: &str) -> Storage {
        connect_backend(BackendConfig::Local(LocalBackendConfig {
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            root: root.to_path_buf(),

            read_concurrency: NonZeroUsize::new(2).unwrap(),
            write_concurrency: NonZeroUsize::new(2).unwrap(),
        }))
        .await
        .unwrap()
    }

    async fn entry(root: &Path, path: &str, size: u64) -> TraversalItem {
        let source = storage(root, "source").await;
        let identity = match source
            .read_source()
            .unwrap()
            .describe(&StoragePath::new(path).unwrap())
            .await
        {
            Ok(descriptor) => descriptor.source_identity,
            Err(_) => SourceIdentity::new(source.identity().clone(), IdentityStrength::PathScoped, path).unwrap(),
        };
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
        TraversalItem::Failure(Box::new(ScanFailure::child(
            EntryOperationFailure::new(
                StoragePath::new(path).unwrap(),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            )
            .unwrap(),
        )))
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

            max_concurrent_files: NonZeroUsize::new(maximum).unwrap(),

            transfer_policy: TransferPolicy::Checkpointed,
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
            vec![entry(source.path(), "a", 5).await, entry(source.path(), "b", 4).await],
            TraversalOutcome::Completed(completion(2, 0)),
        );
        let mut sink = Sink::default();

        let config = config(source.path(), destination.path(), 1).await;
        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.completed_files, 2);
        assert_eq!(report.transferred_bytes, 9);
        assert_eq!(report.peak_inflight_files, 1);
        assert_eq!(std::fs::read(destination.path().join("a")).unwrap(), b"alpha");
        assert_eq!(std::fs::read(destination.path().join("b")).unwrap(), b"beta");
    }

    #[tokio::test]
    async fn forwards_checkpoint_policy_for_large_file_transfer() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let payload = vec![0x5a; 128 * 1024 + 1];
        std::fs::write(source.path().join("large"), &payload).unwrap();
        let session = session(
            vec![entry(source.path(), "large", payload.len() as u64).await],
            TraversalOutcome::Completed(completion(1, 0)),
        );
        let mut sink = Sink::default();

        let config = config(source.path(), destination.path(), 1).await;
        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.completed_files, 1);
        assert_eq!(std::fs::read(destination.path().join("large")).unwrap(), payload);
    }

    #[tokio::test]
    async fn keeps_entry_and_transfer_failures_in_separate_channels() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let session = session(
            vec![entry_failure("denied"), entry(source.path(), "missing", 1).await],
            TraversalOutcome::Completed(completion(1, 1)),
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
            vec![entry(source.path(), "cancelled", 7).await],
            TraversalOutcome::Completed(completion(1, 0)),
        );
        let config = config(source.path(), destination.path(), 1).await;
        config.cancel.cancel();
        let mut sink = Sink::default();

        let report = run_local_transfer(session, config, &mut sink).await.unwrap();

        assert_eq!(report.transfer_failures, 1);
        assert_eq!(sink.transfer_failures.len(), 1);
        assert!(!destination.path().join("cancelled").exists());
    }

    #[tokio::test]
    async fn rejects_inconsistent_traversal_completion_evidence() {
        let source = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let mut sink = Sink::default();
        let error = run_local_transfer(
            session(Vec::new(), TraversalOutcome::Completed(completion(1, 0))),
            config(source.path(), destination.path(), 1).await,
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
            vec![entry(source.path(), "committed", 7).await],
            TraversalOutcome::Completed(completion(1, 0)),
        );
        let mut sink = Sink {
            fail_completion: true,
            ..Sink::default()
        };

        let error = run_local_transfer(session, config(source.path(), destination.path(), 1).await, &mut sink)
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
