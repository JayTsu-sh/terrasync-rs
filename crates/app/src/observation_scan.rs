use std::num::NonZeroUsize;

use async_trait::async_trait;
use data_mover::traversal::{TraversalItem, TraversalOutcome, TraversalSession};
use db::{ClickHouseDatabase, ObservedEntryRecord};

use crate::error::{AppError, Result};

/// 完整 pure-scan projection 的中立统计。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObservationScanReport {
    pub observed_entries: u64,
    pub observed_bytes: u64,
    pub entry_failures: u64,
    pub deleted_entries: u64,
}

/// pure-scan 所需的最小持久化 seam。
#[async_trait]
pub trait ObservationScanSink: Send + Sync {
    async fn begin_generation(&mut self) -> db::Result<u8>;
    async fn write_batch(&self, records: &[ObservedEntryRecord]) -> db::Result<()>;
    /// Publishes atomically or performs its own safe rollback before returning an error.
    async fn publish_generation(&mut self) -> db::Result<u64>;
    async fn abort_generation(&mut self) -> db::Result<()>;
}

#[async_trait]
impl ObservationScanSink for ClickHouseDatabase {
    async fn begin_generation(&mut self) -> db::Result<u8> {
        self.begin_observation_snapshot().await
    }

    async fn write_batch(&self, records: &[ObservedEntryRecord]) -> db::Result<()> {
        self.batch_insert_temp_observations(records).await
    }

    async fn publish_generation(&mut self) -> db::Result<u64> {
        self.publish_observation_snapshot().await
    }

    async fn abort_generation(&mut self) -> db::Result<()> {
        self.abort_observation_snapshot().await
    }
}

/// 将一个 bounded traversal session 投影到数据库，只有完整快照才能提交 generation。
///
/// # Errors
/// 数据库失败、entry failure、取消或 traversal terminal failure 均阻止提交。
pub async fn project_pure_scan(
    mut session: TraversalSession, sink: &mut dyn ObservationScanSink, batch_size: NonZeroUsize,
) -> Result<ObservationScanReport> {
    let generation = sink.begin_generation().await?;
    let mut projection = Projection::new(generation, batch_size.get());
    while let Some(item) = session.next_item().await {
        if let Some(batch) = projection.push(item)
            && let Err(error) = sink.write_batch(&batch).await
        {
            let _ = sink.abort_generation().await;
            return Err(error.into());
        }
    }
    if let Some(batch) = projection.take_batch()
        && let Err(error) = sink.write_batch(&batch).await
    {
        let _ = sink.abort_generation().await;
        return Err(error.into());
    }
    let outcome = match session.finish().await {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = sink.abort_generation().await;
            return Err(error.into());
        }
    };
    projection.finish(outcome, sink).await
}

struct Projection {
    generation: u8,
    batch_size: usize,
    batch: Vec<ObservedEntryRecord>,
    report: ObservationScanReport,
}

impl Projection {
    fn new(generation: u8, batch_size: usize) -> Self {
        Self {
            generation,
            batch_size,
            batch: Vec::with_capacity(batch_size),
            report: ObservationScanReport::default(),
        }
    }

    fn push(&mut self, item: TraversalItem) -> Option<Vec<ObservedEntryRecord>> {
        match item {
            TraversalItem::Entry(entry) => {
                self.report.observed_entries += 1;
                self.report.observed_bytes += entry.size().unwrap_or_default();
                self.batch.push(ObservedEntryRecord::capture(&entry, self.generation));
            }
            TraversalItem::EntryFailure(_) => self.report.entry_failures += 1,
        }
        (self.batch.len() == self.batch_size).then(|| std::mem::take(&mut self.batch))
    }

    fn take_batch(&mut self) -> Option<Vec<ObservedEntryRecord>> {
        (!self.batch.is_empty()).then(|| std::mem::take(&mut self.batch))
    }

    async fn finish(
        mut self, outcome: TraversalOutcome, sink: &mut dyn ObservationScanSink,
    ) -> Result<ObservationScanReport> {
        let TraversalOutcome::Completed(completion) = outcome else {
            sink.abort_generation().await?;
            return Err(AppError::ObservationScanCancelled);
        };
        let failures = self.report.entry_failures.max(completion.entry_failures);
        if failures > 0 {
            sink.abort_generation().await?;
            return Err(AppError::ObservationScanIncomplete {
                entry_failures: failures,
            });
        }
        if self.report.observed_entries != completion.observed_entries {
            sink.abort_generation().await?;
            return Err(AppError::ObservationScanCountMismatch {
                projected: self.report.observed_entries,
                reported: completion.observed_entries,
            });
        }
        self.report.deleted_entries = sink.publish_generation().await?;
        Ok(self.report)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use clickhouse::Client;
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
        StoragePath, Transience,
    };
    use data_mover::traversal::TraversalCompletion;

    use super::*;

    #[derive(Default)]
    struct Sink {
        events: Mutex<Vec<&'static str>>,
        fail_publish: bool,
    }

    #[async_trait]
    impl ObservationScanSink for Sink {
        async fn begin_generation(&mut self) -> db::Result<u8> {
            self.events.lock().unwrap().push("begin");
            Ok(1)
        }
        async fn write_batch(&self, _records: &[ObservedEntryRecord]) -> db::Result<()> {
            self.events.lock().unwrap().push("write");
            Ok(())
        }
        async fn publish_generation(&mut self) -> db::Result<u64> {
            self.events.lock().unwrap().push("publish");
            if self.fail_publish {
                return Err(db::DatabaseError::QueryError("publish failed".to_string()));
            }
            Ok(2)
        }
        async fn abort_generation(&mut self) -> db::Result<()> {
            self.events.lock().unwrap().push("abort");
            Ok(())
        }
    }

    fn entry(path: &str, size: u64) -> TraversalItem {
        let backend = BackendIdentity::new(BackendKind::Local, "fixture").unwrap();
        let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, path).unwrap();
        let observed = data_mover::model::ObservedEntry::new(
            StoragePath::new(path).unwrap(),
            EntryKind::File,
            Some(size),
            None,
            identity,
        )
        .unwrap();
        TraversalItem::Entry(Box::new(observed))
    }

    fn failure() -> TraversalItem {
        TraversalItem::EntryFailure(
            data_mover::model::EntryOperationFailure::new(
                StoragePath::new("denied").unwrap(),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn complete_projection_batches_then_deletes_and_commits() {
        let mut sink = Sink::default();
        let mut projection = Projection::new(1, 2);
        assert!(projection.push(entry("a", 3)).is_none());
        let batch = projection.push(entry("b", 5)).unwrap();
        sink.write_batch(&batch).await.unwrap();
        let report = projection
            .finish(
                TraversalOutcome::Completed(TraversalCompletion {
                    observed_entries: 2,
                    entry_failures: 0,
                }),
                &mut sink,
            )
            .await
            .unwrap();
        assert_eq!(report.observed_bytes, 8);
        assert_eq!(report.deleted_entries, 2);
        assert_eq!(*sink.events.lock().unwrap(), ["write", "publish"]);
    }

    #[tokio::test]
    async fn public_seam_drains_bounded_session_and_publishes_complete_snapshot() {
        let (producer, session) = data_mover::traversal::TraversalSession::bounded(
            NonZeroUsize::new(1).unwrap(),
            tokio_util::sync::CancellationToken::new(),
        );
        let producer_task = tokio::spawn(async move {
            producer.send(entry("a", 3)).await.unwrap();
            producer.send(entry("b", 5)).await.unwrap();
            producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: 2,
                entry_failures: 0,
            })));
        });
        let mut sink = Sink::default();

        let report = project_pure_scan(session, &mut sink, NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();

        producer_task.await.unwrap();
        assert_eq!(report.observed_entries, 2);
        assert_eq!(report.observed_bytes, 8);
        assert_eq!(report.deleted_entries, 2);
        assert_eq!(*sink.events.lock().unwrap(), ["begin", "write", "publish"]);
    }

    #[tokio::test]
    async fn entry_failure_blocks_deletion_and_generation_commit() {
        let mut sink = Sink::default();
        let mut projection = Projection::new(1, 2);
        assert!(projection.push(failure()).is_none());
        let error = projection
            .finish(
                TraversalOutcome::Completed(TraversalCompletion {
                    observed_entries: 0,
                    entry_failures: 1,
                }),
                &mut sink,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::ObservationScanIncomplete { entry_failures: 1 }
        ));
        assert_eq!(*sink.events.lock().unwrap(), ["abort"]);
    }

    #[tokio::test]
    async fn cancellation_blocks_deletion_and_generation_commit() {
        let mut sink = Sink::default();
        let error = Projection::new(1, 2)
            .finish(TraversalOutcome::Cancelled, &mut sink)
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::ObservationScanCancelled));
        assert_eq!(*sink.events.lock().unwrap(), ["abort"]);
    }

    #[tokio::test]
    async fn inconsistent_completion_evidence_aborts_generation() {
        let mut sink = Sink::default();
        let mut projection = Projection::new(1, 2);
        assert!(projection.push(entry("a", 3)).is_none());
        let error = projection
            .finish(
                TraversalOutcome::Completed(TraversalCompletion {
                    observed_entries: 2,
                    entry_failures: 0,
                }),
                &mut sink,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::ObservationScanCountMismatch { .. }));
        assert_eq!(*sink.events.lock().unwrap(), ["abort"]);
    }

    #[tokio::test]
    async fn publish_failure_is_returned_without_reporting_success() {
        let mut sink = Sink {
            fail_publish: true,
            ..Sink::default()
        };
        let error = Projection::new(1, 2)
            .finish(
                TraversalOutcome::Completed(TraversalCompletion {
                    observed_entries: 0,
                    entry_failures: 0,
                }),
                &mut sink,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::DatabaseError(_)));
        assert_eq!(*sink.events.lock().unwrap(), ["publish"]);
    }

    #[tokio::test]
    #[ignore = "requires lab ClickHouse"]
    async fn public_bounded_session_publishes_and_deletes_in_real_clickhouse() {
        use db::{ClickHouseConfig, Database};

        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let database = format!("observation_public_seam_{unique}");
        let dsn = std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".to_string());
        let user = std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
        let password = std::env::var("LAB_CLICKHOUSE_PASSWORD").ok();
        let mut admin = Client::default().with_url(&dsn).with_user(&user);
        if let Some(value) = &password {
            admin = admin.with_password(value);
        }
        admin
            .query(&format!("CREATE DATABASE `{database}`"))
            .execute()
            .await
            .unwrap();
        let config = ClickHouseConfig {
            dsn,
            dial_timeout: 10,
            read_timeout: 30,
            database: database.clone(),
            username: user,
            password,
        };
        let mut sink = ClickHouseDatabase::new(&config, "public_seam");
        sink.initialize().await.unwrap();

        let (producer, session) = TraversalSession::bounded(
            NonZeroUsize::new(1).unwrap(),
            tokio_util::sync::CancellationToken::new(),
        );
        producer.send(entry("persisted", 7)).await.unwrap();
        producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 1,
            entry_failures: 0,
        })));
        let report = project_pure_scan(session, &mut sink, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(report.observed_entries, 1);
        assert_eq!(sink.query_scan_state().await.unwrap(), Some(1));

        let (producer, session) = TraversalSession::bounded(
            NonZeroUsize::new(1).unwrap(),
            tokio_util::sync::CancellationToken::new(),
        );
        producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 0,
            entry_failures: 0,
        })));
        let report = project_pure_scan(session, &mut sink, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(report.deleted_entries, 1);
        assert_eq!(sink.query_scan_state().await.unwrap(), Some(0));

        admin
            .query(&format!("DROP DATABASE `{database}`"))
            .execute()
            .await
            .unwrap();
    }
}
