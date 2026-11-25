//! 复制／同步的目录元数据比对，不承载扫描轮次或扫描取消策略。

use std::sync::Arc;

use data_mover::model::ObservedEntry;
use db::catalogue::ComparisonMode;
use db::{ClickHouseDatabase, DatabaseError};
use task_model::{ChangeKind, EntryChange, MetadataTargetVersion, StorageEntryMessage};
use tracing::trace;

#[cfg(test)]
use crate::scan::ScanType;
use crate::{broadcast::BroadcastForwarder, error::Result};

#[cfg(all(test, unix))]
#[path = "sync_catalog_migration_tests.rs"]
mod migration_tests;

#[cfg(test)]
pub(crate) async fn determine_sync_type(db: &ClickHouseDatabase) -> Result<ScanType> {
    let previous = db.latest_copy_generation().await?;
    Ok(match previous {
        Some(previous)
            if previous.request.source_cleanup.retains_source() && db.copy_baseline_available(&previous).await? =>
        {
            ScanType::Incremental
        }
        _ => ScanType::Full,
    })
}

pub(crate) async fn batch_processing_to_generate_message(
    database: &ClickHouseDatabase, batch: &[Arc<ObservedEntry>], broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) -> Result<()> {
    trace!(is_final_batch, "Comparing catalogue batch");
    let mode = if keep_item {
        ComparisonMode::PublishObserved
    } else {
        ComparisonMode::AwaitCopyAcknowledgement
    };
    let mut changes = database
        .compare_catalogue_batch(batch, mode)
        .await?
        .into_iter()
        .peekable();
    for entry in batch {
        while changes
            .peek()
            .is_some_and(|change| Arc::ptr_eq(&change.item.entry, entry))
        {
            if let Some(change) = changes.next() {
                let message = if !keep_item
                    && change.kind == EntryChange::Changed(ChangeKind::MetadataOnly)
                    && change.metadata_target != MetadataTargetVersion::Current
                {
                    StorageEntryMessage::VersionMetadataChanged {
                        entry: change.item,
                        target: change.metadata_target,
                    }
                } else {
                    change.kind.message(change.item)
                };
                broadcaster.broadcast(message).await?;
            }
        }
        if !keep_item && entry.history().is_some() {
            broadcaster
                .broadcast(StorageEntryMessage::CopyEntryBoundary(entry.clone()))
                .await?;
        }
    }
    if changes.next().is_some() {
        return Err(DatabaseError::ObservationProjectionMismatch {
            field: "catalogue input order",
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::migration_fixtures::request;
    use crate::task_stream::finish_phase;
    use clickhouse::error::Error as ClickHouseError;
    use clickhouse::test::{Mock, handlers};
    use db::{ClickHouseConfig, DatabaseError};
    use reqwest::StatusCode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use task_model::migration::{CopyGeneration, CopyGenerationPhase, CopyGenerationPlan};
    use tokio::time::sleep;

    #[derive(serde::Serialize, clickhouse::Row)]
    struct RunRow {
        sequence: u64,
        execution_id: String,
        revision: u64,
        descriptor: String,
        root_snapshot: String,
    }
    #[derive(serde::Serialize, clickhouse::Row)]
    struct StateRow {
        scan_state: u8,
        copy_execution_id: String,
        state_version: u64,
        copy_baseline_sequence: u64,
    }

    fn fixture_database(mock: &Mock) -> ClickHouseDatabase {
        ClickHouseDatabase::new(
            &ClickHouseConfig {
                mode: Default::default(),
                cluster: None,
                dsn: mock.url().into(),
                database: "default".into(),
                username: "default".into(),
                password: None,
                dial_timeout: 1,
                read_timeout: 1,
            },
            "generation_selection",
        )
        .unwrap_or_else(|error| panic!("fixture catalogue: {error}"))
    }

    #[tokio::test]
    async fn only_a_source_retaining_generation_can_be_an_incremental_baseline() -> Result<()> {
        for (previous, expected) in [
            (None, ScanType::Full),
            (Some(false), ScanType::Full),
            (Some(true), ScanType::Incremental),
        ] {
            let mock = Mock::new();
            let database = fixture_database(&mock);
            if let Some(reserved) = previous {
                let request = request("preview", reserved);
                let generation = CopyGeneration {
                    root_snapshot: None,
                    sequence: 1,
                    revision: 3,
                    plan: CopyGenerationPlan::decide(&request, None, false)?,
                    request,
                    phase: CopyGenerationPhase::Completed,
                    baseline_prepared: true,
                };
                mock.add(handlers::provide([1u64]));
                mock.add(handlers::provide([RunRow {
                    sequence: 1,
                    revision: 3,
                    execution_id: generation.request.execution_id.clone(),
                    descriptor: serde_json::to_string(&generation)?,
                    root_snapshot: String::new(),
                }]));
                if reserved {
                    mock.add(handlers::provide([1u64]));
                    mock.add(handlers::provide([1u64]));
                    mock.add(handlers::provide([StateRow {
                        scan_state: 1,
                        copy_execution_id: generation.request.execution_id,
                        state_version: 4,
                        copy_baseline_sequence: 1,
                    }]));
                }
            } else {
                mock.add(handlers::provide([0u64]));
            }
            assert_eq!(determine_sync_type(&database).await?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn selecting_a_generation_preserves_database_errors_instead_of_guessing_from_job_directory() {
        let mock = Mock::new();
        let database = fixture_database(&mock);
        mock.add(handlers::failure(StatusCode::FORBIDDEN));
        assert!(matches!(
            determine_sync_type(&database).await,
            Err(AppError::DatabaseError(_))
        ));
    }

    #[tokio::test]
    async fn failed_catalogue_batch_stays_typed_emits_no_events_and_drains_phase_tasks() {
        let mock = Mock::new();
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: mock.url().into(),
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let database = ClickHouseDatabase::new(&config, "failed_phase")
            .unwrap_or_else(|error| panic!("fixture catalogue: {error}"));
        mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
        let mut broadcaster = BroadcastForwarder::new(8);
        let mut events = broadcaster.subscribe();
        let completed = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&completed);
        let work = tokio::spawn(async move {
            batch_processing_to_generate_message(&database, &[], broadcaster, true, false).await
        });
        let other = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            done.store(true, Ordering::Release);
            Ok(())
        });
        let result = finish_phase(vec![work, other]).await;
        assert!(matches!(
            result,
            Err(AppError::DatabaseError(DatabaseError::ClickHouseError(
                ClickHouseError::BadResponse(_)
            )))
        ));
        assert!(completed.load(Ordering::Acquire));
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_panicked_phase_worker_prevents_success() {
        let failed = tokio::spawn(async {
            panic!("injected phase panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        assert!(matches!(
            finish_phase(vec![failed]).await,
            Err(AppError::TaskJoinError(_))
        ));
    }
}
