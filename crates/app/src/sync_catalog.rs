//! 复制／同步的目录元数据比对，不承载扫描轮次或扫描取消策略。

use std::sync::Arc;

use data_mover::model::ObservedEntry;
use db::ClickHouseDatabase;
use db::catalogue::ComparisonMode;
use task_model::StorageEntryMessage;
use tracing::trace;

use crate::{broadcast::BroadcastForwarder, error::Result, scan::ScanType};

#[cfg(all(test, unix))]
#[path = "sync_catalog_migration_tests.rs"]
mod migration_tests;

pub(crate) async fn determine_sync_type(db: &ClickHouseDatabase, source_reserved: bool) -> Result<ScanType> {
    let previous = db.copy_catalogue_source_reserved().await?;
    Ok(match previous {
        Some(previous_reserved) if source_reserved || previous_reserved => ScanType::Incremental,
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
    let changes = database.compare_catalogue_batch(batch, mode).await?;
    for change in changes {
        broadcaster.broadcast(change.kind.message(change.item)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::task_stream::finish_phase;
    use clickhouse::error::Error as ClickHouseError;
    use clickhouse::test::{Mock, handlers};
    use db::{ClickHouseConfig, DatabaseError};
    use reqwest::StatusCode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;

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
    async fn deleting_source_uses_incremental_only_after_a_source_retaining_generation() -> Result<()> {
        for (previous, reserved, expected) in [
            (None, false, ScanType::Full),
            (None, true, ScanType::Full),
            (Some(false), false, ScanType::Full),
            (Some(false), true, ScanType::Incremental),
            (Some(true), false, ScanType::Incremental),
            (Some(true), true, ScanType::Incremental),
        ] {
            let mock = Mock::new();
            let database = fixture_database(&mock);
            mock.add(handlers::provide([1u64]));
            mock.add(handlers::provide([1u64]));
            mock.add(handlers::provide(previous));
            assert_eq!(determine_sync_type(&database, reserved).await?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn selecting_a_generation_preserves_database_errors_instead_of_guessing_from_job_directory() {
        let mock = Mock::new();
        let database = fixture_database(&mock);
        mock.add(handlers::failure(StatusCode::FORBIDDEN));
        assert!(matches!(
            determine_sync_type(&database, false).await,
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
