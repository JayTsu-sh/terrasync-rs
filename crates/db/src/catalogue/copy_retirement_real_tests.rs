//! 终态清理只处理本执行及更早的复制事实，实际表和副本独立核对。
use std::{sync::Arc, time::Duration};

use tokio::time::{Instant, timeout};

use super::Fixture;
use crate::migration_fixtures::request;
use crate::observed_fixtures::observation;
use crate::{ClickHouseDatabase, DatabaseError, Result};
use data_mover::model::{BackendKind, EntryKind, ObservedEntry};
use task_model::migration::CopyGenerationPhase;

fn entry(path: &str) -> Arc<ObservedEntry> {
    Arc::new(observation(
        BackendKind::Local,
        path.into(),
        EntryKind::File,
        7,
        [0, 11, 0],
        None,
        None,
        None,
        None,
    ))
}

async fn count(fixture: &Fixture, job: &str) -> Result<u64> {
    Ok(fixture
        .admin
        .clone()
        .with_database(&fixture.config.database)
        .query(&format!("SELECT count() FROM base_{job} FINAL"))
        .fetch_one()
        .await?)
}

async fn seeded(fixture: &Fixture, job: &str, reserved: bool) -> Result<ClickHouseDatabase> {
    let database = fixture.open(job).await?;
    database
        .begin_copy_generation(&request("retirement-endpoints", reserved))
        .await?;
    database.batch_insert_base_record(&[entry("payload")]).await?;
    database.commit_scan_generation().await?;
    Ok(database)
}

async fn independent_replicas_are_empty(fixture: &Fixture, job: &str) -> Result<()> {
    if fixture.config.cluster.is_some() {
        for port in [8123, 8124] {
            let rows: u64 = fixture
                .admin
                .clone()
                .with_database(&fixture.config.database)
                .with_url(format!("http://127.0.0.1:{port}"))
                .query(&format!("SELECT count() FROM __catalogue_base_{job}"))
                .fetch_one()
                .await?;
            assert_eq!(rows, 0, "physical replica {port}");
        }
    }
    assert_eq!(count(fixture, job).await?, 0);
    Ok(())
}

async fn phases_and_job_isolation(fixture: &Fixture) -> Result<()> {
    let neighbor = seeded(fixture, "retained_neighbor", true).await?;
    neighbor
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    for (index, phase) in [
        CopyGenerationPhase::Completed,
        CopyGenerationPhase::Failed,
        CopyGenerationPhase::Cancelled,
    ]
    .into_iter()
    .enumerate()
    {
        let job = format!("retired_phase_{index}");
        // 上一保留源代次的范围外事实也属于该 job 的 base 生命周期。
        let first = seeded(fixture, &job, true).await?;
        first
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await?;
        let moving = first.new_execution_scope();
        moving
            .begin_copy_generation(&request("retirement-endpoints", false))
            .await?;
        moving.batch_insert_base_record(&[entry("selected")]).await?;
        if phase == CopyGenerationPhase::Completed {
            moving.commit_scan_generation().await?;
        }
        moving.finish_copy_generation(phase, None).await?;
        independent_replicas_are_empty(fixture, &job).await?;
        assert_eq!(moving.latest_copy_generation().await?.map(|g| g.phase), Some(phase));
        assert_eq!(moving.copy_catalogue_source_reserved().await?, Some(false));
        assert!(moving.batch_insert_base_record(&[entry("late_ack")]).await.is_err());
        assert_eq!(count(fixture, "retained_neighbor").await?, 1);
        eprintln!(
            "retirement cluster={} phase={phase:?} base_empty=true replicas_empty=true neighbor_retained=true",
            fixture.config.cluster.is_some()
        );
    }
    Ok(())
}

async fn delayed_cleanup_preserves_new_copy_and_unowned_scan_rows(fixture: &Fixture) -> Result<()> {
    let job = "retirement_fence";
    let old = seeded(fixture, job, false).await?;
    let sequence = old.current_copy_generation().await?.sequence;
    old.finish_copy_generation(CopyGenerationPhase::Completed, None).await?;
    let next = old.new_execution_scope();
    next.begin_copy_generation(&request("retirement-endpoints", true))
        .await?;
    next.batch_insert_base_record(&[entry("new_copy")]).await?;
    let generic = fixture.open(job).await?;
    generic.batch_insert_base_record(&[entry("generic_scan")]).await?;
    assert_eq!(count(fixture, job).await?, 2);
    assert!(matches!(
        old.finish_copy_generation(CopyGenerationPhase::Completed, None).await,
        Err(DatabaseError::CopyBaseRetirementFailed { source, .. }) if matches!(*source, DatabaseError::CopyGenerationInvalid { field: "stale_base_retirement" })
    ));
    // 绕过客户端闸门重放旧 mutation，模拟先前响应丢失后的延迟执行。
    let storage = old.catalogue_storage_table(&format!("base_{job}"))?;
    old.execute(
        &format!("ALTER TABLE {storage} DELETE WHERE copy_sequence>0 AND copy_sequence<=? SETTINGS mutations_sync=2"),
        &[sequence.into()],
    )
    .await?;
    assert_eq!(count(fixture, job).await?, 2);
    assert_eq!(
        next.latest_copy_generation().await?.map(|g| g.phase),
        Some(CopyGenerationPhase::Prepared)
    );
    eprintln!(
        "retirement cluster={} delayed_mutation_new_sequence_and_zero_sequence_retained=true",
        fixture.config.cluster.is_some()
    );
    Ok(())
}

async fn faults_keep_explicit_outcomes_and_allow_idempotent_retry(fixture: &Fixture) -> Result<()> {
    for mode in ["flash", "disconnect", "stall", "lost", "cancel"] {
        let job = format!("retired_fault_{mode}");
        let database = seeded(fixture, &job, false).await?;
        fixture.fault(&format!("retirement_{mode}")).await?;
        let start = Instant::now();
        let result = timeout(
            if mode == "cancel" {
                Duration::from_millis(250)
            } else {
                Duration::from_secs(8)
            },
            database.finish_copy_generation(CopyGenerationPhase::Completed, None),
        )
        .await;
        let elapsed = start.elapsed();
        fixture.fault("normal").await?;
        if mode == "cancel" {
            assert!(result.is_err());
            assert_eq!(
                database.latest_copy_generation().await?.map(|g| g.phase),
                Some(CopyGenerationPhase::Prepared)
            );
        } else {
            assert!(
                result
                    .map_err(|_| DatabaseError::ConfigError("retirement test deadline".into()))?
                    .is_err()
            );
            assert_eq!(
                database.latest_copy_generation().await?.map(|g| g.phase),
                Some(CopyGenerationPhase::Failed)
            );
        }
        let remaining = count(fixture, &job).await?;
        assert_eq!(remaining, u64::from(mode != "lost"));
        assert!(database.batch_insert_base_record(&[entry("late_ack")]).await.is_err());
        database
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await?;
        independent_replicas_are_empty(fixture, &job).await?;
        assert_eq!(
            database.latest_copy_generation().await?.map(|g| g.phase),
            Some(CopyGenerationPhase::Completed)
        );
        eprintln!(
            "retirement cluster={} fault={mode} first_elapsed_ms={} remaining_after_fault={remaining} retry_empty=true",
            fixture.config.cluster.is_some(),
            elapsed.as_millis()
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-notification-archive-local.py --copy-generations owned ClickHouse pair and proxy"]
async fn real_source_deleted_base_retirement() -> Result<()> {
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            phases_and_job_isolation(&fixture).await?;
            delayed_cleanup_preserves_new_copy_and_unowned_scan_rows(&fixture).await?;
            faults_keep_explicit_outcomes_and_allow_idempotent_retry(&fixture).await
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}
