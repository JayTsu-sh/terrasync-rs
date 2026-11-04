//! CH 检查点为基准，本地日志只能补入更新的队列状态。
use super::*;
use crate::{config::JobType, consumer::stats::FullStats};

const ROUND: i64 = 1_700_000_000_000_000_000;
fn stats() -> StatsKind {
    StatsKind::Full(FullStats::new(
        JobType::Scan,
        "journal-fixture".into(),
        String::new(),
        String::new(),
    ))
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn stale_empty_journal_does_not_erase_a_newer_checkpoint_event() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let job_dir = directory.path().to_string_lossy();
    let mut original = CallbackState::default();
    let mut counters = stats();
    original
        .initialize_scan("journal-fixture", &job_dir, Some(ROUND), &mut counters)
        .await?;
    original.outbox.enqueue("scan", "first", "first error", None);
    let first = original.outbox.prepare_batch().unwrap();
    assert!(original.outbox.ack(&first.batch_id));
    errors_mut(&mut counters).scan = 1;
    original.persist("journal-fixture", errors(&counters)).await?;
    original.outbox.enqueue("scan", "second", "second error", None);
    let checkpoint = serde_json::to_vec(&original)?;
    let mut resumed = CallbackState::default();
    resumed.restore_checkpoint(serde_json::from_slice(&checkpoint)?)?;
    errors_mut(&mut counters).scan = 2;
    resumed
        .initialize_scan("journal-fixture", &job_dir, Some(ROUND), &mut counters)
        .await?;
    let pending = resumed.outbox.prepare_batch().unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].code, "second");
    assert_eq!(errors(&counters).scan, 2);
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_newer_local_tail_is_restored_without_resetting_error_counters() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let job_dir = directory.path().to_string_lossy();
    let mut original = CallbackState::default();
    let mut counters = stats();
    original
        .initialize_scan("journal-fixture", &job_dir, Some(ROUND), &mut counters)
        .await?;
    let checkpoint = serde_json::to_vec(&original)?;
    original.outbox.enqueue("scan", "unconfirmed-tail", "later error", None);
    errors_mut(&mut counters).scan = 3;
    original.persist("journal-fixture", errors(&counters)).await?;
    let mut resumed = CallbackState::default();
    let attempt = resumed.identity.attempt_id.clone();
    resumed.restore_checkpoint(serde_json::from_slice(&checkpoint)?)?;
    let mut counters = stats();
    errors_mut(&mut counters).scan = 1;
    resumed
        .initialize_scan("journal-fixture", &job_dir, Some(ROUND), &mut counters)
        .await?;
    let pending = resumed.outbox.prepare_batch().unwrap();
    assert_eq!(pending.events[0].code, "unconfirmed-tail");
    assert_eq!(errors(&counters).scan, 3);
    assert_eq!(resumed.identity.attempt_id, attempt);
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn pod_restart_needs_no_local_journal_for_confirmed_outbox_or_counts() -> Result<()> {
    let mut original = CallbackState::default();
    let mut counters = stats();
    original
        .initialize_scan("journal-fixture", "", Some(ROUND), &mut counters)
        .await?;
    original.outbox.enqueue("scan", "confirmed", "checkpointed error", None);
    let checkpoint = serde_json::to_vec(&original)?;
    let directory = tempfile::tempdir()?;
    let mut resumed = CallbackState::default();
    resumed.restore_checkpoint(serde_json::from_slice(&checkpoint)?)?;
    resumed
        .initialize_scan(
            "journal-fixture",
            &directory.path().to_string_lossy(),
            Some(ROUND),
            &mut counters,
        )
        .await?;
    assert_eq!(resumed.outbox.prepare_batch().unwrap().events[0].code, "confirmed");
    Ok(())
}
