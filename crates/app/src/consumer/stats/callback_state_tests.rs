//! 回调只从已确认的数据库 checkpoint 恢复，不读取或写入本地扫描日志。
#![allow(clippy::unwrap_used)]
use super::*;
const ROUND: i64 = 1_700_000_000_000_000_000;

#[test]
fn confirmed_outbox_survives_restart_and_unconfirmed_tail_is_not_mixed_in() -> Result<()> {
    let mut original = CallbackState::default();
    original.initialize_scan("scan", Some(ROUND))?;
    original.outbox.enqueue("scan", "confirmed", "checkpoint error", None);
    let batch = original.outbox.prepare_batch().unwrap();
    let checkpoint = serde_json::to_vec(&original)?;
    original.outbox.enqueue("scan", "tail", "unconfirmed error", None);
    let mut resumed = CallbackState::default();
    let attempt = resumed.identity.attempt_id.clone();
    resumed.restore_checkpoint(serde_json::from_slice(&checkpoint)?)?;
    resumed.initialize_scan("scan", Some(ROUND))?;
    assert_eq!(resumed.identity.attempt_id, attempt);
    let restored = resumed.outbox.prepare_batch().unwrap();
    assert_eq!(restored.batch_id, batch.batch_id);
    assert_eq!(restored.events.len(), 1);
    assert_eq!(restored.events[0].code, "confirmed");
    resumed.outbox.enqueue("scan", "new", "new attempt error", None);
    assert!(resumed.outbox.ack(&restored.batch_id));
    assert_eq!(resumed.outbox.prepare_batch().unwrap().events[0].code, "new");
    Ok(())
}

#[test]
fn new_round_does_not_restore_a_previous_round_queue() -> Result<()> {
    let mut callback = CallbackState::default();
    callback.initialize_scan("scan", Some(ROUND))?;
    callback.outbox.enqueue("scan", "old", "old", None);
    callback.initialize_scan("scan", Some(ROUND + 1))?;
    assert!(callback.outbox.prepare_batch().is_none());
    Ok(())
}
