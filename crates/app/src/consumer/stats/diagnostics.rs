//! 有界诊断明细发件箱；数字累计统计和当前恢复状态由调用方独立维护。

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use task_model::TargetRetention;
use task_model::diagnostics::{
    DiagnosticBatch, DiagnosticEvent, MAX_DIAGNOSTIC_BATCH_EVENTS, safe_diagnostic_label, safe_diagnostic_text,
};

/// 发件箱硬上限，防止错误风暴导致无界内存增长。
pub const MAX_DIAGNOSTIC_QUEUE_EVENTS: usize = 512;

/// 可序列化的有界队列；调用方持锁修改，并把修改后的状态持久化到日志。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticOutbox {
    round_id: String,
    attempt_id: String,
    capacity: usize,
    batch_size: usize,
    #[serde(default)]
    revision: u64,
    next_event: u64,
    next_batch: u64,
    omitted_count: u64,
    pending: VecDeque<DiagnosticEvent>,
    in_flight: Option<DiagnosticBatch>,
}

impl DiagnosticOutbox {
    /// 容量限制覆盖在途和未发送事件；零容量只累计省略数字。
    pub fn new(round_id: String, attempt_id: String, capacity: usize, batch_size: usize) -> Self {
        Self {
            round_id,
            attempt_id,
            capacity: capacity.min(MAX_DIAGNOSTIC_QUEUE_EVENTS),
            batch_size: batch_size.clamp(1, MAX_DIAGNOSTIC_BATCH_EVENTS),
            revision: 0,
            next_event: 0,
            next_batch: 0,
            omitted_count: 0,
            pending: VecDeque::new(),
            in_flight: None,
        }
    }

    /// 队列内容的单调版本，用于比较检查点和本地日志；尝试身份更换不增加版本。
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// 恢复日志后验证身份、容量和在途前缀，拒绝损坏或错轮次的状态。
    pub fn validate_identity(&self, round_id: &str, attempt_id: &str) -> bool {
        self.round_id == round_id
            && self.attempt_id == attempt_id
            && self.capacity <= MAX_DIAGNOSTIC_QUEUE_EVENTS
            && (1..=MAX_DIAGNOSTIC_BATCH_EVENTS).contains(&self.batch_size)
            && self.pending.len() <= self.capacity
            && self
                .pending
                .iter()
                .all(|event| event.round_id == self.round_id && !event.attempt_id.is_empty() && event.is_bounded())
            && self.in_flight.as_ref().is_none_or(|batch| {
                batch.has_digest_id()
                    && batch.events.len() <= self.batch_size
                    && batch.omitted_count <= self.omitted_count
                    && self.pending.iter().take(batch.events.len()).eq(batch.events.iter())
            })
    }

    /// 新执行保留旧在途事件身份，后续事件使用新尝试身份。
    pub fn set_attempt(&mut self, attempt_id: String) {
        self.attempt_id = attempt_id;
    }

    /// 返回是否保留明细；队列满时仍明确累计省略数，不触碰累计错误统计。
    pub fn enqueue(&mut self, origin: &str, code: &str, message: &str, path: Option<&str>) -> bool {
        // 版本耗尽时保留全部现有明细，停止修改队列，避免新状态与旧状态同版本。
        if self.revision == u64::MAX {
            return false;
        }
        self.revision += 1;
        if self.pending.len() >= self.capacity || self.next_event == u64::MAX {
            self.omitted_count = self.omitted_count.saturating_add(1);
            return false;
        }
        self.next_event += 1;
        let event = DiagnosticEvent {
            sequence: self.next_event,
            event_id: self.identifier("event", self.next_event),
            round_id: self.round_id.clone(),
            attempt_id: self.attempt_id.clone(),
            origin: safe_diagnostic_label(origin),
            code: safe_diagnostic_label(code),
            message: safe_diagnostic_text(message),
            path: path.map(safe_diagnostic_text),
            retention: None,
        };
        self.pending.push_back(event);
        true
    }

    pub fn enqueue_retention(&mut self, retention: &TargetRetention, reason: &str) -> bool {
        if !self.enqueue("target", "target_preserved", reason, retention.source_path.as_deref()) {
            return false;
        }
        if let Some(event) = self.pending.back_mut() {
            event.retention = Some(Box::new(retention.clone()));
            event.sanitize();
        }
        true
    }

    /// 失败重试返回同一批次；ACK 前并发入队的事件只进入下一批。
    pub fn prepare_batch(&mut self) -> Option<DiagnosticBatch> {
        if let Some(batch) = &self.in_flight {
            return Some(batch.clone());
        }
        if (self.pending.is_empty() && self.omitted_count == 0)
            || self.next_batch == u64::MAX
            || self.revision == u64::MAX
        {
            return None;
        }
        self.revision += 1;
        self.next_batch += 1;
        let batch = DiagnosticBatch {
            batch_id: self.identifier("batch", self.next_batch),
            events: self.pending.iter().take(self.batch_size).cloned().collect(),
            omitted_count: self.omitted_count,
        };
        self.in_flight = Some(batch.clone());
        Some(batch)
    }

    /// 仅删除匹配批次的前缀；过期或未知 ACK 不改变队列和省略数字。
    pub fn ack(&mut self, batch_id: &str) -> bool {
        if self.revision == u64::MAX {
            return false;
        }
        let Some(batch) = self.in_flight.as_ref().filter(|batch| batch.batch_id == batch_id) else {
            return false;
        };
        if batch.omitted_count > self.omitted_count
            || !self.pending.iter().take(batch.events.len()).eq(batch.events.iter())
        {
            return false;
        }
        self.pending.drain(..batch.events.len());
        self.omitted_count -= batch.omitted_count;
        self.in_flight = None;
        self.revision += 1;
        true
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }
    pub(crate) fn has_omissions(&self) -> bool {
        self.omitted_count != 0
    }

    pub(crate) fn archive_prefix(&self) -> Vec<DiagnosticEvent> {
        self.pending.iter().take(MAX_DIAGNOSTIC_BATCH_EVENTS).cloned().collect()
    }

    /// 仅在已落库或已确认交付后释放相同前缀；迟到的回调 ACK 不能清除新的批次。
    pub(crate) fn release_prefix(&mut self, events: &[DiagnosticEvent]) {
        if !events.is_empty() && self.pending.iter().take(events.len()).eq(events.iter()) {
            self.pending.drain(..events.len());
            self.in_flight = None;
            self.revision = self.revision.saturating_add(1);
        }
    }

    fn identifier(&self, kind: &str, sequence: u64) -> String {
        let identity = (&self.round_id, &self.attempt_id, kind, sequence);
        let bytes = format!(
            "{}:{}:{}:{}:{}",
            identity.0.len(),
            identity.0,
            identity.1,
            identity.2,
            identity.3
        );
        blake3::hash(bytes.as_bytes()).to_hex().to_string()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use task_model::{RetentionScope, SourcePresence, TargetRetentionReason};

    #[test]
    fn diagnostic_batch_preserves_callback_json_and_durable_ids() {
        let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 4, 2);
        assert!(outbox.enqueue("scan", "failure", "failed s3://private.example/bucket", None));
        assert!(outbox.enqueue_retention(
            &TargetRetention {
                source_path: Some("source/data".into()),
                target_path: Some("s3://private.example/target".into()),
                scope: RetentionScope::Entry,
                source_presence: SourcePresence::Unknown,
                reason: TargetRetentionReason::SourceDeletionUnconfirmed,
            },
            "preserved",
        ));
        let batch = outbox.prepare_batch().unwrap();
        let expected = concat!(
            r#"{"batch_id":"f523c7b0c15955f7f2b6096403bd100fffe523c70815f0ef815728394d11e8bb","events":["#,
            r#"{"sequence":1,"event_id":"68eb21f79c026fcd0c25133c7958e4debcacd884ba8460d99dfa382f8375b8c1","#,
            r#""round_id":"round","attempt_id":"attempt","origin":"scan","code":"failure","#,
            r#""message":"failed [redacted]","path":null},"#,
            r#"{"sequence":2,"event_id":"46798af2f361f19ffc67e61cd1be7b2b781eaa21034abc1840c3f60922520200","#,
            r#""round_id":"round","attempt_id":"attempt","origin":"target","code":"target_preserved","#,
            r#""message":"preserved","path":"source/data","retention":{"source_path":"source/data","#,
            r#""target_path":"[redacted]","scope":"entry","source_presence":"unknown","#,
            r#""reason":"source_deletion_unconfirmed"}}],"omitted_count":0}"#,
        );
        assert_eq!(serde_json::to_string(&batch).unwrap(), expected);
        assert_eq!(serde_json::from_str::<DiagnosticBatch>(expected).unwrap(), batch);
    }

    #[test]
    fn retries_keep_batch_and_ack_preserves_concurrent_events() {
        let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 4, 2);
        outbox.enqueue("scan", "reset", "first", None);
        let batch = outbox.prepare_batch().unwrap();
        outbox.enqueue("scan", "reset", "concurrent", None);
        assert_eq!(outbox.prepare_batch(), Some(batch.clone()));
        assert!(!outbox.ack("unknown"));
        assert!(outbox.ack(&batch.batch_id));
        let next = outbox.prepare_batch().unwrap();
        assert_ne!(next.batch_id, batch.batch_id);
        assert_eq!(next.events.len(), 1);
        assert_eq!(next.events[0].message, "concurrent");
        assert!(!outbox.ack(&batch.batch_id));
    }

    #[test]
    fn overflow_after_prepare_is_not_acknowledged_with_earlier_overflow() {
        let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 1, 1);
        outbox.enqueue("scan", "reset", "first", None);
        assert!(!outbox.enqueue("scan", "reset", "omitted", None));
        let batch = outbox.prepare_batch().unwrap();
        assert!(!outbox.enqueue("scan", "reset", "later omitted", None));
        assert!(outbox.ack(&batch.batch_id));
        assert_eq!(outbox.prepare_batch().unwrap().omitted_count, 1);
    }

    #[test]
    fn serialized_inflight_survives_restart_and_identity_changes_ids() {
        let mut first = DiagnosticOutbox::new("round".into(), "a".into(), 2, 1);
        first.enqueue("scan", "reset", "first", None);
        let batch = first.prepare_batch().unwrap();
        let saved = serde_json::to_vec(&first).unwrap();
        let mut restored: DiagnosticOutbox = serde_json::from_slice(&saved).unwrap();
        assert!(restored.validate_identity("round", "a"));
        assert!(!restored.validate_identity("round", "b"));
        assert_eq!(restored.prepare_batch(), Some(batch.clone()));
        for (round, attempt) in [("other", "a"), ("round", "b")] {
            let mut other = DiagnosticOutbox::new(round.into(), attempt.into(), 2, 1);
            other.enqueue("scan", "reset", "first", None);
            assert_ne!(
                other.prepare_batch().unwrap().events[0].event_id,
                batch.events[0].event_id
            );
        }
    }

    #[test]
    fn changing_attempt_preserves_inflight_and_previous_attempt_event_ids() {
        let mut outbox = DiagnosticOutbox::new("round".into(), "first".into(), 4, 2);
        outbox.enqueue("scan", "reset", "first", None);
        let batch = outbox.prepare_batch().unwrap();
        outbox.set_attempt("second".into());
        outbox.enqueue("scan", "reset", "second", None);
        assert!(outbox.validate_identity("round", "second"));
        assert_eq!(outbox.prepare_batch(), Some(batch.clone()));
        outbox.ack(&batch.batch_id);
        let next = outbox.prepare_batch().unwrap();
        assert_eq!(next.events[0].attempt_id, "second");
        assert_ne!(next.events[0].event_id, batch.events[0].event_id);
    }

    #[test]
    fn revisions_order_queue_mutations_but_not_retries_or_attempt_identity() {
        let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 1, 1);
        assert_eq!(outbox.revision(), 0);
        outbox.enqueue("scan", "reset", "first", None);
        assert_eq!(outbox.revision(), 1);
        let batch = outbox.prepare_batch().unwrap();
        assert_eq!(outbox.revision(), 2);
        assert_eq!(outbox.prepare_batch(), Some(batch.clone()));
        assert_eq!(outbox.revision(), 2);
        assert!(!outbox.enqueue("scan", "reset", "omitted", None));
        assert_eq!(outbox.revision(), 3);
        outbox.set_attempt("next".into());
        assert_eq!(outbox.revision(), 3);
        assert!(outbox.ack(&batch.batch_id));
        assert_eq!(outbox.revision(), 4);
        assert!(!outbox.ack(&batch.batch_id));
        assert_eq!(outbox.revision(), 4);
    }
}
