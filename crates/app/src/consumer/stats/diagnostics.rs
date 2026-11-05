//! 有界诊断明细发件箱；数字累计统计和当前恢复状态由调用方独立维护。

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// 单个回调的轮次、执行尝试和递增序号；时间相同而身份不同视为冲突。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackIdentity {
    pub round_id: String,
    pub attempt_id: String,
    pub round_started_at_millis: u64,
    pub attempt_started_at_millis: u64,
    pub sequence: u64,
}

/// 可独立去重的诊断事件；不参与错误累计数字的扣减。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticEvent {
    pub event_id: String,
    pub round_id: String,
    pub attempt_id: String,
    pub origin: String,
    pub code: String,
    pub message: String,
    pub path: Option<String>,
}

/// ACK 前保持不变的明细批次；省略数也只能随对应批次确认。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticBatch {
    pub batch_id: String,
    pub events: Vec<DiagnosticEvent>,
    pub omitted_count: u64,
}

/// 仅在接收事务提交后返回；旧报告无明细时没有批次 ID。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackAck {
    pub batch_id: Option<String>,
}

/// 单事件消息和路径的 UTF-8 字节上限。
pub const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 2048;
const MAX_DIAGNOSTIC_LABEL_BYTES: usize = 128;
/// 单批明细最大数量，同时也是接收端校验上限。
pub const MAX_DIAGNOSTIC_BATCH_EVENTS: usize = 64;
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
                is_digest(&batch.batch_id)
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
            event_id: self.identifier("event", self.next_event),
            round_id: self.round_id.clone(),
            attempt_id: self.attempt_id.clone(),
            origin: safe_diagnostic_label(origin),
            code: safe_diagnostic_label(code),
            message: safe_diagnostic_text(message),
            path: path.map(safe_diagnostic_text),
        };
        self.pending.push_back(event);
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

impl DiagnosticEvent {
    /// 接收端可再次调用，避免未经脱敏的外部回调直接落库。
    pub fn sanitize(&mut self) {
        self.origin = safe_diagnostic_label(&self.origin);
        self.code = safe_diagnostic_label(&self.code);
        self.message = safe_diagnostic_text(&self.message);
        self.path = self.path.as_deref().map(safe_diagnostic_text);
    }

    fn is_bounded(&self) -> bool {
        is_digest(&self.event_id)
            && [&self.origin, &self.code]
                .into_iter()
                .all(|text| text.len() <= MAX_DIAGNOSTIC_LABEL_BYTES)
            && self.message.len() <= MAX_DIAGNOSTIC_TEXT_BYTES
            && self
                .path
                .as_ref()
                .is_none_or(|text| text.len() <= MAX_DIAGNOSTIC_TEXT_BYTES)
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// 将内联 URL 整体隐藏，并隐藏常见凭据键值；限制 UTF-8 字节长度。
pub fn safe_diagnostic_text(text: &str) -> String {
    let mut result = String::new();
    for token in text_prefix(text, MAX_DIAGNOSTIC_TEXT_BYTES).split_whitespace() {
        if !result.is_empty() {
            result.push(' ');
        }
        let lower = token.to_ascii_lowercase();
        let secret_key = [
            "password",
            "passwd",
            "secret",
            "token",
            "credential",
            "authorization",
            "access_key",
            "secret_key",
        ]
        .into_iter()
        .any(|key| lower.contains(key));
        if token.contains("://") || secret_key {
            result.push_str("[redacted]");
        } else {
            result.extend(
                token
                    .chars()
                    .map(|character| if character.is_control() { '\u{fffd}' } else { character }),
            );
        }
        // 凭据可能是独立值、带空格赋值或多词授权头，保守隐藏后续文本。
        if secret_key {
            break;
        }
        if result.len() > MAX_DIAGNOSTIC_TEXT_BYTES {
            break;
        }
    }
    text_prefix(&result, MAX_DIAGNOSTIC_TEXT_BYTES).to_string()
}

fn safe_diagnostic_label(text: &str) -> String {
    text_prefix(&safe_diagnostic_text(text), MAX_DIAGNOSTIC_LABEL_BYTES).to_string()
}

fn text_prefix(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
    fn diagnostic_text_is_bounded_and_redacts_inline_urls_and_credentials() {
        let text = safe_diagnostic_text("failed (s3://alice:secret@host/path?token=value), password=hunter token abc");
        for secret in ["alice", "hunter", "abc", "value"] {
            assert!(!text.contains(secret));
        }
        assert!(safe_diagnostic_text(&"界".repeat(2000)).len() <= MAX_DIAGNOSTIC_TEXT_BYTES);
        assert!(!safe_diagnostic_text("error password = hunter").contains("hunter"));
        assert!(!safe_diagnostic_text("Authorization: Bearer abcdef").contains("abcdef"));
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
