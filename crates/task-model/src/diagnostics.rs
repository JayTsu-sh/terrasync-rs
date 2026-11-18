//! 任务诊断回调的共享值类型、校验与脱敏。

use serde::{Deserialize, Serialize};

use crate::{RetentionScope, TargetRetention};

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
    pub sequence: u64,
    pub event_id: String,
    pub round_id: String,
    pub attempt_id: String,
    pub origin: String,
    pub code: String,
    pub message: String,
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<Box<TargetRetention>>,
}

/// ACK 前保持不变的明细批次；省略数也只能随对应批次确认。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticBatch {
    pub batch_id: String,
    pub events: Vec<DiagnosticEvent>,
    pub omitted_count: u64,
}

/// Web 接收方可返回的批次信息；发送端只依据 HTTP 2xx，不要求此响应体。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackAck {
    pub batch_id: Option<String>,
}

/// 单事件消息和路径的 UTF-8 字节上限。
pub const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 2048;
const MAX_DIAGNOSTIC_LABEL_BYTES: usize = 128;
/// 单批明细最大数量，同时也是接收端校验上限。
pub const MAX_DIAGNOSTIC_BATCH_EVENTS: usize = 64;

impl DiagnosticEvent {
    /// 接收端可再次调用，避免未经脱敏的外部回调直接落库。
    pub fn sanitize(&mut self) {
        self.origin = safe_diagnostic_label(&self.origin);
        self.code = safe_diagnostic_label(&self.code);
        self.message = safe_diagnostic_text(&self.message);
        self.path = self.path.as_deref().map(safe_diagnostic_text);
        if let Some(retention) = &mut self.retention {
            retention.source_path = retention.source_path.as_deref().map(safe_diagnostic_text);
            retention.target_path = retention.target_path.as_deref().map(safe_diagnostic_text);
        }
    }

    /// 校验可恢复的发件箱事件身份、字段长度和保留路径。
    pub fn is_bounded(&self) -> bool {
        self.sequence > 0
            && is_digest(&self.event_id)
            && [&self.origin, &self.code]
                .into_iter()
                .all(|text| text.len() <= MAX_DIAGNOSTIC_LABEL_BYTES)
            && self.message.len() <= MAX_DIAGNOSTIC_TEXT_BYTES
            && self
                .path
                .as_ref()
                .is_none_or(|text| text.len() <= MAX_DIAGNOSTIC_TEXT_BYTES)
            && self.retention.as_ref().is_none_or(|retention| {
                (retention.scope != RetentionScope::Entry
                    || (retention.source_path.is_some() && retention.target_path.is_some()))
                    && [&retention.source_path, &retention.target_path]
                        .into_iter()
                        .all(|path| path.as_ref().is_none_or(|path| path.len() <= MAX_DIAGNOSTIC_TEXT_BYTES))
            })
    }
}

impl DiagnosticBatch {
    /// 校验发件箱生成的批次身份是否为十六进制摘要。
    pub fn has_digest_id(&self) -> bool {
        is_digest(&self.batch_id)
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

/// 对事件来源和代码应用脱敏规则，并限制标签的 UTF-8 字节长度。
pub fn safe_diagnostic_label(text: &str) -> String {
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
mod tests {
    use super::*;

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
}
