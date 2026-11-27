//! 可公开的矩阵诊断；不输出底层自由格式消息或凭据。

use data_mover::model::{BackendSessionFailure, EntryOperationFailure, FailureClass, Operation};
use data_mover::storage::{BackendConnectError, StorageRoleFailure};

#[derive(Clone, Copy)]
pub(super) struct Cause {
    class: Option<FailureClass>,
    operation: Option<Operation>,
    provenance: &'static str,
    reason: Option<&'static str>,
    unknown: bool,
}

impl Cause {
    pub(super) fn class(class: FailureClass) -> Self {
        Self {
            class: Some(class),
            operation: None,
            provenance: "classified",
            reason: None,
            unknown: false,
        }
    }

    pub(super) fn unclassified() -> Self {
        Self {
            class: None,
            operation: None,
            provenance: "unclassified",
            reason: None,
            unknown: false,
        }
    }

    pub(super) fn unknown_outcome(class: FailureClass) -> Self {
        Self {
            reason: Some("OutcomeUnknown"),
            unknown: true,
            ..Self::class(class)
        }
    }

    fn typed(class: FailureClass, operation: Operation, detail: &str) -> Self {
        let reason = smb_unknown_outcome(detail);
        Self {
            class: Some(class),
            operation: Some(operation),
            provenance: if reason.is_some() {
                "adapter_diagnostic"
            } else {
                "typed"
            },
            reason,
            unknown: reason.is_some(),
        }
    }

    pub(super) fn entry(error: &EntryOperationFailure) -> Self {
        Self::typed(error.class(), error.operation(), error.diagnostic())
    }

    pub(super) fn session(error: &BackendSessionFailure) -> Self {
        Self::typed(error.class(), error.operation(), error.diagnostic())
    }

    pub(super) fn role(error: &StorageRoleFailure) -> Self {
        match error {
            StorageRoleFailure::Entry(error) => Self::entry(error),
            StorageRoleFailure::Session(error) => Self::session(error),
        }
    }

    pub(super) fn connect(error: &BackendConnectError, cifs: bool) -> Self {
        if let Some(failure) = error.session_failure() {
            return Self::session(failure);
        }
        if error.is_not_compiled() {
            return Self::typed(FailureClass::Unsupported, Operation::Connect, "");
        }
        let mut cause = Self::unclassified();
        cause.operation = Some(Operation::Connect);
        if cifs
            && let Some(message) = error.to_string().strip_prefix("failed to connect Cifs backend: ")
            && let Some((class, reason)) = smb_connect_cause(message)
        {
            cause.class = Some(class);
            cause.reason = Some(reason);
            cause.provenance = "adapter_diagnostic";
        }
        cause
    }
}

fn smb_unknown_outcome(detail: &str) -> Option<&'static str> {
    if !detail.starts_with("CIFS ") {
        return None;
    }
    for (suffix, reason) in [
        ("Operation outcome is unknown after TimedOut", "TimedOut"),
        ("Operation outcome is unknown after ConnectionLost", "ConnectionLost"),
        (
            "Operation outcome is unknown after SessionInvalidated",
            "SessionInvalidated",
        ),
        (
            "Operation outcome is unknown because the request may have committed",
            "OutcomeUnknown",
        ),
    ] {
        if detail.ends_with(suffix) {
            return Some(reason);
        }
    }
    None
}

fn smb_connect_cause(message: &str) -> Option<(FailureClass, &'static str)> {
    // 固定版本的 CIFS 工厂将 SMB 错误转换为文本。只识别已知状态及传输错误，
    // 并明确标记该降级诊断的来源。永不转发原始文本。
    if let Some(status) = message
        .strip_prefix("Server returned an error message with status: ")
        .or_else(|| message.strip_prefix("Unexpected message status: "))
        .and_then(|status| status.strip_suffix('.'))
    {
        let code = status
            .rsplit_once(" (")
            .and_then(|(_, code)| code.strip_suffix(')'))
            .unwrap_or(status);
        return match code {
            "0xc000006d" | "0xc000006a" | "0xc0000234" | "0xc000006e" | "0xc0000072" => {
                Some((FailureClass::Authentication, "smb_logon_rejected"))
            }
            "0xc0000022" => Some((FailureClass::PermissionDenied, "smb_access_denied")),
            "0xc00000b5" => Some((FailureClass::Timeout, "smb_io_timeout")),
            "0xc00000c9" | "0xc000035c" => Some((FailureClass::Connectivity, "smb_session_lost")),
            _ => Some((FailureClass::Protocol, "smb_status")),
        };
    }
    if message.starts_with("Operation timed out: ")
        || message.starts_with("Transport error: Timed out after ")
        || message.starts_with("SessionSetup timed out after ")
    {
        return Some((FailureClass::Timeout, "smb_deadline"));
    }
    if matches!(
        message,
        "Client connection is stopped" | "Transport error: Not connected"
    ) || ["(os error 101)", "(os error 104)", "(os error 111)", "(os error 113)"]
        .iter()
        .any(|errno| {
            message.ends_with(errno)
                && (message.starts_with("IO Error: ") || message.starts_with("Transport error: IO Error: "))
        })
    {
        return Some((FailureClass::Connectivity, "smb_transport"));
    }
    None
}

pub(super) fn emit(phase: &str, role: &str, endpoint: Option<&str>, cause: Cause, mutation: bool) {
    let unknown = cause.unknown
        || (mutation
            && matches!(
                cause.class,
                None | Some(FailureClass::Connectivity | FailureClass::Timeout | FailureClass::Cancelled)
            ));
    record(
        phase,
        role,
        endpoint,
        cause,
        if unknown { "unknown" } else { "not_indicated" },
    );
}

pub(super) fn record(phase: &str, role: &str, endpoint: Option<&str>, cause: Cause, write_outcome: &str) {
    let write_outcome = if cause.unknown && write_outcome == "not_indicated" {
        "unknown"
    } else {
        write_outcome
    };
    eprintln!(
        "MATRIX_DIAGNOSTIC={}",
        serde_json::json!({
            "phase": phase,
            "role": role,
            "endpoint": endpoint,
            "class": cause.class.map(|class| format!("{class:?}")),
            "operation": cause.operation.map(|operation| format!("{operation:?}")),
            "provenance": cause.provenance,
            "cause": cause.reason,
            "write_outcome": write_outcome,
        })
    );
}

#[cfg(test)]
mod tests {
    use data_mover::model::BackendKind;

    use super::*;

    #[test]
    fn cifs_unknown_outcomes_do_not_replace_the_typed_class_or_operation() {
        for (detail, reason) in [
            (
                "Operation outcome is unknown because the request may have committed",
                "OutcomeUnknown",
            ),
            ("Operation outcome is unknown after TimedOut", "TimedOut"),
            (
                "Operation outcome is unknown after SessionInvalidated",
                "SessionInvalidated",
            ),
        ] {
            let cause = Cause::typed(
                FailureClass::Protocol,
                Operation::Namespace,
                &format!("CIFS delete: {detail}"),
            );
            assert_eq!(cause.class, Some(FailureClass::Protocol));
            assert_eq!(cause.operation, Some(Operation::Namespace));
            assert_eq!(cause.reason, Some(reason));
            assert!(cause.unknown);
        }
    }

    #[test]
    fn pinned_cifs_connector_distinguishes_logon_transport_and_unknown() {
        for (message, class, provenance) in [
            (
                "Server returned an error message with status: Logon Failure (0xc000006d).",
                Some(FailureClass::Authentication),
                "adapter_diagnostic",
            ),
            (
                "Unexpected message status: Access Denied (0xc0000022).",
                Some(FailureClass::PermissionDenied),
                "adapter_diagnostic",
            ),
            (
                "Transport error: IO Error: Connection refused (os error 111)",
                Some(FailureClass::Connectivity),
                "adapter_diagnostic",
            ),
            (
                "Transport error: Timed out after 5s",
                Some(FailureClass::Timeout),
                "adapter_diagnostic",
            ),
            ("invalid config with secret-value", None, "unclassified"),
        ] {
            let cause = Cause::connect(&BackendConnectError::new(BackendKind::Cifs, message), true);
            assert_eq!((cause.class, cause.provenance), (class, provenance));
        }
    }
}
