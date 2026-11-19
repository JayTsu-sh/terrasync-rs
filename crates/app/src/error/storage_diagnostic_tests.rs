//! 日志及callback文本展示契约；有类型的底层原因仍留在source链。
#![allow(clippy::unwrap_used)]
use std::error::Error;

use data_mover::model::{BackendSessionFailure, Operation, Transience};

use super::*;
use crate::consumer::stats::diagnostics::DiagnosticOutbox;

const DECODE_REASON: &str = "READLINK target is not UTF-8: invalid utf-8 sequence of 1 bytes from index 0";

fn entry(reason: &str, class: FailureClass) -> EntryOperationFailure {
    EntryOperationFailure::new(
        StoragePath::new("link").unwrap(),
        Operation::Observe,
        class,
        Transience::Permanent,
        reason,
    )
    .unwrap()
}

#[test]
fn role_entry_display_preserves_decode_reason_and_server_status_with_typed_source() {
    for (reason, class) in [
        (DECODE_REASON, FailureClass::Protocol),
        ("NFS3ERR_ACCES (permission denied)", FailureClass::PermissionDenied),
    ] {
        let error = StorageError::from(StorageRoleFailure::Entry(entry(reason, class)));
        assert!(error.to_string().contains(reason), "{}", error);
        let source = error.source().unwrap().downcast_ref::<StorageRoleFailure>().unwrap();
        assert!(
            matches!(source, StorageRoleFailure::Entry(cause) if cause.class() == class && cause.diagnostic() == reason)
        );
    }
}

#[test]
fn role_session_display_preserves_specific_cause_with_typed_source() {
    let reason = "owned transport connection reset during READLINK";
    let cause = BackendSessionFailure::new(
        Operation::Observe,
        FailureClass::Connectivity,
        Transience::Transient,
        reason,
    )
    .unwrap();
    let error = StorageError::from(StorageRoleFailure::Session(cause));
    assert!(error.to_string().contains(reason), "{}", error);
    assert!(
        matches!(error.source().unwrap().downcast_ref::<StorageRoleFailure>(), Some(StorageRoleFailure::Session(cause)) if cause.class() == FailureClass::Connectivity && cause.transience() == Transience::Transient)
    );
}

#[test]
fn traversal_entry_display_preserves_concrete_cause_and_delivery_context() {
    let error = StorageError::TraversalEntry {
        path: StoragePath::new("link").unwrap(),
        scope: FailureScope::Child,
        delivered: Some(7),
        source: Box::new(entry(DECODE_REASON, FailureClass::Protocol)),
    };
    let text = error.to_string();
    assert!(text.contains(DECODE_REASON), "{text}");
    assert!(text.contains("link") && text.contains("Child") && text.contains("Some(7)"));
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<Box<EntryOperationFailure>>()
            .unwrap()
            .diagnostic(),
        DECODE_REASON
    );
}

#[test]
fn traversal_terminal_display_preserves_entry_and_session_reasons() {
    let reason = "owned traversal session connection reset";
    let causes = [
        TraversalTerminalFailure::ConsumerSource(Box::new(entry(DECODE_REASON, FailureClass::Protocol))),
        TraversalTerminalFailure::Session(
            BackendSessionFailure::new(
                Operation::Traverse,
                FailureClass::Connectivity,
                Transience::Transient,
                reason,
            )
            .unwrap(),
        ),
    ];
    for (cause, expected) in causes.into_iter().zip([DECODE_REASON, reason]) {
        let error = StorageError::from(cause);
        assert!(error.to_string().contains(expected), "{}", error);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<TraversalTerminalFailure>()
                .is_some()
        );
    }
}

#[test]
fn traversal_terminal_without_backend_reason_keeps_its_existing_text() {
    assert_eq!(
        StorageError::from(TraversalTerminalFailure::Internal).to_string(),
        "traversal runtime failed"
    );
}

#[test]
fn history_role_display_does_not_repeat_the_same_diagnostic() {
    let error = StorageError::HistoryCleanupVerification {
        path: StoragePath::new("link").unwrap(),
        version: "source-version".into(),
        target_version: Some("target-version".into()),
        phase: "source_observation",
        source: Box::new(StorageError::from(StorageRoleFailure::Entry(entry(
            DECODE_REASON,
            FailureClass::Protocol,
        )))),
    };
    assert_eq!(error.to_string().matches(DECODE_REASON).count(), 1);
}

#[test]
fn appended_diagnostics_are_redacted_before_display_and_callback_batch() {
    let path = StoragePath::new("link").unwrap();
    for diagnostic in [
        "connection reset nfs://sample-user:sample-value@example.invalid/export",
        "connection reset password = sample-value",
        "connection reset token sample-value",
    ] {
        let cause = entry(diagnostic, FailureClass::Protocol);
        let errors = [
            StorageError::from(StorageRoleFailure::Entry(cause.clone())),
            StorageError::TraversalEntry {
                path: path.clone(),
                scope: FailureScope::Child,
                delivered: None,
                source: Box::new(cause.clone()),
            },
            StorageError::from(TraversalTerminalFailure::ConsumerSource(Box::new(cause))),
        ];
        let mut outbox = DiagnosticOutbox::new("round".into(), "attempt".into(), 3, 3);
        for error in errors {
            let text = error.to_string();
            assert!(text.contains("connection reset [redacted]"), "{text}");
            assert!(!text.contains("sample-value") && !text.contains("sample-user"));
            assert!(outbox.enqueue("scan", "failure", &text, Some(path.as_str())));
        }
        let batch = outbox.prepare_batch().unwrap();
        assert_eq!(batch.events.len(), 3);
        for event in batch.events {
            assert!(event.is_bounded());
            assert!(event.message.contains("connection reset [redacted]"));
            assert!(!event.message.contains("sample-value") && !event.message.contains("sample-user"));
        }
    }
}

#[test]
fn anchored_target_publication_change_preserves_native_verification_cause() {
    let path = StoragePath::new("directory").unwrap();
    let error = StorageError::TargetPublicationChanged {
        path: path.clone(),
        source: Some(Box::new(StorageError::MetadataVerification(
            MetadataVerificationError::VersionChanged { path: path.clone() },
        ))),
    };
    assert!(error.to_string().contains(path.as_str()));
    assert!(matches!(
        error.source().and_then(|cause| cause.downcast_ref::<Box<StorageError>>()).map(Box::as_ref),
        Some(StorageError::MetadataVerification(MetadataVerificationError::VersionChanged { path: actual }))
            if actual == &path
    ));
    let manual = StorageError::TargetPublicationChanged { path, source: None };
    assert!(manual.source().is_none());
}
