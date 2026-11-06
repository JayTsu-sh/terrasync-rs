#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn deletion_failures_keep_their_typed_entry_cause() -> Result<()> {
    for class in [
        FailureClass::PermissionDenied,
        FailureClass::NotFound,
        FailureClass::Cancelled,
    ] {
        let source = EntryOperationFailure::new(
            StoragePath::new("blocked")?,
            Operation::Namespace,
            class,
            Transience::Permanent,
            "deletion denied",
        )?;
        let event = DeleteEvent {
            relative_path: "blocked".into(),
            is_dir: false,
            error: Some(source.to_string()),
            failure: Some(DeleteEventFailure::Entry(source.clone())),
        };
        let error = event.into_deleted().unwrap_err();
        assert!(matches!(&error, StorageError::Role(StorageRoleFailure::Entry(actual)) if actual == &source));
        assert_eq!(error.is_cancelled(), class == FailureClass::Cancelled);
        assert_eq!(error.is_not_found(), class == FailureClass::NotFound);
    }
    Ok(())
}

#[test]
fn unfinished_deletions_cannot_be_counted_as_deleted_paths() {
    for (failure, cancelled) in [
        (DeleteEventFailure::Cancelled, true),
        (DeleteEventFailure::Terminal(DeleteTreeTerminalFailure::Internal), false),
    ] {
        let event = DeleteEvent {
            relative_path: PathBuf::new(),
            is_dir: false,
            error: Some("recursive deletion did not complete".into()),
            failure: Some(failure),
        };
        let error = event.into_deleted().unwrap_err();
        assert_eq!(error.is_cancelled(), cancelled);
    }
}
