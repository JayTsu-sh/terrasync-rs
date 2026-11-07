#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use data_mover::model::{FailureClass, MetadataFamily, MetadataObservation, MetadataObservations, Transience};

use super::{CopyOptions, copy_file};
use crate::error::{AppError, Result, StorageError};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

#[tokio::test]
async fn failed_source_metadata_does_not_block_data_and_reports_every_failed_family() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"readable payload")?;
    let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let entry = observe_path(source.storage(), Path::new("file")).await?.with_metadata(
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            },
            MetadataObservation::Failed {
                class: FailureClass::Timeout,
                transience: Transience::Transient,
            },
        )
        .map_err(StorageError::from)?,
    );
    let bytes = Arc::new(AtomicU64::new(0));
    let error = copy_file(
        &source,
        &target,
        &Arc::new(entry),
        CopyOptions {
            bytes_counter: Some(Arc::clone(&bytes)),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap_err();
    match error {
        AppError::StorageError(StorageError::SourceMetadataAcquisitionFailed { failures, .. }) => {
            assert_eq!(failures.len(), 2);
            assert!(
                failures
                    .iter()
                    .any(|failure| failure.family == MetadataFamily::OwnershipMode
                        && failure.class == FailureClass::PermissionDenied)
            );
            assert!(
                failures
                    .iter()
                    .any(|failure| failure.family == MetadataFamily::Timestamps
                        && failure.class == FailureClass::Timeout)
            );
        }
        other => panic!("expected source metadata acquisition failure, got {other:?}"),
    }
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"readable payload");
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"readable payload");
    assert_eq!(bytes.load(Ordering::Relaxed), b"readable payload".len() as u64);
    Ok(())
}
