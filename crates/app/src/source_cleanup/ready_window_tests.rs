//! 只有全部复制及枚举已完成且无需目录收尾时，才清理有界就绪事实。
#![allow(clippy::unwrap_used)]
use std::{path::Path, sync::Arc, time::Duration};

use data_mover::model::{
    BackendKind, EntryKind, ExtendedAttribute, MetadataObservation, MetadataObservationParts, MetadataProvenance,
    ObservedEntry,
};
use task_model::StorageEntryMessage;
use tokio_util::sync::CancellationToken;

use super::{FullCleanupStage, MAX_BYTES, MAX_ENTRIES, ReadyWindow, observation_weight};
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, CopyStep, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file};
use crate::observed_fixtures::observation;
use crate::scan::observe_path;
use crate::source_cleanup::{CompletedChecks, SourceCleanupCheck};
use crate::storage_factory::{CreateStorageOptions, create_storage};

fn entry(path: &str, kind: EntryKind) -> Arc<ObservedEntry> {
    Arc::new(observation(
        BackendKind::Local,
        path.into(),
        kind,
        0,
        [0; 3],
        None,
        None,
        None,
        None,
    ))
}

fn check() -> SourceCleanupCheck {
    SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new())
}

#[test]
fn retained_entries_are_bounded_and_only_ready_events_are_used() {
    let mut tail = ReadyWindow::new(3);
    let first = entry("first", EntryKind::File);
    let weak = Arc::downgrade(&first);
    tail.observe(StorageEntryMessage::New(first.into()));
    for name in ["a", "b", "c"] {
        tail.observe(StorageEntryMessage::Scanned(entry("inventory", EntryKind::File)));
        tail.observe(StorageEntryMessage::New(entry(name, EntryKind::File).into()));
        assert!(tail.entries.len() <= 3 && tail.bytes <= MAX_BYTES);
    }
    assert!(weak.upgrade().is_some());
    assert_eq!(
        tail.entries
            .iter()
            .map(|(entry, _)| entry.path().as_str())
            .collect::<Vec<_>>(),
        ["first", "a", "b"]
    );
    assert_eq!(tail.attempts, 3);
    assert_eq!(ReadyWindow::new(usize::MAX).limit, MAX_ENTRIES);
    assert_eq!(ReadyWindow::new(0).limit, 1);
}

#[test]
fn directory_or_history_boundary_disables_the_entire_early_phase() {
    for boundary in [
        StorageEntryMessage::New(entry("dir", EntryKind::Directory).into()),
        StorageEntryMessage::CopyEntryCompleted {
            entry: entry("history", EntryKind::File),
            logical_bytes: 0,
        },
    ] {
        // 目录或历史项即使排在已满窗口之后，仍须阻止提前清理。
        let mut tail = ReadyWindow::new(1);
        tail.observe(StorageEntryMessage::New(entry("before", EntryKind::File).into()));
        tail.observe(boundary);
        tail.observe(StorageEntryMessage::New(entry("after", EntryKind::File).into()));
        assert!(tail.disabled && tail.entries.is_empty() && tail.bytes == 0);
    }
}

fn large_entry(path: &str, payload_bytes: usize) -> Arc<ObservedEntry> {
    let metadata = MetadataObservationParts {
        xattrs: MetadataObservation::Value {
            value: vec![ExtendedAttribute::new(b"user.large".to_vec(), vec![1; payload_bytes]).unwrap()],
            provenance: MetadataProvenance::Inline,
        },
        ..Default::default()
    }
    .build()
    .unwrap();
    Arc::new(entry(path, EntryKind::File).as_ref().clone().with_metadata(metadata))
}

#[test]
fn metadata_byte_admission_keeps_a_bounded_window_and_rejects_an_oversized_fact() {
    let mut tail = ReadyWindow::new(64);
    let first = large_entry("first", 1024 * 1024);
    let second = large_entry("second", 1024 * 1024);
    assert!(observation_weight(&first).unwrap() > MAX_BYTES / 2);
    tail.observe(StorageEntryMessage::New(first.into()));
    tail.observe(StorageEntryMessage::New(second.into()));
    assert_eq!(tail.entries.len(), 1);
    assert_eq!(tail.entries[0].0.path().as_str(), "first");
    let oversized = large_entry("oversized", MAX_BYTES / 2);
    assert!(observation_weight(&oversized).unwrap() > MAX_BYTES);
    tail.observe(StorageEntryMessage::New(oversized.into()));
    assert_eq!(tail.entries.len(), 1);
    assert!(tail.bytes <= MAX_BYTES);
}

#[test]
fn prechecked_facts_are_bound_to_both_path_and_source_identity() {
    let original = entry("file", EntryKind::File);
    let other_path = entry("hardlink", EntryKind::File);
    let replacement = observation(
        BackendKind::Local,
        "file".into(),
        EntryKind::File,
        0,
        [0; 3],
        None,
        Some(vec![9; 16]),
        None,
        None,
    );
    let mut completed = CompletedChecks::new();
    completed.insert(original.path().clone(), original.identity_key());
    let check = check().with_prechecked(completed);
    assert!(check.is_prechecked(&original));
    assert!(!check.is_prechecked(&other_path));
    assert!(!check.is_prechecked(&replacement));
}

#[tokio::test]
async fn cleanup_runs_only_after_full_copy_gate_and_does_not_require_a_database() -> Result<()> {
    for copy_ready in [false, true] {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"verified")?;
        let source =
            Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let target =
            Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let item = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
        copy_file(&source, &target, &item, CopyOptions::default(), None).await?;
        let mut broadcaster = BroadcastForwarder::new(2);
        let stage = FullCleanupStage::start(&mut broadcaster, 2);
        assert!(stage.connections().set((source, target)).is_ok());
        broadcaster.broadcast(StorageEntryMessage::New(item.into())).await?;
        assert!(source_root.path().join("file").exists());
        drop(broadcaster);
        let done = stage.complete(copy_ready, check()).await?;
        assert_eq!(done.checked.len(), usize::from(copy_ready));
        assert_eq!(source_root.path().join("file").exists(), !copy_ready);
        assert_eq!(std::fs::read(target_root.path().join("file"))?, b"verified");
    }
    Ok(())
}

#[tokio::test]
async fn first_target_failure_stops_new_deletions_and_preserves_each_source() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let mut broadcaster = BroadcastForwarder::new(2);
    let stage = FullCleanupStage::start(&mut broadcaster, 2);
    assert!(stage.connections().set((source.clone(), target.clone())).is_ok());
    for name in ["a", "b"] {
        std::fs::write(source_root.path().join(name), b"verified")?;
        let item = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
        copy_file(&source, &target, &item, CopyOptions::default(), None).await?;
        broadcaster.broadcast(StorageEntryMessage::New(item.into())).await?;
    }
    std::fs::write(target_root.path().join("a"), b"different length")?;
    drop(broadcaster);
    let Err(failure) = stage.complete(true, check()).await else {
        panic!("unverified target must not authorize purge");
    };
    assert_eq!(
        failure.source_cleanup_entry().map(|(path, _)| path),
        Some(Path::new("a"))
    );
    for name in ["a", "b"] {
        assert_eq!(std::fs::read(source_root.path().join(name))?, b"verified");
    }
    Ok(())
}

#[test]
fn combined_task_and_cleanup_errors_keep_the_path_and_both_diagnostics() {
    let cleanup = AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new("file"), StorageError::Cancelled);
    let error = AppError::MigrationAndSourceCleanupFailed {
        source: Box::new(AppError::ConfigError("original database failure".into())),
        cleanup: Box::new(cleanup),
    };
    assert!(error.to_string().contains("original database failure"));
    assert!(error.to_string().contains("file"));
    assert_eq!(
        error.source_cleanup_entry().map(|(path, _)| path),
        Some(Path::new("file"))
    );
    assert!(error.storage_error().is_some_and(StorageError::is_cancelled));
}
