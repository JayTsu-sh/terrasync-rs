//! 历史清理授权验证真实分段缺口，不能靠单行快照的 complete 误判整个 key。
use super::*;
use crate::observed_fixtures::observation;
use data_mover::model::BackendKind;
use task_model::observation::ObservedItem;

fn row(segment: u32, index: u32, count: u32, complete: bool, latest: bool) -> CopyCatalogueEntry {
    let entry = observation(
        BackendKind::S3,
        "payload".into(),
        EntryKind::File,
        7,
        [0; 3],
        None,
        None,
        None,
        Some((Some(format!("v{segment}-{index}")), latest, false)),
    );
    CopyCatalogueEntry {
        completion: ObservedItem::from(Arc::new(entry)).into(),
        history_segment: segment,
        history_index: index,
        history_records: count,
        history_complete: complete,
    }
}

#[test]
fn all_segments_and_the_final_current_record_are_required() -> Result<()> {
    let mut group = Group::start(&row(0, 0, 2, false, false))?;
    for item in [
        row(0, 0, 2, false, false),
        row(0, 1, 2, false, false),
        row(1, 0, 2, true, false),
    ] {
        group.observe(&item)?;
        assert!(group.anchor().is_err());
    }
    group.observe(&row(1, 1, 2, true, true))?;
    assert_eq!(
        group
            .anchor()?
            .history()
            .and_then(|history| history.latest())
            .and_then(|record| record.version_id()),
        Some("v1-1")
    );
    assert!(group.observe(&row(1, 1, 2, true, true)).is_err());
    Ok(())
}

#[test]
fn missing_prefix_cannot_become_a_complete_history_from_projected_snapshots() {
    for item in [row(1, 0, 1, true, true), row(0, 1, 2, true, true)] {
        assert!(
            item.completion
                .item
                .entry
                .history()
                .is_some_and(|history| history.is_complete())
        );
        assert!(Group::start(&item).is_err());
    }
}

#[test]
fn holes_replays_and_changed_segment_boundaries_refuse_authority() -> Result<()> {
    for next in [
        row(0, 2, 3, false, false),
        row(0, 0, 3, false, false),
        row(0, 1, 2, false, false),
        row(0, 1, 3, true, false),
        row(1, 0, 1, true, true),
        row(2, 0, 1, true, true),
    ] {
        let mut group = Group::start(&row(0, 0, 3, false, false))?;
        group.observe(&row(0, 0, 3, false, false))?;
        assert!(group.observe(&next).is_err());
        assert!(group.anchor().is_err());
    }
    Ok(())
}

#[test]
fn no_current_record_or_early_current_record_never_authorizes_purge() -> Result<()> {
    let mut group = Group::start(&row(0, 0, 1, true, false))?;
    group.observe(&row(0, 0, 1, true, false))?;
    assert!(group.anchor().is_err());
    let mut group = Group::start(&row(0, 0, 2, true, true))?;
    assert!(group.observe(&row(0, 0, 2, true, true)).is_err());
    assert!(group.anchor().is_err());
    Ok(())
}

#[test]
fn catalogue_failure_keeps_the_active_path_and_original_typed_cause() -> Result<()> {
    let worker = HistoryCleanup {
        group: Some(Group::start(&row(0, 0, 1, true, true))?),
    };
    let error = worker.query_failed(db::DatabaseError::ConfigError("read interrupted".into()).into());
    let (path, source) = error
        .source_cleanup_entry()
        .ok_or_else(|| AppError::ConfigError("lost source cleanup context".into()))?;
    assert_eq!(path, Path::new("payload"));
    assert!(
        matches!(source, AppError::DatabaseError(db::DatabaseError::ConfigError(reason)) if reason == "read interrupted")
    );
    Ok(())
}

#[tokio::test]
async fn metadata_cancellation_stays_cancelled_through_history_context() -> Result<()> {
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("payload"), b"retain source")?;
    std::fs::write(target_root.path().join("payload"), b"retain target")?;
    let source = create_storage(
        source_root
            .path()
            .to_str()
            .ok_or_else(|| AppError::ConfigError("path".into()))?,
        CreateStorageOptions::default(),
    )
    .await?;
    let target = create_storage(
        target_root
            .path()
            .to_str()
            .ok_or_else(|| AppError::ConfigError("path".into()))?,
        CreateStorageOptions::default(),
    )
    .await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut entry = row(0, 0, 1, true, true);
    entry.completion.target_version = Some("target-version".into());
    let mut worker = HistoryCleanup::default();
    let error = worker
        .observe(
            entry,
            &source,
            &target,
            &SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), cancel),
        )
        .await
        .err()
        .ok_or_else(|| AppError::ConfigError("cancelled history verification succeeded".into()))?;
    assert!(error.storage_error().is_some_and(StorageError::is_cancelled), "{error}");
    assert!(error.to_string().contains("source_version=v0-0") && error.to_string().contains("phase=target_ready"));
    assert_eq!(std::fs::read(source_root.path().join("payload"))?, b"retain source");
    assert_eq!(std::fs::read(target_root.path().join("payload"))?, b"retain target");
    Ok(())
}
