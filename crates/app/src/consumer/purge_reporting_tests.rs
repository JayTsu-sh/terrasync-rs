//! 永久清理逐版本诊断沿用现有交付机制，数字仅由任务汇总累计一次。
use super::*;
use crate::error::{CopyStep, StorageError};
use crate::source_cleanup;
use data_mover::model::{EntryOperationFailure, FailureClass, Operation, SourceVersion, StoragePath, Transience};
use data_mover::storage::{PurgeFailure, StorageRoleFailure};
use std::path::Path;

fn manager() -> ConsumerManager {
    ConsumerManager {
        consumers: vec![],
        stats_consumer: Some(Arc::new(Mutex::new(StatisticConsumer {
            stats: StatsKind::Full(FullStats::new(
                JobType::Copy,
                "purge-errors".into(),
                String::new(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::Copy),
            job_dir: String::new(),
            callback_url: None,
            generation_commit_result: None,
            migration: None,
            scan_snapshot: None,
            callback: Default::default(),
            pb_handle: None,
        }))),
        callback_guard: None,
        notification_database: None,
        copy_pipeline_prepared: false,
        copy_database_execution: None,
    }
}

fn failure(version: Option<&str>, cancelled: bool) -> Result<AppError> {
    let path = StoragePath::new("payload").map_err(StorageError::from)?;
    let denied = StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "AccessDenied exact historical version",
        )
        .map_err(StorageError::from)?,
    );
    let mut source = PurgeFailure::from(denied.clone());
    source.version = version.map(|id| SourceVersion::Id(id.into()));
    if version.is_some() {
        source.additional.push((SourceVersion::Id("v2".into()), denied.clone()));
        source.additional.push((
            SourceVersion::Id("v3".into()),
            if cancelled {
                StorageRoleFailure::Entry(
                    EntryOperationFailure::new(
                        path.clone(),
                        Operation::Namespace,
                        FailureClass::Cancelled,
                        Transience::Unknown,
                        "cancelled; submitted outcome unknown",
                    )
                    .map_err(StorageError::from)?,
                )
            } else {
                denied
            },
        ));
    }
    Ok(AppError::copy_entry_failure(
        CopyStep::SourceCleanup,
        Path::new("payload"),
        StorageError::SourcePurgeRefused { path, source },
    ))
}

#[tokio::test]
async fn purge_versions_deliver_all_causes_once_without_multiplying_task_errors() -> Result<()> {
    let manager = manager();
    let original = failure(Some("null"), false)?;
    let error = source_cleanup::report_failure(Err(original), &manager)
        .await
        .err()
        .ok_or_else(|| AppError::ConfigError("purge failure disappeared".into()))?;
    manager.record_task_error(&error).await;
    let mut state = manager
        .stats_consumer
        .as_ref()
        .ok_or_else(|| AppError::ConfigError("statistics missing".into()))?
        .lock()
        .await;
    let batch = state
        .callback
        .outbox
        .prepare_batch()
        .ok_or_else(|| AppError::ConfigError("purge details missing".into()))?;
    let details: Vec<_> = batch.events.iter().filter(|event| event.code == "Delete").collect();
    assert_eq!(details.len(), 3);
    for (event, version) in details.iter().zip(["null", "v2", "v3"]) {
        assert_eq!(event.path.as_deref(), Some("payload"));
        assert!(
            event
                .message
                .contains(&format!("source_version={version} phase=SourceCleanup:"))
        );
        assert!(event.message.contains("PermissionDenied") && event.message.contains("AccessDenied"));
    }
    assert_eq!(state.to_report(true).task_error_count, 1);
    assert_eq!(state.to_report(true).target_preserved_count, 1);
    assert!(matches!(
        state.to_report(true).detail,
        ProgressDetail::Full { error_count: 0, .. }
    ));
    assert!(state.callback.outbox.ack(&batch.batch_id));
    assert!(state.callback.outbox.prepare_batch().is_none());
    assert_eq!(state.to_report(true).task_error_count, 1);
    assert_eq!(state.to_report(true).target_preserved_count, 1);
    Ok(())
}

#[tokio::test]
async fn unversioned_whole_operation_failure_does_not_invent_version_details() -> Result<()> {
    let manager = manager();
    manager.record_task_error(&failure(None, false)?).await;
    let mut state = manager
        .stats_consumer
        .as_ref()
        .ok_or_else(|| AppError::ConfigError("statistics missing".into()))?
        .lock()
        .await;
    let batch = state
        .callback
        .outbox
        .prepare_batch()
        .ok_or_else(|| AppError::ConfigError("task failure missing".into()))?;
    assert_eq!(batch.events.len(), 1);
    assert!(batch.events.iter().all(|event| event.code != "Delete"));
    assert_eq!(state.to_report(true).task_error_count, 1);
    Ok(())
}

#[test]
fn cancellation_in_additional_version_preserves_cancel_semantics() -> Result<()> {
    let error = failure(Some("v1"), true)?;
    assert!(error.storage_error().is_some_and(StorageError::is_cancelled));
    let normal = failure(Some("v1"), false)?;
    assert!(!normal.storage_error().is_some_and(StorageError::is_cancelled));
    Ok(())
}
