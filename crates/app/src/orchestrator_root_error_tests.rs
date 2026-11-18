//! 真实根捕获和 Preserve 诊断路径共享收尾；不启动内容管线。
#![allow(clippy::unwrap_used)]

use std::{path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use clickhouse::test::{Mock, handlers};
use data_mover::model::{
    EntryOperationFailure, FailureClass, MetadataObservations, ObservationMode, ObservationPlan, Operation,
    StoragePath, Transience,
};
use data_mover::storage::{Metadata, MetadataMutation, NamespaceReadPolicy, StorageBuilder, StorageRoleFailure};
use db::DatabaseError;
use reqwest::StatusCode;
use task_model::migration::CopyGenerationPhase;
use task_model::{ErrorEvent, SourceCleanupPolicy, StorageEntryMessage, TransferMode};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use utils::app_config::AppConfig;

use super::SyncOrchestrator;
use crate::{
    config::{ConsoleConfig, ConsumerConfig, JobType, SyncJobConfig},
    consumer::{
        ConsumerManager,
        stats::{
            ProgressDetail, ProgressReport,
            callback_state::StatisticsCheckpoint,
            delivery::tests::{Reply, server},
        },
    },
    copy_root::CopyRootMetadata,
    error::{AppError, Result, StorageError},
    migration_fixtures::request,
    storage_factory::{CreateStorageOptions, StorageContext, create_storage},
};

struct RefusedRootMetadata;

#[async_trait]
impl Metadata for RefusedRootMetadata {
    async fn observe(
        &self, path: &StoragePath, _: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        assert!(path.as_str().is_empty());
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Metadata,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "owned root metadata denial",
            )
            .unwrap(),
        ))
    }

    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        panic!("capture-only source must not receive metadata writes");
    }
}

async fn refused_source(path: &Path) -> Result<Arc<StorageContext>> {
    let original = create_storage(&path.to_string_lossy(), CreateStorageOptions::default()).await?;
    let roles = StorageBuilder::new(original.roles.identity().clone(), original.roles.capabilities().clone())
        .read_source(original.roles.read_source()?)
        .staged_destination(original.roles.staged_destination()?)
        .namespace(original.roles.namespace()?)
        .metadata(Arc::new(RefusedRootMetadata))
        .build()
        .unwrap();
    Ok(Arc::new(StorageContext {
        roles,
        block_size: original.block_size,
        reconnect_config: None,
    }))
}

fn command(path: &Path) -> SyncJobConfig {
    SyncJobConfig {
        job_id: "root-preserve-error".into(),
        job_dir: path.to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: "pipeline-must-not-start".into(),
        dest_path: "pipeline-must-not-start".into(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: String::new(),
        progress_callback_url: None,
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: Some(1),
    }
}

async fn preserve_reporting_failure(deliverable: bool) -> Result<()> {
    let source_dir = tempfile::tempdir()?;
    let target_dir = tempfile::tempdir()?;
    std::fs::write(source_dir.path().join("payload"), b"pipeline never started")?;
    let source = refused_source(source_dir.path()).await?;
    let target = Arc::new(create_storage(&target_dir.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let root = CopyRootMetadata::new(
        source,
        target,
        true,
        ObservationPlan::default().with_tags(ObservationMode::BestEffort),
        NamespaceReadPolicy {
            operation_timeout: Duration::from_millis(500),
            recovery_window: Duration::from_millis(100),
            retry_interval: Duration::from_millis(10),
        },
        CancellationToken::new(),
        false,
    );
    let callback = server(vec![if deliverable { Reply::Accepted } else { Reply::Refuse }; 4]).await;
    let mut mock = Mock::new();
    mock.non_exhaustive();
    // 第65条诊断（真实 Preserve 范围事件）触发实际归档请求并被拒绝；最终报告归档也不可用。
    for _ in 0..64 {
        mock.add(handlers::failure(StatusCode::FORBIDDEN));
    }
    let app: AppConfig = serde_json::from_value(serde_json::json!({
        "log": {"max_size": 1, "max_backups": 1, "level": "error"},
        "scan": {"concurrency": 1, "include_tags": false},
        "sync": {"is_source_reserved": true, "concurrency": 1},
        "database": {"enabled": true, "batch_size": 64, "clickhouse": {
            "dsn": mock.url(), "dial_timeout": 1, "read_timeout": 1,
            "database": "default", "username": "default", "password": null
        }}
    }))?;
    let mut manager = ConsumerManager::statistics_only(&ConsumerConfig {
        job_id: "root-preserve-error".into(),
        job_type: JobType::Copy,
        job_dir: String::new(),
        db_config: app.database.clone(),
        console_config: ConsoleConfig {
            raw_command_line: String::new(),
        },
        progress_callback_url: Some(callback.endpoint.clone()),
    })?;
    manager
        .record_statistics((0..64).map(|index| StorageEntryMessage::Diagnostic {
            event: ErrorEvent::Scan,
            path: "earlier-diagnostic".into(),
            reason: format!("earlier-{index}"),
        }))
        .await;
    let mut request = request("root-preserve", true);
    request.execution_id = manager.copy_attempt_id().await?;
    let result = SyncOrchestrator::new_local(command(source_dir.path()))
        .run_planned_local(None, &request, &app, &mut manager, None, &root)
        .await;
    let error = result.err().ok_or(AppError::ScanWorkerStopped)?;
    let AppError::MigrationAndRootMetadataFailed { source, metadata } = &error else {
        panic!("both the reporting and captured root causes must survive: {error:?}");
    };
    assert!(matches!(source.as_ref(), AppError::DiagnosticJournalStopped { source }
        if matches!(source.as_ref(), AppError::DatabaseError(DatabaseError::ClickHouseError(_)))));
    assert!(matches!(metadata.as_ref(), AppError::CopyRootMetadataFailure {
        phase: "root_source_metadata_capture", source, ..
    } if matches!(source.as_ref(), StorageError::SourceMetadataAcquisitionFailed { .. })));
    assert_eq!(manager.get_consumer_count(), 0);
    assert!(!target_dir.path().join("payload").exists());
    let before: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    assert_eq!(before.report.task_error_count, 1, "journal failure already counted");
    manager.record_task_error(&error).await;
    let saved: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
    assert_eq!(saved.report.task_error_count, before.report.task_error_count + 1);
    assert!(matches!(
        saved.report.detail,
        ProgressDetail::Full {
            scanned_files: 0,
            scanned_dirs: 0,
            transferred_bytes: 0,
            error_count: 0,
            ..
        }
    ));
    manager.finish_unpersisted_migration(CopyGenerationPhase::Failed).await;
    timeout(Duration::from_secs(10), manager.end_lifecycle())
        .await
        .map_err(|_| AppError::ScanWorkerStopped)?;
    let mut reports = Vec::new();
    for body in callback.bodies.lock().await.iter() {
        assert!(!body.contains("root_snapshot"));
        reports.push(serde_json::from_str::<ProgressReport>(body)?);
    }
    assert!(!reports.is_empty());
    let events: Vec<_> = reports
        .iter()
        .filter_map(|report| report.diagnostics.as_ref())
        .flat_map(|batch| batch.events.iter())
        .collect();
    if deliverable {
        let root_events: Vec<_> = events
            .iter()
            .filter(|event| event.message.contains("root_source_metadata_capture"))
            .collect();
        assert_eq!(root_events.len(), 1, "captured root error must be delivered once");
        assert_eq!(
            root_events[0].message.matches("root_source_metadata_capture").count(),
            1
        );
        assert!(root_events[0].message.contains("PermissionDenied"));
        assert!(root_events[0].message.contains("Diagnostic processing stopped"));
        assert_eq!(root_events[0].attempt_id, request.execution_id);
    } else {
        let pending: StatisticsCheckpoint = serde_json::from_str(&manager.statistics_checkpoint().await?)?;
        assert!(pending.callback.diagnostics_incomplete);
        let pending = serde_json::to_string(&pending.callback.outbox)?;
        assert!(pending.contains("root_source_metadata_capture"));
        assert!(pending.contains("Diagnostic processing stopped"));
    }
    let final_report = reports.last().unwrap();
    assert!(final_report.is_final);
    assert_eq!(final_report.task_error_count, 2);
    assert!(
        !final_report.diagnostics_complete,
        "failed diagnostic persistence or delivery must remain visible"
    );
    assert_eq!(
        final_report.migration.as_ref().unwrap().execution_id,
        request.execution_id
    );
    assert_eq!(
        std::fs::read(source_dir.path().join("payload"))?,
        b"pipeline never started"
    );
    Ok(())
}

#[tokio::test]
async fn preserve_reporting_failure_keeps_captured_root_failure_through_final_callback() -> Result<()> {
    preserve_reporting_failure(true).await
}

#[tokio::test]
async fn preserve_reporting_failure_retains_both_causes_when_callback_is_unavailable() -> Result<()> {
    preserve_reporting_failure(false).await
}
