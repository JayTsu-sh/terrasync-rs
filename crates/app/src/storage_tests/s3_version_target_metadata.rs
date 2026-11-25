//! 真实 S3 目标 tags 写入失败仍保留所有数据版本，按 entry 计错并交付具体原因。
use std::sync::Arc;

use data_mover::model::{ObservationMode, ObservationPlan, ObservedEntry};
use task_model::StorageEntryMessage;
use task_model::observation::ObservedItem;
use tokio::sync::Mutex;

use super::{TestResult, segmented_sender};
use crate::broadcast::BroadcastForwarder;
use crate::callback::CallbackEndpoint;
use crate::config::JobType;
use crate::consumer::stats::callback_state::CallbackState;
use crate::consumer::stats::{FullStats, ProgressBar, ProgressDetail, StatisticConsumer, StatsKind};
use crate::file_copy::{CopyOptions, VersionCopyScope, copy_versions};
use crate::scan::source::WalkOptions;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

fn consumer(integrity: bool) -> TestResult<StatisticConsumer> {
    Ok(StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            format!("target_metadata_{integrity}"),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: Some(CallbackEndpoint::parse(&std::env::var(
            "LAB_TARGET_METADATA_CALLBACK",
        )?)?),
        generation_committed: None,
        migration: None,
        scan_snapshot: None,
        callback: CallbackState::default(),
        pb_handle: None,
    })
}

async fn scan_entry(source: &StorageContext) -> TestResult<Arc<ObservedEntry>> {
    let scan = source
        .walkdir(
            None,
            WalkOptions {
                observation_plan: Some(
                    ObservationPlan::default()
                        .with_tags(ObservationMode::BestEffort)
                        .with_timestamps(ObservationMode::InlineOnly)
                        .with_ownership_mode(ObservationMode::InlineOnly),
                ),
                ..Default::default()
            },
        )
        .await?;
    let mut entry = None;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(found) = message
            && found.path().as_str() == "payload"
        {
            entry = Some(found);
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    let entry = entry.ok_or("source payload missing")?;
    assert_eq!(entry.history().ok_or("source history missing")?.records().len(), 20);
    Ok(entry)
}

async fn whole_entry(source: &StorageContext, entry: &Arc<ObservedEntry>, integrity: bool) -> TestResult {
    let target = create_storage(
        &format!("{}/whole-{integrity}", std::env::var("LAB_TARGET_METADATA_URL")?),
        CreateStorageOptions::default(),
    )
    .await?;
    let mut consumer = consumer(integrity)?;
    let bytes = consumer.get_bytes_tracker();
    let mut broadcaster = BroadcastForwarder::new(64);
    let mut receiver = broadcaster.subscribe();
    let result = copy_versions(
        ObservedItem::all(entry),
        source,
        &target,
        CopyOptions {
            enable_integrity_check: integrity,
            bytes_counter: Some(bytes.clone()),
            ..Default::default()
        },
        "target-metadata",
        &broadcaster,
        VersionCopyScope::WholeEntry,
    )
    .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.is_reported_copy_failure() && !error.reported_copy_data_failed())
    );
    drop(broadcaster);
    let mut failures = 0;
    let mut failed_entries = 0;
    while let Some(message) = receiver.recv().await {
        let StorageEntryMessage::CopyVersionFailed {
            phase,
            reason,
            first_in_entry,
            ..
        } = &message
        else {
            return Err(format!("metadata failure cannot publish complete facts: {message:?}").into());
        };
        assert_eq!(phase, "target_metadata_apply");
        assert!(
            reason.contains("AccessDenied") && reason.contains("HTTP=403"),
            "{reason}"
        );
        failures += 1;
        failed_entries += usize::from(*first_in_entry);
        consumer.update_statistics(&message);
    }
    assert_eq!((failures, failed_entries, bytes.published()), (20, 1, 140));
    assert!(matches!(
        consumer.to_report(true).detail,
        ProgressDetail::Full {
            error_count: 1,
            transferred_bytes: 140,
            ..
        }
    ));
    let consumer = Arc::new(Mutex::new(consumer));
    StatisticConsumer::send_final_callback(&consumer, None).await;
    assert!(consumer.lock().await.callback.outbox.prepare_batch().is_none());
    eprintln!(
        "target-metadata integrity={integrity} failed_entries=1 version_details=20 data_bytes=140 data_versions_retained=20"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned real S3 target tag refusal from run-version-entry-count-local.py"]
async fn target_metadata_failure_keeps_every_data_version_and_reports_one_failed_entry() -> TestResult {
    let source = Arc::new(
        create_storage(
            &std::env::var("LAB_MIGRATION_SOURCE_URL")?,
            CreateStorageOptions::default(),
        )
        .await?,
    );
    let entry = scan_entry(&source).await?;
    for integrity in [false, true] {
        whole_entry(&source, &entry, integrity).await?;
    }
    for cap in [1, 7] {
        let target = Arc::new(
            create_storage(
                &format!("{}/segments-{cap}", std::env::var("LAB_TARGET_METADATA_URL")?),
                CreateStorageOptions::default(),
            )
            .await?,
        );
        let (result, messages) = segmented_sender(source.clone(), target, &entry, cap).await?;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.is_reported_copy_failure() && !error.reported_copy_data_failed())
        );
        let mut stats = FullStats::new(
            JobType::Copy,
            "target-metadata-segment".into(),
            String::new(),
            String::new(),
        );
        let mut failures = 0;
        for message in messages {
            if let StorageEntryMessage::CopyVersionFailed { phase, .. } = &message {
                assert_eq!(phase, "target_metadata_apply");
                failures += 1;
            }
            stats.update_from_message(&message);
        }
        assert_eq!((failures, stats.error_stats.copy, stats.total_count()), (20, 1, 0));
    }
    Ok(())
}
