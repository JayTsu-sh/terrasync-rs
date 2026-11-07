//! 需要实验室提供两个同名、内容不同的版本，验证恢复路径不会读取最新版本。
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::broadcast::BroadcastForwarder;
use crate::callback::CallbackEndpoint;
use crate::config::JobType;
use crate::consumer::stats::callback_state::CallbackState;
use crate::consumer::stats::{FullStats, ProgressBar, ProgressDetail, StatisticConsumer, StatsKind};
use crate::error::AppError;
use crate::file_copy::CopyOptions;
use crate::file_copy::prepare_directory;
use crate::file_copy::transfer as copy_transfer;
use crate::file_copy::{VersionCopyScope, copy_versions};
use crate::receiver::ReceiverConfig;
use crate::scan::source::WalkOptions;
use crate::sender::{SenderWorkerConfig, sender_worker};
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};
use crate::task_stream::AsyncReceiver;
use data_mover::model::{
    BackendKind, FailureClass, MetadataObservation, MetadataObservations, ObservationMode, ObservationPlan,
    ObservedEntry,
};
use task_model::observation::ObservedItem;
use task_model::{ErrorEvent, StorageEntryMessage, TransferMode};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::Instant;
use transport::in_process::create_in_process_pair;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[path = "s3_version_target_metadata.rs"]
mod target_metadata;

async fn segmented_sender(
    source: Arc<StorageContext>, destination: Arc<StorageContext>, entry: &Arc<ObservedEntry>, cap: usize,
) -> TestResult<(Result<(), AppError>, Vec<StorageEntryMessage>)> {
    let segments = entry.as_ref().clone().into_segments(cap);
    let (tx, rx) = async_channel::bounded(segments.len());
    for entry in segments {
        tx.send(StorageEntryMessage::Scanned(Arc::new(entry))).await?;
    }
    drop(tx);
    let mut broadcaster = BroadcastForwarder::new(64);
    let mut received = broadcaster.subscribe();
    let (transport, _receiver_transport) = create_in_process_pair();
    let active = Arc::new(AtomicUsize::new(0));
    let visited = Arc::new(AtomicUsize::new(0));
    let result = sender_worker(
        0,
        AsyncReceiver::new(rx),
        source,
        destination,
        Arc::new(transport),
        &broadcaster,
        &SenderWorkerConfig {
            target_history: Some(true),
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            large_file_log_threshold: u64::MAX,
        },
        None,
        None,
        &ReceiverConfig {
            job_id: "segmented-copy".into(),
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            job_dir: String::new(),
            transfer_mode: TransferMode::Direct,
        },
        active.clone(),
        visited.clone(),
        Arc::new(AtomicU64::new(0)),
    )
    .await;
    assert_eq!(active.load(Ordering::Relaxed), 0);
    assert_eq!(visited.load(Ordering::Relaxed), 1);
    drop(broadcaster);
    let mut messages = Vec::new();
    while let Some(message) = received.recv().await {
        messages.push(message);
    }
    Ok((result, messages))
}

#[tokio::test]
#[ignore = "requires owned real S3 source and history target"]
async fn segmented_full_copy_counts_one_logical_entry_and_keeps_every_version() -> TestResult {
    let source = Arc::new(
        create_storage(
            &std::env::var("LAB_MIGRATION_SOURCE_URL")?,
            CreateStorageOptions::default(),
        )
        .await?,
    );
    let scan = source.walkdir(None, WalkOptions::default()).await?;
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
    for cap in [1, 7] {
        let target = Arc::new(
            create_storage(
                &format!("{}/segment-success-{cap}", std::env::var("LAB_MIGRATION_HISTORY_URL")?),
                CreateStorageOptions::default(),
            )
            .await?,
        );
        let (result, messages) = segmented_sender(source.clone(), target, &entry, cap).await?;
        result?;
        let mut stats = FullStats::new(JobType::Copy, "segments".into(), String::new(), String::new());
        let progress = ProgressBar::new(JobType::Copy);
        let mut rows = 0;
        let mut completed = 0;
        for message in messages {
            rows += usize::from(matches!(message, StorageEntryMessage::New(_)));
            completed += usize::from(matches!(message, StorageEntryMessage::CopyEntryCompleted { .. }));
            stats.update_from_message(&message);
            progress.update_statistics(&message);
        }
        assert_eq!(
            (rows, completed, stats.total_count(), stats.total_size()),
            (20, 1, 1, 140)
        );
        assert!(matches!(
            progress.to_report("segments").detail,
            ProgressDetail::Full {
                scanned_files: 1,
                error_count: 0,
                total_bytes: 140,
                ..
            }
        ));
        eprintln!("segmented-full cap={cap} version_rows=20 completed_entries=1 logical_bytes=140");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated real S3 slash-key source and destinations"]
async fn nonempty_slash_objects_skip_filesystems_but_copy_complete_keys_and_history_to_objects() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_MIGRATION_SLASH_SOURCE_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let scan = source
        .walkdir(
            None,
            WalkOptions {
                include_tags: true,
                ..Default::default()
            },
        )
        .await?;
    let mut entries = Vec::new();
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(entry) = message {
            entries.push(entry);
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    assert!(entries.iter().any(|entry| entry.path().as_str() == "a/"));
    let root = TempDir::new()?;
    let targets = [
        (root.path().to_str().ok_or("invalid temp root")?.to_string(), true),
        (format!("{}/slash", std::env::var("LAB_MIGRATION_LATEST_URL")?), false),
        (format!("{}/slash", std::env::var("LAB_MIGRATION_HISTORY_URL")?), false),
    ];
    for (url, filesystem) in targets {
        let destination = create_storage(&url, CreateStorageOptions::default()).await?;
        let mut broadcaster = BroadcastForwarder::new(128);
        let mut receiver = broadcaster.subscribe();
        let mut consumer = StatisticConsumer {
            stats: StatsKind::Full(FullStats::new(
                JobType::Copy,
                "slash-objects".into(),
                String::new(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::Copy),
            job_dir: String::new(),
            callback_url: if filesystem {
                Some(CallbackEndpoint::parse(&std::env::var("LAB_MIGRATION_CALLBACK_URL")?)?)
            } else {
                None
            },
            generation_committed: None,
            migration: None,
            scan_snapshot: None,
            callback: CallbackState::default(),
            pb_handle: None,
        };
        for entry in &entries {
            if entry.history().is_some() {
                copy_versions(
                    ObservedItem::all(entry),
                    &source,
                    &destination,
                    CopyOptions {
                        transfer_mode: TransferMode::Direct,
                        ..Default::default()
                    },
                    "slash-objects",
                    &broadcaster,
                    VersionCopyScope::WholeEntry,
                )
                .await?;
            } else {
                assert!(prepare_directory(entry, &destination).await?);
            }
        }
        let mut skipped = 0;
        while let Ok(message) = receiver.try_recv() {
            if let StorageEntryMessage::CopySkipped { entry, reason } = &message {
                assert_eq!(entry.path().as_str(), "a/");
                assert!(!reason.is_empty());
                skipped += 1;
            }
            assert!(!matches!(
                &message,
                StorageEntryMessage::Error { .. } | StorageEntryMessage::CopyVersionFailed { .. }
            ));
            consumer.update_statistics(&message);
        }
        assert_eq!(skipped, usize::from(filesystem));
        let report = consumer.to_report(true);
        assert_eq!(report.skipped_count, u64::from(filesystem));
        assert!(matches!(report.detail, ProgressDetail::Full { error_count: 0, .. }));
        if filesystem {
            assert_eq!(tokio::fs::read(root.path().join("a/file")).await?, b"child");
            assert!(root.path().join("empty").is_dir());
            StatisticConsumer::send_final_callback(&Arc::new(Mutex::new(consumer)), None).await;
        }
        println!("slash_object filesystem={filesystem} skipped_entries={skipped} error_count=0 child_copied=true");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated real versioned S3 source and target"]
async fn required_consumer_failure_stops_history_worker_before_next_version_or_entry() -> TestResult {
    let source = Arc::new(
        create_storage(
            &std::env::var("LAB_MIGRATION_SOURCE_URL")?,
            CreateStorageOptions::default(),
        )
        .await?,
    );
    let destination = Arc::new(
        create_storage(
            &format!("{}/consumer-closed", std::env::var("LAB_MIGRATION_HISTORY_URL")?),
            CreateStorageOptions::default(),
        )
        .await?,
    );
    let scan = source.walkdir(None, WalkOptions::default()).await?;
    let mut found = None;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(entry) = message
            && entry.path().as_str() == "payload"
        {
            found = Some(entry);
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    let entries = found
        .ok_or("source payload missing")?
        .as_ref()
        .clone()
        .into_segments(10);
    assert_eq!(entries.len(), 2);
    let (tx, rx) = async_channel::bounded(2);
    for entry in entries {
        tx.send(StorageEntryMessage::Scanned(Arc::new(entry))).await?;
    }
    drop(tx);
    let mut broadcaster = BroadcastForwarder::new(1);
    let mut blocked = broadcaster.subscribe();
    broadcaster
        .broadcast(StorageEntryMessage::Diagnostic {
            event: ErrorEvent::Copy,
            path: "sentinel".into(),
            reason: "queue is full".into(),
        })
        .await?;
    drop(broadcaster.subscribe());
    let (transport, _receiver_transport) = create_in_process_pair();
    let active = Arc::new(AtomicUsize::new(0));
    let visited = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let result = sender_worker(
        0,
        AsyncReceiver::new(rx),
        source,
        destination,
        Arc::new(transport),
        &broadcaster,
        &SenderWorkerConfig {
            target_history: None,
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            large_file_log_threshold: u64::MAX,
        },
        None,
        None,
        &ReceiverConfig {
            job_id: "consumer-closed".into(),
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            job_dir: String::new(),
            transfer_mode: TransferMode::Direct,
        },
        active.clone(),
        visited.clone(),
        Arc::new(AtomicU64::new(0)),
    )
    .await;
    assert!(matches!(result, Err(AppError::ConsumerDisconnected { .. })));
    assert_eq!(active.load(Ordering::Relaxed), 0);
    assert_eq!(visited.load(Ordering::Relaxed), 1);
    assert!(matches!(
        blocked.recv().await,
        Some(StorageEntryMessage::Diagnostic { .. })
    ));
    assert!(blocked.try_recv().is_err());
    println!(
        "required_consumer_closed=typed_error full_peer_queue=cancelled visited_entries=1 elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated real S3 source with denied version-data reads"]
async fn data_failure_blocks_later_versions_and_counts_each_delivered_entry_once() -> TestResult {
    let source = Arc::new(
        create_storage(
            &std::env::var("LAB_MIGRATION_DENIED_SOURCE_URL")?,
            CreateStorageOptions::default(),
        )
        .await?,
    );
    let scan = source.walkdir(None, WalkOptions::default()).await?;
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
    for cap in [20, 10] {
        let entries = entry.as_ref().clone().into_segments(cap);
        let entry_count = entries.len();
        let (tx, rx) = async_channel::bounded(entry_count);
        for entry in entries {
            tx.send(StorageEntryMessage::Scanned(Arc::new(entry))).await?;
        }
        drop(tx);
        let destination = Arc::new(
            create_storage(
                &format!("{}/data-failure-{cap}", std::env::var("LAB_MIGRATION_HISTORY_URL")?),
                CreateStorageOptions::default(),
            )
            .await?,
        );
        let mut broadcaster = BroadcastForwarder::new(64);
        let mut receiver = broadcaster.subscribe();
        let (transport, _receiver_transport) = create_in_process_pair();
        let active = Arc::new(AtomicUsize::new(0));
        let result = sender_worker(
            0,
            AsyncReceiver::new(rx),
            Arc::clone(&source),
            destination,
            Arc::new(transport),
            &broadcaster,
            &SenderWorkerConfig {
                target_history: None,
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                large_file_log_threshold: u64::MAX,
            },
            None,
            None,
            &ReceiverConfig {
                job_id: "version-data-failure".into(),
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                job_dir: String::new(),
                transfer_mode: TransferMode::Direct,
            },
            Arc::clone(&active),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
        .await;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.is_reported_copy_failure() && error.reported_copy_data_failed())
        );
        assert_eq!(active.load(Ordering::Relaxed), 0);
        drop(broadcaster);
        let mut stats = FullStats::new(JobType::Copy, "data-failure".into(), String::new(), String::new());
        let progress = ProgressBar::new(JobType::Copy);
        let mut failed_entries = 0;
        let mut failed_versions = 0;
        while let Some(message) = receiver.recv().await {
            stats.update_from_message(&message);
            progress.update_statistics(&message);
            match message {
                StorageEntryMessage::CopyVersionFailed {
                    entry,
                    phase,
                    backend,
                    reason,
                    first_in_entry,
                    ..
                } => {
                    assert!(!entry.view().version_id().is_empty());
                    assert_eq!(backend, BackendKind::S3);
                    assert!(reason.contains("AccessDenied"), "{reason}");
                    assert!(reason.contains("HTTP=403"), "{reason}");
                    if failed_versions > 0 {
                        assert_eq!(phase, "blocked_by_prior_version");
                    } else {
                        assert_eq!(phase, "Transfer");
                    }
                    failed_versions += 1;
                    failed_entries += usize::from(first_in_entry);
                }
                other => return Err(format!("failed history must not publish successful facts: {other:?}").into()),
            }
        }
        assert_eq!(failed_versions, 20);
        assert_eq!(failed_entries, 1);
        assert_eq!(stats.total_count(), 0);
        assert!(matches!(
            progress.to_report("data-failure").detail,
            ProgressDetail::Full { error_count: 1, .. }
        ));
        println!("history_segment_cap={cap} failed_entries={failed_entries} version_error_details={failed_versions}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated real S3 source with denied version-tag reads"]
async fn metadata_failures_copy_every_version_but_count_one_failed_entry() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_MIGRATION_DENIED_SOURCE_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let destination = create_storage(
        &std::env::var("LAB_MIGRATION_FAILED_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
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
    let mut consumer = StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            "metadata-failures".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: std::env::var("LAB_MIGRATION_CALLBACK_URL")
            .ok()
            .as_deref()
            .map(CallbackEndpoint::parse)
            .transpose()?,
        generation_committed: None,
        migration: None,
        scan_snapshot: None,
        callback: CallbackState::default(),
        pb_handle: None,
    };
    let mut entry = None;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(found) = &message
            && found.path().as_str() == "payload"
        {
            entry = Some(found.clone());
        }
        if matches!(&message, StorageEntryMessage::Diagnostic { .. }) {
            consumer.update_statistics(&message);
        }
    }
    assert!(scan.take_error().is_some());
    let entry = entry.ok_or("readable source entry was lost after metadata failure")?;
    let records = entry.history().ok_or("source history missing")?.records();
    assert_eq!(records.len(), 20);
    assert!(records.iter().all(|record| matches!(
        record.metadata().map(MetadataObservations::tags),
        Some(MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            ..
        })
    )));
    let mut broadcaster = BroadcastForwarder::new(64);
    let mut receiver = broadcaster.subscribe();
    let result = copy_versions(
        ObservedItem::all(&entry),
        &source,
        &destination,
        CopyOptions::default(),
        "metadata-failures",
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
    while let Some(message) = receiver.recv().await {
        consumer.update_statistics(&message);
    }
    let mut report = consumer.to_report(true);
    assert!(matches!(
        report.detail,
        ProgressDetail::Full {
            scanned_files: 0,
            error_count: 1,
            ..
        }
    ));
    report.diagnostics = consumer.callback.outbox.prepare_batch();
    let failures: Vec<_> = report
        .diagnostics
        .as_ref()
        .ok_or("callback error detail missing")?
        .events
        .iter()
        .filter(|event| event.code == "copy_version_failed")
        .collect();
    assert_eq!(failures.len(), 20);
    for (record, failure) in records.iter().zip(failures) {
        assert!(
            failure
                .message
                .contains(record.version_id().ok_or("source version missing")?)
        );
        assert!(failure.message.contains("phase=source_metadata"));
        assert!(failure.message.contains("PermissionDenied"));
    }
    println!("failed_entry_count=1 version_error_details=20; every version's data remains published");
    if consumer.callback_url.is_some() {
        let consumer = Arc::new(Mutex::new(consumer));
        StatisticConsumer::send_final_callback(&consumer, None).await;
        assert!(consumer.lock().await.callback.outbox.prepare_batch().is_none());
    }
    let source = Arc::new(source);
    for cap in [1, 7] {
        let destination = Arc::new(
            create_storage(
                &format!("{}/segments-{cap}", std::env::var("LAB_MIGRATION_FAILED_URL")?),
                CreateStorageOptions::default(),
            )
            .await?,
        );
        let (result, messages) = segmented_sender(source.clone(), destination, &entry, cap).await?;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.is_reported_copy_failure() && !error.reported_copy_data_failed())
        );
        let mut stats = FullStats::new(
            JobType::Copy,
            "segment-metadata-errors".into(),
            String::new(),
            String::new(),
        );
        let mut errors = 0;
        let mut first = 0;
        for message in messages {
            if let StorageEntryMessage::CopyVersionFailed {
                phase, first_in_entry, ..
            } = &message
            {
                assert_eq!(phase, "source_metadata");
                errors += 1;
                first += usize::from(*first_in_entry);
            }
            stats.update_from_message(&message);
        }
        assert_eq!((errors, first, stats.error_stats.copy), (20, 1, 1));
        assert_eq!(stats.total_count(), 0);
        eprintln!("segmented-metadata-failure cap={cap} version_errors=20 failed_entries=1 target_data_versions=20");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated versioned source and latest/history S3 destinations"]
async fn full_copy_counts_versioned_source_by_delivered_entry() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_MIGRATION_SOURCE_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let scan = source.walkdir(None, WalkOptions::default()).await?;
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
    let versions = entry.history().ok_or("source is not versioned")?.records().len();
    assert_eq!(versions, 20);
    for (variable, expected_versions) in [("LAB_MIGRATION_LATEST_URL", 1), ("LAB_MIGRATION_HISTORY_URL", 20)] {
        let destination = create_storage(&std::env::var(variable)?, CreateStorageOptions::default()).await?;
        let mut broadcaster = BroadcastForwarder::new(64);
        let mut receiver = broadcaster.subscribe();
        copy_versions(
            ObservedItem::all(&entry),
            &source,
            &destination,
            CopyOptions::default(),
            "entry-count-live",
            &broadcaster,
            VersionCopyScope::WholeEntry,
        )
        .await?;
        drop(broadcaster);
        let mut stats = FullStats::new(JobType::Copy, "entry-count-live".into(), String::new(), String::new());
        let progress = ProgressBar::new(JobType::Copy);
        let mut rows = 0;
        let mut completions = 0;
        while let Some(message) = receiver.recv().await {
            if matches!(&message, StorageEntryMessage::New(_)) {
                rows += 1;
            }
            if matches!(&message, StorageEntryMessage::CopyEntryCompleted { .. }) {
                completions += 1;
            }
            stats.update_from_message(&message);
            progress.update_statistics(&message);
        }
        assert_eq!(rows, expected_versions);
        assert_eq!(completions, 1);
        assert_eq!(stats.total_count(), 1);
        assert_eq!(stats.total_size(), expected_versions * 7);
        assert!(matches!(progress.to_report("entry-count-live").detail,
            ProgressDetail::Full { scanned_files: 1, total_bytes, .. } if total_bytes == (expected_versions * 7) as u64));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires LAB_S3_TEST_URL and LAB_S3_OLD_VERSION; payload v1 is 9 MiB + 137 bytes of A"]
async fn checkpointed_copy_reads_requested_historical_version() -> TestResult {
    let source = create_storage(
        &std::env::var("LAB_S3_TEST_URL")?,
        CreateStorageOptions::new(Some(4 * 1024 * 1024), false),
    )
    .await?;
    let destination = TempDir::new()?;
    let target = create_storage(
        destination.path().to_str().ok_or("non UTF-8 test root")?,
        CreateStorageOptions::new(Some(4 * 1024 * 1024), false),
    )
    .await?;
    let old_version = std::env::var("LAB_S3_OLD_VERSION")?;
    let scan = source.walkdir(None, WalkOptions::default()).await?;
    let mut selected = None;
    while let Some(message) = scan.next().await {
        if let StorageEntryMessage::Scanned(entry) = message
            && entry.path().as_str() == "payload"
            && let Some(index) = entry.history().and_then(|history| {
                history
                    .records()
                    .iter()
                    .position(|record| record.version_id() == Some(old_version.as_str()))
            })
        {
            selected = Some(ObservedItem {
                entry,
                version_index: Some(index),
            });
        }
    }
    if let Some(error) = scan.take_error() {
        return Err(error.into());
    }
    let selected = selected.ok_or("requested source version was not listed")?;
    copy_transfer::copy_item(
        &source,
        &target,
        &selected,
        &CopyOptions {
            transfer_mode: TransferMode::Checkpoint,
            enable_integrity_check: true,
            ..Default::default()
        },
        None,
    )
    .await?;
    assert_eq!(
        tokio::fs::read(destination.path().join("payload")).await?,
        vec![b'A'; 9 * 1024 * 1024 + 137]
    );
    Ok(())
}
