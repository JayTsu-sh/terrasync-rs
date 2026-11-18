#![allow(clippy::unwrap_used)]
use super::*;

use std::result::Result as StdResult;
use std::time::Duration;

use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory};
use task_model::observation::ObservedItem;
use tokio::time::timeout;
use transport::in_process::create_in_process_pair;
use transport::message::ReceiverMsg;
use transport::traits::SenderTransport;

use crate::callback::CallbackEndpoint;
use crate::config::JobType;
use crate::consumer::stats::callback_state::CallbackState;
use crate::consumer::stats::{FullStats, IncrementalStats, ProgressBar, ProgressDetail, StatisticConsumer, StatsKind};
use crate::file_copy::{CopyOptions, VersionCopyScope, copy_versions, prepare_directory};
use crate::observed_fixtures::{directory_marker, observation};
use crate::remote_receiver_session::RemoteSessionState;
use crate::storage_factory::{CreateStorageOptions, create_storage};

type TestResult = StdResult<(), Box<dyn std::error::Error>>;

fn content_history() -> Arc<ObservedEntry> {
    let mut records = Vec::new();
    let mut last = None;
    for index in 0..20 {
        let item = observation(
            BackendKind::S3,
            "a/".into(),
            EntryKind::File,
            7,
            [0; 3],
            None,
            None,
            None,
            Some((Some(format!("v{index}")), index == 19, false)),
        );
        records.push(item.history().unwrap().records()[0].clone());
        last = Some(item);
    }
    Arc::new(
        last.unwrap()
            .with_history(Some(Box::new(
                ObjectHistory::whole(records, HistoryOrder::Server).unwrap(),
            )))
            .unwrap(),
    )
}

#[tokio::test]
async fn unsupported_content_history_is_one_skip_without_success_failure_or_counter_reset() -> TestResult {
    let root = tempfile::tempdir()?;
    let destination = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let entry = content_history();
    let mut broadcaster = BroadcastForwarder::new(4);
    let mut receiver = broadcaster.subscribe();
    // 若进入传输，该观察不属于本地 source，会立即失败；跳过决策无需访问源端。
    copy_versions(
        ObservedItem::all(&entry),
        &destination,
        &destination,
        CopyOptions::default(),
        "directory-object",
        &broadcaster,
        VersionCopyScope::WholeEntry,
    )
    .await?;
    let skipped = receiver.recv().await.unwrap();
    assert!(matches!(&skipped, StorageEntryMessage::CopySkipped { entry, reason }
        if entry.history().unwrap().records().len() == 20 && !reason.is_empty()));
    assert!(receiver.try_recv().is_err());
    let mut tracker = SkipTracker::default();
    for item in ObservedItem::all(&entry) {
        assert!(tracker.skip(&item.entry, &destination, &broadcaster).await?);
    }
    assert!(matches!(
        receiver.recv().await,
        Some(StorageEntryMessage::CopySkipped { .. })
    ));
    assert!(receiver.try_recv().is_err());
    for stats in [
        StatsKind::Full(FullStats::new(
            JobType::Copy,
            "skip".into(),
            String::new(),
            String::new(),
        )),
        StatsKind::Incremental(IncrementalStats::new(
            JobType::IncrementalCopy,
            "skip".into(),
            String::new(),
            String::new(),
        )),
    ] {
        let mut consumer = StatisticConsumer {
            progress_bar: ProgressBar::new(stats.job_type().clone()),
            stats,
            job_dir: String::new(),
            callback_url: Some(CallbackEndpoint::parse("http://127.0.0.1/callback")?),
            generation_committed: None,
            migration: None,
            scan_snapshot: None,
            callback: CallbackState::default(),
            pb_handle: None,
        };
        consumer.update_statistics(&skipped);
        let batch = consumer.callback.outbox.prepare_batch().unwrap();
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].code, "copy_skipped");
        consumer.callback.outbox.ack(&batch.batch_id);
        let report = consumer.to_report(true);
        assert_eq!(report.skipped_count, 1);
        assert_eq!(consumer.stats.to_job_result().skipped_count, 1);
        assert_eq!(consumer.stats.to_final_stats().skipped_count, 1);
        match report.detail {
            ProgressDetail::Full {
                scanned_files,
                error_count,
                ..
            } => assert_eq!((scanned_files, error_count), (0, 0)),
            ProgressDetail::Incremental {
                new_count,
                changed_count,
                error_count,
                ..
            } => assert_eq!((new_count, changed_count, error_count), (0, 0, 0)),
        }
    }
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    let marker = Arc::new(directory_marker("empty", Vec::new()));
    assert!(unrepresentable_object(&marker, &destination).is_none());
    assert!(prepare_directory(&marker, &destination).await?);
    assert!(root.path().join("empty").is_dir());
    let child = observation(
        BackendKind::S3,
        "a/file".into(),
        EntryKind::File,
        7,
        [0; 3],
        None,
        None,
        None,
        None,
    );
    assert!(unrepresentable_object(&child, &destination).is_none());
    Ok(())
}

#[tokio::test]
async fn remote_skip_reports_once_per_entry_without_creating_target_or_requesting_payload() -> TestResult {
    let root = tempfile::tempdir()?;
    let destination = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
    let state = RemoteSessionState::default();
    let entry = content_history();
    let (source_transport, receiver_transport) = create_in_process_pair();
    for (index, item) in ObservedItem::all(&entry).enumerate() {
        assert!(
            state
                .skip_unrepresentable(&receiver_transport, &destination, index as i32, &item)
                .await?
        );
    }
    assert!(
        matches!(source_transport.recv().await, Some(ReceiverMsg::CopySkipped { ndx: 0, reason }) if !reason.is_empty())
    );
    assert!(
        timeout(Duration::from_millis(20), source_transport.recv())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    drop(source_transport);
    assert!(
        state
            .skip_unrepresentable(
                &receiver_transport,
                &destination,
                0,
                &ObservedItem::all(&entry).next().unwrap()
            )
            .await
            .is_err()
    );
    Ok(())
}
