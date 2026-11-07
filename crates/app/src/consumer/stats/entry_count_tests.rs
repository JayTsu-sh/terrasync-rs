//! 历史版本的行级确认与 entry 级复制完成分别推进，防止部分完成和版本数放大计数。
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory, ObservedEntry, StoragePath};
use task_model::observation::ObservedItem;
use task_model::{SourcePresence, StorageEntryMessage, TargetRetention, TargetRetentionReason};

use super::callback_state::CallbackState;
use super::journal;
use super::{FullStats, ProgressBar, ProgressDetail, StatisticConsumer, StatsKind};
use crate::config::JobType;
use crate::observed_fixtures::observation;

fn retention_consumer() -> StatisticConsumer {
    StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            "retention".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: None,
        generation_committed: None,
        scan_snapshot: None,
        callback: CallbackState::default(),
        pb_handle: None,
    }
}

#[test]
fn retained_entry_and_unexpanded_scope_are_visible_without_counting_errors_or_fake_entries() {
    let mut consumer = retention_consumer();
    let retention = TargetRetention::entry(
        &StoragePath::new("payload").unwrap(),
        TargetRetentionReason::SourceDeletionUnconfirmed,
    );
    for decision in [
        retention,
        TargetRetention::source_range(TargetRetentionReason::FilteredSelection),
    ] {
        consumer.update_statistics(&StorageEntryMessage::TargetPreserved {
            retention: Box::new(decision),
            reason: "source deletion was not verified; retaining target".into(),
        });
    }
    let batch = consumer.callback.outbox.prepare_batch().unwrap();
    assert_eq!(batch.events.len(), 2);
    assert_eq!(batch.events[0].code, "target_preserved");
    assert_eq!(
        batch.events[0].retention.as_ref().unwrap().source_presence,
        SourcePresence::Unknown
    );
    let report = consumer.to_report(true);
    assert_eq!(report.target_preserved_count, 1);
    assert_eq!(report.task_error_count, 0);
    assert!(matches!(
        report.detail,
        ProgressDetail::Full {
            error_count: 0,
            scanned_files: 0,
            ..
        }
    ));
    assert_eq!(consumer.callback.outbox.prepare_batch().unwrap(), batch);
    assert!(consumer.callback.outbox.ack(&batch.batch_id));
    assert!(consumer.callback.outbox.prepare_batch().is_none());
    assert_eq!(consumer.to_report(true).target_preserved_count, 1);
}

#[test]
fn retained_details_remain_bounded_redacted_and_restorable_without_recounting() {
    let mut consumer = retention_consumer();
    let mut retention = TargetRetention::entry(
        &StoragePath::new("payload").unwrap(),
        TargetRetentionReason::SourceQueryFailed,
    );
    retention.target_path = Some(format!("s3://access:secret@bucket/{}", "x".repeat(5000)));
    consumer.update_statistics(&StorageEntryMessage::TargetPreserved {
        retention: Box::new(retention),
        reason: "cannot verify s3://access:secret@bucket/path".into(),
    });
    let batch = consumer.callback.outbox.prepare_batch().unwrap();
    assert!(batch.events[0].is_bounded());
    let encoded = serde_json::to_string(&batch).unwrap();
    assert!(!encoded.contains("access:secret"));
    assert!(journal::records(&batch.events).is_ok());
    let saved = serde_json::to_vec(&consumer.callback).unwrap();
    let mut restored = CallbackState::default();
    restored
        .restore_checkpoint(serde_json::from_slice(&saved).unwrap())
        .unwrap();
    assert_eq!(restored.target_preserved_count, 1);
    assert_eq!(restored.outbox.prepare_batch().unwrap(), batch);
    assert!(restored.outbox.ack(&batch.batch_id));
    assert_eq!(restored.target_preserved_count, 1);
}

fn history_entry() -> Arc<ObservedEntry> {
    let mut records = Vec::new();
    let mut latest = None;
    for index in 1..=20 {
        let entry = observation(
            BackendKind::S3,
            "payload".into(),
            EntryKind::File,
            index,
            [0; 3],
            None,
            None,
            None,
            Some((Some(format!("v{index}")), index == 20, false)),
        );
        records.push(entry.history().unwrap().records()[0].clone());
        latest = Some(entry);
    }
    Arc::new(
        latest
            .unwrap()
            .with_history(Some(Box::new(
                ObjectHistory::whole(records, HistoryOrder::Server).unwrap(),
            )))
            .unwrap(),
    )
}

fn check_count(stats: &FullStats, progress: &ProgressBar, count: usize, bytes: u64) {
    assert_eq!(stats.total_count(), count);
    assert_eq!(stats.total_size(), bytes as i64);
    match progress.to_report("versions").detail {
        ProgressDetail::Full {
            scanned_files,
            total_bytes,
            ..
        } => {
            assert_eq!(scanned_files, count as u64);
            assert_eq!(total_bytes, bytes);
        }
        other @ ProgressDetail::Incremental { .. } => panic!("unexpected report: {other:?}"),
    }
}

#[test]
fn partial_and_complete_history_entries_have_distinct_counts_and_selected_bytes() {
    let entry = history_entry();
    let mut stats = FullStats::new(JobType::Copy, "versions".into(), String::new(), String::new());
    let progress = ProgressBar::new(JobType::Copy);
    for item in ObservedItem::all(&entry) {
        let message = StorageEntryMessage::New(item);
        stats.update_from_message(&message);
        progress.update_statistics(&message);
        check_count(&stats, &progress, 0, 0);
    }
    let completed = StorageEntryMessage::CopyEntryCompleted {
        entry,
        logical_bytes: 210,
    };
    stats.update_from_message(&completed);
    progress.update_statistics(&completed);
    check_count(&stats, &progress, 1, 210);
}

#[test]
fn latest_only_selection_counts_one_entry_and_only_the_selected_payload() {
    let entry = history_entry();
    let mut stats = FullStats::new(JobType::Copy, "latest".into(), String::new(), String::new());
    let progress = ProgressBar::new(JobType::Copy);
    let message = StorageEntryMessage::New(ObservedItem::all(&entry).last().unwrap());
    stats.update_from_message(&message);
    progress.update_statistics(&message);
    check_count(&stats, &progress, 0, 0);
    let completed = StorageEntryMessage::CopyEntryCompleted {
        entry,
        logical_bytes: 20,
    };
    stats.update_from_message(&completed);
    progress.update_statistics(&completed);
    check_count(&stats, &progress, 1, 20);
}

#[test]
fn multiple_version_errors_count_one_failed_entry_and_keep_every_version_reason() {
    let entry = history_entry();
    let mut consumer = StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            "failed-versions".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: None,
        generation_committed: None,
        scan_snapshot: None,
        callback: CallbackState::default(),
        pb_handle: None,
    };
    for (index, item) in ObservedItem::all(&entry).enumerate() {
        consumer.update_statistics(&StorageEntryMessage::CopyVersionFailed {
            entry: item,
            phase: "source_metadata".into(),
            backend: BackendKind::S3,
            reason: "PermissionDenied: AccessDenied reading object tags".into(),
            first_in_entry: index == 0,
        });
    }
    let mut report = consumer.to_report(true);
    assert!(matches!(
        report.detail,
        ProgressDetail::Full {
            error_count: 1,
            scanned_files: 0,
            ..
        }
    ));
    report.diagnostics = consumer.callback.outbox.prepare_batch();
    let batch = report.diagnostics.as_ref().unwrap();
    assert_eq!(batch.events.len(), 20);
    for (index, event) in batch.events.iter().enumerate() {
        assert_eq!(event.path.as_deref(), Some("payload"));
        assert!(event.message.contains(&format!("version=v{} ", index + 1)));
        assert!(event.message.contains("phase=source_metadata"));
        assert!(event.message.contains("AccessDenied reading object tags"));
    }
    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["detail"]["error_count"], 1);
    assert_eq!(json["final_stats"]["errors"]["copy"], 1);
    assert_eq!(json["final_stats"]["errors"]["scan"], 0);
    assert_eq!(json["diagnostics"]["events"].as_array().unwrap().len(), 20);
}
