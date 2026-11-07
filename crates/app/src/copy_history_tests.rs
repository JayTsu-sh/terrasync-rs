//! 边界与容量测试只处理协议和保留决定；实际历史数据交付由真实 S3 实验室覆盖。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::observed_fixtures::observation;
use crate::storage_factory::{CreateStorageOptions, create_storage};
use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory};
use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

async fn worker(limit: usize) -> (HistoryCopyWorker, Receiver<StorageEntryMessage>, TempDir, TempDir) {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(target.path().join("payload"), b"retained-data").unwrap();
    let from = Arc::new(
        create_storage(&source.path().to_string_lossy(), CreateStorageOptions::default())
            .await
            .unwrap(),
    );
    let to = Arc::new(
        create_storage(&target.path().to_string_lossy(), CreateStorageOptions::default())
            .await
            .unwrap(),
    );
    let mut broadcaster = BroadcastForwarder::new(32);
    let receiver = broadcaster.subscribe();
    let worker = HistoryCopyWorker::new(
        HistoryCopyContext {
            source: from,
            destination: to,
            broadcaster,
            job_id: "history-protocol".into(),
            options: CopyOptions::default(),
        },
        HistoryCopyPolicy {
            active_entry_limit: limit,
            target_history: Some(false),
            completion: HistoryCompletion::Incremental,
        },
    );
    (worker, receiver, source, target)
}

fn history(path: &str, count: usize, markers: bool) -> ObservedEntry {
    let entries: Vec<_> = (0..count)
        .map(|index| {
            observation(
                BackendKind::S3,
                path.into(),
                EntryKind::File,
                if markers { 0 } else { 7 },
                [0; 3],
                None,
                None,
                None,
                Some((Some(format!("v{index}")), index + 1 == count, markers)),
            )
        })
        .collect();
    let records = entries
        .iter()
        .flat_map(|entry| entry.history().unwrap().records().iter().cloned())
        .collect();
    entries
        .last()
        .unwrap()
        .clone()
        .with_history(Some(Box::new(
            ObjectHistory::whole(records, HistoryOrder::Server).unwrap(),
        )))
        .unwrap()
}

#[tokio::test]
async fn unchanged_history_closes_without_a_false_delivery_event() {
    let (mut worker, mut received, _source, target) = worker(1).await;
    let entry = Arc::new(history("payload", 3, false));
    worker
        .process(&StorageEntryMessage::CopyEntryBoundary(entry))
        .await
        .unwrap();
    assert!(worker.entries.is_empty());
    worker.finish().await.unwrap();
    assert!(received.recv().await.is_none());
    assert_eq!(std::fs::read(target.path().join("payload")).unwrap(), b"retained-data");
}

#[tokio::test]
async fn missing_last_segment_reports_one_entry_failure_without_success() {
    let (mut worker, mut received, _source, target) = worker(1).await;
    let segments = history("payload", 3, false).into_segments(1);
    worker
        .process(&StorageEntryMessage::CopyEntryBoundary(Arc::new(segments[0].clone())))
        .await
        .unwrap();
    let result = worker.finish().await;
    assert!(result.as_ref().is_err_and(AppError::is_reported_copy_failure));
    assert!(
        matches!(received.recv().await,Some(StorageEntryMessage::Error {event:ErrorEvent::Copy,reason,..}) if reason.contains("v0"))
    );
    assert!(received.recv().await.is_none());
    assert_eq!(std::fs::read(target.path().join("payload")).unwrap(), b"retained-data");
}

#[tokio::test]
async fn active_history_capacity_is_bounded_and_segment_order_is_checked() {
    let (mut worker, _received, _source, _target) = worker(1).await;
    let left = history("left", 3, false).into_segments(1);
    let right = history("right", 2, false).into_segments(1);
    worker.end_segment(&Arc::new(left[0].clone())).await.unwrap();
    assert!(matches!(
        worker.end_segment(&Arc::new(right[0].clone())).await,
        Err(AppError::CopyHistoryCapacity { limit: 1, .. })
    ));
    assert_eq!(worker.entries.len(), 1);
    assert!(matches!(
        worker.end_segment(&Arc::new(left[2].clone())).await,
        Err(AppError::CopyHistoryOrder {
            expected: 1,
            actual: 2,
            ..
        })
    ));
}

#[tokio::test]
async fn repeated_version_work_is_refused_before_another_operation() {
    let (mut worker, mut received, _source, _target) = worker(1).await;
    let entry = Arc::new(history("payload", 2, true));
    let first = ObservedItem::all(&entry).next().unwrap();
    worker.version(&first, EntryChange::New).await.unwrap();
    assert!(matches!(
        worker.version(&first, EntryChange::New).await,
        Err(AppError::CopyHistoryVersionOrder {
            previous: 0,
            actual: 0,
            ..
        })
    ));
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn latest_delete_marker_retains_target_once_without_delivery() {
    let (mut worker, mut received, _source, target) = worker(1).await;
    for entry in history("payload", 20, true).into_segments(3) {
        worker.copy_segment(&Arc::new(entry)).await.unwrap();
    }
    assert!(worker.entries.is_empty());
    worker.finish().await.unwrap();
    assert!(
        matches!(received.recv().await,Some(StorageEntryMessage::TargetPreserved {retention,..}) if retention.source_path.as_deref()==Some("payload"))
    );
    assert!(received.recv().await.is_none());
    assert_eq!(std::fs::read(target.path().join("payload")).unwrap(), b"retained-data");
}

#[tokio::test]
async fn missing_directory_marker_keeps_the_conservative_handler_and_has_no_delivery() {
    let (mut worker, mut received, _source, target) = worker(1).await;
    let entry = Arc::new(history("payload", 2, false));
    let item = ObservedItem::all(&entry).next().unwrap();
    assert!(
        !worker
            .process(&StorageEntryMessage::Changed {
                entry: item,
                kind: ChangeKind::DirectoryMarkerRemoved,
            })
            .await
            .unwrap()
    );
    worker
        .process(&StorageEntryMessage::CopyEntryBoundary(entry))
        .await
        .unwrap();
    worker.finish().await.unwrap();
    assert!(received.recv().await.is_none());
    assert_eq!(std::fs::read(target.path().join("payload")).unwrap(), b"retained-data");
}
