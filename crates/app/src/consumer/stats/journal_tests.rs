#![allow(clippy::unwrap_used)]
use super::super::{
    ProgressDetail, ProgressReport,
    callback_state::{CallbackState, errors},
    delivery::PreparedCallback,
    delivery::tests::{Reply, consumer, fault, live_database, server},
};
use super::*;
use crate::{broadcast::BroadcastForwarder, observed_fixtures::observation};
use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory, ObservedEntry};
use std::collections::HashSet;
use std::{
    env,
    sync::atomic::{AtomicUsize, Ordering},
};
use task_model::observation::ObservedItem;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn observed() {
    let address = env::var("LAB_NOTIFICATION_PROXY_ADDR").unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let mut stream = TcpStream::connect(&address).await.unwrap();
            stream
                .write_all(format!("GET /observed HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut raw = Vec::new();
            stream.read_to_end(&mut raw).await.unwrap();
            if raw.ends_with(b"yes") {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "explicit isolated ClickHouse single/cluster service and fault proxy required"]
async fn real_cancelled_archive_keeps_unconfirmed_prefix_and_can_replay_it_without_recounting() {
    let live = live_database().await;
    let database = ClickHouseDatabase::new(&live.config, "callback-final").unwrap();
    let server = server(vec![]).await;
    let consumer = consumer(&server.endpoint);
    consumer.lock().await.callback = CallbackState::with_database(Some(database.clone()));
    consumer.lock().await.callback_url = None;
    for group in 0..4 {
        for (index, item) in ObservedItem::all(&history(group)).enumerate() {
            consumer.lock().await.update_statistics(&message(item, index == 0));
        }
    }
    let round = consumer.lock().await.callback.identity.round_id.clone();
    database
        .pending_notifications(&round, NotificationKind::Diagnostic)
        .await
        .unwrap();
    fault("stall").await;
    let actor = consumer.clone();
    let work = tokio::spawn(async move { flush(&actor).await });
    observed().await;
    let started = Instant::now();
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    let cancellation = started.elapsed();
    assert!(cancellation < Duration::from_secs(1));
    assert_eq!(consumer.lock().await.callback.outbox.pending_len(), 80);
    assert_eq!(consumer.lock().await.callback.unconfirmed_archive.len(), 64);
    fault("normal").await;
    flush_all(&consumer).await.unwrap();
    assert_eq!(consumer.lock().await.callback.outbox.pending_len(), 0);
    assert_eq!(errors(&consumer.lock().await.stats).total(), 4);
    let first = database
        .pending_notifications(&round, NotificationKind::Diagnostic)
        .await
        .unwrap();
    assert_eq!(first.len(), 64);
    let tail = database
        .pending_notifications_after(
            &round,
            NotificationKind::Diagnostic,
            Some(&first.last().unwrap().cursor()),
        )
        .await
        .unwrap();
    assert_eq!(tail.len(), 16);
    println!(
        "journal_cancel preserved_errors=80 replayed_unique_rows=80 failed_entries=4 cancellation_ms={}",
        cancellation.as_millis()
    );
}

#[tokio::test]
#[ignore = "explicit isolated ClickHouse single/cluster service and fault proxy required"]
async fn real_persistent_write_failure_stops_producer_even_while_reads_succeed() {
    persistent_failure("persistent_disconnect").await;
    persistent_failure("persistent_stall").await;
}

async fn persistent_failure(mode: &str) {
    let live = live_database().await;
    let database = ClickHouseDatabase::new(&live.config, "callback-final").unwrap();
    let server = server(vec![Reply::NoContent; 8]).await;
    let consumer = consumer(&server.endpoint);
    consumer.lock().await.callback = CallbackState::with_database(Some(database.clone()));
    let round = consumer.lock().await.callback.identity.round_id.clone();
    database
        .pending_notifications(&round, NotificationKind::Diagnostic)
        .await
        .unwrap();
    fault(mode).await;
    let mut broadcaster = BroadcastForwarder::new(8);
    let receiver = broadcaster.subscribe();
    let accepted = Arc::new(AtomicUsize::new(0));
    let sent = accepted.clone();
    let producer = tokio::spawn(async move {
        for group in 0..64 {
            for (index, item) in ObservedItem::all(&history(group)).enumerate() {
                if broadcaster.broadcast(message(item, index == 0)).await.is_err() {
                    return;
                }
                sent.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    let journal = consumer.lock().await.callback.journal.clone();
    let probe_database = database.clone();
    let probe_round = round.clone();
    let recovery = journal.recovery.clone();
    let reads = tokio::spawn(async move {
        loop {
            let _ = journal
                .request(Operation::Read, || {
                    probe_database.pending_notifications(&probe_round, NotificationKind::Diagnostic)
                })
                .await;
            sleep(Duration::from_millis(50)).await;
        }
    });
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(5),
        StatisticConsumer::process(consumer.clone(), receiver),
    )
    .await
    .unwrap();
    let process_finished = Instant::now();
    let identified = recovery.lock().await.first.as_ref().unwrap().0;
    let detection = identified.saturating_duration_since(started);
    let recovery_exit = process_finished.saturating_duration_since(identified);
    reads.abort();
    let _ = reads.await;
    producer.await.unwrap();
    assert!(matches!(result, Err(AppError::DiagnosticJournalStopped { .. })));
    assert!(started.elapsed() < Duration::from_secs(5));
    let count = accepted.load(Ordering::Relaxed);
    assert!((65..1280).contains(&count));
    assert_eq!(errors(&consumer.lock().await.stats).total(), count.div_ceil(20));
    assert_eq!(consumer.lock().await.callback.task_error_count, 1);
    assert!(consumer.lock().await.callback.diagnostics_incomplete);
    fault("normal").await;
    StatisticConsumer::end_with_database(consumer.clone(), None, Some(&database), false).await;
    assert_eq!(errors(&consumer.lock().await.stats).total(), count.div_ceil(20));
    let bodies = server.bodies.lock().await;
    let reports: Vec<ProgressReport> = bodies.iter().map(|raw| serde_json::from_str(raw).unwrap()).collect();
    let events: Vec<_> = reports
        .iter()
        .filter_map(|report| report.diagnostics.as_ref())
        .flat_map(|batch| &batch.events)
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.code == "copy_version_failed")
            .count(),
        count
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.code == "journal_unavailable")
            .count(),
        1
    );
    assert!(!reports.last().unwrap().diagnostics_complete);
    println!(
        "journal_persistent_failure mode={mode} accepted_errors={count} failed_entries={} task_errors=1 detection_ms={} identified_to_process_exit_ms={} total_with_final_ms={} reads_did_not_reset_window=true callback_details_retained=true",
        count.div_ceil(20),
        detection.as_millis(),
        recovery_exit.as_millis(),
        started.elapsed().as_millis()
    );
}

#[tokio::test]
#[ignore = "explicit isolated ClickHouse single/cluster service and fault proxy required"]
async fn real_restored_receipts_and_stale_checkpoint_do_not_resend_accepted_details() {
    let live = live_database().await;
    let database = ClickHouseDatabase::new(&live.config, "callback-final").unwrap();
    let server = server(vec![Reply::NoContent; 3]).await;
    let consumer = consumer(&server.endpoint);
    consumer.lock().await.callback = CallbackState::with_database(Some(database.clone()));
    let empty = serde_json::to_string(&consumer.lock().await.callback).unwrap();
    for (index, item) in ObservedItem::all(&history(0)).enumerate() {
        consumer.lock().await.update_statistics(&message(item, index == 0));
    }
    let old = serde_json::to_string(&consumer.lock().await.callback).unwrap();
    flush_all(&consumer).await.unwrap();
    consumer
        .lock()
        .await
        .callback
        .restore_checkpoint(serde_json::from_str(&empty).unwrap())
        .unwrap();
    let client = StatisticConsumer::build_callback_client(&server.endpoint).unwrap();
    let prepared = PreparedCallback::prepare(&consumer, false).await.unwrap();
    prepared.send(&client, &server.endpoint).await.unwrap();
    prepared.received(&consumer).await;
    let received = {
        let mut state = consumer.lock().await;
        assert_eq!(state.callback.pending_acknowledgements.len(), 20);
        state.callback.unconfirmed_archive = state.callback.pending_acknowledgements.clone();
        serde_json::to_string(&state.callback).unwrap()
    };
    consumer
        .lock()
        .await
        .callback
        .restore_checkpoint(serde_json::from_str(&received).unwrap())
        .unwrap();
    assert_eq!(
        StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false).await,
        Some(false)
    );
    assert!(consumer.lock().await.callback.pending_acknowledgements.is_empty());
    assert!(consumer.lock().await.callback.unconfirmed_archive.is_empty());
    consumer
        .lock()
        .await
        .callback
        .restore_checkpoint(serde_json::from_str(&old).unwrap())
        .unwrap();
    assert_eq!(
        StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false).await,
        Some(false)
    );
    assert_eq!(errors(&consumer.lock().await.stats).total(), 1);
    assert_eq!(consumer.lock().await.callback.outbox.pending_len(), 0);
    let bodies = server.bodies.lock().await;
    let reports: Vec<ProgressReport> = bodies.iter().map(|body| serde_json::from_str(body).unwrap()).collect();
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[0].diagnostics.as_ref().unwrap().events.len(), 20);
    assert!(reports[1..].iter().all(|report| report.diagnostics.is_none()));
    println!(
        "journal_checkpoint discovered_after_empty_checkpoint=20 accepted_errors=20 failed_entries=1 restored_receipts=20 stale_replay_duplicates=0"
    );
}

fn history(group: usize) -> Arc<ObservedEntry> {
    let mut records = Vec::new();
    let mut latest = None;
    for version in 1..=20 {
        let entry = observation(
            BackendKind::S3,
            format!("payload-{group}").into(),
            EntryKind::File,
            7,
            [0; 3],
            None,
            None,
            None,
            Some((Some(format!("v{version}")), version == 20, false)),
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

fn message(item: ObservedItem, first: bool) -> StorageEntryMessage {
    StorageEntryMessage::CopyVersionFailed {
        entry: item,
        phase: "source_metadata".into(),
        backend: BackendKind::S3,
        reason: "source tags read failed: HTTP=403 code=AccessDenied".into(),
        first_in_entry: first,
    }
}

#[test]
fn archive_prefix_release_cannot_clear_later_inflight_events_and_keeps_counts_separate() {
    let mut callback = CallbackState::default();
    callback.outbox.enqueue("copy", "first", "first", None);
    let prefix = callback.outbox.archive_prefix();
    let old = callback.outbox.prepare_batch().unwrap();
    callback.outbox.release_prefix(&prefix);
    callback.outbox.enqueue("copy", "second", "second", None);
    let current = callback.outbox.prepare_batch().unwrap();
    callback.outbox.release_prefix(&prefix);
    assert!(!callback.outbox.ack(&old.batch_id));
    assert_eq!(callback.outbox.prepare_batch(), Some(current));
    let archived = records(&prefix).unwrap();
    assert_eq!(batch(&archived).unwrap().events, prefix);
}

#[tokio::test]
#[ignore = "explicit isolated ClickHouse single/cluster service and fault proxy required"]
async fn real_journal_retains_1280_errors_with_bounded_memory_and_replays_without_recounting() {
    let live = live_database().await;
    let database = ClickHouseDatabase::new(&live.config, "callback-final").unwrap();
    // 前两次 HTTP 未成功，随后每批只需 HTTP 2xx；内部确认响应丢失不能重发 HTTP。
    let mut replies = vec![Reply::Refuse, Reply::Disconnect];
    replies.extend(vec![Reply::NoContent; 20]);
    let server = server(replies).await;
    let consumer = consumer(&server.endpoint);
    consumer.lock().await.callback = CallbackState::with_database(Some(database.clone()));
    let round = consumer.lock().await.callback.identity.round_id.clone();
    let mut broadcaster = BroadcastForwarder::new(32);
    let rx = broadcaster.subscribe();
    let actor = consumer.clone();
    let receiver = tokio::spawn(StatisticConsumer::process(actor, rx));
    fault("lost_ack").await;
    let mut peak = 0;
    for group in 0..64 {
        let entry = history(group);
        for (index, item) in ObservedItem::all(&entry).enumerate() {
            broadcaster.broadcast(message(item, index == 0)).await.unwrap();
            peak = peak.max(consumer.lock().await.callback.outbox.pending_len());
        }
    }
    drop(broadcaster);
    receiver.await.unwrap().unwrap();
    assert!(peak <= 64, "peak={peak}");
    assert_eq!(consumer.lock().await.callback.outbox.pending_len(), 0);
    assert_eq!(errors(&consumer.lock().await.stats).total(), 64);
    assert_eq!(consumer.lock().await.callback.task_error_count, 0);
    let client = StatisticConsumer::build_callback_client(&server.endpoint).unwrap();
    assert!(
        StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false)
            .await
            .is_none()
    );
    assert!(
        StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false)
            .await
            .is_none()
    );
    // 运行时内确认丢失只重试确认写入；不会再次发送已经 2xx 的明细。
    fault("lost_ack").await;
    for _ in 0..20 {
        assert!(
            StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false)
                .await
                .is_some()
        );
        assert_eq!(errors(&consumer.lock().await.stats).total(), 64);
        assert!(consumer.lock().await.callback.pending_acknowledgements.is_empty());
    }
    assert!(
        database
            .pending_notifications(&round, NotificationKind::Diagnostic)
            .await
            .unwrap()
            .is_empty()
    );
    StatisticConsumer::end_with_database(consumer.clone(), None, Some(&database), false).await;
    let bodies = server.bodies.lock().await;
    assert_eq!(bodies.len(), 23);
    let failed: Vec<ProgressReport> = bodies[..2]
        .iter()
        .map(|body| serde_json::from_str(body).unwrap())
        .collect();
    assert_eq!(failed[0].diagnostics, failed[1].diagnostics);
    let reports: Vec<ProgressReport> = bodies[2..]
        .iter()
        .map(|body| serde_json::from_str(body).unwrap())
        .collect();
    let events: Vec<_> = reports
        .iter()
        .filter_map(|report| report.diagnostics.as_ref())
        .flat_map(|batch| &batch.events)
        .collect();
    assert_eq!(events.len(), 1280);
    assert_eq!(
        events.iter().map(|event| &event.event_id).collect::<HashSet<_>>().len(),
        1280
    );
    assert!(
        events
            .iter()
            .all(|event| event.message.contains("version=v") && event.message.contains("AccessDenied"))
    );
    assert!(
        reports.iter().all(|report| report.task_error_count == 0
            && matches!(report.detail, ProgressDetail::Full { error_count: 64, .. }))
    );
    let summary = reports.last().unwrap();
    assert!(summary.is_final && summary.diagnostics_complete && summary.diagnostics.is_none());
    let identity = summary.callback_identity.as_ref().unwrap();
    let saved = database
        .final_report(&round, &identity.attempt_id, identity.sequence)
        .await
        .unwrap()
        .unwrap();
    assert!(saved.is_delivered());
    assert!(!saved.payload_json().contains("callback HTTP request failed"));
    println!(
        "journal_errors=1280 failed_entries=64 memory_observed_peak={peak} recovered_write_reply_losses=2 http_accepted_batches=20 duplicate_events=0 summary_complete=true"
    );
}
