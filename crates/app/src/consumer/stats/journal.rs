//! 错误明细的有界暂存和分页；重放不进入累计统计。
use std::{fmt, future::Future, sync::Arc, time::Duration};

use db::scan::with_scan_cancellation;
use db::{ClickHouseDatabase, DatabaseError, NotificationCursor, NotificationKind, NotificationRecord};
use task_model::StorageEntryMessage;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{
    consumer::StatisticConsumer,
    diagnostics::{DiagnosticBatch, DiagnosticEvent, MAX_DIAGNOSTIC_BATCH_EVENTS},
};
use crate::error::{AppError, Result};

type Consumer = Arc<Mutex<StatisticConsumer>>;
pub(crate) const MAX_PENDING_CONFIRMATIONS: usize = MAX_DIAGNOSTIC_BATCH_EVENTS * 2;
const RECOVERY: Duration = Duration::from_secs(3);
const REQUEST: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct Recovery {
    first: Option<(Instant, Arc<DatabaseError>)>,
    failed_operations: u8,
}

#[derive(Clone, Copy)]
enum Operation {
    Archive = 1,
    Confirm = 4,
    Read = 2,
}

#[derive(Clone, Default)]
pub(crate) struct DiagnosticJournal {
    pub(crate) database: Option<ClickHouseDatabase>,
    pub(crate) producer: Arc<Mutex<()>>,
    pub(crate) delivery: Arc<Mutex<()>>,
    recovery: Arc<Mutex<Recovery>>,
    pub(crate) cursor: Option<NotificationCursor>,
    head_checked_revision: Option<u64>,
    pub(crate) in_flight: Option<Vec<NotificationRecord>>,
    pub(crate) failed: bool,
    pub(crate) terminal: Option<Arc<AppError>>,
}

impl fmt::Debug for DiagnosticJournal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiagnosticJournal")
            .field("database_configured", &self.database.is_some())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl DiagnosticJournal {
    pub(crate) fn new(database: Option<ClickHouseDatabase>) -> Self {
        Self {
            database,
            ..Self::default()
        }
    }
    pub(crate) fn reset_delivery(&mut self) {
        self.cursor = None;
        self.head_checked_revision = None;
        self.in_flight = None;
    }

    /// 所有分页、归档和确认共用同一次连接故障的预算，不按批次重新计时。
    async fn request<T, F, Fut>(&self, kind: Operation, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = db::Result<T>>,
    {
        loop {
            let first = self.recovery.lock().await.first.clone();
            let budget = match &first {
                Some((started, source)) => {
                    let remaining = RECOVERY.saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        return Err(AppError::DiagnosticDatabaseUnavailable { source: source.clone() });
                    }
                    REQUEST.min(remaining)
                }
                None => REQUEST,
            };
            let result = with_scan_cancellation(CancellationToken::new(), None, timeout(budget, operation())).await;
            let error = match result {
                Ok(Ok(value)) => {
                    let mut recovery = self.recovery.lock().await;
                    recovery.failed_operations &= !(kind as u8);
                    if recovery.failed_operations == 0 && recovery.first.take().is_some() {
                        warn!("Diagnostic database connection recovered");
                    }
                    return Ok(value);
                }
                Ok(Err(error)) => error,
                Err(source) => DatabaseError::NotificationRequestTimedOut { source },
            };
            if !error.is_transient_connectivity_failure() {
                return Err(error.into());
            }
            let (class, code) = error.diagnostic_class();
            warn!(
                error_class = class,
                server_code = code,
                "Diagnostic database request failed; retaining exact records"
            );
            let mut recovery = self.recovery.lock().await;
            recovery.failed_operations |= kind as u8;
            let (started, _) = recovery.first.get_or_insert_with(|| (Instant::now(), Arc::new(error)));
            let remaining = RECOVERY.saturating_sub(started.elapsed());
            drop(recovery);
            sleep(Duration::from_millis(50).min(remaining)).await;
        }
    }
}

pub(crate) fn records(events: &[DiagnosticEvent]) -> Result<Vec<NotificationRecord>> {
    events
        .iter()
        .map(|event| {
            Ok(NotificationRecord::new(
                event.round_id.clone(),
                event.attempt_id.clone(),
                event.sequence,
                NotificationKind::Diagnostic,
                serde_json::to_string(event)?,
            )?)
        })
        .collect()
}

pub(crate) fn batch(rows: &[NotificationRecord]) -> Result<DiagnosticBatch> {
    let mut events = Vec::with_capacity(rows.len());
    let mut identity = blake3::Hasher::new();
    identity.update(b"terrasync/diagnostic-page/v1\0");
    for row in rows {
        let event: DiagnosticEvent = serde_json::from_str(row.payload_json())?;
        if !event.is_bounded()
            || event.round_id != row.round_id()
            || event.attempt_id != row.attempt_id()
            || event.sequence != row.sequence()
        {
            return Err(AppError::DiagnosticArchiveInvalid {
                field: "event_identity",
            });
        }
        identity.update(row.record_id().as_bytes());
        events.push(event);
    }
    Ok(DiagnosticBatch {
        batch_id: identity.finalize().to_hex().to_string(),
        events,
        omitted_count: 0,
    })
}

pub(crate) fn is_diagnostic(message: &StorageEntryMessage) -> bool {
    matches!(
        message,
        StorageEntryMessage::Error { .. }
            | StorageEntryMessage::Diagnostic { .. }
            | StorageEntryMessage::CopyVersionFailed { .. }
            | StorageEntryMessage::CopySkipped { .. }
    )
}

/// 无已配置通知目的地时只保留累计数字，逐条写日志，不伪造接收确认。
pub(crate) async fn log_unrouted(consumer: &Consumer) {
    let mut state = consumer.lock().await;
    let events = state.callback.outbox.archive_prefix();
    for event in &events {
        warn!(event_id = %event.event_id, code = %event.code, message = %event.message, path = ?event.path, "Unsent diagnostic");
    }
    state.callback.outbox.release_prefix(&events);
}

pub(crate) async fn record(consumer: &Consumer, message: &StorageEntryMessage) -> Result<()> {
    {
        let mut state = consumer.lock().await;
        if let Some(source) = state.callback.journal.terminal.clone() {
            state.update_statistics(message);
            return Err(AppError::DiagnosticJournalStopped { source });
        }
        if !is_diagnostic(message) {
            state.update_statistics(message);
            return Ok(());
        }
    }
    let gate = consumer.lock().await.callback.journal.producer.clone();
    let _guard = gate.lock().await;
    if consumer.lock().await.callback.outbox.pending_len() >= MAX_DIAGNOSTIC_BATCH_EVENTS {
        if let Err(error) = flush(consumer).await {
            let mut state = consumer.lock().await;
            state.callback.journal.failed = true;
            state.callback.diagnostics_incomplete = true;
            state.update_statistics(message);
            return Err(error);
        }
    }
    consumer.lock().await.update_statistics(message);
    Ok(())
}

pub(crate) async fn flush(consumer: &Consumer) -> Result<()> {
    let (journal, events, url) = {
        let state = consumer.lock().await;
        (
            state.callback.journal.clone(),
            state.callback.outbox.archive_prefix(),
            state.callback_url.clone(),
        )
    };
    if events.is_empty() {
        return Ok(());
    }
    if journal.database.is_some() && !journal.failed {
        return persist_prefix(consumer).await;
    }
    if let Some(url) = url {
        let client =
            StatisticConsumer::build_callback_client(&url).map_err(|_| AppError::DiagnosticDeliveryUnavailable)?;
        let deadline = Instant::now() + RECOVERY;
        loop {
            if timeout(
                deadline.saturating_duration_since(Instant::now()),
                StatisticConsumer::post_callback(consumer, &client, &url, false),
            )
            .await
            .ok()
            .flatten()
            .is_some()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(AppError::DiagnosticDeliveryUnavailable);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
    log_unrouted(consumer).await;
    Ok(())
}

/// 持久化只调用数据库；传输路径持锁时不能递归进入 callback 回退。
pub(crate) async fn persist_prefix(consumer: &Consumer) -> Result<()> {
    let (journal, events) = {
        let state = consumer.lock().await;
        (state.callback.journal.clone(), state.callback.outbox.archive_prefix())
    };
    if events.is_empty() {
        return Ok(());
    }
    let Some(database) = journal.database.as_ref() else {
        return Err(AppError::DiagnosticDeliveryUnavailable);
    };
    let rows = records(&events)?;
    {
        let mut state = consumer.lock().await;
        state.callback.archived_diagnostics = true;
        state.callback.unconfirmed_archive.clone_from(&rows);
    }
    journal
        .request(Operation::Archive, || database.archive_notifications(&rows))
        .await?;
    let mut state = consumer.lock().await;
    state.callback.unconfirmed_archive.clear();
    state.callback.outbox.release_prefix(&events);
    Ok(())
}

pub(crate) async fn flush_all(consumer: &Consumer) -> Result<()> {
    // 尾部收尾与周期发送共用生产锁，避免并行归档清除另一批的未知写结果。
    let gate = consumer.lock().await.callback.journal.producer.clone();
    let _guard = gate.lock().await;
    while consumer.lock().await.callback.outbox.pending_len() != 0 {
        flush(consumer).await?;
    }
    Ok(())
}

/// 回放只构建投递批次，不调用 update_statistics。
pub(crate) async fn pending(consumer: &Consumer, final_check: bool) -> Result<Option<Vec<NotificationRecord>>> {
    let (journal, round, archived, revision) = {
        let state = consumer.lock().await;
        (
            state.callback.journal.clone(),
            state.callback.identity.round_id.clone(),
            state.callback.archived_diagnostics,
            state.callback.outbox.revision(),
        )
    };
    if let Some(rows) = &journal.in_flight {
        return Ok(Some(rows.clone()));
    }
    let Some(database) = journal.database.as_ref().filter(|_| archived && !journal.failed) else {
        return Ok(None);
    };
    let mut rows = journal
        .request(Operation::Read, || {
            database.pending_notifications_after(&round, NotificationKind::Diagnostic, journal.cursor.as_ref())
        })
        .await?;
    if journal.cursor.is_none() {
        consumer.lock().await.callback.journal.head_checked_revision = Some(revision);
    }
    if rows.is_empty() && journal.cursor.is_some() && (final_check || journal.head_checked_revision != Some(revision)) {
        // 无游标检查发现早于游标的迟到写入；最终完整性不能仅看尾页。
        rows = journal
            .request(Operation::Read, || {
                database.pending_notifications(&round, NotificationKind::Diagnostic)
            })
            .await?;
        consumer.lock().await.callback.journal.head_checked_revision = Some(revision);
    }
    if rows.is_empty() {
        return Ok(None);
    }
    consumer.lock().await.callback.journal.in_flight = Some(rows.clone());
    Ok(Some(rows))
}

pub(crate) async fn confirm_pending(consumer: &Consumer, finalizing: bool) -> Result<()> {
    let (journal, rows) = {
        let state = consumer.lock().await;
        (
            state.callback.journal.clone(),
            state.callback.pending_acknowledgements.clone(),
        )
    };
    if rows.is_empty() {
        return Ok(());
    }
    let Some(database) = journal.database.as_ref() else {
        return Ok(());
    };
    for chunk in rows.chunks(MAX_DIAGNOSTIC_BATCH_EVENTS) {
        if finalizing {
            with_scan_cancellation(
                CancellationToken::new(),
                None,
                database.acknowledge_notifications(chunk),
            )
            .await?;
        } else {
            journal
                .request(Operation::Confirm, || database.acknowledge_notifications(chunk))
                .await?;
        }
    }
    let mut state = consumer.lock().await;
    state.callback.pending_acknowledgements.clear();
    state.callback.unconfirmed_archive.retain(|unknown| {
        !rows
            .iter()
            .any(|confirmed| confirmed.record_id() == unknown.record_id())
    });
    Ok(())
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
