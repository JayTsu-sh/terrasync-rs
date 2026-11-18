//! 通知传输独立于迁移结果；最终发送冻结载荷，失败归档不授权删除源端。
use std::{slice::from_ref, sync::Arc, time::Duration};

use db::scan::with_scan_cancellation;
use db::{ClickHouseDatabase, DatabaseError, NotificationKind, NotificationRecord};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use task_model::diagnostics::{CallbackIdentity, DiagnosticBatch};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{consumer::StatisticConsumer, journal};
use crate::callback::CallbackEndpoint;
use crate::error::Result;

const FINAL_ATTEMPTS: u8 = 4;
const RETRY_DELAY: Duration = Duration::from_millis(40);

#[derive(Debug, Error)]
pub(crate) enum DeliveryError {
    #[error("callback HTTP request failed: {0}")]
    Http(#[source] reqwest::Error),
    #[error("callback returned HTTP {status}")]
    HttpStatus { status: StatusCode },
    #[error("callback delivery time budget exhausted")]
    Timeout,
}

pub(crate) struct PreparedCallback {
    job_id: String,
    identity: CallbackIdentity,
    pub(crate) batch_id: Option<String>,
    payload_json: String,
    pub(crate) records: Vec<NotificationRecord>,
    from_database: bool,
    pub(crate) complete: bool,
}

/// `payload_json` 与实际 HTTP body 逐字节相同；HTTP 2xx 仅表示外部接口已接收。
#[derive(Serialize, Deserialize)]
pub(crate) struct FinalCallbackReport {
    pub(crate) job_id: String,
    pub(crate) identity: CallbackIdentity,
    pub(crate) payload_json: String,
}

impl PreparedCallback {
    pub(crate) fn new(consumer: &mut StatisticConsumer, is_final: bool) -> Result<Self> {
        let batch = consumer.callback.outbox.prepare_batch();
        Self::with_batch(consumer, is_final, batch, None)
    }

    fn with_batch(
        consumer: &mut StatisticConsumer, is_final: bool, batch: Option<DiagnosticBatch>,
        archived: Option<Vec<NotificationRecord>>,
    ) -> Result<Self> {
        consumer.callback.identity.sequence = consumer.callback.identity.sequence.saturating_add(1);
        let mut report = consumer.to_report(is_final);
        let batch_id = batch.as_ref().map(|batch| batch.batch_id.clone());
        let from_database = archived.is_some();
        let records = if let Some(rows) = archived {
            rows
        } else if consumer.callback.journal.database.is_some() {
            batch
                .as_ref()
                .map(|batch| journal::records(&batch.events))
                .transpose()?
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let complete = is_final
            && batch.is_none()
            && !consumer.callback.diagnostics_incomplete
            && !consumer.callback.outbox.has_omissions()
            && consumer.callback.pending_acknowledgements.is_empty();
        report.diagnostics = batch;
        report.diagnostics_complete = complete;
        Ok(Self {
            job_id: report.job_id.clone(),
            identity: consumer.callback.identity.clone(),
            batch_id,
            payload_json: serde_json::to_string(&report)?,
            records,
            from_database,
            complete,
        })
    }

    pub(crate) async fn prepare(consumer: &Arc<Mutex<StatisticConsumer>>, is_final: bool) -> Result<Self> {
        let failed = consumer.lock().await.callback.journal.failed;
        if !failed || is_final {
            journal::confirm_pending(consumer, is_final).await?;
        }
        loop {
            let (use_database, pending_memory, gate) = {
                let state = consumer.lock().await;
                (
                    state.callback.journal.database.is_some() && !state.callback.journal.failed,
                    state.callback.outbox.pending_len() != 0,
                    state.callback.journal.producer.clone(),
                )
            };
            if use_database && pending_memory {
                let _guard = gate.lock().await;
                journal::persist_prefix(consumer).await?;
            }
            if let Some(rows) = journal::pending(consumer, is_final).await? {
                let batch = journal::batch(&rows)?;
                return Self::with_batch(&mut *consumer.lock().await, is_final, Some(batch), Some(rows));
            }
            // 从旧 checkpoint 恢复的内存前缀可能已在数据库确认。归档 revision=0
            // 不能撤销该确认；循环释放这些前缀，不能再次发送已被 HTTP 2xx 接收的事件。
            if use_database && consumer.lock().await.callback.outbox.pending_len() != 0 {
                continue;
            }
            return Self::new(&mut *consumer.lock().await, is_final);
        }
    }

    pub(crate) async fn received(&self, consumer: &Arc<Mutex<StatisticConsumer>>) {
        // HTTP 2xx 即接收成功，先清本批待发明细；数据库确认失败只重试内部确认，不重发 HTTP。
        let mut state = consumer.lock().await;
        if let Some(batch_id) = &self.batch_id {
            state.callback.outbox.ack(batch_id);
        }
        if self.from_database {
            state.callback.journal.cursor = self.records.last().map(NotificationRecord::cursor);
            state.callback.journal.in_flight = None;
        }
        if state.callback.journal.database.is_some() {
            for row in &self.records {
                let attempted = !state.callback.journal.failed
                    || self.from_database
                    || state
                        .callback
                        .unconfirmed_archive
                        .iter()
                        .any(|other| other.record_id() == row.record_id());
                if attempted
                    && !state
                        .callback
                        .pending_acknowledgements
                        .iter()
                        .any(|other| other.record_id() == row.record_id())
                {
                    if state.callback.pending_acknowledgements.len() >= journal::MAX_PENDING_CONFIRMATIONS {
                        state.callback.diagnostics_incomplete = true;
                        state.callback.journal.failed = true;
                        break;
                    }
                    state.callback.pending_acknowledgements.push(row.clone());
                }
            }
        }
    }

    pub(crate) async fn send(
        &self, client: &Client, endpoint: &CallbackEndpoint,
    ) -> std::result::Result<(), DeliveryError> {
        let response = client
            .post(endpoint.request_url().clone())
            .header("Content-Type", "application/json")
            .body(self.payload_json.clone())
            .send()
            .await
            .map_err(|error| DeliveryError::Http(error.without_url()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(DeliveryError::HttpStatus { status });
        }
        Ok(())
    }

    async fn retry(
        &self, client: &Client, endpoint: &CallbackEndpoint, deadline: Instant,
    ) -> std::result::Result<(), (u8, DeliveryError)> {
        let mut last = DeliveryError::Timeout;
        let mut attempts = 0;
        for attempt in 1..=FINAL_ATTEMPTS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            // 为后续尝试保留预算，静默丢包不能用尽全部收尾时间。
            let budget = remaining / u32::from(FINAL_ATTEMPTS + 1 - attempt);
            attempts = attempt;
            last = match timeout(budget, self.send(client, endpoint)).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => error,
                Err(_) => DeliveryError::Timeout,
            };
            warn!(job_id = %self.job_id, attempt, error = %last, "Final callback attempt failed");
            if attempt < FINAL_ATTEMPTS {
                sleep(RETRY_DELAY.min(deadline.saturating_duration_since(Instant::now()) / 8)).await;
            }
        }
        Err((attempts, last))
    }

    fn record(&self) -> Option<NotificationRecord> {
        let encoded = serde_json::to_string(&FinalCallbackReport {
            job_id: self.job_id.clone(),
            identity: self.identity.clone(),
            payload_json: self.payload_json.clone(),
        });
        let encoded = match encoded {
            Ok(value) => value,
            Err(error) => {
                warn!(job_id = %self.job_id, %error, "Final report encoding failed");
                return None;
            }
        };
        match NotificationRecord::new(
            self.identity.round_id.clone(),
            self.identity.attempt_id.clone(),
            self.identity.sequence,
            NotificationKind::FinalReport,
            encoded,
        ) {
            Ok(record) => Some(record),
            Err(error) => {
                warn!(job_id = %self.job_id, %error, "Final report could not be encoded for ClickHouse");
                None
            }
        }
    }
}

/// 归档不携带 callback 发送原因。ACK 只改变投递标记，报告原文仍保留供外部查询。
async fn archive(database: &ClickHouseDatabase, record: &NotificationRecord, ack: bool, remaining: &mut Duration) {
    let budget = if ack { *remaining } else { *remaining / 2 };
    let started = Instant::now();
    let records = from_ref(record);
    let work = async {
        if ack {
            database.acknowledge_notifications(records).await
        } else {
            database.archive_notifications(records).await
        }
    };
    // 取消的任务仍能保存终态；收尾不继承已耗尽的扫描恢复窗口。
    let result = with_scan_cancellation(CancellationToken::new(), None, timeout(budget, work)).await;
    *remaining = remaining.saturating_sub(started.elapsed());
    match result {
        Ok(Ok(())) => (),
        Ok(Err(error)) => warn!(round_id = %record.round_id(), %error, "Final report archival was not confirmed"),
        Err(_) => warn!(round_id = %record.round_id(), "Final report archival timed out with unknown write outcome"),
    }
}

pub(crate) async fn send_final(
    consumer: &Arc<Mutex<StatisticConsumer>>, client: Option<&Client>, endpoint: Option<&CallbackEndpoint>,
    database: Option<&ClickHouseDatabase>, delivery_budget: Duration, mut archive_budget: Duration,
) {
    if database.is_none() && (client.is_none() || endpoint.is_none()) {
        return;
    }
    if let Some(database) = database {
        consumer.lock().await.callback.journal.database = Some(database.clone());
    }
    let gate = consumer.lock().await.callback.journal.delivery.clone();
    let _delivery = gate.lock().await;
    let failed = consumer.lock().await.callback.journal.failed;
    if !failed && database.is_some() {
        let started = Instant::now();
        let result = timeout(archive_budget / 2, journal::flush_all(consumer)).await;
        archive_budget = archive_budget.saturating_sub(started.elapsed());
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => StatisticConsumer::diagnostic_failure(consumer, error).await,
            Err(source) => {
                StatisticConsumer::diagnostic_failure(
                    consumer,
                    DatabaseError::NotificationRequestTimedOut { source }.into(),
                )
                .await;
            }
        }
    }
    let mut delivery_remaining = delivery_budget;
    loop {
        let started = Instant::now();
        let result = timeout(archive_budget / 2, PreparedCallback::prepare(consumer, true)).await;
        archive_budget = archive_budget.saturating_sub(started.elapsed());
        let prepared = match result {
            Ok(Ok(value)) => value,
            failure => {
                if let Ok(Err(error)) = failure {
                    StatisticConsumer::diagnostic_failure(consumer, error).await;
                } else if let Err(source) = failure {
                    StatisticConsumer::diagnostic_failure(
                        consumer,
                        DatabaseError::NotificationRequestTimedOut { source }.into(),
                    )
                    .await;
                }
                match PreparedCallback::new(&mut *consumer.lock().await, true) {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(%error, "Final callback serialization failed");
                        return;
                    }
                }
            }
        };
        let record = database.and_then(|_| prepared.record());
        if let (Some(database), Some(record)) = (database, &record) {
            archive(database, record, false, &mut archive_budget).await;
        }
        let (Some(client), Some(endpoint)) = (client, endpoint) else {
            return;
        };
        let started = Instant::now();
        if let Err((attempts, error)) = prepared.retry(client, endpoint, started + delivery_remaining).await {
            warn!(job_id = %prepared.job_id, attempts, %error, "Final callback was not accepted; retaining pending diagnostics");
            return;
        }
        delivery_remaining = delivery_remaining.saturating_sub(started.elapsed());
        prepared.received(consumer).await;
        if let (Some(database), Some(record)) = (database, &record) {
            archive(database, record, true, &mut archive_budget).await;
        }
        if prepared.complete || prepared.batch_id.is_none() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
pub(crate) mod tests;
