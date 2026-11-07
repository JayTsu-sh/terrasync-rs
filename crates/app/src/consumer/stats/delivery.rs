//! 通知传输独立于迁移结果；最终发送冻结载荷，失败归档不授权删除源端。
use std::{slice::from_ref, sync::Arc, time::Duration};

use db::scan::with_scan_cancellation;
use db::{ClickHouseDatabase, NotificationKind, NotificationRecord};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{
    consumer::StatisticConsumer,
    diagnostics::{CallbackAck, CallbackIdentity},
};
use crate::callback::CallbackEndpoint;

const FINAL_ATTEMPTS: u8 = 4;
const RETRY_DELAY: Duration = Duration::from_millis(40);

#[derive(Debug, Error)]
pub(crate) enum DeliveryError {
    #[error("callback HTTP request failed: {0}")]
    Http(#[source] reqwest::Error),
    #[error("callback did not acknowledge the diagnostic batch")]
    MissingAck,
    #[error("callback acknowledgement could not be read: {0}")]
    InvalidAck(#[source] reqwest::Error),
    #[error("callback delivery time budget exhausted")]
    Timeout,
}

pub(crate) struct PreparedCallback {
    job_id: String,
    identity: CallbackIdentity,
    pub(crate) batch_id: Option<String>,
    payload_json: String,
}

/// `payload_json` 与实际 HTTP body 逐字节相同；未收到 ACK 不能标为已交付。
#[derive(Serialize, Deserialize)]
pub(crate) struct FinalCallbackReport {
    pub(crate) job_id: String,
    pub(crate) identity: CallbackIdentity,
    pub(crate) payload_json: String,
}

impl PreparedCallback {
    pub(crate) fn new(
        consumer: &mut StatisticConsumer, is_final: bool,
    ) -> std::result::Result<Self, serde_json::Error> {
        consumer.callback.identity.sequence = consumer.callback.identity.sequence.saturating_add(1);
        let batch = consumer.callback.outbox.prepare_batch();
        let mut report = consumer.to_report(is_final);
        let batch_id = batch.as_ref().map(|batch| batch.batch_id.clone());
        report.diagnostics = batch;
        Ok(Self {
            job_id: report.job_id.clone(),
            identity: consumer.callback.identity.clone(),
            batch_id,
            payload_json: serde_json::to_string(&report)?,
        })
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
            .and_then(Response::error_for_status)
            .map_err(|error| DeliveryError::Http(error.without_url()))?;
        if let Some(batch_id) = &self.batch_id {
            let ack = response
                .json::<CallbackAck>()
                .await
                .map_err(|error| DeliveryError::InvalidAck(error.without_url()))?;
            if ack.batch_id.as_ref() != Some(batch_id) {
                return Err(DeliveryError::MissingAck);
            }
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
    // 只累计 HTTP 耗时，数据库收尾另有共享预算，不按批次重置。
    let mut delivery_remaining = delivery_budget;
    loop {
        let prepared = match PreparedCallback::new(&mut *consumer.lock().await, true) {
            Ok(value) => value,
            Err(error) => {
                warn!(%error, "Final callback serialization failed");
                return;
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
            warn!(job_id = %prepared.job_id, attempts, %error, "Final callback undelivered; report retains its pending delivery state");
            return;
        }
        delivery_remaining = delivery_remaining.saturating_sub(started.elapsed());
        if let (Some(database), Some(record)) = (database, &record) {
            archive(database, record, true, &mut archive_budget).await;
        }
        let mut state = consumer.lock().await;
        if let Some(batch_id) = &prepared.batch_id {
            state.callback.outbox.ack(batch_id);
        }
        if state.callback.outbox.prepare_batch().is_none() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
