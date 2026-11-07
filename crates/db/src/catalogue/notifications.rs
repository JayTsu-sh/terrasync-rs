//! 有界通知归档；数据事实、错误计数和 HTTP 发送均由任务层负责。

use clickhouse::Row;
use serde::{Deserialize, Serialize, de::IgnoredAny};
use std::fmt;
use utils::sanitize_job_id;

use super::ClickHouseDatabase;
use super::layout::CatalogueEngine;
use crate::error::{DatabaseError, Result};
use crate::execution::queries::execute_request;
use crate::execution::read::QuorumRead;
use crate::execution::recovery::RequestSafety;

pub const MAX_NOTIFICATION_BATCH_RECORDS: usize = 64;
pub const MAX_NOTIFICATION_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_NOTIFICATION_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_NOTIFICATION_ID_BYTES: usize = 256;
const COLUMNS: &str = "round_id, attempt_id, sequence, kind, record_id, payload_json, delivery_revision";
const DEFINITION: &str = "round_id String, attempt_id String, sequence UInt64, kind UInt8, \
    record_id String, payload_json String, delivery_revision UInt64";

/// 错误明细与最终报告分开查询，避免把最终报告当作普通错误事件重发。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotificationKind {
    Diagnostic,
    FinalReport,
}

impl NotificationKind {
    const fn code(self) -> u8 {
        match self {
            Self::Diagnostic => 1,
            Self::FinalReport => 2,
        }
    }
}

/// 调用方先脱敏，完整 JSON 原样保存；ACK 不改变载荷或原始执行身份。
#[derive(Clone, Deserialize, Row, Serialize)]
pub struct NotificationRecord {
    round_id: String,
    attempt_id: String,
    sequence: u64,
    kind: u8,
    record_id: String,
    payload_json: String,
    delivery_revision: u64,
}

/// 有界分页提示；空的尾页不代替无游标的最终待投递检查。
#[derive(Clone, Debug)]
pub struct NotificationCursor {
    round_id: String,
    attempt_id: String,
    sequence: u64,
    kind: u8,
    record_id: String,
}

impl NotificationCursor {
    fn validate(&self) -> Result<()> {
        validate_id(&self.round_id, "cursor_round_id")?;
        validate_id(&self.attempt_id, "cursor_attempt_id")?;
        if self.sequence == 0
            || !matches!(self.kind, 1 | 2)
            || self.record_id.len() != 64
            || !self.record_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(DatabaseError::NotificationRecordInvalid {
                field: "cursor_identity",
            });
        }
        Ok(())
    }
}

impl fmt::Debug for NotificationRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationRecord")
            .field("round_id", &self.round_id)
            .field("attempt_id", &self.attempt_id)
            .field("sequence", &self.sequence)
            .field("kind", &self.kind)
            .field("record_id", &self.record_id)
            .field("payload_bytes", &self.payload_json.len())
            .field("delivery_revision", &self.delivery_revision)
            .finish_non_exhaustive()
    }
}

impl NotificationRecord {
    pub fn new(
        round_id: String, attempt_id: String, sequence: u64, kind: NotificationKind, payload_json: String,
    ) -> Result<Self> {
        let mut record = Self {
            round_id,
            attempt_id,
            sequence,
            kind: kind.code(),
            record_id: String::new(),
            payload_json,
            delivery_revision: 0,
        };
        record.validate_fields()?;
        record.record_id = record.content_id();
        Ok(record)
    }

    pub fn record_id(&self) -> &str {
        &self.record_id
    }
    pub fn round_id(&self) -> &str {
        &self.round_id
    }
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub const fn is_delivered(&self) -> bool {
        self.delivery_revision == 1
    }

    pub fn cursor(&self) -> NotificationCursor {
        NotificationCursor {
            round_id: self.round_id.clone(),
            attempt_id: self.attempt_id.clone(),
            sequence: self.sequence,
            kind: self.kind,
            record_id: self.record_id.clone(),
        }
    }

    fn content_id(&self) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"terrasync/notification-archive/v1\0");
        for value in [&self.round_id, &self.attempt_id, &self.payload_json] {
            hash.update(&(value.len() as u64).to_le_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(&self.sequence.to_le_bytes());
        hash.update(&[self.kind]);
        hash.finalize().to_hex().to_string()
    }

    fn validate(&self) -> Result<()> {
        self.validate_fields()?;
        if self.record_id != self.content_id() {
            return Err(DatabaseError::NotificationRecordInvalid {
                field: "content_digest",
            });
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<()> {
        validate_id(&self.round_id, "round_id")?;
        validate_id(&self.attempt_id, "attempt_id")?;
        if self.sequence == 0 || !matches!(self.kind, 1 | 2) || self.delivery_revision > 1 {
            return Err(DatabaseError::NotificationRecordInvalid {
                field: "identity_or_state",
            });
        }
        if self.payload_json.len() > MAX_NOTIFICATION_RECORD_BYTES {
            return Err(DatabaseError::NotificationRecordInvalid { field: "payload_bytes" });
        }
        serde_json::from_str::<IgnoredAny>(&self.payload_json)
            .map_err(|source| DatabaseError::NotificationPayloadJson { source })?;
        Ok(())
    }
}

fn validate_id(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_NOTIFICATION_ID_BYTES || value.contains('\0') {
        return Err(DatabaseError::NotificationRecordInvalid { field });
    }
    Ok(())
}

fn validate_batch(records: &[NotificationRecord]) -> Result<()> {
    let bytes = records.iter().map(|record| record.payload_json.len()).sum::<usize>();
    if records.len() > MAX_NOTIFICATION_BATCH_RECORDS || bytes > MAX_NOTIFICATION_BATCH_BYTES {
        return Err(DatabaseError::NotificationBatchTooLarge {
            records: records.len(),
            bytes,
        });
    }
    for record in records {
        record.validate()?;
    }
    Ok(())
}

/// ACK 借用原始载荷，避免在内存中另复制整批 JSON。
#[derive(Row, Serialize)]
struct WriteRecord<'a> {
    round_id: &'a str,
    attempt_id: &'a str,
    sequence: u64,
    kind: u8,
    record_id: &'a str,
    payload_json: &'a str,
    delivery_revision: u64,
}

impl ClickHouseDatabase {
    /// 外部查询按完整执行身份读取最终报告；HTTP ACK 后仍可查询原文。
    pub async fn final_report(
        &self, round_id: &str, attempt_id: &str, sequence: u64,
    ) -> Result<Option<NotificationRecord>> {
        validate_id(round_id, "round_id")?;
        validate_id(attempt_id, "attempt_id")?;
        if sequence == 0 {
            return Err(DatabaseError::NotificationRecordInvalid { field: "sequence" });
        }
        self.ensure_notification_archive().await?;
        let table = self.catalogue_query_table(&self.notification_table())?;
        let sql = format!(
            "SELECT {COLUMNS} FROM {table} FINAL WHERE round_id=? AND attempt_id=? AND sequence=? AND kind=? LIMIT 2 SETTINGS optimize_move_to_prewhere=0"
        );
        let mut records: Vec<NotificationRecord> = self
            .sync_client
            .query(&sql)
            .bind(round_id)
            .bind(attempt_id)
            .bind(sequence)
            .bind(NotificationKind::FinalReport.code())
            .fetch_quorum_all()
            .await?;
        validate_batch(&records)?;
        if records.len() > 1 {
            return Err(DatabaseError::NotificationRecordInvalid {
                field: "conflicting_final_report",
            });
        }
        Ok(records.pop())
    }

    fn notification_table(&self) -> String {
        format!("notifications_{}", sanitize_job_id(&self.job_id))
    }

    async fn ensure_notification_archive(&self) -> Result<()> {
        self.notifications_initialized
            .get_or_try_init(|| async {
                let table = self.notification_table();
                self.create_catalogue_table_identified(
                    &table,
                    DEFINITION,
                    "(round_id, attempt_id, sequence, kind, record_id)",
                    CatalogueEngine::ReplacingVersion("delivery_revision"),
                    Some(&table),
                )
                .await
            })
            .await?;
        Ok(())
    }

    /// ACK 丢失可以重放完全相同的记录；返回错误时不宣称数据没有写入。
    pub async fn archive_notifications(&self, records: &[NotificationRecord]) -> Result<()> {
        self.write_notifications(records, 0).await
    }

    /// 仅在 HTTP 收到匹配 ACK 后调用。迟到的 revision=0 写入不能撤销已确认状态。
    pub async fn acknowledge_notifications(&self, records: &[NotificationRecord]) -> Result<()> {
        self.write_notifications(records, 1).await
    }

    async fn write_notifications(&self, records: &[NotificationRecord], revision: u64) -> Result<()> {
        validate_batch(records)?;
        if records.is_empty() {
            return Ok(());
        }
        self.ensure_notification_archive().await?;
        let _write = self.catalogue_write_guard().await;
        let table = self.notification_table();
        let mut hash = blake3::Hasher::new();
        hash.update(b"terrasync/notification-batch/v1\0");
        hash.update(&revision.to_le_bytes());
        for record in records {
            hash.update(record.record_id.as_bytes());
        }
        let client = self
            .sync_client
            .clone()
            .with_validation(false)
            .with_option("async_insert", "0")
            .with_option("deduplicate_insert", "enable")
            .with_option("insert_deduplication_token", hash.finalize().to_hex().to_string());
        execute_request(RequestSafety::IdempotentWrite, || {
            let client = client.clone();
            let table = &table;
            async move {
                let mut insert = client.insert::<WriteRecord<'_>>(table).await?;
                for record in records {
                    insert
                        .write(&WriteRecord {
                            round_id: &record.round_id,
                            attempt_id: &record.attempt_id,
                            sequence: record.sequence,
                            kind: record.kind,
                            record_id: &record.record_id,
                            payload_json: &record.payload_json,
                            delivery_revision: revision,
                        })
                        .await?;
                }
                insert.end().await?;
                Ok(())
            }
        })
        .await
    }

    /// 按事件和字节双重边界取一批；按 ACK 状态查询，迟到的插入不会被游标跳过。
    /// 只有生产者已收束、归档写入全部确认且最终无游标核对为空，才能声明完整投递。
    pub async fn pending_notifications(
        &self, round_id: &str, kind: NotificationKind,
    ) -> Result<Vec<NotificationRecord>> {
        self.pending_notifications_after(round_id, kind, None).await
    }

    /// 仅按完整排序键跳过已处理前缀，避免每批反复扫描全部已 ACK 记录。
    /// 发送方结束时仍须调用无游标查询，发现未知结果请求的迟到事件。
    pub async fn pending_notifications_after(
        &self, round_id: &str, kind: NotificationKind, after: Option<&NotificationCursor>,
    ) -> Result<Vec<NotificationRecord>> {
        validate_id(round_id, "round_id")?;
        if let Some(cursor) = after {
            cursor.validate()?;
            if cursor.round_id != round_id || cursor.kind != kind.code() {
                return Err(DatabaseError::NotificationRecordInvalid { field: "cursor_scope" });
            }
        }
        self.ensure_notification_archive().await?;
        let table = self.catalogue_query_table(&self.notification_table())?;
        let after_clause = if after.is_some() {
            " AND (attempt_id, sequence, kind, record_id) > (?, ?, ?, ?)"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {COLUMNS} FROM (SELECT {COLUMNS}, sum(length(payload_json)) OVER \
             (ORDER BY attempt_id, sequence, record_id ROWS UNBOUNDED PRECEDING) AS batch_bytes FROM \
             (SELECT {COLUMNS} FROM {table} FINAL WHERE round_id=? AND kind=? AND delivery_revision=0{after_clause} \
             ORDER BY attempt_id, sequence, record_id LIMIT {MAX_NOTIFICATION_BATCH_RECORDS})) \
             WHERE batch_bytes <= {MAX_NOTIFICATION_BATCH_BYTES} ORDER BY attempt_id, sequence, record_id \
             SETTINGS optimize_move_to_prewhere=0"
        );
        let mut query = self.sync_client.query(&sql).bind(round_id).bind(kind.code());
        if let Some(cursor) = after {
            query = query
                .bind(&cursor.attempt_id)
                .bind(cursor.sequence)
                .bind(cursor.kind)
                .bind(&cursor.record_id);
        }
        let records: Vec<NotificationRecord> = query.fetch_quorum_all().await?;
        validate_batch(&records)?;
        Ok(records)
    }
}

#[cfg(test)]
mod tests;
