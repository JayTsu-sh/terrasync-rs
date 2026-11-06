//! Catalogue projections, queries and bounded delivery to task consumers.

use std::time::{Duration, Instant};

use clickhouse::{error::Error as ClickHouseError, query::Query};
use data_mover::model::ObservedEntry;
use task_model::ChangeKind;
use tokio::sync::mpsc;
use tracing::{debug, trace};
use utils::sanitize_job_id;

use super::ClickHouseDatabase;
use super::comparison::JoinStrategy;
use crate::error::{DatabaseError, Result};
use crate::execution::queries::{check_scan_cancellation, scan_retry_delay};
use crate::execution::read::QuorumRead;
use crate::get_scan_base_table_name;
use crate::schema::columns::SNAPSHOT_COLUMNS;
use crate::schema::identity::PERSISTED_IDENTITY_PREFIX;
use crate::schema::records::{SnapshotRow, StorageEntryRecord};

fn records_to_entry_iter(records: Vec<StorageEntryRecord>) -> Result<Box<dyn Iterator<Item = ObservedEntry> + Send>> {
    let entries = records
        .into_iter()
        .map(|row| row.to_observation())
        .collect::<Result<Vec<_>>>()?;
    Ok(Box::new(entries.into_iter()))
}

impl ClickHouseDatabase {
    /// 通用检测方法：对比临时表与主表，按 `query_builder` 构建 SQL 查询符合条件的记录
    pub(super) async fn detect_items(
        &self, query_type: &str, query_builder: impl Fn(&str, &str, &JoinStrategy) -> String,
    ) -> Result<Vec<StorageEntryRecord>> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_ref()
            .ok_or_else(|| DatabaseError::TableNotFound("Temporary table not created".to_string()))?;
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let (temp_non_null, base_non_null) = self.check_file_handle_status(temp_table_name, &base_table_name).await?;

        let strategy = JoinStrategy::from_file_handle_status(temp_non_null, base_non_null);

        let query = query_builder(temp_table_name, &base_table_name, &strategy);
        trace!("Querying {}: {}", query_type, query);

        let rows = self
            .sync_client
            .query(&query)
            .fetch_quorum_all::<StorageEntryRecord>()
            .await?;

        debug!("Found {} {} files", rows.len(), query_type);
        Ok(rows)
    }

    /// 返回 (`temp_non_null`, `base_non_null`)：两表各自非 NULL `file_handle` 的数量，
    /// 用于决定 JOIN 策略（见 `JoinStrategy::from_file_handle_status`）。
    pub(super) async fn check_file_handle_status(
        &self, temp_table_name: &str, base_table_name: &str,
    ) -> Result<(usize, usize)> {
        let query = format!(
            "SELECT t.non_null, b.non_null \
             FROM (SELECT if(countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle, '{PERSISTED_IDENTITY_PREFIX}')) = 0, count(file_handle), 0) AS non_null FROM {temp_table_name}) t \
             CROSS JOIN (SELECT if(countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle, '{PERSISTED_IDENTITY_PREFIX}')) = 0, count(file_handle), 0) AS non_null FROM {base_table_name}) b"
        );

        debug!(
            "Checking file_handle status for temporary table '{}' and base table '{}'",
            temp_table_name, base_table_name
        );

        let (tn, bn): (u64, u64) = self.sync_client.query(&query).fetch_quorum_one().await?;

        // 旧驱动句柄与新的中立 identity key 不可混用；升级过渡代按路径比较。
        let (temp_non_null, base_non_null) = if tn == 0 || bn == 0 {
            (0, 0)
        } else {
            (tn as usize, bn as usize)
        };

        trace!(
            "file_handle status - temp non_null={}, base non_null={}",
            temp_non_null, base_non_null
        );

        Ok((temp_non_null, base_non_null))
    }

    /// 检测临时表中新增的文件（不存在于主表）
    pub async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = ObservedEntry> + Send>> {
        if self.is_copy_recovery().await {
            return records_to_entry_iter(self.copy_replay_items().await?);
        }
        let records = self
            .detect_items("new", |temp_table, base_table, strategy| {
                strategy.build_detect_new_sql(temp_table, base_table)
            })
            .await?;

        records_to_entry_iter(records)
    }

    pub async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (ObservedEntry, ChangeKind)> + Send>> {
        if self.is_copy_recovery().await {
            return Ok(Box::new(std::iter::empty()));
        }
        // 分别查询三种变更（三类条件互斥），用 try_join! 并发执行以降低整体延迟
        let (data_only, metadata_only, both, marker_removed) = tokio::try_join!(
            self.detect_items("changed(data)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::DataOnly)
            }),
            self.detect_items("changed(meta)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::MetadataOnly)
            }),
            self.detect_items("changed(both)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::Both)
            }),
            self.detect_items("changed(marker removed)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::DirectoryMarkerRemoved)
            }),
        )?;

        let iter = data_only
            .into_iter()
            .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::DataOnly)))
            .chain(
                metadata_only
                    .into_iter()
                    .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::MetadataOnly))),
            )
            .chain(
                both.into_iter()
                    .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::Both))),
            );
        let iter = iter.chain(marker_removed.into_iter().map(|row| {
            row.to_observation()
                .map(|entry| (entry, ChangeKind::DirectoryMarkerRemoved))
        }));
        let entries = iter.collect::<Result<Vec<_>>>()?;
        Ok(Box::new(entries.into_iter()))
    }

    /// 获取指定表的记录总数（使用 FINAL 确保去重）
    pub async fn get_count(&self, table_name: &str) -> Result<u64> {
        let full_table_name = format!("{}_{}", table_name, sanitize_job_id(&self.job_id));
        let query = format!("SELECT COUNT() FROM {full_table_name} FINAL");

        let count = self.sync_client.query(&query).fetch_quorum_one::<u64>().await?;

        Ok(count)
    }

    pub async fn query_storage_entry(
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>,
        tx: mpsc::Sender<ObservedEntry>,
    ) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        debug!("[query_storage_entry] base table name: {}", base_table_name);

        let mut where_conditions = Vec::new();
        let mut ext_bind: Option<String> = None;

        if let Some(dir_val) = is_dir {
            where_conditions.push(format!("is_dir = {dir_val}"));
        }

        if let Some(symlink_val) = is_symlink {
            where_conditions.push(format!("is_symlink = {symlink_val}"));
        }

        if let Some(ext_val) = extension {
            where_conditions.push("ext ILIKE ?".to_string());
            ext_bind = Some(format!("%{ext_val}"));
        }

        let where_clause = if where_conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_conditions.join(" AND "))
        };

        let query = format!("SELECT {SNAPSHOT_COLUMNS} FROM {base_table_name} FINAL {where_clause}");

        let mut q = self.sync_client.query(&query);
        if let Some(ext) = ext_bind {
            q = q.bind(ext);
        }
        self.send_catalogue_records(q, tx).await
    }
}

impl ClickHouseDatabase {
    pub(super) async fn send_catalogue_records(&self, query: Query, tx: mpsc::Sender<ObservedEntry>) -> Result<()> {
        let started = Instant::now();
        let mut delivered = false;
        let mut retrying = false;
        loop {
            check_scan_cancellation()?;
            let request = if retrying {
                query.clone().with_option("load_balancing", "random")
            } else {
                query.clone()
            };
            let mut cursor = request.fetch::<SnapshotRow>()?;
            loop {
                match cursor.next().await {
                    Ok(Some(record)) => {
                        tx.send(record.into_observation()?).await.map_err(|error| {
                            DatabaseError::QueryError(format!("Failed to send storage entry: {error}"))
                        })?;
                        delivered = true;
                    }
                    Ok(None) => return Ok(()),
                    Err(ClickHouseError::BadResponse(message))
                        if !delivered
                            && message.contains("REPLICA_IS_NOT_IN_QUORUM")
                            && started.elapsed() < Duration::from_secs(30) =>
                    {
                        retrying = true;
                        scan_retry_delay(Duration::from_millis(50)).await?;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
}
