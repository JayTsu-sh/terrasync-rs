//! Native observation batches and acknowledged versions written to the catalogue.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use data_mover::model::ObservedEntry;
use serde_json::Value;
use task_model::StorageEntryMessage;
use task_model::observation::{ObservedItem, ObservedView, ObservedViews};
use tracing::trace;
use tracing::{debug, info};

use super::ClickHouseDatabase;
use crate::error::{DatabaseError, Result};
use crate::execution::queries::ScanExecute;
use crate::execution::queries::check_scan_cancellation;
use crate::execution::recovery::RequestSafety;
use crate::get_incremental_scan_base_table_name;
use crate::get_scan_base_table_name;
use crate::get_tar_manifest_table_name;
use crate::scan::rows::NativeScanRecord;
use crate::schema::records::{IncrementalStorageEntryRecord, TarManifestRecord};

fn observation_views(entry: &Arc<ObservedEntry>) -> ObservedViews<'_> {
    ObservedView::all(entry)
}

impl ClickHouseDatabase {
    pub(super) fn native_insert_safety(&self) -> RequestSafety {
        if self.native_dedup_ready.load(Ordering::Acquire) {
            RequestSafety::IdempotentWrite
        } else {
            RequestSafety::UnconfirmedWrite
        }
    }

    /// 执行SQL语句，支持参数化查询
    ///
    /// 注意：由于ClickHouse execute API的限制，当前无法获取受影响的行数
    pub(crate) async fn execute(&self, sql: &str, params: &[Value]) -> Result<()> {
        check_scan_cancellation()?;
        let _write = self.catalogue_write_guard().await;
        debug!("Executing ClickHouse statement: {}", sql);

        let client = self.catalogue_write_client(&self.sync_client);
        let mut query = client.query(sql);

        // 绑定参数
        for param in params {
            if let Some(s) = param.as_str() {
                query = query.bind(s);
            } else if let Some(n) = param.as_i64() {
                query = query.bind(n);
            } else if let Some(b) = param.as_bool() {
                query = query.bind(b);
            } else {
                query = query.bind(param.to_string());
            }
        }

        query.execute_scan().await?;

        Ok(())
    }

    /// 根据 `relative_path` 列表批量删除指定表中的记录
    pub(super) async fn batch_delete_record(&self, table_name: &str, deleted_paths: &[String]) -> Result<()> {
        info!(
            "Batch deleting {} records from table: {}",
            deleted_paths.len(),
            table_name
        );

        if deleted_paths.is_empty() {
            return Ok(());
        }

        let delete_query = self.catalogue_delete_statement(table_name, &vec!["?"; deleted_paths.len()].join(", "))?;
        let paths = deleted_paths.iter().cloned().map(Value::from).collect::<Vec<_>>();
        self.execute(&delete_query, &paths).await?;

        info!("Successfully batch deleted {} base records", deleted_paths.len());
        Ok(())
    }

    /// 通用批量插入方法，支持同步和异步客户端模式
    pub(super) async fn batch_insert<'a>(
        &self, table_name: &str, records: &'a [Arc<ObservedEntry>], use_async_client: bool,
    ) -> Result<()> {
        self.insert_views(
            table_name,
            Box::new(records.iter().flat_map(observation_views)),
            use_async_client,
        )
        .await
    }

    pub(super) async fn insert_views<'a>(
        &self, table_name: &str, views: Box<dyn Iterator<Item = ObservedView<'a>> + Send + 'a>, use_async_client: bool,
    ) -> Result<()> {
        let mut views = views.peekable();
        if views.peek().is_none() {
            return Ok(());
        }
        let current_state = self.get_cached_scan_state().await?;
        let _write = self.catalogue_write_guard().await;
        debug!(
            "During batch inserting to table {}, current_state: {:?}",
            table_name, current_state
        );

        let mut record_count = 0;

        let client = if use_async_client {
            &self.async_client
        } else {
            &self.sync_client
        };
        let client = self.catalogue_write_client(client).with_validation(false);

        let mut insert = client
            .insert::<NativeScanRecord>(table_name)
            .await
            .map_err(DatabaseError::from)?;

        for view in views {
            let record = NativeScanRecord::capture(view, current_state)?;
            insert.write(&record).await.map_err(DatabaseError::from)?;
            record_count += 1;
        }

        insert.end().await.map_err(DatabaseError::from)?;

        info!("Successfully inserted {} records to table {}", record_count, table_name);
        Ok(())
    }

    /// 批量插入增量扫描记录（StorageEntryMessage → `IncrementalStorageEntryRecord`）
    pub async fn batch_insert_incremental_record(&self, messages: &[StorageEntryMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let _write = self.catalogue_write_guard().await;

        let table_name = get_incremental_scan_base_table_name(&self.job_id);

        let records = messages
            .iter()
            .try_fold(Vec::new(), |mut records, message| -> Result<_> {
                records.extend(IncrementalStorageEntryRecord::from_message(message)?);
                Ok(records)
            })?;

        let record_count = records.len();

        let client = self.catalogue_write_client(&self.async_client);
        let mut insert = client
            .insert::<IncrementalStorageEntryRecord>(&table_name)
            .await
            .map_err(DatabaseError::from)?;

        for record in &records {
            trace!(
                "Inserting record: {:?} in incremental scan table {}",
                record, table_name
            );

            insert.write(record).await.map_err(DatabaseError::from)?;
        }

        insert.end().await.map_err(DatabaseError::from)?;

        info!(
            "Successfully inserted {} events to incremental scan table {}",
            record_count, table_name
        );
        Ok(())
    }

    /// 批量插入记录到临时表（async client）
    pub async fn batch_insert_temp_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("No temporary table available".to_string()))?;

        // 使用 async client，wait_for_async_insert=1 保证 end() 返回时数据已 flush
        self.batch_insert(temp_table_name, records, true).await
    }

    /// 批量插入记录到主表（async client）
    pub async fn batch_insert_base_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, records, true).await
    }

    pub async fn batch_insert_completed_records(&self, records: &[ObservedItem]) -> Result<()> {
        let table = get_scan_base_table_name(&self.job_id);
        self.insert_views(&table, Box::new(records.iter().map(ObservedItem::view)), true)
            .await
    }

    /// 更新主表中的单条记录（利用 `ReplacingMergeTree` 插入覆盖）
    pub async fn update_base_record(&self, record: &Arc<ObservedEntry>) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, &[Arc::clone(record)], false).await
    }

    /// 批量删除主表中的记录（按 `relative_path`）
    pub async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.batch_delete_record(&table_name, deleted_paths).await
    }

    pub async fn batch_insert_tar_manifest(&self, tar_path: &str, entries: &[Arc<ObservedEntry>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let _write = self.catalogue_write_guard().await;

        let table_name = get_tar_manifest_table_name(&self.job_id);

        let client = self.catalogue_write_client(&self.async_client);
        let mut insert = client
            .insert::<TarManifestRecord>(&table_name)
            .await
            .map_err(DatabaseError::from)?;

        for entry in entries {
            insert
                .write(&TarManifestRecord::from_entry(entry, tar_path))
                .await
                .map_err(DatabaseError::from)?;
        }

        insert.end().await.map_err(DatabaseError::from)?;

        info!(
            "Successfully inserted {} tar manifest records to {}",
            entries.len(),
            table_name
        );
        Ok(())
    }
}
