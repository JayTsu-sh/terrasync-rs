//! Native observation batches and acknowledged versions written to the catalogue.

use std::sync::Arc;

use data_mover::model::ObservedEntry;
use serde_json::Value;
use task_model::observation::ObservedItem;
use task_model::{CopyCompletion, CopyReadiness, StorageEntryMessage};
use tracing::trace;
use tracing::{debug, info};

use super::ClickHouseDatabase;
use super::versions::{CatalogueInputs, CatalogueViews};
use crate::error::{DatabaseError, Result};
use crate::execution::queries::{
    ScanExecute, check_scan_cancellation, execute_request, execution_active, idempotent_insert_client,
};
use crate::execution::recovery::RequestSafety;
use crate::get_incremental_scan_base_table_name;
use crate::get_scan_base_table_name;
use crate::get_tar_manifest_table_name;
use crate::scan::rows::NativeScanRecord;
use crate::schema::records::{IncrementalStorageEntryRecord, TarManifestRecord};

impl ClickHouseDatabase {
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
    pub(super) async fn batch_insert(
        &self, table_name: &str, records: &[Arc<ObservedEntry>], use_async_client: bool,
    ) -> Result<()> {
        self.insert_copy_views(table_name, CatalogueInputs::Observed(records), use_async_client)
            .await
    }

    async fn insert_copy_views(
        &self, table_name: &str, inputs: CatalogueInputs<'_>, use_async_client: bool,
    ) -> Result<()> {
        check_scan_cancellation()?;
        let _base = self.base_write_guard().await?;
        let versions = self.catalogue_versions().await;
        let mut views = CatalogueViews::new(inputs, versions).peekable();
        if views.peek().is_none() {
            return Ok(());
        }
        let current_state = self.get_cached_scan_state().await?;
        let (copy_execution_id, copy_sequence) = {
            let lifecycle = self.generation_lifecycle.lock().await;
            if lifecycle.copy_generation.is_some() && lifecycle.working.is_none() {
                return Err(DatabaseError::CopyExecutionUnavailable);
            }
            (
                lifecycle.copy_execution_id.clone().unwrap_or_default(),
                lifecycle
                    .copy_generation
                    .as_ref()
                    .map_or(0, |generation| generation.sequence),
            )
        };
        let _write = self.catalogue_write_guard().await;
        debug!(
            "During batch inserting to table {}, current_state: {:?}",
            table_name, current_state
        );

        let client = if use_async_client {
            &self.async_client
        } else {
            &self.sync_client
        };
        let client = self.catalogue_write_client(client).with_validation(false);
        // 同一逻辑批次共用去重身份；有恢复作用域时用同步批次提交，避免异步缓冲确认不明。
        let client = if execution_active() {
            idempotent_insert_client(&client).with_option("async_insert", "0")
        } else {
            client
        };
        // W、执行身份、顺序和版本槽在恢复循环外冻结，重试不改变记录语义。
        let record_count = execute_request(RequestSafety::IdempotentWrite, || async {
            let mut record_count = 0;
            let mut insert = client
                .insert::<NativeScanRecord>(table_name)
                .await
                .map_err(DatabaseError::from)?;

            for (view, completion) in CatalogueViews::new(inputs, versions) {
                let mut record = NativeScanRecord::capture(view, current_state)?;
                if let Some(completion) = completion {
                    record.copy_path_confirmed = completion.readiness == CopyReadiness::Ready;
                    record.copy_data_confirmed = true;
                    record.copy_target_version = completion.target_version.as_deref();
                }
                record.version_id = versions.key(view);
                record.copy_execution_id = &copy_execution_id;
                record.copy_sequence = copy_sequence;
                insert.write(&record).await.map_err(DatabaseError::from)?;
                record_count += 1;
            }

            insert.end().await.map_err(DatabaseError::from)?;
            Ok(record_count)
        })
        .await?;

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

        let mut records = records;
        let execution_id = self.generation_lifecycle.lock().await.copy_execution_id.clone();
        if let Some(execution_id) = execution_id {
            for record in &mut records {
                record.copy_execution_id.clone_from(&execution_id);
            }
        }
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
        self.insert_copy_views(&table, CatalogueInputs::Selected(records), true)
            .await
    }

    pub async fn batch_insert_copy_completions(&self, records: &[CopyCompletion]) -> Result<()> {
        let table = get_scan_base_table_name(&self.job_id);
        self.insert_copy_views(&table, CatalogueInputs::Copied(records), true)
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
