//! 共享表的集群扫描存储。所有读写在这里绑定 job_id，业务层不拼接集群 SQL。
#[cfg(test)]
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

use clickhouse::Client;
use clickhouse::error::Error as ClickHouseError;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use storage_adapter::{EntryEnum, StorageEntryMessage};
use utils::sanitize_job_id;

#[cfg(test)]
use self::record::ClusterStorageEntryRecord;
use self::record::{ClusterIncrementalStorageEntryRecord, ClusterScanResumeState};
use crate::clickhouse::scan_resume::{FINALIZING, ScanResumeState};
use crate::clickhouse::scan_resume_statistics::ScanResumeStatistics;
use crate::clickhouse::{FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION, FILE_SCAN_COLUMNS_DEFINITION, build_base_client};
use crate::common::FILE_SCAN_COLUMNS_LIST;
#[cfg(test)]
use crate::common::{DeletionStatus, classify_deletion_status};
use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
#[cfg(test)]
use crate::traits::{IncrementalStorageEntryRecord, StorageEntryRecord};

#[cfg(test)]
mod compare;
mod record;
mod statistics;

pub(crate) struct ClusterScanStore {
    client: Client,
    cluster: String,
    database: String,
    job_id: String,
    epoch: AtomicU64,
    generation: AtomicU8,
    batch_id: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
struct CommittedScan {
    job_id: String,
    generation: u8,
    epoch: u64,
    ver: i64,
}

#[derive(Serialize, clickhouse::Row)]
struct EntryKey {
    job_id: String,
    batch_id: u64,
    relative_path: String,
    version_id: String,
}

fn identifier(value: &str) -> Result<String> {
    if value.is_empty() || !value.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_') {
        return Err(DatabaseError::ConfigError(
            "ClickHouse cluster/database identifiers must contain only letters, digits and underscores".into(),
        ));
    }
    Ok(format!("`{value}`"))
}

/// 顺序 quorum 写入遇到其他任务尚未完成的 quorum 时退避重试；所有目标行均可幂等重放。
async fn retry_quorum<F, Fut>(mut operation: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let start = Instant::now();
    loop {
        match operation().await {
            Err(DatabaseError::ClickHouseError(ClickHouseError::BadResponse(ref message)))
                if message.contains("UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE")
                    && start.elapsed() < Duration::from_secs(30) =>
            {
                sleep(Duration::from_millis(50)).await;
            }
            result => return result,
        }
    }
}

impl ClusterScanStore {
    pub(crate) fn new(config: &ClickHouseConfig, cluster: &str, job_id: &str) -> Result<Self> {
        identifier(cluster)?;
        identifier(&config.database)?;
        let client = build_base_client(config)
            .with_option("insert_distributed_sync", "1")
            .with_option("async_insert", "0")
            .with_option("insert_quorum", "auto")
            .with_option("insert_quorum_parallel", "0")
            .with_option("select_sequential_consistency", "1")
            .with_option("load_balancing", "in_order")
            .with_option("prefer_localhost_replica", "0")
            .with_option("skip_unavailable_shards", "0")
            .with_option("optimize_skip_unused_shards", "1")
            .with_option("distributed_product_mode", "local")
            .with_option("distributed_ddl_task_timeout", "60");
        Ok(Self {
            client,
            cluster: cluster.into(),
            database: config.database.clone(),
            job_id: sanitize_job_id(job_id),
            epoch: AtomicU64::new(0),
            generation: AtomicU8::new(0),
            batch_id: 0,
        })
    }

    pub(crate) async fn ping(&self) -> Result<()> {
        self.client.query("SELECT 1").fetch_one::<u8>().await?;
        let count: u64 = self
            .client
            .query("SELECT count() FROM system.clusters WHERE cluster=?")
            .bind(&self.cluster)
            .fetch_one()
            .await?;
        if count == 0 {
            return Err(DatabaseError::ConfigError(
                "Configured ClickHouse cluster does not exist".into(),
            ));
        }
        Ok(())
    }

    /// 一个 job 固定归属一个 shard，与 DeepSync 一致；不同 job 在 shard 间分布。
    /// 对同一 job 的基线、事件和批次执行本地 JOIN，不跨任务构建全局集合。
    async fn create_table(&self, name: &str, columns: &str, engine: &str, partition: &str, order: &str) -> Result<()> {
        let cluster = identifier(&self.cluster)?;
        let database = identifier(&self.database)?;
        let local = format!("{name}_local");
        let path = format!("/clickhouse/terrasync/{}/{local}/{{shard}}", self.database);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {database}.{local} ON CLUSTER {cluster} ({columns}) ENGINE = {engine}('{path}', '{{replica}}') PARTITION BY {partition} ORDER BY {order} SETTINGS allow_nullable_key=1"
        );
        self.client.query(&sql).execute().await?;
        self.client.query(&format!("CREATE TABLE IF NOT EXISTS {database}.{name} ON CLUSTER {cluster} AS {database}.{local} ENGINE = Distributed({cluster}, {database}, {local}, cityHash64(job_id))")).execute().await?;
        Ok(())
    }

    pub(crate) async fn initialize(&self) -> Result<()> {
        let columns = self
            .client
            .query("SELECT name FROM system.columns WHERE database=? AND table='base_local'")
            .bind(&self.database)
            .fetch_all::<String>()
            .await?;
        if !columns.is_empty()
            && !["job_id", "epoch", "batch_id", "current_state", "is_deleted"]
                .iter()
                .all(|name| columns.iter().any(|column| column == name))
        {
            return Err(DatabaseError::ConfigError(
                "ClickHouse base table belongs to an incompatible schema; use a dedicated TerraSync database".into(),
            ));
        }

        let columns = format!(
            "job_id String, epoch UInt64, batch_id UInt64 DEFAULT 0, ver DateTime64(9) DEFAULT now64(9), is_deleted UInt8 DEFAULT 0, {FILE_SCAN_COLUMNS_DEFINITION}"
        );
        self.create_versioned_table(
            "base",
            &format!("{columns}, INDEX file_handle_idx file_handle TYPE bloom_filter(0.01) GRANULARITY 1"),
            "job_id",
            "(job_id,relative_path,version_id)",
            true,
        )
        .await?;
        self.create_table(
            "scan_resume_batch",
            &columns,
            "ReplicatedMergeTree",
            "(job_id,batch_id)",
            "(job_id,relative_path,version_id)",
        )
        .await?;
        self.create_table(
            "scan_resume_keys",
            "job_id String,batch_id UInt64,relative_path String,version_id String",
            "ReplicatedMergeTree",
            "(job_id,batch_id)",
            "(job_id,relative_path,version_id)",
        )
        .await?;
        let events = format!(
            "job_id String,epoch UInt64,ver DateTime64(9) DEFAULT now64(9),{FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION}"
        );
        self.create_versioned_table(
            "incremental",
            &events,
            "job_id",
            "(job_id,epoch,operation_type,relative_path,version_id)",
            false,
        )
        .await?;
        self.create_versioned_table(
            "state",
            "job_id String,generation UInt8,epoch UInt64,ver DateTime64(9) DEFAULT now64(9)",
            "job_id",
            "job_id",
            false,
        )
        .await?;
        self.create_checkpoint_table().await?;
        Ok(())
    }

    async fn create_versioned_table(
        &self, name: &str, columns: &str, partition: &str, order: &str, tombstones: bool,
    ) -> Result<()> {
        let cluster = identifier(&self.cluster)?;
        let database = identifier(&self.database)?;
        let local = format!("{name}_local");
        let deleted = if tombstones { ",is_deleted" } else { "" };
        let path = format!("/clickhouse/terrasync/{}/{local}/{{shard}}", self.database);
        self.client.query(&format!("CREATE TABLE IF NOT EXISTS {database}.{local} ON CLUSTER {cluster} ({columns}) ENGINE = ReplicatedReplacingMergeTree('{path}','{{replica}}',ver{deleted}) PARTITION BY {partition} ORDER BY {order} SETTINGS allow_nullable_key=1")).execute().await?;
        self.client.query(&format!("CREATE TABLE IF NOT EXISTS {database}.{name} ON CLUSTER {cluster} AS {database}.{local} ENGINE = Distributed({cluster},{database},{local},cityHash64(job_id))")).execute().await?;
        Ok(())
    }

    async fn create_checkpoint_table(&self) -> Result<()> {
        let columns = "job_id String,id UInt8,schema_version UInt8,version UInt64,request_key String,generation UInt8,incremental Bool,phase UInt8,event_time Int64,observed_entries UInt64,checkpoint Array(UInt8),statistics String DEFAULT ''";
        let cluster = identifier(&self.cluster)?;
        let database = identifier(&self.database)?;
        let path = format!("/clickhouse/terrasync/{}/scan_resume_local/{{shard}}", self.database);
        self.client.query(&format!("CREATE TABLE IF NOT EXISTS {database}.scan_resume_local ON CLUSTER {cluster} ({columns}) ENGINE = ReplicatedReplacingMergeTree('{path}','{{replica}}',version) PARTITION BY job_id ORDER BY job_id")).execute().await?;
        self.client.query(&format!("CREATE TABLE IF NOT EXISTS {database}.scan_resume ON CLUSTER {cluster} AS {database}.scan_resume_local ENGINE = Distributed({cluster},{database},scan_resume_local,cityHash64(job_id))")).execute().await?;
        Ok(())
    }

    pub(crate) async fn clear_job(&self) -> Result<()> {
        let database = identifier(&self.database)?;
        let cluster = identifier(&self.cluster)?;
        for table in ["base_local", "incremental_local", "state_local", "scan_resume_local"] {
            let exists: u64 = self
                .client
                .query("SELECT count() FROM clusterAllReplicas(?,system.tables) WHERE database=? AND name=?")
                .bind(&self.cluster)
                .bind(&self.database)
                .bind(table)
                .fetch_one()
                .await?;
            if exists > 0 {
                self.client
                    .query(&format!(
                        "ALTER TABLE {database}.{table} ON CLUSTER {cluster} DROP PARTITION ?"
                    ))
                    .bind(&self.job_id)
                    .execute()
                    .await?;
            }
        }
        self.cleanup_abandoned_batches().await
    }

    async fn cleanup_abandoned_batches(&self) -> Result<()> {
        let database = identifier(&self.database)?;
        let cluster = identifier(&self.cluster)?;
        for table in ["scan_resume_batch", "scan_resume_keys"] {
            let exists: u64 = self
                .client
                .query("SELECT count() FROM system.tables WHERE database=? AND name=?")
                .bind(&self.database)
                .bind(table)
                .fetch_one()
                .await?;
            if exists == 0 {
                continue;
            }
            let batches = self
                .client
                .query(&format!(
                    "SELECT DISTINCT batch_id FROM {database}.{table} WHERE job_id=?"
                ))
                .bind(&self.job_id)
                .fetch_all::<u64>()
                .await?;
            for batch in batches {
                self.client
                    .query(&format!(
                        "ALTER TABLE {database}.{table}_local ON CLUSTER {cluster} DROP PARTITION (?,?)"
                    ))
                    .bind(&self.job_id)
                    .bind(batch)
                    .execute()
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn load_resume_state(&self) -> Result<Option<ScanResumeState>> {
        let database = identifier(&self.database)?;
        self.initialize().await?;
        self.cleanup_abandoned_batches().await?;
        let row=self.client.query(&format!("SELECT id,schema_version,version,request_key,generation,incremental,phase,event_time,observed_entries,checkpoint,statistics FROM {database}.scan_resume FINAL WHERE job_id=?")).bind(&self.job_id).fetch_one::<ScanResumeState>().await;
        match row {
            Ok(state) => {
                state.validate(&state.request_key)?;
                if state.event_time < 0 {
                    return Err(DatabaseError::ConversionError("Negative scan epoch".into()));
                }
                self.epoch.store(state.event_time as u64, Ordering::Relaxed);
                Ok(Some(state))
            }
            Err(ClickHouseError::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn save_resume_state(&self, state: &ScanResumeState) -> Result<()> {
        retry_quorum(|| async move {
            self.epoch.store(state.event_time as u64, Ordering::Relaxed);
            self.generation.store(state.generation, Ordering::Relaxed);
            let mut insert = self.client.insert::<ClusterScanResumeState>("scan_resume").await?;
            insert
                .write(&ClusterScanResumeState::new(self.job_id.clone(), state.clone()))
                .await?;
            insert.end().await?;
            Ok(())
        })
        .await
    }

    async fn committed(&self) -> Result<Option<CommittedScan>> {
        let database = identifier(&self.database)?;
        match self
            .client
            .query(&format!(
                "SELECT job_id,generation,epoch,ver FROM {database}.state FINAL WHERE job_id=?"
            ))
            .bind(&self.job_id)
            .fetch_one::<CommittedScan>()
            .await
        {
            Ok(row) => Ok(Some(row)),
            Err(ClickHouseError::RowNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) async fn resume_execution_is_incremental(&self) -> Result<bool> {
        let database = identifier(&self.database)?;
        let exists: u64 = self
            .client
            .query("SELECT count() FROM system.tables WHERE database=? AND name='scan_resume'")
            .bind(&self.database)
            .fetch_one()
            .await?;
        if exists == 0 {
            return Ok(false);
        }
        match self
            .client
            .query(&format!(
                "SELECT incremental,phase FROM {database}.scan_resume FINAL WHERE job_id=?"
            ))
            .bind(&self.job_id)
            .fetch_one::<(bool, u8)>()
            .await
        {
            Ok((incremental, phase)) if phase <= FINALIZING => return Ok(incremental),
            Ok(_) | Err(ClickHouseError::RowNotFound) => {}
            Err(error) => return Err(error.into()),
        }
        self.has_baseline().await
    }
    pub(crate) async fn has_baseline(&self) -> Result<bool> {
        Ok(self.committed().await?.is_some())
    }
    pub(crate) async fn results_exist(&self) -> Result<bool> {
        let database = identifier(&self.database)?;
        // 共享表不能按表是否存在判定 job 是否已有基线。
        let count: u64 = self
            .client
            .query(&format!(
                "SELECT count() FROM {database}.scan_resume FINAL WHERE job_id=?"
            ))
            .bind(&self.job_id)
            .fetch_one()
            .await?;
        Ok(count > 0)
    }
    pub(crate) async fn begin_scan_generation(&self) -> Result<u8> {
        Ok(1 - self.committed().await?.map_or(0, |state| state.generation))
    }
    pub(crate) async fn restore_resume_generation(&self, state: &ScanResumeState) -> Result<bool> {
        self.epoch.store(state.event_time as u64, Ordering::Relaxed);
        self.generation.store(state.generation, Ordering::Relaxed);
        if let Some(committed) = self.committed().await? {
            if committed.epoch == state.event_time as u64 {
                if state.phase != FINALIZING {
                    return Err(DatabaseError::TransactionError(
                        "Running checkpoint conflicts with completed epoch".into(),
                    ));
                }
                return Ok(true);
            }
            if committed.generation == state.generation {
                return Err(DatabaseError::TransactionError(
                    "Saved scan generation conflicts with baseline".into(),
                ));
            }
        }
        Ok(false)
    }
    pub(crate) async fn commit_scan_generation(&self) -> Result<()> {
        retry_quorum(|| async move {
            let database = identifier(&self.database)?;
            let epoch = self.epoch.load(Ordering::Relaxed);
            let generation: u8 = self
                .client
                .query(&format!(
                    "SELECT generation FROM {database}.scan_resume FINAL WHERE job_id=?"
                ))
                .bind(&self.job_id)
                .fetch_one()
                .await?;
            let mut insert = self.client.insert::<CommittedScan>("state").await?;
            insert
                .write(&CommittedScan {
                    job_id: self.job_id.clone(),
                    generation,
                    epoch,
                    ver: epoch as i64,
                })
                .await?;
            insert.end().await?;
            Ok(())
        })
        .await
    }
    pub(crate) async fn resume_event_epoch(&self, now: i64) -> Result<i64> {
        let database = identifier(&self.database)?;
        let last: i64 = self
            .client
            .query(&format!(
                "SELECT ifNull(max(event_time),0) FROM {database}.scan_resume FINAL WHERE job_id=?"
            ))
            .bind(&self.job_id)
            .fetch_one()
            .await?;
        Ok(now.max(
            last.checked_add(1)
                .ok_or_else(|| DatabaseError::ConversionError("Scan epoch overflow".into()))?,
        ))
    }
    pub(crate) async fn prepare_incremental(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn begin_resume_batch(&mut self) -> Result<()> {
        self.drop_batch().await?;
        self.batch_id = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        Ok(())
    }
    #[cfg(test)]
    async fn write_entries(&self, table: &str, entries: &[Arc<EntryEnum>], batch_id: u64) -> Result<()> {
        retry_quorum(|| async move {
            if entries.is_empty() {
                return Ok(());
            }
            let epoch = self.epoch.load(Ordering::Relaxed);
            let mut insert = self.client.insert::<ClusterStorageEntryRecord>(table).await?;
            for entry in entries {
                let record = StorageEntryRecord::from_entry_enum(entry, self.generation.load(Ordering::Relaxed));
                insert
                    .write(&ClusterStorageEntryRecord::new(
                        self.job_id.clone(),
                        epoch,
                        batch_id,
                        record,
                    ))
                    .await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }
    #[cfg(test)]
    pub(crate) async fn write_resume_temp_records(&self, entries: &[Arc<EntryEnum>]) -> Result<()> {
        self.write_entries("scan_resume_batch", entries, self.batch_id).await
    }
    #[cfg(test)]
    pub(crate) async fn write_resume_base_records(&self, entries: &[Arc<EntryEnum>]) -> Result<()> {
        self.write_entries("base", entries, 0).await
    }
    #[cfg(test)]
    pub(crate) async fn insert_temp_to_base_table(&self, _excluded: &[(String, String)]) -> Result<()> {
        retry_quorum(|| async move {

        let database = identifier(&self.database)?;
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        self.client.query(&format!("INSERT INTO {database}.base (job_id,epoch,ver,{columns}) SELECT job_id,epoch,ver,{columns} FROM {database}.scan_resume_batch WHERE job_id=? AND batch_id=?")).bind(&self.job_id).bind(self.batch_id).execute().await?;
        Ok(())

        }).await
    }
    #[cfg(test)]
    pub(crate) async fn write_resume_events(&self, messages: &[StorageEntryMessage], event_time: i64) -> Result<()> {
        retry_quorum(|| async move {
            if messages.is_empty() {
                return Ok(());
            }
            let mut insert = self
                .client
                .insert::<ClusterIncrementalStorageEntryRecord>("incremental")
                .await?;
            for message in messages {
                let mut record = IncrementalStorageEntryRecord::from_message(message);
                record.create_at = event_time;
                insert
                    .write(&ClusterIncrementalStorageEntryRecord::new(
                        self.job_id.clone(),
                        event_time as u64,
                        record,
                    ))
                    .await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }
    async fn drop_batch(&self) -> Result<()> {
        let database = identifier(&self.database)?;
        if self.batch_id == 0 {
            return Ok(());
        }
        let cluster = identifier(&self.cluster)?;
        for table in ["scan_resume_batch_local", "scan_resume_keys_local"] {
            self.client
                .query(&format!(
                    "ALTER TABLE {database}.{table} ON CLUSTER {cluster} DROP PARTITION (?,?)"
                ))
                .bind(&self.job_id)
                .bind(self.batch_id)
                .execute()
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn cleanup_resume_batch(&self) -> Result<()> {
        self.cleanup_abandoned_batches().await
    }

    #[cfg(test)]
    pub(crate) async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
        let database = identifier(&self.database)?;
        let records = self
            .client
            .query(&format!(
                "SELECT {} FROM {database}.base FINAL WHERE job_id=? AND epoch!=? AND is_deleted=0",
                *FILE_SCAN_COLUMNS_LIST
            ))
            .bind(&self.job_id)
            .bind(self.epoch.load(Ordering::Relaxed))
            .fetch_all::<StorageEntryRecord>()
            .await?;
        let mut no_handles = Vec::new();
        let mut handles = Vec::new();
        for record in records {
            if record
                .file_handle
                .as_ref()
                .is_some_and(|handle| handle.starts_with("54534934"))
            {
                handles.push(record);
            } else {
                no_handles.push(record);
            }
        }
        let mut groups = HashMap::new();
        for chunk in handles.chunks(100_000) {
            let keys: Vec<_> = chunk
                .iter()
                .filter_map(|record| record.file_handle.as_deref())
                .collect();
            let rows = self
                .client
                .query(&format!(
                    "SELECT {} FROM {database}.base FINAL WHERE job_id=? AND file_handle IN ? AND is_deleted=0",
                    *FILE_SCAN_COLUMNS_LIST
                ))
                .bind(&self.job_id)
                .bind(&keys)
                .fetch_all::<StorageEntryRecord>()
                .await?;
            for row in rows {
                if let Some(handle) = &row.file_handle {
                    groups.entry(handle.clone()).or_insert_with(Vec::new).push(row);
                }
            }
        }
        Ok(Box::new(
            classify_deletion_status(no_handles, handles, &groups).into_iter(),
        ))
    }
    pub(crate) async fn delete_resume_entries(&self, entries: &[(String, String)]) -> Result<()> {
        retry_quorum(|| async move {

        let database = identifier(&self.database)?;
        if entries.is_empty() {
            return Ok(());
        }
        let keys_id = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                ?
                .as_nanos(),
        )
        ?;
        let mut insert = self.client.insert::<EntryKey>("scan_resume_keys").await?;
        for (path, version) in entries {
            insert
                .write(&EntryKey {
                    job_id: self.job_id.clone(),
                    batch_id: keys_id,
                    relative_path: path.clone(),
                    version_id: version.clone(),
                })
                .await?;
        }
        insert.end().await?;
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        self.client.query(&format!("INSERT INTO {database}.base (job_id,epoch,ver,is_deleted,{columns}) SELECT job_id,?,fromUnixTimestamp64Nano(?),1,{columns} FROM {database}.base FINAL WHERE job_id=? AND is_deleted=0 AND epoch!=? AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {database}.scan_resume_keys WHERE job_id=? AND batch_id=?)")).bind(self.epoch.load(Ordering::Relaxed)).bind(self.epoch.load(Ordering::Relaxed) as i64).bind(&self.job_id).bind(self.epoch.load(Ordering::Relaxed)).bind(&self.job_id).bind(keys_id).execute().await?;
        self.client
            .query(&format!(
                "ALTER TABLE {database}.scan_resume_keys_local ON CLUSTER {} DROP PARTITION (?,?)",
                identifier(&self.cluster)?
            ))
            .bind(&self.job_id)
            .bind(keys_id)
            .execute()
            .await?;
        Ok(())

        }).await
    }
}

#[cfg(test)]
mod tests;

/// 仅清理指定任务的共享表分区；不删除共享表，也不影响其他任务。
pub async fn clear_cluster_scan_data(config: &ClickHouseConfig, cluster: &str, job_id: &str) -> Result<()> {
    ClusterScanStore::new(config, cluster, job_id)?.clear_job().await
}

mod native;
