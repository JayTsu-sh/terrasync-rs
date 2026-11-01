//! 扫描持久化接口：单机兼容旧作业；集群使用按 job_id 隔离的共享表。
use crate::clickhouse::scan_resume::ScanResumeState;
use crate::clickhouse::scan_resume_statistics::ScanResumeStatistics;
use crate::cluster_scan::ClusterScanStore;
use crate::traits::StorageEntryRecord;
use crate::{
    ClickHouseConfig, ClickHouseDatabase, Database, INCREMENTAL_SCAN_TABLE_BASE_NAME, Result, base_table_name_for_job,
};
use data_mover::model::ObservedEntry;
use std::sync::Arc;
use storage_adapter::observed_scan::ObservedScanEvent;

enum Store {
    Single(ClickHouseDatabase),
    Cluster(ClusterScanStore),
}
/// 删除来自数据库已有行，直接保留查询事实；旧行没有 snapshot 时也不伪造原生身份或链接目标。
pub struct ScanDeletion {
    pub from: StorageEntryRecord,
    pub to: Option<StorageEntryRecord>,
}
impl ScanDeletion {
    pub fn item(&self) -> &StorageEntryRecord {
        self.to.as_ref().unwrap_or(&self.from)
    }
    pub fn operation(&self) -> &'static str {
        if self.to.is_some() { "rename" } else { "deleted" }
    }
}

pub struct ScanStore {
    inner: Store,
    job_id: String,
}

macro_rules! delegate {
    ($($name:ident($($param:ident:$ty:ty),*) -> $ret:ty;)*) => {$ (
        pub async fn $name(&self,$($param:$ty),*) -> $ret {
            match &self.inner { Store::Single(store)=>store.$name($($param),*).await, Store::Cluster(store)=>store.$name($($param),*).await }
        }
    )*};
}
impl ScanStore {
    pub fn new(config: &ClickHouseConfig, cluster: Option<&str>, job_id: &str) -> Result<Self> {
        let inner = match cluster.filter(|value| !value.is_empty()) {
            Some(cluster) => Store::Cluster(ClusterScanStore::new(config, cluster, job_id)?),
            None => Store::Single(ClickHouseDatabase::new(config, job_id)),
        };
        Ok(Self {
            inner,
            job_id: job_id.into(),
        })
    }
    pub async fn initialize(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.initialize().await,
            Store::Cluster(_) => Ok(()),
        }
    }
    pub async fn prepare_incremental(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await,
            Store::Cluster(store) => store.prepare_incremental().await,
        }
    }
    pub async fn has_baseline(&self) -> Result<bool> {
        match &self.inner {
            Store::Single(store) => store.table_exists(&base_table_name_for_job(&self.job_id)).await,
            Store::Cluster(store) => store.has_baseline().await,
        }
    }
    pub async fn results_exist(&self) -> Result<bool> {
        match &self.inner {
            Store::Single(store) => store.table_exists(&base_table_name_for_job(&self.job_id)).await,
            Store::Cluster(store) => store.results_exist().await,
        }
    }
    pub async fn begin_resume_batch(&mut self) -> Result<()> {
        match &mut self.inner {
            Store::Single(store) => store.begin_resume_batch().await,
            Store::Cluster(store) => store.begin_resume_batch().await,
        }
    }
    pub async fn detect_resume_batch_events(
        &mut self, entries: &[Arc<ObservedEntry>],
    ) -> Result<Vec<ObservedScanEvent>> {
        match &mut self.inner {
            Store::Single(store) => store.compare_native(entries).await,
            Store::Cluster(store) => store.compare_native(entries).await,
        }
    }
    pub async fn write_observations(&self, entries: &[Arc<ObservedEntry>], temporary: bool) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.write_native(entries, temporary).await,
            Store::Cluster(store) => store.write_native(entries, temporary).await,
        }
    }
    pub async fn write_resume_events(&self, events: &[ObservedScanEvent], time: i64) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.events_native(events, time).await,
            Store::Cluster(store) => store.events_native(events, time).await,
        }
    }
    pub async fn detect_deleted_items(&self) -> Result<Vec<ScanDeletion>> {
        match &self.inner {
            Store::Single(store) => store.delete_native().await,
            Store::Cluster(store) => store.delete_native().await,
        }
    }
    pub async fn write_deletions(&self, events: &[ScanDeletion], time: i64) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.deletions_native(events, time).await,
            Store::Cluster(store) => store.deletions_native(events, time).await,
        }
    }
    pub async fn merge_observations(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.merge_native().await,
            Store::Cluster(store) => store.merge_native().await,
        }
    }
    delegate! {
        ping()->Result<()>;
        resume_execution_is_incremental()->Result<bool>;
        load_resume_state()->Result<Option<ScanResumeState>>;
        save_resume_state(state:&ScanResumeState)->Result<()>;
        restore_resume_generation(state:&ScanResumeState)->Result<bool>;
        begin_scan_generation()->Result<u8>;
        commit_scan_generation()->Result<()>;
        resume_event_epoch(now:i64)->Result<i64>;
        cleanup_resume_batch()->Result<()>;





        delete_resume_entries(entries:&[(String,String)])->Result<()>;
        resume_statistics(generation:u8,event_time:Option<i64>)->Result<ScanResumeStatistics>;
    }
}
