//! 扫描持久化接口：单机兼容旧作业；集群使用按 job_id 隔离的共享表。
use crate::scan::cluster::ClusterScanStore;
use crate::schema::entry::EntrySummary as StorageEntryRecord;
use crate::{
    ClickHouseConfig, ClickHouseDatabase, DatabaseError, INCREMENTAL_SCAN_TABLE_BASE_NAME, Result,
    get_scan_base_table_name,
};
use data_mover::model::ObservedEntry;
use std::sync::Arc;
use task_model::observation::ObservedScanEvent;

mod checkpoint;
pub(crate) mod model;
pub use model::*;
pub(crate) mod candidate_cache;
pub(crate) mod cluster;
pub(crate) mod compare;
pub(crate) mod rows;
pub use crate::execution::{ConnectivityWatch, ScanDatabaseExecution, ScanQueryCancellation, with_scan_cancellation};
use checkpoint::ScanCuts;

enum Store {
    Single(ClickHouseDatabase),
    Cluster(ClusterScanStore),
}
/// 数据库派生的轮次最终事件，保留实际查询事实，不伪造原生身份或链接目标。
#[derive(Clone, Debug)]
pub enum ScanFinalEvent {
    Added(StorageEntryRecord),
    Deleted(StorageEntryRecord),
    Renamed {
        from_path: String,
        from_version: String,
        to: StorageEntryRecord,
    },
}
impl ScanFinalEvent {
    pub fn item(&self) -> &StorageEntryRecord {
        match self {
            Self::Added(row) | Self::Deleted(row) => row,
            Self::Renamed { to, .. } => to,
        }
    }
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Added(_) => "new",
            Self::Deleted(_) => "deleted",
            Self::Renamed { .. } => "rename",
        }
    }
    pub fn previous_path(&self) -> Option<String> {
        match self {
            Self::Renamed { from_path, .. } => Some(from_path.clone()),
            _ => None,
        }
    }
    pub fn deletion_key(&self) -> Option<(String, String)> {
        match self {
            Self::Added(_) => None,
            Self::Deleted(row) => Some((row.relative_path.clone(), row.version_id.clone())),
            Self::Renamed {
                from_path,
                from_version,
                ..
            } => Some((from_path.clone(), from_version.clone())),
        }
    }
}

pub struct ScanStore {
    inner: Store,
    job_id: String,
    queries: ScanQueryCancellation,
    cuts: ScanCuts,
}

macro_rules! delegate {
    ($($name:ident($($param:ident:$ty:ty),*) -> $ret:ty;)*) => {$ (
        pub async fn $name(&self,$($param:$ty),*) -> $ret {
            match &self.inner { Store::Single(store)=>store.$name($($param),*).await, Store::Cluster(store)=>store.$name($($param),*).await }
        }
    )*};
}
impl ScanStore {
    /// 普通元数据入口不能把活动扫描的 working 表当作已发布结果。
    pub(crate) async fn ensure_metadata_available(config: &ClickHouseConfig, job: &str) -> Result<()> {
        ScanCuts::ensure_metadata_available(config, job).await
    }
    /// 集群批次独享临时分区，连接池与扫描轮次共享；单机保留串行兼容路径。
    pub fn batch_writer(&self) -> Option<Self> {
        match &self.inner {
            Store::Cluster(store) => Some(Self {
                inner: Store::Cluster(store.batch_writer()),
                job_id: self.job_id.clone(),
                queries: self.queries.clone(),
                cuts: self.cuts.clone(),
            }),
            Store::Single(_) => None,
        }
    }

    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        let cluster = config.cluster_name()?;
        let queries = ScanQueryCancellation::new(config, cluster, job_id)?;
        let client = queries.data_client(config)?;
        let inner = match cluster.filter(|value| !value.is_empty()) {
            Some(cluster) => Store::Cluster(ClusterScanStore::with_client(config, cluster, job_id, client.clone())?),
            None => Store::Single(ClickHouseDatabase::with_client(config, job_id, client.clone())?),
        };
        Ok(Self {
            cuts: ScanCuts::with_client(config, job_id, queries.comment(), client)?,
            inner,
            job_id: job_id.into(),
            queries,
        })
    }
    pub fn query_cancellation(&self) -> ScanQueryCancellation {
        self.queries.clone()
    }
    /// 先排空前次执行的 DDL/mutation，再读取恢复状态；旧 reset 也必须先完成。
    pub async fn load_resume_state(&self) -> Result<Option<ScanResumeState>> {
        self.cuts.await_prior_operations().await?;
        match &self.inner {
            Store::Single(store) => store.load_resume_state().await,
            Store::Cluster(store) => store.load_resume_state().await,
        }
    }
    /// 在 generation 的回标、旧状态失效等操作之前封存本轮原始基线。
    pub async fn capture_scan_baseline(&self) -> Result<String> {
        self.cuts.capture().await
    }
    pub async fn restore_scan_baseline(&self, state: &ScanResumeState) -> Result<()> {
        self.cuts.restore(&state.baseline_snapshot).await
    }
    /// 调用方已经过 writer 屏障；不可变快照 ACK 之后才提交统计与源游标。
    pub async fn seal_resume_state(&self, state: &mut ScanResumeState) -> Result<()> {
        state.checkpoint_snapshot = self.cuts.capture().await?;
        self.save_resume_state(state).await?;
        self.cleanup_scan_snapshots(state).await?;
        Ok(())
    }
    pub async fn restore_scan_checkpoint(&self, state: &ScanResumeState) -> Result<()> {
        self.cuts.restore(&state.checkpoint_snapshot).await
    }
    /// 清理失败只留下可回收对象，不把已经提交的 manifest 改判失败。
    pub async fn cleanup_scan_snapshots(&self, state: &ScanResumeState) -> Result<()> {
        let keep = if state.active() {
            vec![state.baseline_snapshot.as_str(), state.checkpoint_snapshot.as_str()]
        } else {
            Vec::new()
        };
        if let Err(error) = self.cuts.cleanup(&keep).await {
            if matches!(error, DatabaseError::ScanCancelled) {
                return Err(error);
            }
            tracing::warn!(%error, "Deferred scan metadata snapshot cleanup");
        }
        Ok(())
    }
    pub async fn initialize(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.initialize().await,
            Store::Cluster(_) => Ok(()),
        }
    }
    /// 表布局及清理 SQL 由具体数据库实现负责，上层只指定作业。
    pub async fn clear_job(&self) -> Result<()> {
        self.cuts.await_prior_operations().await?;
        match &self.inner {
            Store::Cluster(store) => store.clear_job().await,
            Store::Single(store) => {
                let job = utils::sanitize_job_id(&self.job_id);
                for prefix in [
                    "base",
                    "state",
                    "scan_state",
                    "incremental",
                    "scan_resume",
                    "scan_resume_batch",
                    "scan_resume_event_keys",
                    "scan_resume_delete_keys",
                ] {
                    store.drop_table_by_name(&format!("{prefix}_{job}")).await?;
                }
                Ok(())
            }
        }?;
        self.cuts.cleanup(&[]).await
    }
    pub async fn prepare_incremental(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await,
            Store::Cluster(store) => store.prepare_incremental().await,
        }
    }
    pub async fn prepare_native_schema(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.prepare_native_schema().await,
            Store::Cluster(store) => store.prepare_native_schema().await,
        }
    }
    pub async fn has_baseline(&self) -> Result<bool> {
        match &self.inner {
            Store::Single(store) => store.has_committed_baseline().await,
            Store::Cluster(store) => store.has_baseline().await,
        }
    }
    pub async fn results_exist(&self) -> Result<bool> {
        match &self.inner {
            Store::Single(store) => store.table_exists(&get_scan_base_table_name(&self.job_id)).await,
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
    /// 候选事件已持久化后，随当前基线保存这些候选的可校验摘要。
    pub async fn write_candidate_observations(
        &self, entries: &[Arc<ObservedEntry>], candidates: &[ObservedScanEvent],
    ) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.write_native_candidates(entries, false, candidates).await,
            Store::Cluster(store) => store.write_native_candidates(entries, false, candidates).await,
        }
    }
    pub async fn write_observation_keys(&self, entries: &[Arc<ObservedEntry>]) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.write_native_keys(entries).await,
            Store::Cluster(store) => store.write_native_keys(entries).await,
        }
    }
    pub async fn write_resume_events(&self, events: &[ObservedScanEvent], time: i64) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.events_native(events, time).await,
            Store::Cluster(store) => store.events_native(events, time).await,
        }
    }
    pub async fn resolve_final_events(&self, time: i64) -> Result<Vec<ScanFinalEvent>> {
        match &self.inner {
            Store::Single(store) => store.resolve_final_events_native(time).await,
            Store::Cluster(store) => store.resolve_final_events_native(time).await,
        }
    }
    pub async fn write_final_events(&self, events: &[ScanFinalEvent], time: i64) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.write_final_events_native(events, time).await,
            Store::Cluster(store) => store.write_final_events_native(events, time).await,
        }
    }
    pub async fn delete_resume_entries(&self, entries: &[(String, String)]) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.delete_previous_scan_entries(entries).await,
            Store::Cluster(store) => store.delete_resume_entries(entries).await,
        }
    }
    pub async fn cleanup_candidates(&self, time: i64) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.cleanup_candidates(time).await,
            Store::Cluster(store) => store.cleanup_candidates(time).await,
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
        save_resume_state(state:&ScanResumeState)->Result<()>;
        restore_resume_generation(state:&ScanResumeState)->Result<bool>;
        begin_scan_generation()->Result<u8>;
        commit_scan_generation()->Result<()>;
        resume_event_epoch(now:i64)->Result<i64>;
        cleanup_resume_batch()->Result<()>;






        resume_statistics(generation:u8,event_time:Option<i64>)->Result<ScanResumeStatistics>;
        resume_event_statistics(epoch:i64)->Result<ScanEventStatistics>;
    }
}
