//! 扫描持久化接口：单机兼容旧作业；集群使用按 job_id 隔离的共享表。
use crate::clickhouse::scan_resume::ScanResumeState;
use crate::clickhouse::scan_resume_statistics::{ScanEventStatistics, ScanResumeStatistics};
use crate::cluster_scan::ClusterScanStore;
use crate::traits::StorageEntryRecord;
use crate::{
    ClickHouseConfig, ClickHouseDatabase, Database, INCREMENTAL_SCAN_TABLE_BASE_NAME, Result, base_table_name_for_job,
};
use data_mover::model::ObservedEntry;
use std::sync::Arc;
use storage_adapter::observed_scan::ObservedScanEvent;

pub(crate) mod directory_statistics;
pub(crate) mod queries;
pub use directory_statistics::{DirectoryStatisticsStore, ScanDirectoryStatisticsRow};
pub use queries::{ScanQueryCancellation, with_scan_cancellation};

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
    directory_statistics: DirectoryStatisticsStore,
}

macro_rules! delegate {
    ($($name:ident($($param:ident:$ty:ty),*) -> $ret:ty;)*) => {$ (
        pub async fn $name(&self,$($param:$ty),*) -> $ret {
            match &self.inner { Store::Single(store)=>store.$name($($param),*).await, Store::Cluster(store)=>store.$name($($param),*).await }
        }
    )*};
}
impl ScanStore {
    /// 集群批次独享临时分区，连接池与扫描轮次共享；单机保留串行兼容路径。
    pub fn batch_writer(&self) -> Option<Self> {
        match &self.inner {
            Store::Cluster(store) => Some(Self {
                inner: Store::Cluster(store.batch_writer()),
                job_id: self.job_id.clone(),
                queries: self.queries.clone(),
                directory_statistics: self.directory_statistics.clone(),
            }),
            Store::Single(_) => None,
        }
    }

    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        let cluster = config.cluster_name()?;
        let queries = ScanQueryCancellation::new(config, cluster)?;
        let mut inner = match cluster.filter(|value| !value.is_empty()) {
            Some(cluster) => Store::Cluster(ClusterScanStore::new(config, cluster, job_id)?),
            None => Store::Single(ClickHouseDatabase::new(config, job_id)?),
        };
        match &mut inner {
            Store::Single(store) => store.tag_scan_queries(queries.comment()),
            Store::Cluster(store) => store.tag_scan_queries(queries.comment()),
        }
        let directory_statistics = match &inner {
            Store::Single(store) => store.directory_statistics_ledger(),
            Store::Cluster(store) => store.directory_statistics_ledger()?,
        };
        Ok(Self {
            inner,
            job_id: job_id.into(),
            queries,
            directory_statistics,
        })
    }
    pub fn query_cancellation(&self) -> ScanQueryCancellation {
        self.queries.clone()
    }
    pub async fn initialize(&self) -> Result<()> {
        match &self.inner {
            Store::Single(store) => store.initialize().await,
            Store::Cluster(_) => Ok(()),
        }
    }
    /// 表布局及清理 SQL 由具体数据库实现负责，上层只指定作业。
    pub async fn clear_job(&self) -> Result<()> {
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
        // 原游标和结果全部失效后才能清贡献；中途失败仍保留可恢复检查点所需的历史。
        self.directory_statistics.clear_job().await
    }
    /// 目录 producer 可独立持有的同一执行域句柄，不启用额外批次 writer。
    pub fn directory_statistics_store(&self) -> DirectoryStatisticsStore {
        self.directory_statistics.clone()
    }
    /// 目录贡献是追加历史；读取 cut 前先过滤版本，不能用 FINAL 丢弃旧检查点。
    pub async fn prepare_directory_statistics(&self) -> Result<()> {
        self.directory_statistics.prepare().await
    }
    /// 同一轮的版本须单调推进，恢复时先截去未提交的后续版本。
    pub async fn write_directory_statistics(
        &self, round: i64, version: u64, rows: &[ScanDirectoryStatisticsRow],
    ) -> Result<()> {
        self.directory_statistics.write(round, version, rows).await
    }
    /// 仅返回指定目录中在 cut 时已存在的贡献，不加载整轮目录到内存。
    pub async fn load_directory_statistics(
        &self, round: i64, cut: u64, paths: &[String],
    ) -> Result<Vec<ScanDirectoryStatisticsRow>> {
        self.directory_statistics.load(round, cut, paths).await
    }
    /// 持有作业执行锁且停止旧 writer 后调用，确认删除完成才开始新的恢复写入。
    pub async fn truncate_directory_statistics(&self, round: i64, committed_cut: u64) -> Result<()> {
        self.directory_statistics.truncate(round, committed_cut).await
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
        load_resume_state()->Result<Option<ScanResumeState>>;
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
