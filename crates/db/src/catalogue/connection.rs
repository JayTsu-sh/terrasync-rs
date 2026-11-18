//! ClickHouse 目录的连接构造及已提交基线保护。

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use clickhouse::Client;
use tokio::sync::OnceCell;
use tracing::debug;

use super::{CatalogueLifecycle, ClickHouseDatabase, ClusterCatalogue};
use crate::execution::{ScanQueryCancellation, read::QuorumRead};
use crate::{config::ClickHouseConfig, error::Result, execution::base_client, scan::ScanStore};

impl ClickHouseDatabase {
    /// 新执行复用连接池并隔离游标／代次状态；Clone 仍只用于同一执行的 worker。
    #[must_use]
    pub fn new_execution_scope(&self) -> Self {
        let mut next = self.clone();
        next.scan_temp_table_name = None;
        next.cached_scan_state = AtomicU8::new(255);
        next.resume_base_has_typed_handles = None;
        next.generation_lifecycle = Arc::new(CatalogueLifecycle::default());
        next
    }

    /// 连接唯一的 ClickHouse 目录实现，保留原始有类型的连接失败。
    pub async fn connect(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        let catalogue = Self::new(config, job_id)?;
        ScanStore::ensure_metadata_available(config, job_id).await?;
        catalogue.ping().await?;
        Ok(catalogue)
    }

    /// 创建独立任务的目录实例；任务内通过 Clone 共享代次和写入协调状态。
    ///
    /// # Errors
    /// 部署模式、集群名称或集群数据库标识符无效时返回配置错误；此时尚未发起网络请求。
    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        Self::with_client(config, job_id, base_client(config)?)
    }

    /// 为事实管线创建独立、可取消的传输，保留已经接受的代次及写入身份。
    /// 原实例继续用于独立的最终报告和代次收尾，不受此传输取消影响。
    ///
    /// # Errors
    /// 连接或集群配置无效时返回配置错误，尚不发送网络请求。
    pub fn with_query_cancellation(&self) -> Result<(Self, ScanQueryCancellation)> {
        let config = &self.connection_config;
        let queries = ScanQueryCancellation::new(config, config.cluster_name()?, &self.job_id)?;
        let (sync_client, async_client) =
            catalogue_clients(self.catalogue_cluster.as_ref(), queries.data_client(config)?);
        let mut scoped = self.clone();
        scoped.sync_client = sync_client;
        scoped.async_client = async_client;
        Ok((scoped, queries))
    }

    pub(crate) fn with_client(config: &ClickHouseConfig, job_id: &str, client: Client) -> Result<Self> {
        let catalogue_cluster = ClusterCatalogue::from_config(config, job_id)?;
        let (sync_client, async_client) = catalogue_clients(catalogue_cluster.as_ref(), client);

        Ok(Self {
            connection_config: Arc::new(config.clone()),
            catalogue_cluster,
            sync_client,
            async_client,
            job_id: job_id.to_string(),
            scan_temp_table_name: None,
            cached_scan_state: AtomicU8::new(255),
            resume_base_has_typed_handles: None,
            generation_lifecycle: Arc::new(CatalogueLifecycle::default()),
            notifications_initialized: Arc::new(OnceCell::new()),
        })
    }
}

impl Clone for ClickHouseDatabase {
    fn clone(&self) -> Self {
        Self {
            connection_config: Arc::clone(&self.connection_config),
            sync_client: self.sync_client.clone(),
            catalogue_cluster: self.catalogue_cluster.clone(),
            async_client: self.async_client.clone(),
            job_id: self.job_id.clone(),
            scan_temp_table_name: self.scan_temp_table_name.clone(),
            cached_scan_state: AtomicU8::new(self.cached_scan_state.load(Ordering::Relaxed)),
            resume_base_has_typed_handles: self.resume_base_has_typed_handles,
            generation_lifecycle: Arc::clone(&self.generation_lifecycle),
            notifications_initialized: Arc::clone(&self.notifications_initialized),
        }
    }
}

fn catalogue_clients(cluster: Option<&ClusterCatalogue>, client: Client) -> (Client, Client) {
    match cluster {
        Some(cluster) => {
            let client = cluster.client(client);
            (client.clone(), client)
        }
        None => (
            client.clone(),
            client
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "1"),
        ),
    }
}

impl ClickHouseDatabase {
    /// 通过 SELECT 1 测试连接
    pub async fn ping(&self) -> Result<()> {
        self.sync_client.query("SELECT 1").fetch_quorum_one::<u8>().await?;

        debug!("ClickHouse connection established successfully");
        Ok(())
    }
}
