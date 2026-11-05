//! ClickHouse 目录的连接构造及已提交基线保护。
use super::{ClickHouseDatabase, ClusterCatalogue, shared_generation_lifecycle};
use crate::{config::ClickHouseConfig, error::Result, execution::base_client, scan::ScanStore, traits::Database};
use clickhouse::Client;
use std::sync::atomic::{AtomicBool, AtomicU8};

impl ClickHouseDatabase {
    /// 连接唯一的 ClickHouse 目录实现，保留原始有类型的连接失败。
    pub async fn connect(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        let catalogue = Self::new(config, job_id)?;
        ScanStore::ensure_metadata_available(config, job_id).await?;
        catalogue.ping().await?;
        Ok(catalogue)
    }

    /// 创建新的`ClickHouse`数据库实例
    ///
    /// # Errors
    /// 部署模式、集群名称或集群数据库标识符无效时返回配置错误；此时尚未发起网络请求。
    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        Self::with_client(config, job_id, base_client(config)?)
    }

    pub(crate) fn with_client(config: &ClickHouseConfig, job_id: &str, client: Client) -> Result<Self> {
        let catalogue_cluster = ClusterCatalogue::from_config(config, job_id)?;
        let (sync_client, async_client) = if let Some(cluster) = &catalogue_cluster {
            let client = cluster.client(client);
            (client.clone(), client)
        } else {
            (
                client.clone(),
                client
                    .with_option("async_insert", "1")
                    .with_option("wait_for_async_insert", "1"),
            )
        };

        Ok(Self {
            catalogue_cluster,
            sync_client,
            async_client,
            job_id: job_id.to_string(),
            scan_temp_table_name: None,
            cached_scan_state: AtomicU8::new(255),
            resume_base_has_typed_handles: None,
            native_dedup_ready: AtomicBool::new(false),
            generation_lifecycle: shared_generation_lifecycle(config, job_id),
        })
    }
}
