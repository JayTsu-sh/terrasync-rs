//! 通用目录的部署差异：业务查询复用同一投影，集群 DDL 和物理表名只在这里出现。
use super::{ClickHouseDatabase, ExcludeRecord};
use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
use crate::execution::queries::{check_scan_cancellation, scan_retry_delay};
use crate::schema::identifier;
use crate::traits::StorageEntryRecord;
use clickhouse::{Client, error::Error as ClickHouseError, query::Query};
use std::time::{Duration, Instant};
use storage_adapter::ObservedEntry;
use tokio::sync::MutexGuard;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct ClusterCatalogue {
    cluster: String,
    database: String,
    job_id: String,
}

#[derive(Clone, Copy)]
pub(super) enum CatalogueEngine {
    Replacing,
    MergeTree,
}

impl CatalogueEngine {
    fn name(self, replicated: bool) -> &'static str {
        match (self, replicated) {
            (Self::Replacing, false) => "ReplacingMergeTree",
            (Self::Replacing, true) => "ReplicatedReplacingMergeTree",
            (Self::MergeTree, false) => "MergeTree",
            (Self::MergeTree, true) => "ReplicatedMergeTree",
        }
    }
}

impl ClusterCatalogue {
    pub(super) fn from_config(config: &ClickHouseConfig, job_id: &str) -> Result<Option<Self>> {
        let cluster = config.cluster_name()?;
        cluster
            .map(|cluster| {
                identifier(&config.database)?;
                Ok(Self {
                    cluster: cluster.into(),
                    database: config.database.clone(),
                    job_id: job_id.into(),
                })
            })
            .transpose()
    }

    pub(super) fn client(&self, client: Client) -> Client {
        client
            .with_option("insert_distributed_sync", "1")
            .with_option("async_insert", "0")
            .with_option("insert_quorum", "auto")
            .with_option("insert_quorum_parallel", "0")
            .with_option("select_sequential_consistency", "1")
            .with_option("prefer_localhost_replica", "0")
            .with_option("load_balancing", "in_order")
            .with_option("skip_unavailable_shards", "0")
            .with_option("distributed_product_mode", "global")
            .with_option("distributed_ddl_task_timeout", "60")
    }

    fn local(table: &str) -> Result<String> {
        identifier(&format!("__catalogue_{table}"))
    }

    fn on_cluster(&self) -> Result<String> {
        Ok(format!("ON CLUSTER {}", identifier(&self.cluster)?))
    }

    fn create_statements(
        &self, table: &str, columns: &str, order: &str, engine: CatalogueEngine,
    ) -> Result<[String; 2]> {
        let local = Self::local(table)?;
        let public = identifier(table)?;
        let on_cluster = self.on_cluster()?;
        // 表被 EXCHANGE 后仍保留自己的 Keeper 路径；每次创建必须分配全新路径。
        let path = format!(
            "/terrasync/catalogue/{}/{}/{{shard}}",
            self.database,
            Uuid::new_v4().simple()
        );
        let engine = engine.name(true);
        let local_sql = format!(
            "CREATE TABLE IF NOT EXISTS {local} {on_cluster} ({columns}) ENGINE = {engine}('{path}', '{{replica}}') \
             ORDER BY {order} SETTINGS allow_nullable_key = 1"
        );
        // 同一 job 的所有目录表落在同一 shard，跨表比较不会拆散相同路径或硬链接。
        let public_sql = format!(
            "CREATE TABLE IF NOT EXISTS {public} {on_cluster} AS {local} \
             ENGINE = Distributed(?, ?, ?, cityHash64(?))"
        );
        Ok([local_sql, public_sql])
    }
}

impl ClickHouseDatabase {
    pub(super) fn catalogue_write_client(&self, client: &Client) -> Client {
        if self.catalogue_cluster.is_some() {
            // 0/1 代次会复用相同 payload，但新一轮确认不能被历史 block 去重吞掉。
            client
                .clone()
                .with_option("insert_deduplication_token", Uuid::new_v4().to_string())
        } else {
            client.clone()
        }
    }
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
            let mut cursor = request.fetch::<StorageEntryRecord>()?;
            loop {
                match cursor.next().await {
                    Ok(Some(record)) => {
                        tx.send(record.to_observation()?).await.map_err(|error| {
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

    pub(super) async fn catalogue_write_guard(&self) -> Option<MutexGuard<'_, ()>> {
        if self.catalogue_cluster.is_some() {
            Some(self.generation_lifecycle.writes.lock().await)
        } else {
            None
        }
    }

    pub(super) async fn add_catalogue_column(&self, table: &str, definition: &str) -> Result<()> {
        self.execute(
            &format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {definition}",
                self.catalogue_storage_table(table)?
            ),
            &[],
        )
        .await?;
        if let Some(cluster) = &self.catalogue_cluster {
            self.execute(
                &format!(
                    "ALTER TABLE {} {} ADD COLUMN IF NOT EXISTS {definition}",
                    identifier(table)?,
                    cluster.on_cluster()?
                ),
                &[],
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn create_catalogue_exclusions(&self, table: &str) -> Result<()> {
        if self.catalogue_cluster.is_none() {
            self.execute(
                &format!("CREATE TABLE {table} (relative_path String, version_id String) ENGINE = Memory"),
                &[],
            )
            .await?;
            return Ok(());
        }
        self.create_catalogue_table(
            table,
            "relative_path String, version_id String",
            "(relative_path, version_id)",
            CatalogueEngine::MergeTree,
        )
        .await
    }

    pub(super) async fn write_catalogue_exclusions(&self, table: &str, paths: &[(String, String)]) -> Result<()> {
        let _write = self.catalogue_write_guard().await;
        let client = self.catalogue_write_client(&self.sync_client);
        let mut insert = client.insert::<ExcludeRecord>(table).await?;
        for (path, version) in paths {
            insert
                .write(&ExcludeRecord {
                    relative_path: path.clone(),
                    version_id: version.clone(),
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub(super) async fn create_catalogue_table(
        &self, table: &str, columns: &str, order: &str, engine: CatalogueEngine,
    ) -> Result<()> {
        let Some(cluster) = &self.catalogue_cluster else {
            self.execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table} ({columns}) ENGINE = {}() ORDER BY {order} SETTINGS allow_nullable_key = 1",
                engine.name(false)
            ), &[]).await?;
            return Ok(());
        };
        self.validate_catalogue_frontend(table).await?;
        let [local, public] = cluster.create_statements(table, columns, order, engine)?;
        self.execute(&local, &[]).await?;
        self.execute(
            &public,
            &[
                cluster.cluster.clone().into(),
                cluster.database.clone().into(),
                format!("__catalogue_{table}").into(),
                cluster.job_id.clone().into(),
            ],
        )
        .await?;
        Ok(())
    }

    async fn validate_catalogue_frontend(&self, table: &str) -> Result<()> {
        let Some(cluster) = &self.catalogue_cluster else {
            return Ok(());
        };
        let engines = self
            .sync_client
            .query("SELECT engine, engine_full FROM clusterAllReplicas(?, system.tables) WHERE database=? AND name=?")
            .bind(&cluster.cluster)
            .bind(&cluster.database)
            .bind(table)
            .fetch_all::<(String, String)>()
            .await?;
        let expected = format!(
            "Distributed('{}','{}','__catalogue_{table}',",
            cluster.cluster, cluster.database
        );
        if engines.iter().any(|(engine, definition)| {
            engine != "Distributed" || !definition.replace([' ', '`'], "").starts_with(&expected)
        }) {
            return Err(DatabaseError::ConfigError(format!(
                "ClickHouse cluster catalogue {table} already exists with a different layout or route; use a separate database or migrate it before enabling cluster mode"
            )));
        }
        Ok(())
    }

    pub(super) fn catalogue_storage_table(&self, table: &str) -> Result<String> {
        match &self.catalogue_cluster {
            Some(cluster) => Ok(format!("{} {}", ClusterCatalogue::local(table)?, cluster.on_cluster()?)),
            None => identifier(table),
        }
    }

    pub(super) fn catalogue_sorting_table(&self, table: &str) -> String {
        if self.catalogue_cluster.is_some() {
            format!("__catalogue_{table}")
        } else {
            table.into()
        }
    }

    pub(super) fn catalogue_query_table(&self, table: &str) -> Result<String> {
        match &self.catalogue_cluster {
            Some(cluster) => Ok(format!("{}.{}", identifier(&cluster.database)?, identifier(table)?)),
            None => Ok(table.into()),
        }
    }

    pub(super) fn catalogue_delete_statement(&self, table: &str, placeholders: &str) -> Result<String> {
        match &self.catalogue_cluster {
            Some(_) => Ok(format!(
                "ALTER TABLE {} DELETE WHERE relative_path IN ({placeholders}) SETTINGS mutations_sync = 2",
                self.catalogue_storage_table(table)?
            )),
            None => Ok(format!(
                "DELETE FROM {} WHERE relative_path IN ({placeholders})",
                identifier(table)?
            )),
        }
    }

    pub(super) async fn drop_catalogue_table(&self, table: &str) -> Result<()> {
        let public = identifier(table)?;
        if let Some(cluster) = &self.catalogue_cluster {
            self.execute(
                &format!("DROP TABLE IF EXISTS {public} {} SYNC", cluster.on_cluster()?),
                &[],
            )
            .await?;
            let local = ClusterCatalogue::local(table)?;
            self.execute(
                &format!("DROP TABLE IF EXISTS {local} {} SYNC", cluster.on_cluster()?),
                &[],
            )
            .await?;
        } else {
            self.execute(&format!("DROP TABLE IF EXISTS {public}"), &[]).await?;
        }
        Ok(())
    }
}
