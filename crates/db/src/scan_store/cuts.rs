//! 扫描检查点的不可变元数据快照。先确认 base/events，最后提交引用二者的 manifest。
//! 所有暂存均位于 ClickHouse；恢复两表可重试，恢复结束前不得开始新的扫描写入。
use super::queries::{ScanExecute, check_scan_cancellation, job_query_prefix, scan_retry_delay};
use crate::clickhouse::{replica_read::QuorumRead, scan_resume::FINALIZING};
use crate::{ClickHouseConfig, DatabaseError, Result, clickhouse::build_base_client, cluster_scan::identifier};
use clickhouse::{
    Client,
    error::Error as ClickHouseError,
    sql::{Bind, Identifier},
};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use utils::sanitize_job_id;
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct ScanCuts {
    client: Client,
    database: String,
    job: String,
    prefix: String,
    cluster: Option<String>,
    comment: String,
}

impl ScanCuts {
    pub(super) async fn ensure_metadata_available(config: &ClickHouseConfig, job: &str) -> Result<()> {
        let cuts = Self::new(config, job, "")?;
        let table = if cuts.cluster.is_some() {
            "scan_resume".into()
        } else {
            format!("scan_resume_{}", cuts.job)
        };
        if !cuts.table_exists(&table).await? {
            return Ok(());
        }
        let scope = if cuts.cluster.is_some() { "job_id=?" } else { "id=1" };
        let client = cuts
            .client
            .clone()
            .with_option("select_sequential_consistency", "1")
            .with_option("insert_quorum_parallel", "0");
        let query = client.query(&format!(
            "SELECT phase FROM {} FINAL WHERE {scope} ORDER BY version DESC LIMIT 1",
            cuts.qualified(&table)?
        ));
        let query = if cuts.cluster.is_some() {
            query.bind(&cuts.job)
        } else {
            query
        };
        match query.fetch_quorum_one::<u8>().await {
            Ok(phase) if phase <= FINALIZING => Err(DatabaseError::TransactionError(
                "This database job has an unfinished scan; resume or reset it before other metadata operations".into(),
            )),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) | Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }
    pub(super) fn new(config: &ClickHouseConfig, job: &str, comment: &str) -> Result<Self> {
        let job = sanitize_job_id(job);
        let prefix = format!("scan_cut_{}_", &blake3::hash(job.as_bytes()).to_hex()[..16]);
        Ok(Self {
            client: build_base_client(config)
                .with_option("log_comment", comment)
                .with_option("distributed_ddl_task_timeout", "60")
                .with_option("distributed_ddl_output_mode", "throw")
                .with_option("skip_unavailable_shards", "0"),
            database: config.database.clone(),
            cluster: config
                .cluster_name()?
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
            job,
            prefix,
            comment: comment.to_owned(),
        })
    }

    fn name(&self, token: &str, events: bool) -> Result<String> {
        if token.len() != 32 || !token.bytes().all(|value| value.is_ascii_hexdigit()) {
            return Err(DatabaseError::TransactionError(
                "Missing or invalid scan metadata snapshot".into(),
            ));
        }
        Ok(format!(
            "{}{token}_{}",
            self.prefix,
            if events { "events" } else { "base" }
        ))
    }

    fn qualified(&self, table: &str) -> Result<String> {
        let mut database = String::new();
        Identifier(&self.database)
            .write(&mut database)
            .map_err(DatabaseError::ConfigError)?;
        // 这是 Query 的模板片段；库名中的问号须保留为字面量，而非 bind 占位符。
        Ok(format!("{}.{}", database.replace('?', "??"), identifier(table)?))
    }

    fn source(&self, events: bool) -> String {
        let table = if events { "incremental" } else { "base" };
        match self.cluster {
            Some(_) => table.into(),
            None => format!("{table}_{}", self.job),
        }
    }

    fn on_cluster(&self) -> Result<String> {
        self.cluster.as_deref().map_or_else(
            || Ok(String::new()),
            |name| Ok(format!(" ON CLUSTER {}", identifier(name)?)),
        )
    }

    async fn synchronize(&self, table: &str) -> Result<()> {
        if self.cluster.is_some() {
            self.client
                .query(&format!(
                    "SYSTEM SYNC REPLICA{} {} LIGHTWEIGHT",
                    self.on_cluster()?,
                    self.qualified(table)?
                ))
                .execute_scan()
                .await?;
        }
        Ok(())
    }

    /// 调用方已排空所有 writer；两个快照确认后才允许保存游标和统计。
    pub(super) async fn capture(&self) -> Result<String> {
        self.await_prior_operations().await?;
        let token = Uuid::new_v4().simple().to_string();
        for events in [false, true] {
            let cut = self.name(&token, events)?;
            let source = self.source(events);
            self.clone_table(&source, &cut, events).await?;
        }
        Ok(token)
    }

    async fn clone_table(&self, source: &str, cut: &str, events: bool) -> Result<()> {
        if self.cluster.is_none() {
            let (source, cut) = (self.qualified(source)?, self.qualified(cut)?);
            self.client
                .query(&format!("CREATE TABLE {cut} AS {source}"))
                .execute_scan()
                .await?;
            self.client
                .query(&format!("ALTER TABLE {cut} ATTACH PARTITION ALL FROM {source}"))
                .execute_scan()
                .await?;
            return Ok(());
        }
        let local = format!("{cut}_local");
        let source_local = format!("{source}_local");
        self.synchronize(&source_local).await?;
        let (target, original) = (self.qualified(&local)?, self.qualified(&source_local)?);
        let path = format!("/clickhouse/terrasync/{}/{local}/{{shard}}", self.database);
        let tombstone = if events { "" } else { ",is_deleted" };
        self.client.query(&format!("CREATE TABLE {target}{} AS {original} ENGINE=ReplicatedReplacingMergeTree('{path}','{{replica}}',ver{tombstone})", self.on_cluster()?)).execute_scan().await?;
        self.client
            .query(&format!(
                "CREATE TABLE {}{} AS {target} ENGINE=Distributed({},{},{},cityHash64(job_id))",
                self.qualified(cut)?,
                self.on_cluster()?,
                identifier(self.cluster.as_deref().unwrap_or_default())?,
                identifier(&self.database)?,
                identifier(&local)?
            ))
            .execute_scan()
            .await?;
        // 共享表只克隆当前 job 的分区；绝不能克隆其他任务或交换整张共享表。
        self.client
            .query(&format!(
                "ALTER TABLE {target}{} ATTACH PARTITION ? FROM {original}",
                self.on_cluster()?
            ))
            .bind(&self.job)
            .execute_scan()
            .await?;
        self.synchronize(&local).await
    }

    async fn table_exists(&self, table: &str) -> Result<bool> {
        let count: u64 = self
            .client
            .query("SELECT count() FROM system.tables WHERE database=? AND name=?")
            .bind(&self.database)
            .bind(table)
            .fetch_one()
            .await?;
        Ok(count == 1)
    }

    /// 先验证两个快照均存在再修改 working；任一阶段失败后再次调用可恢复到同一事实。
    pub(super) async fn restore(&self, token: &str) -> Result<()> {
        self.await_prior_operations().await?;
        let pair = [self.name(token, false)?, self.name(token, true)?];
        for table in &pair {
            if !self.table_exists(table).await? {
                return Err(DatabaseError::TransactionError(
                    "Saved scan metadata snapshot is missing; refusing partial recovery".into(),
                ));
            }
            if self.cluster.is_some() {
                self.synchronize(&format!("{table}_local")).await?;
            }
        }
        for (events, table) in [false, true].into_iter().zip(pair) {
            self.restore_table(&table, &self.source(events)).await?;
        }
        Ok(())
    }

    async fn restore_table(&self, cut: &str, source: &str) -> Result<()> {
        let suffix = if self.cluster.is_some() { "_local" } else { "" };
        let source_local = format!("{source}{suffix}");
        let source = self.qualified(&source_local)?;
        let cut = self.qualified(&format!("{cut}{suffix}"))?;
        // 不依赖新版本的空分区 REPLACE 选项；恢复期间 working 仅供本执行访问。
        // 中途失败再次从同一不可变 cut 重建，空快照也可以准确清除未确认尾部。
        let partition = if self.cluster.is_some() { "?" } else { "tuple()" };
        let query = self.client.query(&format!(
            "ALTER TABLE {source}{} DROP PARTITION {partition}",
            self.on_cluster()?
        ));
        let query = if self.cluster.is_some() {
            query.bind(&self.job)
        } else {
            query
        };
        query.execute_scan().await?;
        self.await_prior_operations().await?;
        let partition = if self.cluster.is_some() { "?" } else { "ALL" };
        let query = self.client.query(&format!(
            "ALTER TABLE {source}{} ATTACH PARTITION {partition} FROM {cut}",
            self.on_cluster()?
        ));
        let query = if self.cluster.is_some() {
            query.bind(&self.job)
        } else {
            query
        };
        query.execute_scan().await?;
        self.await_prior_operations().await?;
        self.synchronize(&source_local).await
    }

    /// HTTP 查询退出后，DDL/mutation 仍可能在服务端执行。跨执行按稳定 job 标签排空，
    /// 旧 reset 及最终化 DELETE 也必须先结束，不能只等待分区恢复语句。
    pub(super) async fn await_prior_operations(&self) -> Result<()> {
        let start = Instant::now();
        loop {
            check_scan_cancellation()?;
            let remaining = Duration::from_secs(30).saturating_sub(start.elapsed());
            let checks = async {
                Ok::<_, DatabaseError>(
                    self.pending_submitters()
                        .await?
                        .saturating_add(self.pending_ddl().await?)
                        .saturating_add(self.pending_mutations().await?),
                )
            };
            let pending = timeout(remaining.min(Duration::from_secs(5)), checks)
                .await
                .map_err(|_| {
                    DatabaseError::TransactionError("Timed out checking previous scan database operations".into())
                })??;
            if pending == 0 {
                return Ok(());
            }
            if start.elapsed() >= Duration::from_secs(30) {
                return Err(DatabaseError::TransactionError(
                    "Previous scan database operations are still pending; refusing new writes".into(),
                ));
            }
            scan_retry_delay(Duration::from_millis(50)).await?;
        }
    }

    async fn pending_submitters(&self) -> Result<u64> {
        let table = if let Some(cluster) = &self.cluster {
            format!("clusterAllReplicas({}, system.processes)", identifier(cluster)?)
        } else {
            "system.processes".into()
        };
        Ok(self
            .client
            .query(&format!(
                "SELECT count() FROM {table} WHERE startsWith(Settings['log_comment'],?) AND Settings['log_comment']!=?"
            ))
            .bind(job_query_prefix(&self.database, &self.job))
            .bind(&self.comment)
            .fetch_one()
            .await?)
    }

    async fn pending_ddl(&self) -> Result<u64> {
        let Some(cluster) = &self.cluster else {
            return Ok(0);
        };
        // 旧执行按完整的限定库名和 job 字面量匹配，避免相似库名互相阻塞。
        // 新执行用稳定的数据库/job 标签覆盖所有类型的 DDL。
        let count = self.client.query("SELECT count() FROM system.distributed_ddl_queue WHERE cluster=? AND position(query,?)>0 AND (startsWith(settings['log_comment'],?) OR position(query,?)>0) AND (status IS NULL OR status NOT IN ('Finished','Removing'))")
            .bind(cluster).bind(format!("{}.", identifier(&self.database)?)).bind(job_query_prefix(&self.database, &self.job))
            .bind(format!("'{}'", self.job)).fetch_one().await?;
        Ok(count)
    }

    async fn pending_mutations(&self) -> Result<u64> {
        let table = if let Some(cluster) = &self.cluster {
            format!("clusterAllReplicas({}, system.mutations)", identifier(cluster)?)
        } else {
            "system.mutations".into()
        };
        let suffix = if self.cluster.is_some() { "_local" } else { "" };
        let scope = if self.cluster.is_some() {
            " AND position(command,?)>0"
        } else {
            ""
        };
        let query = self
            .client
            .query(&format!(
                "SELECT count() FROM {table} WHERE database=? AND table IN (?,?) AND NOT is_done{scope}"
            ))
            .bind(&self.database)
            .bind(format!("{}{suffix}", self.source(false)))
            .bind(format!("{}{suffix}", self.source(true)));
        let query = if self.cluster.is_some() {
            query.bind(format!("'{}'", self.job))
        } else {
            query
        };
        Ok(query.fetch_one().await?)
    }

    /// manifest 确认之后清理未引用对象；清理失败不允许撤销已经确认的 checkpoint。
    pub(super) async fn cleanup(&self, keep: &[&str]) -> Result<()> {
        check_scan_cancellation()?;
        let tables = self
            .client
            .query("SELECT name FROM system.tables WHERE database=? AND startsWith(name,?)")
            .bind(&self.database)
            .bind(&self.prefix)
            .fetch_all::<String>()
            .await?;
        for table in tables {
            if keep
                .iter()
                .any(|token| !token.is_empty() && table.starts_with(&format!("{}{token}_", self.prefix)))
            {
                continue;
            }
            self.client
                .query(&format!(
                    "DROP TABLE IF EXISTS {}{} SYNC",
                    self.qualified(&table)?,
                    self.on_cluster()?
                ))
                .execute_scan()
                .await?;
        }
        Ok(())
    }
}
