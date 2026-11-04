//! 目录绝对贡献的追加日志：未提交版本不替换旧 cut，恢复先同步截断后续版本。
use super::queries::{ScanExecute, check_scan_cancellation};
use crate::clickhouse::replica_read::QuorumRead;
use crate::cluster_scan::{identifier, retry_quorum};
use crate::{DatabaseError, Result};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::yield_now;
use utils::sanitize_job_id;

const COLUMNS: &str = "job_id String, round Int64, version UInt64, revision UInt64, directory String, payload String";
const ORDER: &str = "(job_id,round,directory,version,revision)";
const TABLE: &str = "scan_directory_statistics";
static NEXT_REVISION: AtomicU64 = AtomicU64::new(0);

/// payload 由扫描层编码；数据库仅维护目录、轮次与检查点之间的版本关系。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, clickhouse::Row)]
pub struct ScanDirectoryStatisticsRow {
    pub directory: String,
    pub payload: String,
}

#[derive(Serialize, clickhouse::Row)]
struct Contribution<'a> {
    job_id: &'a str,
    round: i64,
    version: u64,
    revision: u64,
    directory: &'a str,
    payload: &'a str,
}

#[derive(Clone)]
struct ClusterLayout {
    database: String,
    cluster: String,
    keeper_path: String,
}

/// 独立目录贡献句柄；克隆共享准备状态和写入门禁，并保留所属扫描的查询取消标记。
#[derive(Clone)]
pub struct DirectoryStatisticsStore {
    client: Client,
    table: String,
    job: String,
    cluster: Option<ClusterLayout>,
    prepared: Arc<Mutex<bool>>,
    writes: Arc<Mutex<()>>,
}

fn next_revision() -> Result<u64> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let previous = NEXT_REVISION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
            previous.checked_add(1).map(|next| next.max(now))
        })
        .map_err(|_| DatabaseError::ConversionError("Directory statistics revision overflow".into()))?;
    Ok(now.max(previous + 1))
}

impl DirectoryStatisticsStore {
    pub(crate) fn single(client: Client, job: &str) -> Self {
        let job = sanitize_job_id(job);
        Self {
            client,
            table: format!("{TABLE}_{job}"),
            job,
            cluster: None,
            prepared: Arc::new(Mutex::new(false)),
            writes: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn cluster(client: Client, database: &str, cluster: &str, job: &str) -> Result<Self> {
        let mut ledger = Self::single(client, job);
        let database_identifier = identifier(database)?;
        ledger.table = format!("{database_identifier}.{TABLE}");
        ledger.cluster = Some(ClusterLayout {
            database: database_identifier,
            cluster: identifier(cluster)?,
            keeper_path: format!("/clickhouse/terrasync/{database}/{TABLE}_local/{{shard}}"),
        });
        Ok(ledger)
    }

    /// 按当前部署建立追加日志；克隆句柄共享一次准备结果。
    pub async fn prepare(&self) -> Result<()> {
        check_scan_cancellation()?;
        let mut prepared = self.prepared.lock().await;
        if !*prepared {
            self.create_table().await?;
            *prepared = true;
        }
        Ok(())
    }

    async fn create_table(&self) -> Result<()> {
        if let Some(layout) = &self.cluster {
            let local = format!("{}.{TABLE}_local", layout.database);
            self.client
                .query(&format!(
                    "CREATE TABLE IF NOT EXISTS {local} ON CLUSTER {} ({COLUMNS}) \
                     ENGINE = ReplicatedMergeTree(?, '{{replica}}') PARTITION BY job_id ORDER BY {ORDER}",
                    layout.cluster
                ))
                .bind(&layout.keeper_path)
                .execute_scan()
                .await?;
            self.client
                .query(&format!(
                    "CREATE TABLE IF NOT EXISTS {} ON CLUSTER {} AS {local} \
                     ENGINE = Distributed({}, {}, {TABLE}_local, cityHash64(job_id))",
                    self.table, layout.cluster, layout.cluster, layout.database
                ))
                .execute_scan()
                .await
        } else {
            self.client
                .query(&format!(
                    "CREATE TABLE IF NOT EXISTS {} ({COLUMNS}) ENGINE = MergeTree() ORDER BY {ORDER}",
                    self.table
                ))
                .execute_scan()
                .await
        }
    }

    /// 保存一个检查点版本的绝对贡献；批内目录必须唯一，版本由扫描层单调推进。
    pub async fn write(&self, round: i64, version: u64, rows: &[ScanDirectoryStatisticsRow]) -> Result<()> {
        check_scan_cancellation()?;
        if rows.is_empty() {
            return Ok(());
        }
        validate_rows(rows).await?;
        self.prepare().await?;
        let _write = self.writes.lock().await;
        let revision = next_revision()?;
        // 已拒绝的前序 quorum 可安全重试；同一次逻辑写入保持相同 revision 和去重 token。
        let client = self.client.clone().with_option(
            "insert_deduplication_token",
            format!("directory-statistics-{}-{round}-{version}-{revision}", self.job),
        );
        retry_quorum(|| async {
            let mut insert = client.insert::<Contribution<'_>>(&self.table).await?;
            for (index, row) in rows.iter().enumerate() {
                checkpoint(index).await?;
                insert
                    .write(&Contribution {
                        job_id: &self.job,
                        round,
                        version,
                        revision,
                        directory: &row.directory,
                        payload: &row.payload,
                    })
                    .await?;
            }
            check_scan_cancellation()?;
            insert.end().await?;
            Ok(())
        })
        .await
    }

    /// 只取指定目录在 cut 时的最新贡献；不存在的目录不返回，重复输入键只返回一次。
    pub async fn load(&self, round: i64, cut: u64, paths: &[String]) -> Result<Vec<ScanDirectoryStatisticsRow>> {
        check_scan_cancellation()?;
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        self.prepare().await?;
        let mut unique = HashSet::with_capacity(paths.len());
        let mut requested = Vec::with_capacity(paths.len());
        for (index, path) in paths.iter().enumerate() {
            checkpoint(index).await?;
            if unique.insert(path) {
                requested.push(path);
            }
        }
        let mut rows = Vec::with_capacity(requested.len());
        // 服务端参数避免长目录名扩大 SQL 解析长度；分批限制请求和返回值的峰值。
        for paths in requested.chunks(256) {
            check_scan_cancellation()?;
            let batch = self
                .client
                .query(&format!(
                    "SELECT directory,argMax(payload,(version,revision)) AS payload FROM {} \
                 WHERE job_id=? AND round=? AND version<=? AND directory IN {{paths:Array(String)}} \
                 GROUP BY directory ORDER BY directory",
                    self.table
                ))
                .bind(&self.job)
                .bind(round)
                .bind(cut)
                .param("paths", paths)
                .fetch_quorum_all::<ScanDirectoryStatisticsRow>()
                .await?;
            rows.extend(batch);
        }
        Ok(rows)
    }

    /// 恢复写入前同步删除未提交版本；调用方须持有作业锁并已排空旧 writer。
    pub async fn truncate(&self, round: i64, cut: u64) -> Result<()> {
        self.prepare().await?;
        let _write = self.writes.lock().await;
        let (table, on_cluster) = self.mutation_target();
        self.client
            .query(&format!(
                "ALTER TABLE {table}{on_cluster} DELETE WHERE job_id=? AND round=? AND version>?"
            ))
            .bind(&self.job)
            .bind(round)
            .bind(cut)
            .with_option("mutations_sync", "2")
            .execute_scan()
            .await
    }

    pub(crate) async fn clear_job(&self) -> Result<()> {
        let _write = self.writes.lock().await;
        let (table, on_cluster) = self.mutation_target();
        if self.cluster.is_some() {
            // 表尚未建立时不引入新共享表；其他作业的目录历史不受影响。
            let exists: u8 = self
                .client
                .query(&format!("EXISTS TABLE {}", self.table))
                .fetch_quorum_one()
                .await?;
            if exists == 0 {
                return Ok(());
            }
            self.client
                .query(&format!("ALTER TABLE {table}{on_cluster} DROP PARTITION ?"))
                .bind(&self.job)
                .with_option("alter_sync", "2")
                .execute_scan()
                .await?;
        } else {
            self.client
                .query(&format!("DROP TABLE IF EXISTS {table}"))
                .execute_scan()
                .await?;
        }
        *self.prepared.lock().await = false;
        Ok(())
    }

    fn mutation_target(&self) -> (String, String) {
        match &self.cluster {
            Some(layout) => (
                format!("{}.{TABLE}_local", layout.database),
                format!(" ON CLUSTER {}", layout.cluster),
            ),
            None => (self.table.clone(), String::new()),
        }
    }
}

async fn checkpoint(index: usize) -> Result<()> {
    if index.is_multiple_of(1024) {
        check_scan_cancellation()?;
        yield_now().await;
        check_scan_cancellation()?;
    }
    Ok(())
}

async fn validate_rows(rows: &[ScanDirectoryStatisticsRow]) -> Result<()> {
    let mut paths = HashSet::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        checkpoint(index).await?;
        if !paths.insert(&row.directory) {
            return Err(DatabaseError::ConversionError(
                "Directory statistics batch contains duplicate directory keys".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
