//! A catalogue's working generation, durable commit and recovery state.

use std::sync::atomic::Ordering;

use clickhouse::{error::Error as ClickHouseError, query::Query};
use serde_json::Value;
use tracing::debug;

use super::ClickHouseDatabase;
use super::lifecycle::{CopyFactRange, ScanGenerationLifecycle};
use crate::error::{DatabaseError, Result};
use crate::execution::queries::execute_request;
use crate::execution::queries::idempotent_insert_client;
use crate::execution::read::QuorumRead;
use crate::execution::recovery::RequestSafety;
use crate::{get_scan_base_table_name, get_scan_state_table_name, scan::ScanResumeState};

// 每次复制代次留出准备、提交和普通扫描覆盖三个版本，旧请求的到达顺序不能改变胜者。
const STATE_VERSIONS_PER_COPY: u64 = 3;

pub(super) fn copy_state_version(sequence: u64) -> Result<u64> {
    sequence
        .checked_mul(STATE_VERSIONS_PER_COPY)
        .filter(|version| *version <= u64::MAX - (STATE_VERSIONS_PER_COPY - 1))
        .ok_or(DatabaseError::CopyGenerationInvalid {
            field: "state_version_overflow",
        })
}

impl ClickHouseDatabase {
    /// 查询 `scan_state` 表，返回 id=1 的 `scan_state` 值。
    ///
    /// `Ok(None)` 仅表示表存在但没有状态行；查询、连接等数据库错误会原样上抛。
    pub async fn query_scan_state(&self) -> Result<Option<u8>> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let query = format!("SELECT scan_state FROM {table_name} FINAL WHERE id = 1");

        match self.sync_client.query(&query).fetch_quorum_one::<u8>().await {
            Ok(scan_state) => Ok(Some(scan_state)),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// 获取缓存的 `scan_state`，如果缓存为空（255）则查询数据库
    pub(super) async fn get_cached_scan_state(&self) -> Result<u8> {
        let working = self.generation_lifecycle.lock().await.working;
        if let Some(working) = working {
            return Ok(working);
        }
        let cached = self.cached_scan_state.load(Ordering::Relaxed);
        if cached != 255 {
            return Ok(cached);
        }
        let state = self.query_scan_state().await?.unwrap_or(0);
        self.cached_scan_state.store(state, Ordering::Relaxed);
        Ok(state)
    }

    /// 普通扫描覆盖当前复制代次的状态；后续复制使用更高代次写入。
    pub async fn insert_scan_state(&self, scan_state: u8) -> Result<()> {
        let version = self.ordinary_scan_state_version().await?;
        self.insert_scan_state_with_baseline(scan_state, true, version).await
    }

    async fn ordinary_scan_state_version(&self) -> Result<u64> {
        let table = get_scan_state_table_name(&self.job_id);
        let current = self
            .sync_client
            .query(&format!("SELECT max(state_version) FROM {table} WHERE id=1"))
            .fetch_quorum_one::<u64>()
            .await?;
        if current == 0 {
            Ok(0)
        } else {
            Ok(copy_state_version(current / STATE_VERSIONS_PER_COPY)? + (STATE_VERSIONS_PER_COPY - 1))
        }
    }

    pub(crate) async fn persisted_scan_commit_query(&self, state: &ScanResumeState) -> Result<Query> {
        let version = self.ordinary_scan_state_version().await?;
        let table = get_scan_state_table_name(&self.job_id);
        let manifest = self.resume_table();
        let client = idempotent_insert_client(&self.sync_client);
        Ok(client
            .query(&format!(
                "INSERT INTO {table} (id,scan_state,baseline_committed,state_version,scan_epoch) \
                 SELECT 1,?,true,?,? WHERE throwIf((SELECT argMax(tuple(event_time,generation,phase,version),version) \
                 FROM {manifest} WHERE id=1) != tuple(?,?,?,?), 'Scan manifest changed before commit')=0"
            ))
            .bind(state.generation)
            .bind(version)
            .bind(state.event_time)
            .bind(state.event_time)
            .bind(state.generation)
            .bind(state.phase)
            .bind(state.version))
    }

    pub(crate) async fn persisted_scan_state(&self) -> Result<(u8, i64)> {
        let table = get_scan_state_table_name(&self.job_id);
        self.sync_client
            .query(&format!("SELECT scan_state,scan_epoch FROM {table} FINAL WHERE id=1"))
            .fetch_quorum_one()
            .await
    }

    pub(super) async fn insert_scan_state_with_baseline(
        &self, scan_state: u8, baseline_committed: bool, version: u64,
    ) -> Result<()> {
        let _write = self.catalogue_write_guard().await;
        let table_name = get_scan_state_table_name(&self.job_id);
        let insert_sql =
            format!("INSERT INTO {table_name} (id, scan_state, baseline_committed, state_version) VALUES (?, ?, ?, ?)");

        debug!("Inserting scan state: id=1, scan_state={}", scan_state);

        let client = idempotent_insert_client(&self.catalogue_write_client(&self.sync_client));
        execute_request(RequestSafety::IdempotentWrite, || {
            let query = client
                .query(&insert_sql)
                .bind(1u8)
                .bind(scan_state)
                .bind(baseline_committed)
                .bind(version);
            async move { query.execute().await.map_err(DatabaseError::from) }
        })
        .await?;

        debug!("Inserted scan state record: id=1, scan_state={}", scan_state);
        Ok(())
    }

    pub(super) async fn publish_copy_scan_state(
        &self, scan_state: u8, baseline_committed: bool, execution_id: &str, facts: CopyFactRange,
    ) -> Result<()> {
        let table = get_scan_state_table_name(&self.job_id);
        let version = copy_state_version(facts.last)? + u64::from(baseline_committed);
        let baseline = facts.first;
        self.execute(&format!("INSERT INTO {table} (id,scan_state,baseline_committed,copy_execution_id,state_version,copy_baseline_sequence) VALUES (1,{scan_state},{baseline_committed},?,{version},{baseline})"),
            &[Value::from(execution_id)]).await
    }

    /// 将上次未提交的 working 快照重新标记为 committed，作为同 generation 重试的基线。
    ///
    /// `ReplacingMergeTree` 可能已合并掉更早的 committed 物理行，不能通过删除 working 行恢复；
    /// 同步 mutation 原地回标可保留失败轮已安全写入的条目，并让重试正确识别之后的缺失项。
    pub(super) async fn rebase_uncommitted_generation(&self, committed: u8, working: u8) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        if !self.table_exists(&base_table_name).await? {
            return Ok(());
        }
        self.execute(
            &format!(
                "ALTER TABLE {} UPDATE current_state = ? \
                 WHERE current_state = ? SETTINGS mutations_sync = 2",
                self.catalogue_storage_table(&base_table_name)?
            ),
            &[Value::from(committed), Value::from(working)],
        )
        .await?;
        Ok(())
    }

    pub async fn begin_scan_generation(&self) -> Result<u8> {
        self.abandon_scan_resume().await?;
        let mut lifecycle = self.generation_lifecycle.lock().await;
        *lifecycle = ScanGenerationLifecycle::default();
        let committed = self.query_scan_state().await?.ok_or_else(|| {
            DatabaseError::TransactionError("Cannot begin scan generation before initialization".to_string())
        })?;
        if committed > 1 {
            return Err(DatabaseError::ConversionError(format!(
                "Invalid committed scan generation {committed}; expected 0 or 1"
            )));
        }
        let working = 1 - committed;
        self.rebase_uncommitted_generation(committed, working).await?;
        lifecycle.working = Some(working);
        Ok(working)
    }

    pub async fn commit_scan_generation(&self) -> Result<()> {
        let mut lifecycle = self.generation_lifecycle.lock().await;
        let working = lifecycle
            .working
            .ok_or_else(|| DatabaseError::TransactionError("Cannot commit scan generation before begin".to_string()))?;
        if lifecycle.committed {
            return Ok(());
        }
        if let Some(execution_id) = &lifecycle.copy_execution_id {
            let facts = lifecycle
                .copy_fact_range
                .ok_or(DatabaseError::CopyExecutionUnavailable)?;
            self.publish_copy_scan_state(working, true, execution_id, facts).await?;
        } else {
            self.insert_scan_state(working).await?;
        }
        self.cached_scan_state.store(working, Ordering::Relaxed);
        lifecycle.committed = true;
        lifecycle.copy_recovery = false;
        Ok(())
    }
}
