//! A catalogue's working generation, durable commit and recovery state.

use std::sync::atomic::Ordering;

use clickhouse::error::Error as ClickHouseError;
use serde_json::Value;
use tracing::debug;

use super::ClickHouseDatabase;
use super::lifecycle::ScanGenerationLifecycle;
use crate::error::{DatabaseError, Result};
use crate::execution::queries::execute_request;
use crate::execution::queries::idempotent_insert_client;
use crate::execution::read::QuorumRead;
use crate::execution::recovery::RequestSafety;
use crate::{get_scan_base_table_name, get_scan_state_table_name};

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

    /// 插入或更新扫描状态（id=1，利用 `ReplacingMergeTree` 自动去重）
    pub async fn insert_scan_state(&self, scan_state: u8) -> Result<()> {
        self.insert_scan_state_with_baseline(scan_state, true).await
    }

    pub(super) async fn insert_scan_state_with_baseline(&self, scan_state: u8, baseline_committed: bool) -> Result<()> {
        let _write = self.catalogue_write_guard().await;
        let table_name = get_scan_state_table_name(&self.job_id);
        let insert_sql = format!("INSERT INTO {table_name} (id, scan_state, baseline_committed) VALUES (?, ?, ?)");

        debug!("Inserting scan state: id=1, scan_state={}", scan_state);

        let client = idempotent_insert_client(&self.catalogue_write_client(&self.sync_client));
        execute_request(RequestSafety::IdempotentWrite, || {
            let query = client
                .query(&insert_sql)
                .bind(1u8)
                .bind(scan_state)
                .bind(baseline_committed);
            async move { query.execute().await.map_err(DatabaseError::from) }
        })
        .await?;

        debug!("Inserted scan state record: id=1, scan_state={}", scan_state);
        Ok(())
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
        self.insert_scan_state(working).await?;
        self.cached_scan_state.store(working, Ordering::Relaxed);
        lifecycle.committed = true;
        lifecycle.copy_recovery = false;
        Ok(())
    }
}
