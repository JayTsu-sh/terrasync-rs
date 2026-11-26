//! 先记录已接受策略，再准备本执行的基线；上一删源轮次一律重新全量。
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use task_model::migration::{
    CopyGeneration, CopyGenerationPhase, CopyGenerationPlan, CopyRequestDescriptor, MigrationMode,
};

use super::ClickHouseDatabase;
use super::generation::copy_state_version;
use super::lifecycle::CopyFactRange;
use crate::error::{DatabaseError, Result};
use crate::execution::read::QuorumRead;
use crate::schema::columns::CATALOGUE_COLUMNS as FILE_SCAN_COLUMNS_LIST;
use crate::schema::records::StorageEntryRecord;
use crate::{DeletionStatus, get_scan_base_table_name, get_scan_state_table_name};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyGenerationStatus {
    Fresh,
    Recovering,
}

#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
struct CopyState {
    scan_state: u8,
    copy_execution_id: String,
    state_version: u64,
    copy_baseline_sequence: u64,
}

#[cfg(test)]
#[path = "copy_generation_real_tests.rs"]
mod real_tests;
#[cfg(test)]
#[path = "copy_generation_tests.rs"]
mod tests;

impl ClickHouseDatabase {
    /// 同一执行域只接受一次代次；调用方持有 job 独占权，不能把它当作跨主机锁。
    pub async fn begin_copy_generation(&self, request: &CopyRequestDescriptor) -> Result<CopyGenerationStatus> {
        request.validate()?;
        let mut lifecycle = self.generation_lifecycle.lock().await;
        if lifecycle.copy_generation.is_some() {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "execution_scope_reused",
            });
        }
        self.create_copy_runs_table().await?;
        let previous = self.latest_copy_generation().await?;
        let baseline = if let Some(previous) = &previous {
            self.bound_copy_baseline(previous).await?
        } else {
            if self.table_exists(&get_scan_state_table_name(&self.job_id)).await?
                && !self.copy_generation_state().await?.copy_execution_id.is_empty()
            {
                return Err(DatabaseError::CopyGenerationInvalid {
                    field: "binding_unavailable",
                });
            }
            None
        };
        let plan = CopyGenerationPlan::decide(request, previous.as_ref(), baseline.is_some())?;
        if previous
            .as_ref()
            .is_some_and(|prior| prior.request.execution_id == request.execution_id)
        {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "execution_id_reused",
            });
        }
        let sequence = previous
            .as_ref()
            .map_or(0, |prior| prior.sequence)
            .checked_add(1)
            .ok_or(DatabaseError::CopyGenerationInvalid {
                field: "sequence_overflow",
            })?;
        copy_state_version(sequence)?;
        let recovering = plan.mode == MigrationMode::Incremental
            && previous
                .as_ref()
                .is_some_and(|prior| prior.phase != CopyGenerationPhase::Completed);
        let first = if plan.mode == MigrationMode::Full {
            sequence
        } else {
            baseline.ok_or(DatabaseError::CopyExecutionUnavailable)?
        };
        let facts = CopyFactRange { first, last: sequence };
        lifecycle.copy_generation = Some(CopyGeneration {
            root_snapshot: None,
            sequence,
            revision: 1,
            request: request.clone(),
            plan,
            phase: CopyGenerationPhase::Accepted,
            baseline_prepared: false,
            target_reconciliation_completed: Some(false),
        });
        // 接受记录在清基线、回标及启动扫描之前确认，准备失败也不会误用更早轮次。
        let generation = lifecycle
            .copy_generation
            .as_mut()
            .ok_or(DatabaseError::CopyExecutionUnavailable)?;
        self.save_copy_generation(generation).await?;
        self.initialize().await?;
        let saved = self.copy_generation_state().await?;
        let committed = saved.scan_state;
        let working = 1 - committed;
        let base = self.catalogue_storage_table(&get_scan_base_table_name(&self.job_id))?;
        if generation.plan.mode == MigrationMode::Full {
            self.execute(
                &format!("ALTER TABLE {base} DELETE WHERE copy_sequence < ? SETTINGS mutations_sync=2"),
                &[Value::from(sequence)],
            )
            .await?;
        } else {
            // 未确认行作为墓碑保留；只删这些物理行会复活同键更早的已确认快照。
            // 比较和收尾均排除它们，只有新的复制 ACK 才能恢复该目标槽的就绪证明。
            // 范围外事实跨多轮保留；每轮统一回到 C，观察之后才写 W。
            let facts = CopyFactRange {
                first,
                last: sequence - 1,
            }
            .predicate();
            self.execute(&format!("ALTER TABLE {base} UPDATE current_state={committed} WHERE current_state!={committed} AND {facts} SETTINGS mutations_sync=2"), &[]).await?;
        }
        self.prepare_copy_scan_state(committed, &request.execution_id, facts)
            .await?;
        generation.baseline_prepared = true;
        generation.phase = CopyGenerationPhase::Prepared;
        generation.revision = 2;
        self.save_copy_generation(generation).await?;
        lifecycle.working = Some(working);
        lifecycle.copy_recovery = false;
        lifecycle.copy_generation_recovering = recovering;
        lifecycle.copy_execution_id = Some(request.execution_id.clone());
        lifecycle.copy_fact_range = Some(facts);
        self.cached_scan_state.store(working, Ordering::Relaxed);
        Ok(if recovering {
            CopyGenerationStatus::Recovering
        } else {
            CopyGenerationStatus::Fresh
        })
    }

    pub async fn copy_baseline_available(&self, previous: &CopyGeneration) -> Result<bool> {
        Ok(self.bound_copy_baseline(previous).await?.is_some())
    }

    /// 全量起点随准备与提交状态写入，后继只继承上一已绑定的起点。
    async fn bound_copy_baseline(&self, previous: &CopyGeneration) -> Result<Option<u64>> {
        if !previous.baseline_prepared
            || !self.table_exists(&get_scan_base_table_name(&self.job_id)).await?
            || !self.table_exists(&get_scan_state_table_name(&self.job_id)).await?
        {
            return Ok(None);
        }
        let state = self.copy_generation_state().await?;
        if state.copy_execution_id != previous.request.execution_id {
            return Ok(None);
        }
        let baseline = state.copy_baseline_sequence;
        let prepared_version = copy_state_version(previous.sequence)?;
        if !(prepared_version..=prepared_version + 1).contains(&state.state_version)
            || baseline > previous.sequence
            || (previous.plan.mode == MigrationMode::Full && baseline != previous.sequence)
        {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "copy_baseline_sequence",
            });
        }
        Ok(Some(baseline))
    }

    pub(super) async fn copy_scan_commit_confirmed(
        &self, generation: u8, execution_id: &str, facts: CopyFactRange,
    ) -> Result<bool> {
        let state = self.copy_generation_state().await?;
        Ok(state.state_version == copy_state_version(facts.last)? + 1
            && state.copy_execution_id == execution_id
            && state.copy_baseline_sequence == facts.first
            && state.scan_state == generation)
    }

    async fn copy_generation_state(&self) -> Result<CopyState> {
        let table = get_scan_state_table_name(&self.job_id);
        let state = self
            .sync_client
            .query(&format!(
                "SELECT scan_state,copy_execution_id,state_version,copy_baseline_sequence FROM {table} FINAL WHERE id=1"
            ))
            .fetch_quorum_one::<CopyState>()
            .await?;
        if state.scan_state > 1 {
            return Err(DatabaseError::ConversionError(format!(
                "Invalid copy generation {}; expected 0 or 1",
                state.scan_state
            )));
        }
        if (!state.copy_execution_id.is_empty() && state.state_version == 0)
            || state.copy_execution_id.is_empty() != (state.copy_baseline_sequence == 0)
        {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "copy_state_version",
            });
        }
        Ok(state)
    }

    /// 本轮是否接续未完成的复制代次；不改变已确认事实的增量比较。
    pub async fn copy_generation_recovering(&self) -> bool {
        self.generation_lifecycle.lock().await.copy_generation_recovering
    }

    pub(super) async fn is_copy_recovery(&self) -> bool {
        self.generation_lifecycle.lock().await.copy_recovery
    }

    pub(super) async fn copy_replay_items(&self) -> Result<Vec<StorageEntryRecord>> {
        let table = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("copy comparison staging".into()))?;
        // 按源端历史位置返回逐版本快照；只有复制完成 ACK 才能确认对应版本。
        self.sync_client
            .query(&format!(
                "SELECT {} FROM {table} ORDER BY relative_path, history_segment, history_index, version_id",
                *FILE_SCAN_COLUMNS_LIST
            ))
            .fetch_quorum_all()
            .await
    }

    pub(super) async fn copy_recovery_deletions(
        &self, rows: Vec<StorageEntryRecord>, working: u8,
    ) -> Result<Vec<DeletionStatus>> {
        let active = self.replayed_paths(&rows, working).await?;
        let mut retired = Vec::new();
        let mut deleted = Vec::new();
        let mut emitted = HashSet::new();
        for row in rows {
            if active.contains(&row.relative_path) {
                retired.push((row.relative_path, row.version_id));
            } else if emitted.insert(row.relative_path.clone()) {
                deleted.push(DeletionStatus::Deleted(row.to_observation()?));
            }
        }
        self.retire_copy_versions(&retired, working).await?;
        Ok(deleted)
    }

    async fn replayed_paths(&self, old: &[StorageEntryRecord], working: u8) -> Result<HashSet<String>> {
        let table = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
        let facts = CopyFactRange::and_predicate(self.copy_fact_range().await?);
        let paths: HashSet<_> = old.iter().map(|row| row.relative_path.as_str()).collect();
        let paths: Vec<_> = paths.into_iter().collect();
        let mut active = HashSet::new();
        for paths in bounded_keys(&paths, |path| path.len()) {
            let placeholders = vec!["?"; paths.len()].join(",");
            let mut query = self.sync_client.query(&format!("SELECT DISTINCT relative_path FROM {table} FINAL WHERE current_state={working}{facts} AND relative_path IN ({placeholders})"));
            for path in paths {
                query = query.bind(*path);
            }
            active.extend(query.fetch_quorum_all::<String>().await?);
        }
        Ok(active)
    }

    async fn retire_copy_versions(&self, keys: &[(String, String)], working: u8) -> Result<()> {
        let table = self.catalogue_storage_table(&get_scan_base_table_name(&self.job_id))?;
        let facts = CopyFactRange::and_predicate(self.copy_fact_range().await?);
        for keys in bounded_keys(keys, |(path, version)| path.len().saturating_add(version.len())) {
            let placeholders = vec!["(?,?)"; keys.len()].join(",");
            let values: Vec<_> = keys
                .iter()
                .flat_map(|(path, version)| [Value::from(path.clone()), Value::from(version.clone())])
                .collect();
            self.execute(&format!("ALTER TABLE {table} DELETE WHERE current_state!={working}{facts} AND (relative_path,version_id) IN ({placeholders}) SETTINGS mutations_sync=2"), &values).await?;
        }
        Ok(())
    }
}

// SQL 参数经客户端转义后成为文本；同时约束键数和字节数，覆盖合法长路径。
fn bounded_keys<T>(keys: &[T], size: impl Fn(&T) -> usize) -> Vec<&[T]> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < keys.len() {
        let mut end = start;
        let mut bytes = 0usize;
        while end < keys.len() && end - start < 256 {
            let next = size(&keys[end]).saturating_mul(2).saturating_add(32);
            if end > start && bytes.saturating_add(next) > 16_384 {
                break;
            }
            bytes = bytes.saturating_add(next);
            end += 1;
        }
        chunks.push(&keys[start..end]);
        start = end;
    }
    chunks
}
