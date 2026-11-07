//! 独立于 base 的已接受代次；结构化请求、实际计划和准备状态使用同一份记录。
use clickhouse::error::Error as ClickHouseError;
use serde::Deserialize;
use serde_json::Value;
use task_model::migration::{CopyGeneration, CopyGenerationPhase};
use utils::sanitize_job_id;

use super::{ClickHouseDatabase, layout::CatalogueEngine};
use crate::{DatabaseError, Result, execution::read::QuorumRead};

const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;

#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct CopyRunRow {
    pub sequence: u64,
    pub execution_id: String,
    pub revision: u64,
    pub descriptor: String,
}

impl ClickHouseDatabase {
    fn copy_runs_table(&self) -> String {
        format!("generations_{}", sanitize_job_id(&self.job_id))
    }

    pub(super) async fn create_copy_runs_table(&self) -> Result<()> {
        let table = self.copy_runs_table();
        self.create_catalogue_table_identified(
            &table,
            "sequence UInt64, execution_id String, revision UInt64, descriptor String",
            "sequence",
            CatalogueEngine::ReplacingVersion("revision"),
            Some(&table),
        )
        .await
    }

    /// base 被清空或普通扫描覆盖后，原绑定及上一已接受请求仍然可查询。
    pub async fn latest_copy_generation(&self) -> Result<Option<CopyGeneration>> {
        let table = self.copy_runs_table();
        if !self.table_exists(&table).await? {
            return Ok(None);
        }
        let table = self.catalogue_query_table(&table)?;
        let row = match self
            .sync_client
            .query(&format!(
                "SELECT sequence,execution_id,revision,descriptor FROM {table} FINAL ORDER BY sequence DESC LIMIT 1"
            ))
            .fetch_quorum_one::<CopyRunRow>()
            .await
        {
            Ok(row) => row,
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if row.descriptor.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "descriptor_size",
            });
        }
        let generation: CopyGeneration =
            serde_json::from_str(&row.descriptor).map_err(|source| DatabaseError::CopyGenerationJson { source })?;
        generation.request.validate()?;
        if row.sequence == 0
            || row.sequence != generation.sequence
            || row.execution_id != generation.request.execution_id
            || row.revision != generation.revision
            || row.revision == 0
            || (generation.phase == CopyGenerationPhase::Accepted && generation.baseline_prepared)
            || (generation.phase == CopyGenerationPhase::Prepared && !generation.baseline_prepared)
            || (generation.phase == CopyGenerationPhase::Completed && !generation.baseline_prepared)
        {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "stored_generation",
            });
        }
        Ok(Some(generation))
    }

    pub(super) async fn save_copy_generation(&self, generation: &CopyGeneration) -> Result<()> {
        let descriptor =
            serde_json::to_string(generation).map_err(|source| DatabaseError::CopyGenerationJson { source })?;
        if descriptor.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "descriptor_size",
            });
        }
        let table = self.copy_runs_table();
        // 重试使用同一 sequence／execution_id／revision 和原文，不将超时解释成未执行。
        self.execute(
            &format!("INSERT INTO {table} (sequence,execution_id,revision,descriptor) VALUES (?,?,?,?)"),
            &[
                Value::from(generation.sequence),
                Value::from(generation.request.execution_id.clone()),
                Value::from(generation.revision),
                Value::from(descriptor),
            ],
        )
        .await
    }

    /// 只有持有本执行域的调用方可以结束该代次；拒绝请求没有可覆盖的已接受记录。
    pub async fn finish_copy_generation(&self, phase: CopyGenerationPhase) -> Result<()> {
        if !matches!(
            phase,
            CopyGenerationPhase::Completed | CopyGenerationPhase::Failed | CopyGenerationPhase::Cancelled
        ) {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "terminal_phase",
            });
        }
        let mut lifecycle = self.generation_lifecycle.lock().await;
        let committed = lifecycle.committed;
        let Some(generation) = &mut lifecycle.copy_generation else {
            return Ok(());
        };
        if phase == CopyGenerationPhase::Completed && !committed {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "uncommitted_completion",
            });
        }
        if generation.phase != phase {
            generation.revision = generation
                .revision
                .checked_add(1)
                .ok_or(DatabaseError::CopyGenerationInvalid {
                    field: "revision_overflow",
                })?;
            generation.phase = phase;
        }
        self.save_copy_generation(generation).await
    }

    pub async fn current_copy_generation(&self) -> Result<CopyGeneration> {
        self.generation_lifecycle
            .lock()
            .await
            .copy_generation
            .clone()
            .ok_or(DatabaseError::CopyExecutionUnavailable)
    }
}
