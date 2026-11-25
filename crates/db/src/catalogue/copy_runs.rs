//! 独立于 base 的已接受代次；结构化请求、实际计划和准备状态使用同一份记录。
use clickhouse::error::Error as ClickHouseError;
use data_mover::model::{BackendIdentity, EntryKind, ObservedEntry};
use serde::Deserialize;
use serde_json::Value;
use task_model::migration::{CopyGeneration, CopyGenerationPhase, CopyRequestDescriptor};
use utils::sanitize_job_id;

use super::{ClickHouseDatabase, layout::CatalogueEngine};
use crate::{DatabaseError, Result, execution::read::QuorumRead, get_scan_base_table_name};

const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;

#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct CopyRunRow {
    pub sequence: u64,
    pub execution_id: String,
    pub revision: u64,
    pub descriptor: String,
    pub root_snapshot: String,
}

impl ClickHouseDatabase {
    fn copy_runs_table(&self) -> String {
        format!("generations_{}", sanitize_job_id(&self.job_id))
    }

    pub(super) async fn create_copy_runs_table(&self) -> Result<()> {
        let table = self.copy_runs_table();
        self.create_catalogue_table_identified(
            &table,
            "sequence UInt64, execution_id String, revision UInt64, descriptor String, root_snapshot String",
            "sequence",
            CatalogueEngine::ReplacingVersion("revision"),
            Some(&table),
        )
        .await
    }

    /// base 被清空或普通扫描覆盖后，原绑定及上一已接受请求仍然可查询。
    pub async fn latest_copy_generation(&self) -> Result<Option<CopyGeneration>> {
        self.read_copy_generation(None).await
    }

    pub(super) async fn read_copy_generation(&self, execution_id: Option<&str>) -> Result<Option<CopyGeneration>> {
        let table = self.copy_runs_table();
        if !self.table_exists(&table).await? {
            return Ok(None);
        }
        let table = self.catalogue_query_table(&table)?;
        let filter = if execution_id.is_some() {
            " WHERE execution_id=?"
        } else {
            ""
        };
        let sql = format!(
            "SELECT sequence,execution_id,revision,descriptor,hex(root_snapshot) AS root_snapshot FROM {table} FINAL{filter} ORDER BY sequence DESC LIMIT 1"
        );
        let query = self.sync_client.query(&sql);
        let query = match execution_id {
            Some(execution_id) => query.bind(execution_id),
            None => query,
        };
        let row = match query.fetch_quorum_one::<CopyRunRow>().await {
            Ok(row) => row,
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if row.descriptor.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "descriptor_size",
            });
        }
        let mut generation: CopyGeneration =
            serde_json::from_str(&row.descriptor).map_err(|source| DatabaseError::CopyGenerationJson { source })?;
        generation.request.validate()?;
        if !row.root_snapshot.is_empty() {
            generation.root_snapshot = Some(
                hex::decode(row.root_snapshot).map_err(|source| DatabaseError::CopyRootSnapshotEncoding { source })?,
            );
        }
        validate_root_snapshot(&generation.request, generation.root_snapshot.as_deref())?;
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
        validate_root_snapshot(&generation.request, generation.root_snapshot.as_deref())?;
        let descriptor =
            serde_json::to_string(generation).map_err(|source| DatabaseError::CopyGenerationJson { source })?;
        if descriptor.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "descriptor_size",
            });
        }
        let table = self.copy_runs_table();
        // 二进制经 hex 绑定而非逐字节 JSON Value 数组，控制大 ACL/xattr 的临时内存。
        // 重试使用同一 sequence／execution_id／revision 和原文，不将超时解释成未执行。
        self.execute(
            &format!(
                "INSERT INTO {table} (sequence,execution_id,revision,descriptor,root_snapshot) VALUES (?,?,?,?,unhex(?))"
            ),
            &[
                Value::from(generation.sequence),
                Value::from(generation.request.execution_id.clone()),
                Value::from(generation.revision),
                Value::from(descriptor),
                Value::from(hex::encode(generation.root_snapshot.as_deref().unwrap_or_default())),
            ],
        )
        .await
    }

    /// 调用方先收束 worker、目录收尾及源清理；删源代次结束后不保留增量 base。
    /// 清理只修改本代及更早的复制事实，新代和普通扫描的事实不能被延迟清理删除。
    pub async fn finish_copy_generation(
        &self, phase: CopyGenerationPhase, root_snapshot: Option<Vec<u8>>,
    ) -> Result<()> {
        if !matches!(
            phase,
            CopyGenerationPhase::Completed | CopyGenerationPhase::Failed | CopyGenerationPhase::Cancelled
        ) {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "terminal_phase",
            });
        }
        let lifecycle = self.generation_lifecycle.lock().await;
        let Some(generation) = lifecycle.copy_generation.clone() else {
            return Ok(());
        };
        if phase == CopyGenerationPhase::Completed && !lifecycle.committed {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "uncommitted_completion",
            });
        }
        validate_root_snapshot(&generation.request, root_snapshot.as_deref())?;
        drop(lifecycle);
        // 不能持有 state 等待在途 base 写入，否则会与读取代次身份的 writer 互锁。
        let mut base = if generation.request.source_cleanup.retains_source() {
            None
        } else {
            Some(self.generation_lifecycle.base_closed.write().await)
        };
        let cleanup = if let Some(closed) = base.as_mut() {
            **closed = true;
            self.retire_copy_base(&generation)
                .await
                .map_err(|source| DatabaseError::CopyBaseRetirementFailed {
                    sequence: generation.sequence,
                    source: Box::new(source),
                })
        } else {
            Ok(())
        };
        let terminal = if cleanup.is_err() && phase == CopyGenerationPhase::Completed {
            CopyGenerationPhase::Failed
        } else {
            phase
        };
        let reporting = self.save_copy_terminal(terminal, root_snapshot).await;
        match (cleanup, reporting) {
            (Ok(()), result) | (result, Ok(())) => result,
            (Err(cleanup), Err(reporting)) => Err(DatabaseError::CopyBaseRetirementReportingFailed {
                cleanup: Box::new(cleanup),
                reporting: Box::new(reporting),
            }),
        }
    }

    async fn save_copy_terminal(&self, phase: CopyGenerationPhase, root_snapshot: Option<Vec<u8>>) -> Result<()> {
        let mut lifecycle = self.generation_lifecycle.lock().await;
        let generation = lifecycle
            .copy_generation
            .as_mut()
            .ok_or(DatabaseError::CopyExecutionUnavailable)?;
        validate_root_snapshot(&generation.request, root_snapshot.as_deref())?;
        let root_changed = root_snapshot
            .as_ref()
            .is_some_and(|snapshot| generation.root_snapshot.as_ref() != Some(snapshot));
        if generation.phase != phase || root_changed {
            generation.revision = generation
                .revision
                .checked_add(1)
                .ok_or(DatabaseError::CopyGenerationInvalid {
                    field: "revision_overflow",
                })?;
            generation.phase = phase;
            if let Some(snapshot) = root_snapshot {
                generation.root_snapshot = Some(snapshot);
            }
        }
        self.save_copy_generation(generation).await
    }

    async fn retire_copy_base(&self, generation: &CopyGeneration) -> Result<()> {
        let current = self.latest_copy_generation().await?;
        if !current.is_some_and(|current| {
            current.sequence == generation.sequence && current.request.execution_id == generation.request.execution_id
        }) {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "stale_base_retirement",
            });
        }
        let table = get_scan_base_table_name(&self.job_id);
        if !self.table_exists(&table).await? {
            return Ok(());
        }
        if generation.baseline_prepared && !self.copy_baseline_available(generation).await? {
            return Err(DatabaseError::CopyGenerationInvalid {
                field: "stale_base_retirement",
            });
        }
        // 序列条件必须位于服务端 mutation 内，不能只凭之前的 latest 查询执行全表清理。
        // 普通扫描序列为零；未知归属的行保持原样并明确报错，避免误删后来扫描的数据。
        let storage = self.catalogue_storage_table(&table)?;
        self.execute(
            &format!(
                "ALTER TABLE {storage} DELETE WHERE copy_sequence>0 AND copy_sequence<=? SETTINGS mutations_sync=2"
            ),
            &[Value::from(generation.sequence)],
        )
        .await?;
        let query = self.catalogue_query_table(&table)?;
        let remaining = self
            .sync_client
            .query(&format!("SELECT count() FROM {query}"))
            .fetch_quorum_one::<u64>()
            .await?;
        if remaining != 0 {
            return Err(DatabaseError::CopyBaseRetirementIncomplete {
                sequence: generation.sequence,
                remaining,
            });
        }
        Ok(())
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

/// 根原值沿用普通 entry 的原生编码及后端绑定，不能把任意文件／历史写成根事实。
fn validate_root_snapshot(request: &CopyRequestDescriptor, snapshot: Option<&[u8]>) -> Result<()> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let binding = &request.binding.source;
    let backend = BackendIdentity::new(binding.backend_kind.parse()?, binding.namespace.clone())?;
    let entry = ObservedEntry::decode_snapshot(snapshot, &backend)?;
    if !entry.path().as_str().is_empty() || entry.kind() != EntryKind::Directory || entry.history().is_some() {
        return Err(DatabaseError::CopyGenerationInvalid { field: "root_snapshot" });
    }
    Ok(())
}
