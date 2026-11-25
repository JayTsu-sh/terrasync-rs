//! 按复制执行查询已经归档的普通文件变更。

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use task_model::migration::{CopyGenerationPhase, MigrationMode};

use super::ClickHouseDatabase;
use crate::execution::read::QuorumRead;
use crate::{Result, get_incremental_scan_base_table_name};

const MAX_PAGE_SIZE: u32 = 200;

#[derive(Debug, Deserialize, Row, Serialize)]
pub struct CopyChange {
    pub kind: String,
    pub path: String,
    pub previous_path: Option<String>,
}

#[derive(Debug, Deserialize, Row, Serialize)]
pub struct CopyChangeCount {
    pub kind: String,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct CopyChangePage {
    pub execution_id: String,
    pub phase: CopyGenerationPhase,
    pub counts: Vec<CopyChangeCount>,
    pub changes: Vec<CopyChange>,
    pub next_offset: Option<u64>,
}

impl ClickHouseDatabase {
    /// 查询已归档的结果。失败、取消或在途执行只报告当前可确认的范围。
    pub async fn incremental_copy_changes(
        &self, execution_id: &str, offset: u64, limit: u32,
    ) -> Result<Option<CopyChangePage>> {
        let Some(generation) = self.read_copy_generation(Some(execution_id)).await? else {
            return Ok(None);
        };
        if generation.plan.mode != MigrationMode::Incremental {
            return Ok(None);
        }

        let mut page = CopyChangePage {
            execution_id: execution_id.to_owned(),
            phase: generation.phase,
            counts: Vec::new(),
            changes: Vec::new(),
            next_offset: None,
        };
        let table = get_incremental_scan_base_table_name(&self.job_id);
        if !self.table_exists(&table).await? {
            return Ok(Some(page));
        }
        let table = self.catalogue_query_table(&table)?;
        // 按逻辑条目投影，重试写入同一个已完成变更时，计数和分页都只保留一次。
        let changes = format!(
            "SELECT DISTINCT operation_type AS kind, relative_path AS path, if(operation_type='rename',comment,NULL) AS previous_path
             FROM {table}
             WHERE copy_execution_id=? AND is_regular_file AND operation_type IN
             ('new','data_changed','metadata_changed','both_changed','rename')"
        );
        page.counts = self
            .sync_client
            .query(&format!(
                "SELECT kind,count() AS count FROM ({changes}) GROUP BY kind ORDER BY kind"
            ))
            .bind(execution_id)
            .fetch_quorum_all()
            .await?;
        let limit = limit.clamp(1, MAX_PAGE_SIZE) as usize;
        page.changes = self
            .sync_client
            .query(&format!("{changes} ORDER BY path,kind,previous_path LIMIT ? OFFSET ?"))
            .bind(execution_id)
            .bind(limit + 1)
            .bind(offset)
            .fetch_quorum_all()
            .await?;
        if page.changes.len() > limit {
            page.changes.truncate(limit);
            page.next_offset = offset.checked_add(limit as u64);
        }
        Ok(Some(page))
    }
}
