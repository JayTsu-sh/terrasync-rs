//! Catalogue deletion and rename classification.

use std::collections::{HashMap, HashSet};

use data_mover::model::ObservedEntry;
use tracing::{debug, trace};

use super::ClickHouseDatabase;
use super::comparison::JoinStrategy;
use super::lifecycle::CopyFactRange;
use crate::execution::read::QuorumRead;
use crate::get_scan_base_table_name;
use crate::schema::records::StorageEntryRecord;
use crate::{DatabaseError, Result};

/// Represents the status of a deleted item, either a permanent deletion or a rename
///
/// `Deleted` 保持不 box——这是热路径上最常见的变体，每条删除记录都会走这里，
/// 多一次堆分配影响显著。`Renamed` 已 box 双字段以压缩枚举本体 size。
/// 变体间仍有 size 差异，对当前用法可接受，豁免 `large_enum_variant` lint。
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DeletionStatus {
    /// The item was permanently deleted
    Deleted(ObservedEntry),
    /// The item was renamed, with the old and new entries.
    Renamed(Box<ObservedEntry>, Box<ObservedEntry>),
}

/// 将已确认目标的缺失路径与本轮源观察按稳定身份关联；不按历史行数或 ctime 猜测。
#[allow(clippy::implicit_hasher)]
pub(crate) fn classify_deletion_status(
    no_fh_records: Vec<StorageEntryRecord>, fh_records: Vec<StorageEntryRecord>,
    fh_groups: &HashMap<String, Vec<StorageEntryRecord>>,
) -> Result<Vec<DeletionStatus>> {
    let mut deletion_statuses: Vec<DeletionStatus> = Vec::with_capacity(no_fh_records.len() + fh_records.len());
    let mut old_counts = HashMap::new();
    for record in &fh_records {
        if let Some(identity) = &record.file_handle {
            *old_counts.entry(identity).or_insert(0usize) += 1;
        }
    }
    let old_counts: HashMap<_, _> = old_counts
        .into_iter()
        .map(|(key, count)| (key.clone(), count))
        .collect();

    // 无 file_handle → 全部 Deleted
    for record in no_fh_records {
        deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
    }

    // 有 file_handle → 根据 groups 判断
    for record in fh_records {
        if let Some(fh) = &record.file_handle {
            match fh_groups.get(fh).map(Vec::as_slice) {
                None | Some([]) => {
                    trace!(
                        "file_handle {} has no pending rename observation; retiring old target path",
                        fh
                    );
                    deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
                }
                Some([current])
                    if old_counts.get(fh) == Some(&1)
                        && current.relative_path != record.relative_path
                        && current.is_dir == record.is_dir
                        && current.is_symlink == record.is_symlink =>
                {
                    deletion_statuses.push(DeletionStatus::Renamed(
                        Box::new(record.to_observation()?),
                        Box::new(current.to_observation()?),
                    ));
                }
                Some(entries) => {
                    return Err(DatabaseError::RenameAssociationInvalid {
                        path: record.relative_path.clone(),
                        reason: "stable identity does not uniquely associate a confirmed old path with a different current path of the same kind",
                        confirmed_paths: old_counts.get(fh).copied().unwrap_or_default(),
                        observed_paths: entries.len(),
                    });
                }
            }
        } else {
            // 理论上不应该到达这里，但安全处理
            deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
        }
    }

    Ok(deletion_statuses)
}

impl ClickHouseDatabase {
    /// 检测已删除或重命名的文件（old-state 记录 + `file_handle` 分组判定）
    pub async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
        let base_table_name = get_scan_base_table_name(&self.job_id);

        // begin 之后必须使用共享生命周期中的 working generation；持久 state 只有 commit
        // 成功后才推进，直接读取它会把本轮 working 记录误判为 old-state 并全部删除。
        let current_state = self.get_cached_scan_state().await?;
        debug!("During detect_deleted_items, current_state is {}", current_state);

        let facts = self.copy_fact_range().await?;
        let query = JoinStrategy::build_detect_deleted_sql(&base_table_name, facts);

        let rows = self
            .sync_client
            .query(&query)
            .bind(1 - current_state)
            .fetch_quorum_all::<StorageEntryRecord>()
            .await?;

        debug!("Found {} old-state entries", rows.len());
        if rows.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        if self.is_copy_recovery().await {
            return Ok(Box::new(
                self.copy_recovery_deletions(rows, current_state).await?.into_iter(),
            ));
        }

        let deletion_statuses = self.classify_deleted_candidates(&base_table_name, rows, facts).await?;

        debug!("Found {} deleted or renamed entries", deletion_statuses.len());

        Ok(Box::new(deletion_statuses.into_iter()))
    }
    /// Classify candidate rows after the query and recovery decision, without copying observations.
    async fn classify_deleted_candidates(
        &self, base_table_name: &str, rows: Vec<StorageEntryRecord>, facts: Option<CopyFactRange>,
    ) -> Result<Vec<DeletionStatus>> {
        let (no_fh_records, fh_records): (Vec<_>, Vec<_>) =
            rows.into_iter().partition(|record| record.file_handle.is_none());
        let unique_fh_set: HashSet<String> = fh_records
            .iter()
            .filter_map(|record| record.file_handle.clone())
            .collect();

        // 第三步：有 file_handle → 分批批量查询
        let mut fh_groups: HashMap<String, Vec<StorageEntryRecord>> = HashMap::new();

        if !fh_records.is_empty() {
            const BATCH_SIZE: usize = 10_000;
            let unique_fh_list: Vec<&str> = unique_fh_set.iter().map(String::as_str).collect();
            let current_state = self.get_cached_scan_state().await?;

            // 每批 10K 个 fh，避免 SQL 过长
            for chunk in unique_fh_list.chunks(BATCH_SIZE) {
                let batch_query = JoinStrategy::build_batch_fh_query_sql(base_table_name, chunk.len(), facts);

                let mut q = self.sync_client.query(&batch_query);
                for fh in chunk {
                    q = q.bind(*fh);
                }
                // 复制已经确认的新目标不能再被旧路径覆盖；纯扫描仍关联全部当前观察。
                let q = q.bind(current_state).bind(facts.is_none());
                let batch_rows = q.fetch_quorum_all::<StorageEntryRecord>().await?;

                for row in batch_rows {
                    if let Some(fh) = &row.file_handle {
                        fh_groups.entry(fh.clone()).or_default().push(row);
                    }
                }
            }
        }

        classify_deletion_status(no_fh_records, fh_records, &fh_groups)
    }
}
