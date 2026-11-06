//! Catalogue deletion and rename classification.

use std::collections::{HashMap, HashSet};

use data_mover::model::ObservedEntry;
use tracing::{debug, error, trace};

use super::ClickHouseDatabase;
use super::comparison::JoinStrategy;
use crate::Result;
use crate::execution::read::QuorumRead;
use crate::get_scan_base_table_name;
use crate::schema::records::StorageEntryRecord;

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

/// 从 `detect_deleted_items` 提取的纯函数，对 `file_handle` 分组结果进行分类判断
///
/// 根据 `fh_groups` 中每个 `file_handle` 对应的记录数量判断是 `Deleted` 还是 `Renamed`：
/// - 1 条记录 → `Deleted`
/// - 2 条记录 → `Renamed`（按候选旧路径匹配 from／to）
/// - 其他 → error 日志
#[allow(clippy::implicit_hasher)]
pub(crate) fn classify_deletion_status(
    no_fh_records: Vec<StorageEntryRecord>, fh_records: Vec<StorageEntryRecord>,
    fh_groups: &HashMap<String, Vec<StorageEntryRecord>>,
) -> Result<Vec<DeletionStatus>> {
    let mut deletion_statuses: Vec<DeletionStatus> = Vec::with_capacity(no_fh_records.len() + fh_records.len());

    // 无 file_handle → 全部 Deleted
    for record in no_fh_records {
        deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
    }

    // 有 file_handle → 根据 groups 判断
    for record in fh_records {
        if let Some(fh) = &record.file_handle {
            match fh_groups.get(fh).map(Vec::as_slice) {
                Some(entries) if entries.len() == 1 => {
                    trace!("file_handle {} has 1 entry, Deleted", fh);
                    deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
                }
                Some(entries) if entries.len() == 2 => {
                    // `record` 来自 old-state 查询，其 relative_path 即旧路径。
                    // 用路径精确匹配 from/to，避免依赖 ctime——父目录 rename 不更新
                    // 子文件 ctime（NFS v3 行为），ctime 相等时排序不确定会导致 swap。
                    let (from, to) = if entries[0].relative_path == record.relative_path.as_str() {
                        (entries[0].to_observation()?, entries[1].to_observation()?)
                    } else {
                        (entries[1].to_observation()?, entries[0].to_observation()?)
                    };
                    trace!("file_handle {} has 2 entries, Renamed", fh);
                    deletion_statuses.push(DeletionStatus::Renamed(Box::new(from), Box::new(to)));
                }
                Some(entries) => {
                    error!(
                        "file_handle {} has unexpected {} entries: {:?}",
                        fh,
                        entries.len(),
                        entries
                    );
                }
                None => {
                    error!("file_handle {} not found in batch query results", fh);
                    deletion_statuses.push(DeletionStatus::Deleted(record.to_observation()?));
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

        // 第一步：查询所有 old-state 记录（LIMIT 1 BY 替代 FINAL）
        let query = JoinStrategy::build_detect_deleted_sql(&base_table_name);

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

        let deletion_statuses = self.classify_deleted_candidates(&base_table_name, rows).await?;

        debug!("Found {} deleted or renamed entries", deletion_statuses.len());

        Ok(Box::new(deletion_statuses.into_iter()))
    }
    /// Classify candidate rows after the query and recovery decision, without copying observations.
    async fn classify_deleted_candidates(
        &self, base_table_name: &str, rows: Vec<StorageEntryRecord>,
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

            // 每批 10K 个 fh，避免 SQL 过长
            for chunk in unique_fh_list.chunks(BATCH_SIZE) {
                let batch_query = JoinStrategy::build_batch_fh_query_sql(base_table_name, chunk.len());

                let mut q = self.sync_client.query(&batch_query);
                for fh in chunk {
                    q = q.bind(*fh);
                }
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
