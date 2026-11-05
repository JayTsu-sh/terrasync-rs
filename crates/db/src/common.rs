//! Common types and utilities for database implementations

use std::collections::HashMap;

use data_mover::model::ObservedEntry;
use tracing::{error, trace};

use crate::Result;
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
/// - 2 条记录 → `Renamed`（ctime 小 = from，ctime 大 = to）
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
