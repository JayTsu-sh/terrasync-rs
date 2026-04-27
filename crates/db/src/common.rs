//! Common types and utilities for database implementations

use std::collections::HashMap;
use std::sync::LazyLock;

use data_mover::EntryEnum;
use tracing::{error, trace};

use crate::traits::StorageEntryRecord;

/// Represents the status of a deleted item, either a permanent deletion or a rename
#[derive(Clone, Debug)]
pub enum DeletionStatus {
    /// The item was permanently deleted
    Deleted(EntryEnum),
    /// The item was renamed, with the old and new entries.
    ///
    /// 两个字段对称 box，避免 `clippy::large_enum_variant` 抱怨
    /// （`EntryEnum` 较大，不 box 会让整个枚举的 size 被最大变体拉高）。
    Renamed(Box<EntryEnum>, Box<EntryEnum>),
}

/// 文件扫描列名列表（不含 `version_count`）
pub const FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT: &str = "name, relative_path, size, ext, ctime, mtime, atime, mode, storage_type, is_symlink, is_dir, is_regular_file, hard_links, current_state, uid, gid, ino, file_handle, version_id, tags";

/// 通过 `FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT` 生成的完整列名列表，包含 `version_count`
pub static FILE_SCAN_COLUMNS_LIST: LazyLock<String> =
    LazyLock::new(|| format!("{FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT}, version_count"));

/// 基础扫描表的占位符列表（通过 `FILE_SCAN_COLUMNS_LIST` 计算）
pub static FILE_SCAN_COLUMNS_PLACEHOLDERS: LazyLock<String> = LazyLock::new(|| {
    let column_count = FILE_SCAN_COLUMNS_LIST.split(',').count();
    vec!["?"; column_count].join(", ")
});

/// 带表前缀"t."的完整列名列表（不含 `version_count`）
///
/// 用于SQL语句中的带表前缀列名列表，如SELECT语句中的表别名前缀
pub static FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX: LazyLock<String> = LazyLock::new(|| {
    FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|col| format!("t.{col}"))
        .collect::<Vec<_>>()
        .join(", ")
});

/// 带表前缀"t."的完整列名列表（包含 `version_count`）
///
/// 通过 `FILE_SCAN_COLUMNS_LIST` 生成的完整列名列表，包含 `t.version_count`
pub static FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX: LazyLock<String> = LazyLock::new(|| {
    FILE_SCAN_COLUMNS_LIST
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|col| format!("t.{col}"))
        .collect::<Vec<_>>()
        .join(", ")
});

/// 从 `detect_deleted_items` 提取的纯函数，对 `file_handle` 分组结果进行分类判断
///
/// 根据 `fh_groups` 中每个 `file_handle` 对应的记录数量判断是 `Deleted` 还是 `Renamed`：
/// - 1 条记录 → `Deleted`
/// - 2 条记录 → `Renamed`（ctime 小 = from，ctime 大 = to）
/// - 其他 → error 日志
#[allow(clippy::implicit_hasher)]
pub fn classify_deletion_status(
    no_fh_records: Vec<StorageEntryRecord>, fh_records: Vec<StorageEntryRecord>,
    fh_groups: &HashMap<String, Vec<StorageEntryRecord>>,
) -> Vec<DeletionStatus> {
    let mut deletion_statuses: Vec<DeletionStatus> = Vec::with_capacity(no_fh_records.len() + fh_records.len());

    // 无 file_handle → 全部 Deleted
    for record in no_fh_records {
        deletion_statuses.push(DeletionStatus::Deleted(record.to_entry_enum()));
    }

    // 有 file_handle → 根据 groups 判断
    for record in fh_records {
        if let Some(fh) = &record.file_handle {
            match fh_groups.get(fh).map(Vec::as_slice) {
                Some(entries) if entries.len() == 1 => {
                    trace!("file_handle {} has 1 entry, Deleted", fh);
                    deletion_statuses.push(DeletionStatus::Deleted(record.to_entry_enum()));
                }
                Some(entries) if entries.len() == 2 => {
                    // `record` 来自 old-state 查询，其 relative_path 即旧路径。
                    // 用路径精确匹配 from/to，避免依赖 ctime——父目录 rename 不更新
                    // 子文件 ctime（NFS v3 行为），ctime 相等时排序不确定会导致 swap。
                    let (from, to) = if entries[0].relative_path == record.relative_path.as_str() {
                        (entries[0].to_entry_enum(), entries[1].to_entry_enum())
                    } else {
                        (entries[1].to_entry_enum(), entries[0].to_entry_enum())
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
                    deletion_statuses.push(DeletionStatus::Deleted(record.to_entry_enum()));
                }
            }
        } else {
            // 理论上不应该到达这里，但安全处理
            deletion_statuses.push(DeletionStatus::Deleted(record.to_entry_enum()));
        }
    }

    deletion_statuses
}

/// 生成 `version_count` 的 JOIN 子句和 SELECT 表达式（优化 4）
///
/// 返回 `(join_clause, select_expr)`：
/// - `join_clause`: LEFT JOIN 预聚合子查询
/// - `select_expr`: CASE WHEN 表达式
#[macro_export]
macro_rules! generate_version_count_join_sql {
    ($base_table:expr) => {{
        let join_clause = format!(
            "LEFT JOIN (SELECT relative_path, count() as cnt FROM {} GROUP BY relative_path) as vc \
             ON t.relative_path = vc.relative_path",
            $base_table
        );
        let select_expr = "CASE WHEN t.version_count IS NOT NULL \
             THEN CAST(t.version_count - COALESCE(vc.cnt, 0) AS UInt32) \
             ELSE NULL END as version_count"
            .to_string();
        (join_clause, select_expr)
    }};
}
