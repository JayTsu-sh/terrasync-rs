//! 扫描及目录共用的 ClickHouse 列定义与查询投影。
use std::sync::LazyLock;

/// 生成文件扫描列定义的宏
///
/// 支持无参调用（基础列）或带 `$prefix` / `$suffix` 的调用（在基础列前后拼接额外列）。
#[macro_export]
macro_rules! file_scan_base_columns {
    // 内部宏，包含基础列定义
    (@base) => {
        r"
    name String DEFAULT '',
    relative_path String,
    size UInt64,
    ext Nullable(String) DEFAULT NULL,
    ctime DateTime64(9) DEFAULT 0,
    mtime DateTime64(9) DEFAULT 0,
    atime DateTime64(9) DEFAULT 0,
    mode Nullable(UInt32) DEFAULT NULL,
    storage_type String DEFAULT '',
    is_symlink Bool DEFAULT 0,
    is_dir Bool DEFAULT 0,
    is_regular_file Bool DEFAULT 0,
    hard_links UInt32 DEFAULT 0,
    current_state UInt8,
    copy_path_confirmed Bool DEFAULT true,
    copy_data_confirmed Bool DEFAULT true,
    copy_target_version Nullable(String) DEFAULT NULL,
    copy_execution_id String DEFAULT '',
    copy_sequence UInt64 DEFAULT 0,
    uid Nullable(UInt32) DEFAULT NULL,
    gid Nullable(UInt32) DEFAULT NULL,
    ino Nullable(UInt64) DEFAULT NULL,
    file_handle Nullable(String) DEFAULT NULL,
    version_id String DEFAULT '',
    source_version_id String DEFAULT '',
    tags Nullable(String) DEFAULT NULL,
    marker_state Nullable(String) DEFAULT NULL,
    metadata_digests Array(String) DEFAULT [],
    identity_key String DEFAULT '',
    backend_kind String DEFAULT '',
    backend_id String DEFAULT '',
    entry_kind String DEFAULT '',
    size_observed Bool DEFAULT 0,
    modified_unix_nanos Nullable(Int128) DEFAULT NULL,
    entry_snapshot Array(UInt8) DEFAULT [],
    metadata_hash Nullable(String) DEFAULT NULL,
    history_segment UInt32 DEFAULT 0,
    history_index UInt32 DEFAULT 0,
    INDEX identity_key_idx identity_key TYPE bloom_filter GRANULARITY 1
"
    };

    // 生成基础列定义
    () => {
        file_scan_base_columns!(@base)
    };

    // 带前缀和后缀的调用
    ($prefix:expr, $suffix:expr) => {
        concat!($prefix, file_scan_base_columns!(@base), $suffix)
    };
}

/// `ClickHouse`数据库实现文件扫描记录的标准列定义
///
/// 定义了存储文件扫描结果的表结构，包含文件路径、大小、扩展名、时间戳、权限等信息。
/// 所有相关表（主表和临时表）都使用此结构定义。
pub(crate) const FILE_SCAN_COLUMNS_DEFINITION: &str = file_scan_base_columns!();

/// 文件增量扫描记录的标准列定义
///
/// 定义了存储文件增量扫描结果的表结构，包含操作类型、文件路径、大小、扩展名、时间戳、权限等信息。
/// 增量扫描表使用此结构定义。
pub(crate) const FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION: &str = file_scan_base_columns!(
    r#"
    operation_type String,
"#,
    r#"
    ,create_at DateTime64(9),
    comment Nullable(String),
"#
);

/// 目录查询和扫描摘要共用的字段顺序。
pub(crate) const FILE_SCAN_COLUMNS_LIST: &str = "name, relative_path, size, ext, ctime, mtime, atime, mode, storage_type, is_symlink, is_dir, is_regular_file, hard_links, current_state, uid, gid, ino, file_handle, version_id, tags, marker_state, metadata_digests";

/// 原生观察流式回读的最小查询投影，与 `SnapshotRow` 字段顺序一致。
pub(crate) const SNAPSHOT_COLUMNS: &str = "relative_path,backend_kind,backend_id,entry_snapshot";

/// 已确认复制事实回读同时携带实际目标版本；源版本仍由原生 snapshot 表达。
pub(crate) const COPY_SNAPSHOT_COLUMNS: &str =
    "relative_path,backend_kind,backend_id,entry_snapshot,source_version_id,copy_target_version";

pub(crate) static CATALOGUE_COLUMNS: LazyLock<String> =
    LazyLock::new(|| format!("{},backend_kind,backend_id,entry_snapshot", FILE_SCAN_COLUMNS_LIST));
pub(crate) static CATALOGUE_COLUMNS_WITH_T_PREFIX: LazyLock<String> = LazyLock::new(|| {
    CATALOGUE_COLUMNS
        .split(',')
        .map(|name| format!("t.{}", name.trim()))
        .collect::<Vec<_>>()
        .join(",")
});

#[cfg(test)]
mod tests {
    use super::FILE_SCAN_COLUMNS_DEFINITION;

    #[test]
    fn native_observation_columns_share_the_scan_table_definition() {
        for column in [
            "identity_key String",
            "backend_kind String",
            "entry_kind String",
            "size_observed Bool",
            "modified_unix_nanos Nullable(Int128)",
            "entry_snapshot Array(UInt8)",
            "copy_execution_id String DEFAULT ''",
            "INDEX identity_key_idx identity_key TYPE bloom_filter",
        ] {
            assert!(FILE_SCAN_COLUMNS_DEFINITION.contains(column));
        }
    }
}
