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
    uid Nullable(UInt32) DEFAULT NULL,
    gid Nullable(UInt32) DEFAULT NULL,
    ino Nullable(UInt64) DEFAULT NULL,
    file_handle Nullable(String) DEFAULT NULL,
    version_id String DEFAULT '',
    tags Nullable(String) DEFAULT NULL,
    version_count Nullable(UInt32) DEFAULT NULL,
    marker_state Nullable(String) DEFAULT NULL,
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

/// 文件扫描列名列表（不含 `version_count`）
pub const FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT: &str = "name, relative_path, size, ext, ctime, mtime, atime, mode, storage_type, is_symlink, is_dir, is_regular_file, hard_links, current_state, uid, gid, ino, file_handle, version_id, tags";

/// 通过 `FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT` 生成的完整列名列表，包含 `version_count`
pub static FILE_SCAN_COLUMNS_LIST: LazyLock<String> =
    LazyLock::new(|| format!("{FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT}, version_count, marker_state"));

pub(crate) static CATALOGUE_COLUMNS: LazyLock<String> =
    LazyLock::new(|| format!("{},backend_kind,backend_id,entry_snapshot", *FILE_SCAN_COLUMNS_LIST));
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
            "INDEX identity_key_idx identity_key TYPE bloom_filter",
        ] {
            assert!(FILE_SCAN_COLUMNS_DEFINITION.contains(column));
        }
    }
}
