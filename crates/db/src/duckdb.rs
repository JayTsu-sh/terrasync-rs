// 标准库
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

// 外部crate
use async_trait;
use duckdb::{Connection, params};
use storage_v2::{ChangeKind, EntryEnum, StorageEntryMessage};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

// 内部模块
use crate::common::{
    DeletionStatus, FILE_SCAN_COLUMNS_LIST, FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX,
    FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX, FILE_SCAN_COLUMNS_PLACEHOLDERS, classify_deletion_status,
};
use crate::config::DuckDBConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::{Database, IncrementalStorageEntryRecord, StorageEntryRecord, TarManifestRecord};
use crate::{
    INCREMENTAL_SCAN_TABLE_BASE_NAME, SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME,
    TAR_MANIFEST_TABLE_BASE_NAME, generate_scan_temp_table_name, get_incremental_scan_base_table_name,
    get_scan_base_table_name, get_scan_state_table_name, get_tar_manifest_table_name,
};
use utils::sanitize_job_id;

/// DuckDB SQL 构建策略：基于路径 vs 基于 file_handle
///
/// 根据临时表和主表中 file_handle 的非空记录数动态决定 JOIN 策略
#[derive(Debug, Clone, Copy, PartialEq)]
enum DuckDBJoinStrategy {
    /// 使用 relative_path + version_id 进行 JOIN
    Path,
    /// 使用 file_handle 进行 JOIN
    FileHandle,
}

impl DuckDBJoinStrategy {
    /// 根据临时表和主表的 file_handle 状态决定 JOIN 策略
    fn from_file_handle_status(
        temp_non_null: usize, base_non_null: usize, base_total: usize, temp_total: usize,
    ) -> Self {
        if temp_non_null == 0 && base_non_null == 0 && base_total > 0 && temp_total > 0 {
            Self::Path
        } else {
            Self::FileHandle
        }
    }

    /// 构建 detect_new_items 的 SQL（DuckDB 方言）
    fn build_detect_new_sql(&self, temp: &str, base: &str, vc_join: &str, vc_expr: &str) -> String {
        match self {
            Self::Path => format!(
                "SELECT {cols}, {vc_expr} \
                 FROM {temp} t \
                 LEFT JOIN {base} f ON t.relative_path = f.relative_path \
                   AND ((t.version_id IS NULL AND f.version_id IS NULL) OR t.version_id = f.version_id) \
                 {vc_join} \
                 WHERE f.relative_path IS NULL \
                 ORDER BY t.relative_path, t.version_id",
                cols = *FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX,
            ),
            Self::FileHandle => format!(
                "SELECT {cols}, {vc_expr} \
                 FROM {temp} t \
                 LEFT JOIN {base} f ON t.file_handle = f.file_handle \
                 {vc_join} \
                 WHERE f.file_handle IS NULL \
                 ORDER BY t.relative_path, t.version_id",
                cols = *FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX,
            ),
        }
    }

    /// 构建 detect_changed_items 的 SQL（DuckDB 方言）
    ///
    /// 根据 `ChangeKind` 生成不同 WHERE 子句，三种 kind 互斥：
    /// - `DataOnly`：size 或 mtime 变了；mode/uid/gid 均未变
    /// - `MetadataOnly`：size + mtime 均未变；mode/uid/gid 至少一项变了（chmod/chown）
    /// - `Both`：内容和属性都变了
    ///
    /// 注意：Path 模式保留原有的 `version_id = ''` 守卫，仅在空 version_id 时比较 mtime
    fn build_detect_changed_sql(&self, temp: &str, base: &str, kind: ChangeKind) -> String {
        let meta_changed = "(t.mode IS DISTINCT FROM f.mode OR t.uid IS DISTINCT FROM f.uid \
                            OR t.gid IS DISTINCT FROM f.gid)";
        let meta_unchanged = "(t.mode IS NOT DISTINCT FROM f.mode AND t.uid IS NOT DISTINCT FROM f.uid \
                              AND t.gid IS NOT DISTINCT FROM f.gid)";
        match self {
            Self::Path => {
                // Path 模式：version_id 非空时只比较 size；空 version_id（NAS 场景）时同时比 mtime
                let data_changed = "(t.size != f.size OR (t.version_id = '' AND t.mtime != f.mtime))";
                let data_unchanged = "(t.size = f.size AND (t.version_id != '' OR t.mtime = f.mtime))";
                let kind_filter = match kind {
                    ChangeKind::DataOnly => format!("{data_changed} AND {meta_unchanged}"),
                    ChangeKind::MetadataOnly => format!("{data_unchanged} AND {meta_changed}"),
                    ChangeKind::Both => format!("{data_changed} AND {meta_changed}"),
                };
                format!(
                    "SELECT {cols} \
                     FROM {temp} t JOIN {base} f ON t.relative_path = f.relative_path AND t.version_id = f.version_id \
                     WHERE {kind_filter} AND f.is_dir = 0 \
                     ORDER BY t.relative_path, t.version_id",
                    cols = *FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX,
                )
            }
            Self::FileHandle => {
                let data_changed = "(t.mtime != f.mtime OR t.size != f.size)";
                let data_unchanged = "(t.mtime = f.mtime AND t.size = f.size)";
                let kind_filter = match kind {
                    ChangeKind::DataOnly => format!("{data_changed} AND {meta_unchanged}"),
                    ChangeKind::MetadataOnly => format!("{data_unchanged} AND {meta_changed}"),
                    ChangeKind::Both => format!("{data_changed} AND {meta_changed}"),
                };
                format!(
                    "SELECT {cols} \
                     FROM {temp} t JOIN {base} f ON t.file_handle = f.file_handle \
                       AND t.version_id = f.version_id \
                       AND t.relative_path = f.relative_path \
                     WHERE {kind_filter} AND f.is_dir = 0 \
                     ORDER BY t.relative_path, t.version_id",
                    cols = *FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX,
                )
            }
        }
    }

    /// 构建 detect_deleted_items 的 SQL（DuckDB 方言）
    ///
    /// DuckDB 用 INSERT OR REPLACE + PK 保证唯一性，无需 LIMIT 1 BY 去重
    fn build_detect_deleted_sql(base: &str) -> String {
        format!(
            "SELECT {} FROM {} WHERE current_state = ? ORDER BY relative_path, version_id",
            *FILE_SCAN_COLUMNS_LIST, base
        )
    }

    /// 构建批量查询 file_handle 的 SQL（用于 detect_deleted_items 的批量重命名检测）
    fn build_batch_fh_query_sql(base: &str, batch_size: usize) -> String {
        let placeholders = vec!["?"; batch_size].join(", ");
        format!(
            "SELECT {} FROM {} WHERE file_handle IN ({}) ORDER BY file_handle, ctime, version_id",
            *FILE_SCAN_COLUMNS_LIST, base, placeholders
        )
    }
}

/// 生成 DuckDB 版本的 version_count JOIN 子句和 SELECT 表达式
///
/// DuckDB 使用 `COUNT(*)` 和 `CAST(... AS INTEGER)` 而非 ClickHouse 的 `count()` / `UInt32`
fn generate_duckdb_version_count_join(base_table: &str) -> (String, String) {
    let join_clause = format!(
        "LEFT JOIN (SELECT relative_path, COUNT(*) as cnt FROM {} GROUP BY relative_path) as vc \
         ON t.relative_path = vc.relative_path",
        base_table
    );
    let select_expr = "CASE WHEN t.version_count IS NOT NULL \
         THEN CAST(t.version_count - COALESCE(vc.cnt, 0) AS INTEGER) \
         ELSE NULL END as version_count"
        .to_string();
    (join_clause, select_expr)
}

/// DuckDB数据库实现
/// 提供DuckDB数据库的所有操作功能
pub struct DuckDBDatabase {
    /// DuckDB配置信息
    config: DuckDBConfig,
    /// 任务ID，用于表命名隔离
    job_id: String,
    /// 临时扫描表名（可选）
    pub scan_temp_table_name: Option<String>,
    /// 缓存的 scan_state 值（255 = 未缓存哨兵值）
    cached_scan_state: AtomicU8,
}

impl DuckDBDatabase {
    /// 通用的检测方法，用于检测新增、变更或删除的文件
    async fn detect_items(
        &self, query_type: &str, query_builder: impl Fn(&str, &str, &DuckDBJoinStrategy) -> String,
    ) -> Result<Vec<StorageEntryRecord>> {
        let temp_table_name = self.scan_temp_table_name.as_ref().ok_or_else(|| {
            error!("Temporary table name is None, cannot query {}", query_type);
            DatabaseError::UnsupportedType("Temporary table not created".to_string())
        })?;
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let (temp_total, temp_non_null, base_total, base_non_null) =
            self.check_file_handle_status(temp_table_name, &base_table_name)?;

        let strategy =
            DuckDBJoinStrategy::from_file_handle_status(temp_non_null, base_non_null, base_total, temp_total);
        debug!(
            "detect_items({}): temp_total={}, temp_non_null={}, base_total={}, base_non_null={}, strategy={:?}",
            query_type, temp_total, temp_non_null, base_total, base_non_null, strategy
        );

        let query = query_builder(temp_table_name, &base_table_name, &strategy);

        trace!(
            "Querying {} from '{}' against '{}'. SQL: {}",
            query_type, temp_table_name, base_table_name, query
        );

        let records = self.with_connection(|conn| {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            let mut rows = stmt.query([]).map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let mut records: Vec<StorageEntryRecord> = Vec::new();
            while let Some(row) = rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                let record = DuckDBDatabase::from_row(&row)?;
                trace!("{} file: {:?}", query_type, record);
                records.push(record);
            }

            debug!("Found {} {} files", records.len(), query_type);
            Ok(records)
        })?;

        Ok(records)
    }

    /// 检查临时表和主表的file_handle字段状态（合并为单次连接）
    fn check_file_handle_status(
        &self, temp_table_name: &str, base_table_name: &str,
    ) -> Result<(usize, usize, usize, usize)> {
        let temp_query = format!(
            "SELECT COUNT(*) as total, COUNT(file_handle) as non_null FROM {}",
            temp_table_name
        );
        let base_query = format!(
            "SELECT COUNT(*) as total, COUNT(file_handle) as non_null FROM {}",
            base_table_name
        );

        debug!(
            "Checking file_handle status for temp='{}' base='{}'",
            temp_table_name, base_table_name
        );

        self.with_connection(|conn| {
            let (temp_total, temp_non_null) = {
                let mut stmt = conn
                    .prepare(&temp_query)
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                match stmt.query_row([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))) {
                    Ok((total, non_null)) => (total as usize, non_null as usize),
                    Err(e) => {
                        warn!(
                            "Failed to query temp table file_handle status: {}, defaulting to (0, 0)",
                            e
                        );
                        (0, 0)
                    }
                }
            };

            let (base_total, base_non_null) = {
                let mut stmt = conn
                    .prepare(&base_query)
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                match stmt.query_row([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))) {
                    Ok((total, non_null)) => (total as usize, non_null as usize),
                    Err(e) => {
                        warn!(
                            "Failed to query base table file_handle status: {}, defaulting to (0, 0)",
                            e
                        );
                        (0, 0)
                    }
                }
            };

            trace!(
                "file_handle status - temp: total={}, non_null={}; base: total={}, non_null={}",
                temp_total, temp_non_null, base_total, base_non_null
            );

            Ok((temp_total, temp_non_null, base_total, base_non_null))
        })
    }
}

/// 生成文件扫描列定义的宏
/// 用于生成DuckDB文件扫描相关的列定义字符串
macro_rules! file_scan_base_columns {
    // 内部宏，包含基础列定义
    (@base) => {
        r#"
    name TEXT,
    relative_path TEXT NOT NULL,
    size BIGINT,
    ext TEXT,
    ctime BIGINT,
    mtime BIGINT,
    atime BIGINT,
    mode INTEGER,
    storage_type TEXT NOT NULL,
    is_symlink bool,
    is_dir bool,
    is_regular_file bool,
    hard_links INTEGER,
    current_state INTEGER,
    uid INTEGER,
    gid INTEGER,
    ino UBIGINT,
    file_handle BLOB,
    version_id TEXT NOT NULL,
    tags TEXT,
    version_count INTEGER,
"#
    };

    // 生成基础列定义
    () => {
        file_scan_base_columns!(@base)
    };

    // 生成带前缀和后缀的列定义
    ($prefix:expr, $suffix:expr) => {
        concat!($prefix, file_scan_base_columns!(@base), $suffix)
    };
}

/// 文件扫描记录的标准列定义
/// 用于创建主扫描表和临时扫描表
const FILE_SCAN_COLUMNS_DEFINITION: &str =
    concat!(file_scan_base_columns!(), "    PRIMARY KEY (relative_path, version_id)");

/// 文件扫描记录的标准列定义
/// 用于创建主扫描表和临时扫描表
const FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION: &str = concat!(
    r#"    operation_type TEXT,
"#,
    file_scan_base_columns!(),
    r#"    create_at BIGINT,
    comment TEXT
"#
);

impl DuckDBDatabase {
    /// 创建新的DuckDB数据库实例
    pub fn new(config: DuckDBConfig, job_id: &str) -> Self {
        Self {
            config,
            job_id: job_id.to_string(),
            scan_temp_table_name: None,
            cached_scan_state: AtomicU8::new(255),
        }
    }

    /// 获取缓存的 scan_state 值，避免每次 batch 操作都查询 DB
    ///
    /// 首次调用时从 DB 查询并缓存，后续调用直接返回缓存值。
    /// 255 为未缓存哨兵值。
    async fn get_cached_scan_state(&self) -> Result<u8> {
        let cached = self.cached_scan_state.load(Ordering::Relaxed);
        if cached != 255 {
            return Ok(cached);
        }
        let state = match self.query_scan_state().await {
            Ok(s) => s,
            Err(e) => {
                // state 表不存在时降级为默认值 0（同 ClickHouse 处理）
                debug!(
                    "Failed to query scan_state (table may not exist), using default 0: {}",
                    e
                );
                0
            }
        };
        self.cached_scan_state.store(state, Ordering::Relaxed);
        Ok(state)
    }

    /// 创建数据库连接
    fn create_connection(&self) -> Result<Connection> {
        let path = self.config.get_path()?;
        debug!("Opening DuckDB connection at path: {}", path);
        Connection::open(&path).map_err(|e| {
            let error_msg = format!("Failed to connect to DuckDB at {}: {}", path, e);
            error!("{}", error_msg);
            DatabaseError::ConnectionError(error_msg)
        })
    }

    /// 使用数据库连接执行操作
    fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T>,
    {
        let mut conn = self.create_connection()?;
        f(&mut conn)
    }

    /// 从数据库行解析 StorageEntryRecord
    fn from_row(row: &duckdb::Row) -> Result<StorageEntryRecord> {
        let name: String = row.get(0).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let relative_path: String = row.get(1).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let size: u64 = row.get(2).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let ext: Option<String> = row.get(3).ok();
        let ctime: i64 = row.get(4).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let mtime: i64 = row.get(5).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let atime: i64 = row.get(6).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let mode: Option<u32> = row.get(7).ok();
        let storage_type: String = row.get(8).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let is_symlink: bool = row.get(9).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let is_dir: bool = row.get(10).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let is_regular_file: bool = row.get(11).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let hard_links: u32 = row.get(12).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let current_state: i32 = row.get(13).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let uid: Option<u32> = row.get(14).ok();
        let gid: Option<u32> = row.get(15).ok();
        let ino: Option<u64> = row.get(16).ok();
        let file_handle: Option<String> = row
            .get::<_, Option<Vec<u8>>>(17)
            .ok()
            .flatten()
            .map(|bytes| hex::encode(bytes));

        let version_id: String = row.get(18).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
        let tags_str: Option<String> = row.get(19).ok();
        let tags = tags_str.and_then(|s| serde_json::from_str(&s).ok());
        let version_count: Option<u32> = row.get(20).ok();

        Ok(StorageEntryRecord {
            name,
            relative_path,
            size,
            ext,
            ctime,
            mtime,
            atime,
            mode,
            storage_type,
            is_symlink,
            is_dir,
            is_regular_file,
            hard_links,
            current_state: current_state as u8,
            uid,
            gid,
            ino,
            file_handle,
            version_id,
            tags,
            version_count,
        })
    }

    /// 创建主扫描表
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating DuckDB scan base table: {}", table_name);
        self.with_connection(|conn| {
            conn.execute(&create_table_sql, []).map_err(|e| {
                error!("Failed to create DuckDB scan base table '{}': {}", table_name, e);
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })
    }

    /// 创建扫描状态表
    pub async fn create_scan_state_table(&self) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, scan_state INTEGER)",
            table_name
        );

        debug!("Creating DuckDB scan state table: {}", table_name);
        self.with_connection(|conn| {
            conn.execute(&create_table_sql, []).map_err(|e| {
                error!("Failed to create DuckDB scan state table '{}': {}", table_name, e);
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })
    }

    /// 创建增量扫描表
    async fn create_incremental_scan_table(&self) -> Result<()> {
        let table_name = get_incremental_scan_base_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name, FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating DuckDB incremental scan base table: {}", table_name);

        self.with_connection(|conn| {
            conn.execute(&create_table_sql, []).map_err(|e| {
                error!("Failed to create DuckDB incremental scan table '{}': {}", table_name, e);
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })
    }

    /// 创建 tar manifest 表
    async fn create_tar_manifest_table_impl(&self) -> Result<()> {
        let table_name = get_tar_manifest_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                tar_path VARCHAR, \
                entry_path VARCHAR, \
                size UBIGINT, \
                ext VARCHAR, \
                mtime BIGINT, \
                mode UINTEGER, \
                storage_type VARCHAR, \
                is_dir BOOLEAN, \
                is_symlink BOOLEAN, \
                uid UINTEGER, \
                gid UINTEGER, \
                version_id VARCHAR, \
                tags VARCHAR\
            )",
            table_name
        );

        debug!("Creating DuckDB tar manifest table: {}", table_name);
        self.with_connection(|conn| {
            conn.execute(&create_table_sql, []).map_err(|e| {
                error!("Failed to create DuckDB tar manifest table '{}': {}", table_name, e);
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })
    }

    /// 删除所有以指定前缀开头的表，返回已删除表名列表
    pub async fn drop_tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let query = format!("SELECT table_name FROM information_schema.tables WHERE table_name LIKE ? ESCAPE '\\'",);

        let table_names: Vec<String> = self.with_connection(|conn| {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            let mut rows = stmt
                .query(params![format!("{}%", prefix)])
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let mut tables = Vec::new();
            while let Some(row) = rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                let name: String = row.get(0).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                tables.push(name);
            }
            Ok(tables)
        })?;

        for table_name in &table_names {
            self.drop_table_by_name(table_name).await?;
        }

        debug!("Dropped {} tables with prefix '{}'", table_names.len(), prefix);
        Ok(table_names)
    }

    /// 查询scan_state表，返回id=1的scan_state值
    /// 当记录不存在时返回错误
    pub async fn query_scan_state(&self) -> Result<u8> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let query = format!("SELECT scan_state FROM {} WHERE id = 1", table_name);

        self.with_connection(|conn| {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            let mut rows = stmt.query([]).map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            match rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                Some(row) => {
                    let scan_state: i32 = row.get(0).map_err(|e| DatabaseError::QueryError(e.to_string()))?;
                    Ok(scan_state as u8)
                }
                None => Err(DatabaseError::QueryError(
                    "No scan state record found for id=1".to_string(),
                )),
            }
        })
    }

    /// 同步插入scan_state表，id固定为1
    pub async fn insert_scan_state(&self, scan_state: u8) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let insert_sql = format!("INSERT OR REPLACE INTO {} (id, scan_state) VALUES (?, ?)", table_name);

        debug!("Inserting scan state: id=1, scan_state={}", scan_state);

        self.with_connection(|conn| {
            conn.execute(&insert_sql, params![1, scan_state as i32])
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            Ok(())
        })
    }

    async fn batch_delete_record(&self, table_name: &str, deleted_paths: &[String]) -> Result<()> {
        info!(
            "Batch deleting {} records from table: {}",
            deleted_paths.len(),
            table_name
        );

        if deleted_paths.is_empty() {
            debug!("No records to delete");
            return Ok(());
        }

        const BATCH_SIZE: usize = 1000;

        self.with_connection(|conn| {
            conn.execute("BEGIN TRANSACTION", []).map_err(|e| {
                DatabaseError::QueryError(format!("Failed to begin transaction for batch delete: {}", e))
            })?;

            let result = (|| -> Result<()> {
                for chunk in deleted_paths.chunks(BATCH_SIZE) {
                    let placeholders = vec!["?"; chunk.len()].join(", ");
                    let delete_query = format!("DELETE FROM {} WHERE relative_path IN ({})", table_name, placeholders);

                    let params: Vec<Box<dyn duckdb::ToSql>> = chunk
                        .iter()
                        .map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql>)
                        .collect();
                    let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

                    conn.execute(&delete_query, param_refs.as_slice()).map_err(|e| {
                        DatabaseError::QueryError(format!(
                            "Failed to batch delete {} records from '{}': {}",
                            chunk.len(),
                            table_name,
                            e
                        ))
                    })?;
                }
                Ok(())
            })();

            if let Err(e) = result {
                // DELETE 失败：回滚事务，避免部分提交
                let _ = conn.execute("ROLLBACK TRANSACTION", []);
                return Err(e);
            }

            conn.execute("COMMIT TRANSACTION", []).map_err(|e| {
                DatabaseError::QueryError(format!("Failed to commit transaction for batch delete: {}", e))
            })?;

            Ok(())
        })?;

        info!(
            "Successfully batch deleted {} records from table: {}",
            deleted_paths.len(),
            table_name
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl Database for DuckDBDatabase {
    /// 创建当前对象的Box克隆
    fn clone_box(&self) -> Box<dyn Database> {
        let mut new_db = DuckDBDatabase::new(self.config.clone(), &self.job_id);
        new_db.scan_temp_table_name = self.scan_temp_table_name.clone();
        new_db
            .cached_scan_state
            .store(self.cached_scan_state.load(Ordering::Relaxed), Ordering::Relaxed);
        Box::new(new_db)
    }

    async fn initialize(&self) -> Result<()> {
        self.create_table(SCAN_BASE_TABLE_BASE_NAME).await?;
        self.create_table(SCAN_STATE_TABLE_BASE_NAME).await?;
        self.insert_scan_state(0).await?;
        Ok(())
    }

    /// 通过 SELECT 1 测试连接
    async fn ping(&self) -> Result<()> {
        self.with_connection(|conn| {
            conn.execute("SELECT 1", []).map_err(|e| {
                let error_msg = format!("Database connection ping failed: {}", e);
                error!("{}", error_msg);
                DatabaseError::ConnectionError(error_msg)
            })?;
            Ok(())
        })
    }

    /// 根据表类型名称分派到对应的建表方法
    async fn create_table(&self, table_name: &str) -> Result<()> {
        match table_name {
            SCAN_BASE_TABLE_BASE_NAME => self.create_scan_base_table().await,
            SCAN_STATE_TABLE_BASE_NAME => self.create_scan_state_table().await,
            INCREMENTAL_SCAN_TABLE_BASE_NAME => self.create_incremental_scan_table().await,
            TAR_MANIFEST_TABLE_BASE_NAME => self.create_tar_manifest_table_impl().await,
            _ => Err(DatabaseError::UnsupportedType(format!("Unknown table: {}", table_name))),
        }
    }

    /// 批量插入增量记录
    async fn batch_insert_incremental_record(&self, messages: &[StorageEntryMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let table_name = get_incremental_scan_base_table_name(&self.job_id);

        let storage_records: Vec<IncrementalStorageEntryRecord> = messages
            .iter()
            .map(|message| IncrementalStorageEntryRecord::from_message(message))
            .collect();

        let record_count = storage_records.len();

        self.with_connection(|conn| {
            let mut appender = conn
                .appender(&table_name)
                .map_err(|e| DatabaseError::QueryError(format!("Failed to create appender: {}", e)))?;

            for record in &storage_records {
                let ext = record.ext.as_deref().unwrap_or("");
                let comment = record.comment.as_deref().unwrap_or("");
                let ino = record.ino.unwrap_or_default();
                let tags_json = record.tags.as_ref().and_then(|tags| serde_json::to_string(tags).ok());
                let file_handle_bytes = record.file_handle.as_ref().and_then(|s| hex::decode(s).ok());

                trace!("Inserting record: {:?}", record);

                appender
                    .append_row(params![
                        record.operation_type.clone(),
                        record.name.clone(),
                        record.relative_path.clone(),
                        record.size,
                        ext,
                        record.ctime,
                        record.mtime,
                        record.atime,
                        record.mode.unwrap_or_default(),
                        record.storage_type.clone(),
                        record.is_symlink as i32,
                        record.is_dir as i32,
                        record.is_regular_file as i32,
                        record.hard_links,
                        record.current_state as i32,
                        record.uid,
                        record.gid,
                        ino,
                        file_handle_bytes,
                        record.version_id.clone(),
                        tags_json,
                        record.version_count,
                        record.create_at,
                        comment
                    ])
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            }

            appender
                .flush()
                .map_err(|e| DatabaseError::QueryError(format!("Failed to flush appender: {}", e)))?;

            Ok(())
        })?;

        debug!("Successfully inserted {} events to incremental table", record_count);
        Ok(())
    }

    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let temp_table_name = generate_scan_temp_table_name();
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            temp_table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating DuckDB scan temporary table: {}", temp_table_name);

        self.with_connection(|conn| {
            conn.execute(&create_table_sql, []).map_err(|e| {
                error!(
                    "Failed to create DuckDB scan temporary table '{}': {}",
                    temp_table_name, e
                );
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })?;

        trace!("DuckDB scan temporary table '{}' created successfully", temp_table_name);
        self.scan_temp_table_name = Some(temp_table_name);
        Ok(())
    }

    async fn drop_scan_temporary_table(&mut self) -> Result<()> {
        if let Some(temp_table_name) = &self.scan_temp_table_name {
            let drop_table_sql = format!("DROP TABLE IF EXISTS {}", temp_table_name);

            debug!("Dropping DuckDB scan temporary table: {}", temp_table_name);
            self.with_connection(|conn| {
                conn.execute(&drop_table_sql, []).map_err(|e| {
                    error!(
                        "Failed to drop DuckDB scan temporary table '{}': {}",
                        temp_table_name, e
                    );
                    DatabaseError::QueryError(e.to_string())
                })?;
                Ok(())
            })?;

            debug!("DuckDB scan temporary table '{}' dropped successfully", temp_table_name);

            self.scan_temp_table_name = None;
        } else {
            debug!("No temporary table to drop");
        }
        Ok(())
    }

    /// 删除指定表（IF EXISTS）
    async fn drop_table_by_name(&self, table_name: &str) -> Result<()> {
        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", table_name);
        debug!("Dropping DuckDB table: {}", table_name);
        self.with_connection(|conn| {
            conn.execute(&drop_table_sql, []).map_err(|e| {
                error!("Failed to drop DuckDB table '{}': {}", table_name, e);
                DatabaseError::QueryError(e.to_string())
            })?;
            Ok(())
        })?;
        Ok(())
    }

    /// 通用批量插入方法，用于向指定表插入StorageEntry记录

    async fn batch_insert_temp_record(&self, records: &[Arc<EntryEnum>]) -> Result<()> {
        let temp_table_name = self.scan_temp_table_name.as_deref().ok_or_else(|| {
            error!("Scan temporary table name is None, cannot insert records");
            DatabaseError::QueryError("Scan temporary table name is None".to_string())
        })?;

        if records.is_empty() {
            debug!("No events to insert ");
            return Ok(());
        }

        let current_state = self.get_cached_scan_state().await?;

        let storage_records: Vec<StorageEntryRecord> = records
            .iter()
            .map(|entry| StorageEntryRecord::from_entry_enum(entry.as_ref(), current_state))
            .collect();

        let record_count = storage_records.len();

        self.with_connection(|conn| {
            let mut appender = conn
                .appender(temp_table_name)
                .map_err(|e| DatabaseError::QueryError(format!("Failed to create appender: {}", e)))?;

            for record in &storage_records {
                let ext = record.ext.as_deref().unwrap_or("");
                let tags_json = record.tags.as_ref().and_then(|tags| serde_json::to_string(tags).ok());
                let file_handle_value = record.file_handle.as_ref().map(|s| s.as_bytes());

                trace!("Inserting record: {:?}", record);
                appender
                    .append_row(params![
                        record.name.clone(),
                        record.relative_path.clone(),
                        record.size,
                        ext,
                        record.ctime,
                        record.mtime,
                        record.atime,
                        record.mode.unwrap_or_default(),
                        record.storage_type.clone(),
                        record.is_symlink,
                        record.is_dir,
                        record.is_regular_file,
                        record.hard_links,
                        record.current_state as i32,
                        record.uid,
                        record.gid,
                        record.ino,
                        file_handle_value,
                        record.version_id.clone(),
                        tags_json,
                        record.version_count
                    ])
                    .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            }

            appender
                .flush()
                .map_err(|e| DatabaseError::QueryError(format!("Failed to flush appender: {}", e)))?;
            Ok(())
        })?;

        debug!("Successfully inserted {} events to temporary table", record_count);
        Ok(())
    }

    async fn batch_insert_base_record(&self, records: &[Arc<EntryEnum>]) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        debug!("Inserting {} records to base table {}", records.len(), base_table_name);

        if records.is_empty() {
            debug!("No events to insert");
            return Ok(());
        }

        let current_state = self.get_cached_scan_state().await?;

        let storage_records: Vec<StorageEntryRecord> = records
            .iter()
            .map(|entry| StorageEntryRecord::from_entry_enum(entry.as_ref(), current_state))
            .collect();

        let record_count = storage_records.len();

        self.with_connection(|conn| {
            // 使用 INSERT OR REPLACE 处理重复 key（如 rename 后同路径再次 insert）
            let sql = format!(
                "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
                base_table_name, *FILE_SCAN_COLUMNS_LIST, *FILE_SCAN_COLUMNS_PLACEHOLDERS,
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| DatabaseError::QueryError(format!("Failed to prepare insert: {}", e)))?;

            for record in &storage_records {
                let ext = record.ext.as_deref().unwrap_or("");
                let mode = record.mode.unwrap_or_default();
                let is_symlink = record.is_symlink as i32;
                let is_dir = record.is_dir as i32;
                let is_regular_file = record.is_regular_file as i32;
                let ino = record.ino.unwrap_or_default();
                let tags_json = record.tags.as_ref().and_then(|tags| serde_json::to_string(tags).ok());
                let file_handle_value = record.file_handle.as_ref().map(|s| s.as_bytes());

                trace!("Inserting record: {:?}", record);
                // 参数顺序必须与 FILE_SCAN_COLUMNS_LIST 一致：
                // ...tags, version_count
                stmt.execute(params![
                    record.name.clone(),
                    record.relative_path.clone(),
                    record.size,
                    ext,
                    record.ctime,
                    record.mtime,
                    record.atime,
                    mode,
                    record.storage_type.clone(),
                    is_symlink,
                    is_dir,
                    is_regular_file,
                    record.hard_links,
                    record.current_state as i32,
                    record.uid,
                    record.gid,
                    ino,
                    file_handle_value,
                    record.version_id.clone(),
                    tags_json,
                    record.version_count
                ])
                .map_err(|e| {
                    DatabaseError::QueryError(format!(
                        "Failed to insert record with relative_path '{}': {}",
                        record.relative_path, e
                    ))
                })?;
            }

            Ok(())
        })?;

        debug!("Successfully inserted {} events to base table", record_count);
        Ok(())
    }

    async fn update_base_record(&self, record: &Arc<EntryEnum>) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        info!(
            "Updating record in base table {} for relative_path {:?}",
            table_name,
            record.get_relative_path()
        );

        let current_state = self.get_cached_scan_state().await?;

        let storage_record = StorageEntryRecord::from_entry_enum(record.as_ref(), current_state);
        let ext = storage_record.ext.as_deref().unwrap_or("");

        self.with_connection(|conn| {
            let update_sql = format!(
                "UPDATE {} SET
                name = ?,
                size = ?,
                ext = ?,
                ctime = ?,
                mtime = ?,
                atime = ?,
                mode = ?,
                storage_type = ?,
                is_symlink = ?,
                is_dir = ?,
                is_regular_file = ?,
                hard_links = ?,
                current_state = ?,
                uid = ?,
                gid = ?,
                ino = ?,
                file_handle = ?,
                tags = ?,
                version_count = ?
                WHERE relative_path = ?",
                table_name
            );

            info!(
                "Executing update SQL: {} with relative_path {:?}",
                update_sql, storage_record.relative_path
            );

            let tags_json = storage_record
                .tags
                .as_ref()
                .and_then(|tags| serde_json::to_string(tags).ok());
            let file_handle_value = storage_record.file_handle.as_ref();

            conn.execute(
                &update_sql,
                params![
                    storage_record.name,
                    storage_record.size,
                    ext,
                    storage_record.ctime,
                    storage_record.mtime,
                    storage_record.atime,
                    storage_record.mode.unwrap_or_default(),
                    storage_record.storage_type,
                    storage_record.is_symlink,
                    storage_record.is_dir,
                    storage_record.is_regular_file,
                    storage_record.hard_links,
                    storage_record.current_state,
                    storage_record.uid,
                    storage_record.gid,
                    storage_record.ino,
                    file_handle_value,
                    tags_json,
                    storage_record.version_count.unwrap_or_default(),
                    storage_record.relative_path
                ],
            )
            .map_err(|e| DatabaseError::QueryError(format!("Failed to update record: {}", e)))?;

            info!(
                "Successfully updated record for relative_path {:?}",
                storage_record.relative_path
            );
            Ok(())
        })
    }

    async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.batch_delete_record(&table_name, deleted_paths).await
    }

    /// 切换 scan_state（0↔1）
    async fn switch_scan_state(&self) -> Result<()> {
        let current_state = self.get_cached_scan_state().await?;
        let new_state = 1 - current_state;

        self.insert_scan_state(new_state).await?;
        self.cached_scan_state.store(new_state, Ordering::Relaxed);

        debug!("Switched scan state: {} -> {}", current_state, new_state);

        Ok(())
    }

    /// 将临时表记录合并到主表（INSERT OR REPLACE INTO SELECT），可排除指定路径
    async fn insert_temp_to_base_table(&self, excluded_paths: &[(String, String)]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_ref()
            .ok_or_else(|| DatabaseError::UnsupportedType("No temporary table available".to_string()))?;
        let base_table_name = get_scan_base_table_name(&self.job_id);

        debug!(
            "Inserting data from temporary table {} to base table {} (excluded: {} paths)",
            temp_table_name,
            base_table_name,
            excluded_paths.len()
        );

        let excluded = excluded_paths.to_vec();
        let temp_name = temp_table_name.clone();

        self.with_connection(move |conn| {
            if excluded.is_empty() {
                let insert_sql = format!(
                    "INSERT OR REPLACE INTO {} ({}) SELECT {} FROM {}",
                    base_table_name, *FILE_SCAN_COLUMNS_LIST, *FILE_SCAN_COLUMNS_LIST, temp_name
                );
                conn.execute(&insert_sql, []).map_err(|e| {
                    DatabaseError::QueryError(format!("Failed to insert data from temp to base table: {}", e))
                })?;
            } else {
                // DuckDB: 使用 WHERE NOT IN 过滤排除路径
                let placeholders: Vec<String> = excluded.iter().map(|_| "(?, ?)".to_string()).collect();
                let insert_sql = format!(
                    "INSERT OR REPLACE INTO {} ({}) SELECT {} FROM {} \
                     WHERE (relative_path, version_id) NOT IN (VALUES {})",
                    base_table_name,
                    *FILE_SCAN_COLUMNS_LIST,
                    *FILE_SCAN_COLUMNS_LIST,
                    temp_name,
                    placeholders.join(", ")
                );

                let mut stmt = conn
                    .prepare(&insert_sql)
                    .map_err(|e| DatabaseError::QueryError(format!("Failed to prepare insert: {}", e)))?;

                let params: Vec<Box<dyn duckdb::types::ToSql>> = excluded
                    .iter()
                    .flat_map(|(path, vid)| {
                        vec![
                            Box::new(path.clone()) as Box<dyn duckdb::types::ToSql>,
                            Box::new(vid.clone()) as Box<dyn duckdb::types::ToSql>,
                        ]
                    })
                    .collect();

                let param_refs: Vec<&dyn duckdb::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

                stmt.execute(param_refs.as_slice()).map_err(|e| {
                    DatabaseError::QueryError(format!("Failed to insert data from temp to base table: {}", e))
                })?;
            }

            info!(
                "Successfully inserted records from {} to {}",
                temp_name, base_table_name
            );
            Ok(())
        })
    }

    /// 查询在最新扫描中新增的文件
    async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = EntryEnum> + Send>> {
        let records = self
            .detect_items("new", |temp_table, base_table, strategy| {
                let (vc_join, vc_expr) = generate_duckdb_version_count_join(base_table);
                strategy.build_detect_new_sql(temp_table, base_table, &vc_join, &vc_expr)
            })
            .await?;

        let entries: Vec<EntryEnum> = records.into_iter().map(|record| record.to_entry_enum()).collect();
        Ok(Box::new(entries.into_iter()))
    }

    /// 查询在最新扫描中发生变更的文件，按 `ChangeKind` 分三类返回
    async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (EntryEnum, ChangeKind)> + Send>> {
        // 三类互斥条件的查询并发执行（DuckDB 每次 with_connection 新建连接，可安全并发）
        let (data_only, metadata_only, both) = tokio::try_join!(
            self.detect_items("changed(data)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::DataOnly)
            }),
            self.detect_items("changed(meta)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::MetadataOnly)
            }),
            self.detect_items("changed(both)", |temp, base, strategy| {
                strategy.build_detect_changed_sql(temp, base, ChangeKind::Both)
            }),
        )?;

        let iter = data_only
            .into_iter()
            .map(|r| (r.to_entry_enum(), ChangeKind::DataOnly))
            .chain(
                metadata_only
                    .into_iter()
                    .map(|r| (r.to_entry_enum(), ChangeKind::MetadataOnly)),
            )
            .chain(both.into_iter().map(|r| (r.to_entry_enum(), ChangeKind::Both)));
        Ok(Box::new(iter))
    }

    /// 查询在上一次扫描中存在但在最新扫描中缺失的文件
    ///
    /// 使用批量查询替代逐条 file_handle 查询（消除 N+1 问题），
    /// 通过 classify_deletion_status 纯函数分类 Deleted / Renamed 状态。
    async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let current_state = self.get_cached_scan_state().await?;
        debug!("During detect_deleted_items, current_state is {}", current_state);

        // 第一步：查询所有 old-state 记录
        let query = DuckDBJoinStrategy::build_detect_deleted_sql(&base_table_name);

        let rows = self.with_connection(|conn| {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;
            let mut rows = stmt
                .query(params![1 - current_state as i32])
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let mut records = Vec::new();
            while let Some(row) = rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                let record = DuckDBDatabase::from_row(&row)?;
                records.push(record);
            }
            Ok(records)
        })?;

        debug!("Found {} old-state entries", rows.len());
        if rows.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        // 第二步：分离有/无 file_handle 的记录
        let mut no_fh_records: Vec<StorageEntryRecord> = Vec::new();
        let mut fh_records: Vec<StorageEntryRecord> = Vec::new();
        let mut unique_fh_set: HashSet<String> = HashSet::new();

        for record in rows {
            if let Some(fh) = &record.file_handle {
                unique_fh_set.insert(fh.clone());
                fh_records.push(record);
            } else {
                no_fh_records.push(record);
            }
        }

        // 第三步：有 file_handle → 分批批量查询
        let mut fh_groups: HashMap<String, Vec<StorageEntryRecord>> = HashMap::new();

        if !fh_records.is_empty() {
            let unique_fh_list: Vec<String> = unique_fh_set.into_iter().collect();

            // 每批 10K 个 fh，避免 SQL 过长
            const BATCH_SIZE: usize = 10_000;
            for chunk in unique_fh_list.chunks(BATCH_SIZE) {
                let batch_query = DuckDBJoinStrategy::build_batch_fh_query_sql(&base_table_name, chunk.len());

                let batch_rows = self.with_connection(|conn| {
                    let mut stmt = conn
                        .prepare(&batch_query)
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

                    // file_handle 在 DuckDB 中是 BLOB 类型，传入 hex 字符串参数
                    let params: Vec<Box<dyn duckdb::ToSql>> = chunk
                        .iter()
                        .map(|fh| Box::new(fh.clone()) as Box<dyn duckdb::ToSql>)
                        .collect();
                    let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();

                    let mut rows = stmt
                        .query(param_refs.as_slice())
                        .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

                    let mut records = Vec::new();
                    while let Some(row) = rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                        let record = DuckDBDatabase::from_row(&row)?;
                        records.push(record);
                    }
                    Ok(records)
                })?;

                for row in batch_rows {
                    if let Some(fh) = &row.file_handle {
                        fh_groups.entry(fh.clone()).or_default().push(row);
                    }
                }
            }
        }

        // 第四步：使用纯函数分类
        let deletion_statuses = classify_deletion_status(no_fh_records, fh_records, &fh_groups);

        debug!("Found {} deleted or renamed entries", deletion_statuses.len());

        Ok(Box::new(deletion_statuses.into_iter()))
    }

    /// 获取指定表的记录总数
    async fn get_count(&self, table_name: &str) -> Result<u64> {
        let full_table_name = match table_name {
            SCAN_BASE_TABLE_BASE_NAME => get_scan_base_table_name(&self.job_id),
            SCAN_STATE_TABLE_BASE_NAME => get_scan_state_table_name(&self.job_id),
            INCREMENTAL_SCAN_TABLE_BASE_NAME => get_incremental_scan_base_table_name(&self.job_id),
            _ => format!("{}_{}", table_name, sanitize_job_id(&self.job_id)),
        };
        let query = format!("SELECT COUNT(*) FROM {}", full_table_name);

        self.with_connection(|conn| {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let count: u64 = stmt
                .query_row([], |row| row.get(0))
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            Ok(count)
        })
    }

    async fn query_storage_entry(
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>, tx: mpsc::Sender<EntryEnum>,
    ) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let mut where_conditions = Vec::new();
        let mut query_params: Vec<Box<dyn duckdb::ToSql + Send>> = Vec::new();

        if let Some(dir_val) = is_dir {
            where_conditions.push("is_dir = ?".to_string());
            query_params.push(Box::new(dir_val));
        }

        if let Some(symlink_val) = is_symlink {
            where_conditions.push("is_symlink = ?".to_string());
            query_params.push(Box::new(symlink_val));
        }

        if let Some(ext_val) = extension {
            where_conditions.push("ext ILIKE ?".to_string());
            query_params.push(Box::new(format!("%{}", ext_val)));
        }

        let where_clause = if where_conditions.is_empty() {
            "".to_string()
        } else {
            format!(" WHERE {}", where_conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {} FROM {}{} ORDER BY relative_path, version_id",
            *FILE_SCAN_COLUMNS_LIST, base_table_name, where_clause
        );

        let config = self.config.clone();
        let tx_clone = tx.clone();

        tokio::task::spawn_blocking(move || {
            let path = config
                .get_path()
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let conn = Connection::open(&path).map_err(|e| {
                let error_msg = format!("Failed to connect to DuckDB at {}: {}", path, e);
                DatabaseError::ConnectionError(error_msg)
            })?;

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            let param_refs: Vec<&dyn duckdb::ToSql> =
                query_params.iter().map(|p| p.as_ref() as &dyn duckdb::ToSql).collect();
            let mut rows = stmt
                .query(param_refs.as_slice())
                .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

            while let Some(row) = rows.next().map_err(|e| DatabaseError::QueryError(e.to_string()))? {
                let record = DuckDBDatabase::from_row(&row)?;
                let entry_enum = record.to_entry_enum();

                if let Err(err) = tx_clone.blocking_send(entry_enum) {
                    return Err(DatabaseError::QueryError(format!(
                        "Failed to send storage entry: {}",
                        err
                    )));
                }
            }

            Ok(())
        })
        .await
        .map_err(|e| DatabaseError::QueryError(format!("spawn_blocking panicked: {e}")))??;

        Ok(())
    }

    async fn create_tar_manifest_table(&self) -> Result<()> {
        self.create_tar_manifest_table_impl().await
    }

    async fn batch_insert_tar_manifest(&self, records: &[TarManifestRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let table_name = get_tar_manifest_table_name(&self.job_id);

        self.with_connection(|conn| {
            let mut appender = conn
                .appender(&table_name)
                .map_err(|e| DatabaseError::QueryError(format!("Failed to create appender: {}", e)))?;

            for record in records {
                let ext = record.ext.as_deref().unwrap_or("");
                let tags_json = record.tags.as_deref().unwrap_or("");
                appender
                    .append_row(duckdb::params![
                        record.tar_path,
                        record.entry_path,
                        record.size,
                        ext,
                        record.mtime,
                        record.mode.unwrap_or(0),
                        record.storage_type,
                        record.is_dir,
                        record.is_symlink,
                        record.uid.unwrap_or(0),
                        record.gid.unwrap_or(0),
                        record.version_id,
                        tags_json,
                    ])
                    .map_err(|e| DatabaseError::QueryError(format!("Failed to append tar manifest record: {}", e)))?;
            }

            appender
                .flush()
                .map_err(|e| DatabaseError::QueryError(format!("Failed to flush appender: {}", e)))?;
            Ok(())
        })
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        self.with_connection(|conn| {
            let mut stmt = conn
                .prepare("SELECT count(*) FROM information_schema.tables WHERE table_name = ?")
                .map_err(|e| DatabaseError::QueryError(format!("Failed to check table existence: {}", e)))?;
            let count: i64 = stmt
                .query_row(params![table_name], |row| row.get(0))
                .map_err(|e| DatabaseError::QueryError(format!("Failed to check table existence: {}", e)))?;
            Ok(count > 0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_strategy_from_file_handle_status() {
        // 两表都无 fh 且有数据 → Path
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(0, 0, 100, 50),
            DuckDBJoinStrategy::Path
        );

        // temp 有 fh → FileHandle
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(10, 0, 100, 50),
            DuckDBJoinStrategy::FileHandle
        );

        // base 有 fh → FileHandle
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(0, 10, 100, 50),
            DuckDBJoinStrategy::FileHandle
        );

        // 两表都有 fh → FileHandle
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(10, 10, 100, 50),
            DuckDBJoinStrategy::FileHandle
        );

        // base 为空 → FileHandle（即使 fh 全为 0）
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(0, 0, 0, 50),
            DuckDBJoinStrategy::FileHandle
        );

        // temp 为空 → FileHandle
        assert_eq!(
            DuckDBJoinStrategy::from_file_handle_status(0, 0, 100, 0),
            DuckDBJoinStrategy::FileHandle
        );
    }

    #[test]
    fn test_detect_new_sql_path_mode() {
        let (vc_join, vc_expr) = generate_duckdb_version_count_join("base_job1");
        let sql = DuckDBJoinStrategy::Path.build_detect_new_sql("temp_abc", "base_job1", &vc_join, &vc_expr);
        assert!(sql.contains("LEFT JOIN base_job1 f ON t.relative_path = f.relative_path"));
        assert!(sql.contains("WHERE f.relative_path IS NULL"));
        assert!(sql.contains("version_id"));
        // Path 模式 JOIN 条件不使用 file_handle（SELECT 列表中仍有 t.file_handle）
        assert!(!sql.contains("ON t.file_handle"));
    }

    #[test]
    fn test_detect_new_sql_fh_mode() {
        let (vc_join, vc_expr) = generate_duckdb_version_count_join("base_job1");
        let sql = DuckDBJoinStrategy::FileHandle.build_detect_new_sql("temp_abc", "base_job1", &vc_join, &vc_expr);
        assert!(sql.contains("LEFT JOIN base_job1 f ON t.file_handle = f.file_handle"));
        assert!(sql.contains("WHERE f.file_handle IS NULL"));
    }

    #[test]
    fn test_detect_changed_sql_path_mode_data_only() {
        let sql = DuckDBJoinStrategy::Path.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::DataOnly);
        assert!(sql.contains("JOIN base_job1 f ON t.relative_path = f.relative_path AND t.version_id = f.version_id"));
        assert!(sql.contains("t.size != f.size"));
        assert!(sql.contains("t.version_id = '' AND t.mtime != f.mtime"));
        // DataOnly 要求 mode/uid/gid 均未变
        assert!(sql.contains("t.mode IS NOT DISTINCT FROM f.mode"));
        assert!(sql.contains("t.uid IS NOT DISTINCT FROM f.uid"));
        assert!(sql.contains("t.gid IS NOT DISTINCT FROM f.gid"));
    }

    #[test]
    fn test_detect_changed_sql_path_mode_metadata_only() {
        let sql = DuckDBJoinStrategy::Path.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::MetadataOnly);
        // MetadataOnly：size 未变 + mtime 未变（受 version_id 约束）+ 属性变了
        assert!(sql.contains("t.size = f.size"));
        assert!(sql.contains("t.mode IS DISTINCT FROM f.mode"));
        assert!(sql.contains("t.uid IS DISTINCT FROM f.uid"));
        assert!(sql.contains("t.gid IS DISTINCT FROM f.gid"));
    }

    #[test]
    fn test_detect_changed_sql_path_mode_both() {
        let sql = DuckDBJoinStrategy::Path.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::Both);
        assert!(sql.contains("t.size != f.size"));
        assert!(sql.contains("t.mode IS DISTINCT FROM f.mode"));
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_data_only() {
        let sql =
            DuckDBJoinStrategy::FileHandle.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::DataOnly);
        // 路径等值条件：仅对路径未变的条目判 Changed，避免把 rename+changed 误判为纯 Changed
        assert!(sql.contains("t.file_handle = f.file_handle"));
        assert!(sql.contains("t.version_id = f.version_id"));
        assert!(sql.contains("t.relative_path = f.relative_path"));
        assert!(sql.contains("t.mtime != f.mtime"));
        assert!(sql.contains("t.size != f.size"));
        assert!(sql.contains("t.mode IS NOT DISTINCT FROM f.mode"));
        assert!(!sql.contains("version_id = ''"));
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_metadata_only() {
        let sql =
            DuckDBJoinStrategy::FileHandle.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::MetadataOnly);
        assert!(sql.contains("t.mtime = f.mtime"));
        assert!(sql.contains("t.size = f.size"));
        assert!(sql.contains("t.mode IS DISTINCT FROM f.mode"));
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_both() {
        let sql = DuckDBJoinStrategy::FileHandle.build_detect_changed_sql("temp_abc", "base_job1", ChangeKind::Both);
        assert!(sql.contains("t.mtime != f.mtime"));
        assert!(sql.contains("t.mode IS DISTINCT FROM f.mode"));
    }

    #[test]
    fn test_detect_deleted_sql() {
        let sql = DuckDBJoinStrategy::build_detect_deleted_sql("base_job1");
        assert!(sql.contains("FROM base_job1"));
        assert!(sql.contains("WHERE current_state = ?"));
    }

    #[test]
    fn test_batch_fh_query_sql() {
        let sql = DuckDBJoinStrategy::build_batch_fh_query_sql("base_job1", 3);
        assert!(sql.contains("WHERE file_handle IN (?, ?, ?)"));
        assert!(sql.contains("ORDER BY file_handle, ctime, version_id"));
    }

    #[test]
    fn test_version_count_join_sql() {
        let (join_clause, select_expr) = generate_duckdb_version_count_join("base_job1");
        assert!(join_clause.contains("LEFT JOIN (SELECT relative_path, COUNT(*) as cnt FROM base_job1"));
        assert!(select_expr.contains("CAST(t.version_count - COALESCE(vc.cnt, 0) AS INTEGER)"));
    }
}
