//! `ClickHouse`数据库实现模块
//!
//! 提供与ClickHouse数据库交互的所有操作功能，包括表的创建、查询、插入和删除等。

// 标准库
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

// 外部crate
use async_trait::async_trait;
use clickhouse::Client;
use data_mover::{ChangeKind, EntryEnum, StorageEntryMessage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

// 内部模块
use crate::common::{
    DeletionStatus, FILE_SCAN_COLUMNS_LIST, FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX,
    FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX, classify_deletion_status,
};
use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::{Database, IncrementalStorageEntryRecord, QueryResult, StorageEntryRecord, TarManifestRecord};
use crate::{
    INCREMENTAL_SCAN_TABLE_BASE_NAME, SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME,
    TAR_MANIFEST_TABLE_BASE_NAME, generate_scan_temp_table_name, generate_version_count_join_sql,
    get_incremental_scan_base_table_name, get_scan_base_table_name, get_scan_state_table_name,
    get_tar_manifest_table_name,
};
use utils::sanitize_job_id;

/// `ClickHouse`数据库实现
///
/// 提供与ClickHouse数据库交互的所有操作功能，包括表的创建、数据的插入、查询和删除等。
pub struct ClickHouseDatabase {
    /// ClickHouse同步客户端，用于执行同步操作如查询和简单插入
    sync_client: Client,
    /// ClickHouse异步客户端，配置了异步插入功能，用于大批量数据插入
    async_client: Client,
    /// 任务ID，用于在多任务环境下表命名的隔离和区分
    job_id: String,
    /// 当前使用的临时扫描表名，可选值，创建临时表时设置，删除时清空
    pub scan_temp_table_name: Option<String>,
    /// 缓存的 `scan_state` 值（255 = 未缓存），避免每次 `batch_insert` 都查询数据库
    cached_scan_state: AtomicU8,
}

/// 生成文件扫描列定义的宏
///
/// 支持无参调用（基础列）或带 `$prefix` / `$suffix` 的调用（在基础列前后拼接额外列）。
#[macro_export]
macro_rules! file_scan_base_columns {
    // 内部宏，包含基础列定义
    (@base) => {
        r"
    name String,
    relative_path String,
    size UInt64,
    ext Nullable(String),
    ctime DateTime64(9),
    mtime DateTime64(9),
    atime DateTime64(9),
    mode Nullable(UInt32),
    storage_type String,
    is_symlink Bool,
    is_dir Bool,
    is_regular_file Bool,
    hard_links UInt32,
    current_state UInt8,
    uid Nullable(UInt32),
    gid Nullable(UInt32),
    ino Nullable(UInt64),
    file_handle Nullable(String),
    version_id String,
    tags Nullable(String),
    version_count Nullable(UInt32),
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
const FILE_SCAN_COLUMNS_DEFINITION: &str = file_scan_base_columns!();

/// 文件增量扫描记录的标准列定义
///
/// 定义了存储文件增量扫描结果的表结构，包含操作类型、文件路径、大小、扩展名、时间戳、权限等信息。
/// 增量扫描表使用此结构定义。
const FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION: &str = file_scan_base_columns!(
    r#"
    operation_type String,
"#,
    r#"
    create_at DateTime64(9),
    comment Nullable(String),
"#
);

/// Memory 排除表记录（优化 3）
#[derive(clickhouse::Row, Serialize, Deserialize)]
struct ExcludeRecord {
    relative_path: String,
    version_id: String,
}

/// JOIN 策略：path-based 或 file_handle-based
///
/// 决定增量扫描对比查询使用 `relative_path+version_id` 还是 `file_handle` 作为 JOIN 键。
/// 当两表的 `file_handle` 全为 NULL 时使用 Path 模式，否则使用 `FileHandle` 模式。
enum JoinStrategy {
    /// 基于 `relative_path` + `version_id` 的对比
    Path,
    /// 基于 `file_handle` 的对比
    FileHandle,
}

impl JoinStrategy {
    /// 根据临时表和主表的 `file_handle` 统计信息决定 JOIN 策略
    fn from_file_handle_status(
        temp_non_null: usize, base_non_null: usize, base_total: usize, temp_total: usize,
    ) -> Self {
        if temp_non_null == 0 && base_non_null == 0 && base_total > 0 && temp_total > 0 {
            Self::Path
        } else {
            Self::FileHandle
        }
    }

    /// 构建 `detect_new` SQL：temp 中存在但 base 中不存在的记录
    ///
    /// `version_count_join` 和 `version_count_expr` 由 `generate_version_count_join_sql!` 生成
    fn build_detect_new_sql(
        &self, temp: &str, base: &str, version_count_join: &str, version_count_expr: &str,
    ) -> String {
        let columns = &*FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX;
        match self {
            Self::Path => format!(
                "SELECT {columns}, {version_count_expr} FROM {temp} as t \
                 {version_count_join} \
                 WHERE (t.relative_path, t.version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {base}) \
                 ORDER BY t.relative_path, t.version_id"
            ),
            Self::FileHandle => format!(
                "SELECT {columns}, {version_count_expr} FROM {temp} as t \
                 {version_count_join} \
                 WHERE t.file_handle NOT IN \
                 (SELECT file_handle FROM {base} WHERE file_handle IS NOT NULL) \
                 ORDER BY t.relative_path, t.version_id"
            ),
        }
    }

    /// 构建 `detect_changed` SQL：temp 与 base 都存在但存在变更
    ///
    /// 根据 `ChangeKind` 生成不同 WHERE 子句，三种 kind 互斥：
    /// - `DataOnly`：size 或 mtime 变了；mode/uid/gid 均未变
    /// - `MetadataOnly`：size + mtime 均未变；mode/uid/gid 至少一项变了（chmod/chown）
    /// - `Both`：内容和属性都变了
    fn build_detect_changed_sql(&self, temp: &str, base: &str, kind: ChangeKind) -> String {
        let t_columns = &*FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX;
        let data_changed = "(t.size != f.size OR t.mtime != f.mtime)";
        let data_unchanged = "(t.size = f.size AND t.mtime = f.mtime)";
        // mode/uid/gid 在 S3 等协议中为 Nullable 且常常为 NULL；ClickHouse 三值逻辑下
        // `NULL = NULL` 返回 NULL（WHERE 视为 false），会让所有 ChangeKind 都不匹配。
        // 改用显式 NULL-safe 等价：`a = b OR (a IS NULL AND b IS NULL)`。
        // 此形式不依赖任何 sentinel，不会把真实值（如 mode=0）误等同于 NULL，
        // 也不依赖"两侧来自同一后端"这种隐含不变量。
        let mode_eq = "(t.mode = f.mode OR (t.mode IS NULL AND f.mode IS NULL))";
        let uid_eq = "(t.uid = f.uid OR (t.uid IS NULL AND f.uid IS NULL))";
        let gid_eq = "(t.gid = f.gid OR (t.gid IS NULL AND f.gid IS NULL))";
        let meta_changed = format!("(NOT {mode_eq} OR NOT {uid_eq} OR NOT {gid_eq})");
        let meta_unchanged = format!("({mode_eq} AND {uid_eq} AND {gid_eq})");

        let kind_filter = match kind {
            ChangeKind::DataOnly => format!("{data_changed} AND {meta_unchanged}"),
            ChangeKind::MetadataOnly => format!("{data_unchanged} AND {meta_changed}"),
            ChangeKind::Both => format!("{data_changed} AND {meta_changed}"),
        };

        match self {
            Self::Path => format!(
                "SELECT {t_columns} FROM {temp} t \
                 JOIN (SELECT relative_path, version_id, size, mtime, mode, uid, gid, is_dir \
                       FROM {base} \
                       WHERE (relative_path, version_id) IN \
                             (SELECT relative_path, version_id FROM {temp}) \
                       ORDER BY relative_path, version_id \
                       LIMIT 1 BY (relative_path, version_id) \
                 ) f ON t.relative_path = f.relative_path AND t.version_id = f.version_id \
                 WHERE {kind_filter} AND f.is_dir = 0 \
                 ORDER BY t.relative_path, t.version_id"
            ),
            Self::FileHandle => format!(
                "SELECT {t_columns} FROM {temp} t \
                 JOIN (SELECT file_handle, relative_path, size, mtime, mode, uid, gid, is_dir \
                       FROM {base} \
                       WHERE file_handle IN \
                             (SELECT file_handle FROM {temp} WHERE file_handle IS NOT NULL) \
                       ORDER BY file_handle, relative_path \
                       LIMIT 1 BY (file_handle, relative_path) \
                 ) f ON t.file_handle = f.file_handle \
                   AND t.relative_path = f.relative_path \
                 WHERE {kind_filter} AND f.is_dir = 0 \
                 ORDER BY t.relative_path, t.version_id"
            ),
        }
    }

    /// 构建 `detect_deleted` SQL：base 中 old-state 的记录
    /// 使用 FINAL 确保 `ReplacingMergeTree` 去重后再按 `current_state` 过滤
    fn build_detect_deleted_sql(base: &str) -> String {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        format!(
            "SELECT {columns} FROM {base} FINAL \
             WHERE current_state = ? \
             ORDER BY relative_path, version_id"
        )
    }

    /// 构建批量 `file_handle` 查询 SQL（优化 2）
    fn build_batch_fh_query_sql(base: &str, batch_size: usize) -> String {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        let placeholders = vec!["?"; batch_size].join(", ");
        format!(
            "SELECT {columns} FROM {base} \
             WHERE file_handle IN ({placeholders}) \
             ORDER BY file_handle, ctime, version_id \
             LIMIT 1 BY (relative_path, version_id)"
        )
    }
}

/// 将 `StorageEntryRecord` 列表转换为 `EntryEnum` 迭代器
fn records_to_entry_iter(records: Vec<StorageEntryRecord>) -> Box<dyn Iterator<Item = EntryEnum> + Send> {
    Box::new(records.into_iter().map(|r| r.to_entry_enum()))
}

/// 构建基础 `ClickHouse` 客户端（含 url/database/username/password）
fn build_base_client(config: &ClickHouseConfig) -> Client {
    let mut client = Client::default()
        .with_url(&config.dsn)
        .with_database(config.database.clone())
        .with_user(config.username.clone());
    if let Some(password) = &config.password {
        client = client.with_password(password);
    }
    client
}

impl ClickHouseDatabase {
    /// 创建新的`ClickHouse`数据库实例
    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Self {
        let sync_client = build_base_client(config);
        let async_client = build_base_client(config)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");

        Self {
            sync_client,
            async_client,
            job_id: job_id.to_string(),
            scan_temp_table_name: None,
            cached_scan_state: AtomicU8::new(255),
        }
    }

    /// 执行SQL语句，支持参数化查询
    ///
    /// 注意：由于ClickHouse execute API的限制，当前无法获取受影响的行数
    pub(crate) async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        debug!("Executing ClickHouse statement: {}", sql);

        let mut query = self.sync_client.query(sql);

        // 绑定参数
        for param in params {
            if let Some(s) = param.as_str() {
                query = query.bind(s);
            } else if let Some(n) = param.as_i64() {
                query = query.bind(n);
            } else if let Some(b) = param.as_bool() {
                query = query.bind(b);
            } else {
                query = query.bind(param.to_string());
            }
        }

        query.execute().await.map_err(DatabaseError::ClickHouseError)?;

        Ok(QueryResult {
            rows: Vec::new(),
            affected_rows: 0, // ClickHouse execute返回()，无法获取affected_rows
            last_insert_id: None,
        })
    }

    /// 创建主扫描表（ReplacingMergeTree，按 `relative_path` + `version_id` 排序）
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} ({FILE_SCAN_COLUMNS_DEFINITION}) ENGINE = ReplacingMergeTree() ORDER BY (relative_path, version_id) SETTINGS allow_nullable_key = 1"
        );

        info!("Creating ClickHouse scan base table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 创建扫描状态表（id=1, `scan_state` 0/1）
    pub async fn create_scan_state_table(&self) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (id UInt8, scan_state UInt8) ENGINE = ReplacingMergeTree() ORDER BY id"
        );

        info!("Creating ClickHouse scan state table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 创建增量扫描表（按 `operation_type` + `relative_path` + `create_at` 排序）
    async fn create_incremental_scan_table(&self) -> Result<()> {
        let table_name = get_incremental_scan_base_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} ({FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION}) ENGINE = ReplacingMergeTree() ORDER BY (operation_type, relative_path, create_at)"
        );

        info!("Creating ClickHouse incremental scan base table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 创建 tar manifest 表
    ///
    /// 记录每个 .tar 文件内部包含的条目清单
    async fn create_tar_manifest_table_impl(&self) -> Result<()> {
        let table_name = get_tar_manifest_table_name(&self.job_id);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (\
                tar_path String, \
                entry_path String, \
                size UInt64, \
                ext Nullable(String), \
                mtime Int64, \
                mode Nullable(UInt32), \
                storage_type String, \
                is_dir Bool, \
                is_symlink Bool, \
                uid Nullable(UInt32), \
                gid Nullable(UInt32), \
                version_id String, \
                tags Nullable(String)\
            ) ENGINE = ReplacingMergeTree() ORDER BY (tar_path, entry_path, version_id)"
        );

        info!("Creating ClickHouse tar manifest table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 删除所有以指定前缀开头的表，返回已删除表名列表
    pub async fn drop_tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        // 转义 LIKE 通配符，防止 prefix 中含有 % 或 _ 匹配到计划外的表
        let escaped = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let query = format!(
            "SELECT name FROM system.tables WHERE name LIKE '{escaped}%' ESCAPE '\\\\' AND database = currentDatabase()"
        );

        let table_names: Vec<String> = self
            .sync_client
            .query(&query)
            .fetch_all::<String>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let mut dropped_tables = Vec::new();
        for table_name in table_names {
            self.drop_table_by_name(&table_name).await?;
            dropped_tables.push(table_name);
        }

        debug!("Dropped {} tables with prefix '{}'", dropped_tables.len(), prefix);
        Ok(dropped_tables)
    }

    /// 检查指定表名是否存在于当前数据库中
    pub async fn check_table_exists(&self, table_name: &str) -> Result<bool> {
        let count: u64 = self
            .sync_client
            .query("SELECT count() FROM system.tables WHERE name = ? AND database = currentDatabase()")
            .bind(table_name)
            .fetch_one()
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to check table existence: {e}")))?;
        Ok(count > 0)
    }

    /// 查询 `scan_state` 表，返回 id=1 的 `scan_state` 值
    pub async fn query_scan_state(&self) -> Result<u8> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let query = format!("SELECT scan_state FROM {table_name} FINAL WHERE id = 1");

        let scan_state = self
            .sync_client
            .query(&query)
            .fetch_one::<u8>()
            .await
            .map_err(|e| match e {
                clickhouse::error::Error::RowNotFound => {
                    DatabaseError::QueryError("No scan state record found for id=1".to_string())
                }
                _ => DatabaseError::QueryError(format!("Failed to query scan_state table: {e}")),
            })?;

        Ok(scan_state)
    }

    /// 获取缓存的 `scan_state`，如果缓存为空（255）则查询数据库
    async fn get_cached_scan_state(&self) -> Result<u8> {
        let cached = self.cached_scan_state.load(Ordering::Relaxed);
        if cached != 255 {
            return Ok(cached);
        }
        let state = match self.query_scan_state().await {
            Ok(s) => s,
            Err(e) => {
                // state 表不存在时降级为默认值 0。
                // get_cached_scan_state 仅在首次 batch_insert 时调用（后续用缓存），
                // 若 DB 真的不可用，后续 batch_insert 本身会报错，此处宽松处理可接受。
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

    /// 插入或更新扫描状态（id=1，利用 `ReplacingMergeTree` 自动去重）
    pub async fn insert_scan_state(&self, scan_state: u8) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let insert_sql = format!("INSERT INTO {table_name} (id, scan_state) VALUES (?, ?)");

        debug!("Inserting scan state: id=1, scan_state={}", scan_state);

        self.sync_client
            .query(&insert_sql)
            .bind(1u8)
            .bind(scan_state)
            .execute()
            .await
            .map_err(DatabaseError::ClickHouseError)?;

        debug!("Inserted scan state record: id=1, scan_state={}", scan_state);
        Ok(())
    }

    /// 通用检测方法：对比临时表与主表，按 `query_builder` 构建 SQL 查询符合条件的记录
    async fn detect_items(
        &self, query_type: &str, query_builder: impl Fn(&str, &str, &JoinStrategy) -> String,
    ) -> Result<Vec<StorageEntryRecord>> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_ref()
            .ok_or_else(|| DatabaseError::UnsupportedType("Temporary table not created".to_string()))?;
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let (temp_total, temp_non_null, base_total, base_non_null) =
            self.check_file_handle_status(temp_table_name, &base_table_name).await?;

        let strategy = JoinStrategy::from_file_handle_status(temp_non_null, base_non_null, base_total, temp_total);

        let query = query_builder(temp_table_name, &base_table_name, &strategy);
        trace!("Querying {}: {}", query_type, query);

        let rows = self
            .sync_client
            .query(&query)
            .fetch_all::<StorageEntryRecord>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        debug!("Found {} {} files", rows.len(), query_type);
        Ok(rows)
    }

    /// 返回 (`temp_total`, `temp_non_null`, `base_total`, `base_non_null`) 用于确定 JOIN 策略
    async fn check_file_handle_status(
        &self, temp_table_name: &str, base_table_name: &str,
    ) -> Result<(usize, usize, usize, usize)> {
        let query = format!(
            "SELECT t.total, t.non_null, b.total, b.non_null \
             FROM (SELECT count(*) AS total, count(file_handle) AS non_null FROM {temp_table_name}) t \
             CROSS JOIN (SELECT count(*) AS total, count(file_handle) AS non_null FROM {base_table_name}) b"
        );

        debug!(
            "Checking file_handle status for temporary table '{}' and base table '{}'",
            temp_table_name, base_table_name
        );

        let (tt, tn, bt, bn): (u64, u64, u64, u64) = match self.sync_client.query(&query).fetch_one().await {
            Ok(row) => row,
            Err(e) => {
                warn!("Failed to query file_handle status: {}, defaulting to (0, 0, 0, 0)", e);
                (0, 0, 0, 0)
            }
        };

        let temp_total = tt as usize;
        let temp_non_null = tn as usize;
        let base_total = bt as usize;
        let base_non_null = bn as usize;

        trace!(
            "file_handle status - Temporary table: total={}, non_null={}; Base table: total={}, non_null={}",
            temp_total, temp_non_null, base_total, base_non_null
        );

        Ok((temp_total, temp_non_null, base_total, base_non_null))
    }

    /// 根据 `relative_path` 列表批量删除指定表中的记录
    async fn batch_delete_record(&self, table_name: &str, deleted_paths: &[String]) -> Result<()> {
        info!(
            "Batch deleting {} records from table: {}",
            deleted_paths.len(),
            table_name
        );

        if deleted_paths.is_empty() {
            return Ok(());
        }

        let delete_query = format!(
            "DELETE FROM {} WHERE relative_path IN ({})",
            table_name,
            vec!["?"; deleted_paths.len()].join(", ")
        );

        let mut delete_stmt = self.sync_client.query(&delete_query);
        for relative_path in deleted_paths {
            delete_stmt = delete_stmt.bind(relative_path);
        }

        delete_stmt
            .execute()
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to batch delete base records: {e}")))?;

        info!("Successfully batch deleted {} base records", deleted_paths.len());
        Ok(())
    }

    /// 通用批量插入方法，支持同步和异步客户端模式
    async fn batch_insert(&self, table_name: &str, records: &[Arc<EntryEnum>], use_async_client: bool) -> Result<()> {
        if records.is_empty() {
            debug!("No records to insert into table {}", table_name);
            return Ok(());
        }

        info!(
            "Inserting {} records to table {} in {} mode",
            records.len(),
            table_name,
            if use_async_client { "async" } else { "sync" }
        );

        let current_state = self.get_cached_scan_state().await?;
        debug!(
            "During batch inserting to table {}, current_state: {:?}",
            table_name, current_state
        );

        let record_count = records.len();

        let client = if use_async_client {
            &self.async_client
        } else {
            &self.sync_client
        };

        let mut insert = client
            .insert::<StorageEntryRecord>(table_name)
            .await
            .map_err(DatabaseError::ClickHouseError)?;

        for entry in records {
            let record = StorageEntryRecord::from_entry_enum(entry.as_ref(), current_state);
            trace!("Inserting record: {:?} in table {}", record, table_name);
            insert.write(&record).await.map_err(DatabaseError::ClickHouseError)?;
        }

        insert.end().await.map_err(DatabaseError::ClickHouseError)?;

        info!("Successfully inserted {} records to table {}", record_count, table_name);
        Ok(())
    }
}

#[async_trait]
impl Database for ClickHouseDatabase {
    fn clone_box(&self) -> Box<dyn Database> {
        let new_db = Self {
            sync_client: self.sync_client.clone(),
            async_client: self.async_client.clone(),
            job_id: self.job_id.clone(),
            scan_temp_table_name: self.scan_temp_table_name.clone(),
            cached_scan_state: AtomicU8::new(self.cached_scan_state.load(Ordering::Relaxed)),
        };

        Box::new(new_db)
    }

    /// 初始化：创建主扫描表 + 状态表，设置初始 `scan_state=0`
    async fn initialize(&self) -> Result<()> {
        self.create_table(SCAN_BASE_TABLE_BASE_NAME).await?;
        self.create_table(SCAN_STATE_TABLE_BASE_NAME).await?;
        self.insert_scan_state(0).await?;
        Ok(())
    }

    /// 通过 SELECT 1 测试连接
    async fn ping(&self) -> Result<()> {
        self.sync_client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        debug!("ClickHouse connection established successfully");
        Ok(())
    }

    /// 根据表类型名称分派到对应的建表方法
    async fn create_table(&self, table_name: &str) -> Result<()> {
        match table_name {
            SCAN_BASE_TABLE_BASE_NAME => self.create_scan_base_table().await,
            SCAN_STATE_TABLE_BASE_NAME => self.create_scan_state_table().await,
            INCREMENTAL_SCAN_TABLE_BASE_NAME => self.create_incremental_scan_table().await,
            TAR_MANIFEST_TABLE_BASE_NAME => self.create_tar_manifest_table_impl().await,
            _ => Err(DatabaseError::UnsupportedType(format!("Unknown table: {table_name}"))),
        }
    }

    /// 批量插入增量扫描记录（StorageEntryMessage → `IncrementalStorageEntryRecord`）
    async fn batch_insert_incremental_record(&self, messages: &[StorageEntryMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let table_name = get_incremental_scan_base_table_name(&self.job_id);

        let records: Vec<IncrementalStorageEntryRecord> = messages
            .iter()
            .map(IncrementalStorageEntryRecord::from_message)
            .collect();

        let record_count = records.len();

        let mut insert = self
            .async_client
            .insert::<IncrementalStorageEntryRecord>(&table_name)
            .await
            .map_err(DatabaseError::ClickHouseError)?;

        for record in &records {
            trace!(
                "Inserting record: {:?} in incremental scan table {}",
                record, table_name
            );

            insert.write(record).await.map_err(DatabaseError::ClickHouseError)?;
        }

        insert.end().await.map_err(DatabaseError::ClickHouseError)?;

        info!(
            "Successfully inserted {} events to incremental scan table {}",
            record_count, table_name
        );
        Ok(())
    }

    /// 创建扫描临时表（MergeTree，自动生成唯一表名）
    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let temp_table_name = generate_scan_temp_table_name();
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {temp_table_name} ({FILE_SCAN_COLUMNS_DEFINITION}) ENGINE = MergeTree() ORDER BY (relative_path, version_id) SETTINGS allow_nullable_key = 1"
        );

        debug!("Creating ClickHouse scan temporary table: {}", temp_table_name);
        self.execute(&create_table_sql, &[]).await?;

        trace!(
            "ClickHouse scan temporary table '{}' created successfully",
            temp_table_name
        );
        self.scan_temp_table_name = Some(temp_table_name);
        Ok(())
    }

    /// 删除扫描临时表并清空 `scan_temp_table_name`
    async fn drop_scan_temporary_table(&mut self) -> Result<()> {
        if let Some(temp_table_name) = &self.scan_temp_table_name {
            let drop_table_sql = format!("DROP TABLE IF EXISTS {temp_table_name}");

            debug!("Dropping ClickHouse scan temporary table: {}", temp_table_name);
            self.execute(&drop_table_sql, &[]).await?;

            debug!(
                "ClickHouse scan temporary table '{}' dropped successfully",
                temp_table_name
            );

            self.scan_temp_table_name = None;
        } else {
            debug!("No temporary table to drop");
        }
        Ok(())
    }

    /// 删除指定表（IF EXISTS）
    async fn drop_table_by_name(&self, table_name: &str) -> Result<()> {
        let drop_table_sql = format!("DROP TABLE IF EXISTS `{table_name}`");
        debug!("Dropping ClickHouse table: {}", table_name);
        self.execute(&drop_table_sql, &[]).await?;
        Ok(())
    }

    /// 批量插入记录到临时表（async client）
    async fn batch_insert_temp_record(&self, records: &[Arc<EntryEnum>]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::UnsupportedType("No temporary table available".to_string()))?;

        // 使用 async client，wait_for_async_insert=1 保证 end() 返回时数据已 flush
        self.batch_insert(temp_table_name, records, true).await
    }

    /// 批量插入记录到主表（async client）
    async fn batch_insert_base_record(&self, records: &[Arc<EntryEnum>]) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, records, true).await
    }

    /// 更新主表中的单条记录（利用 `ReplacingMergeTree` 插入覆盖）
    async fn update_base_record(&self, record: &Arc<EntryEnum>) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, &[Arc::clone(record)], false).await
    }

    /// 批量删除主表中的记录（按 `relative_path`）
    async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.batch_delete_record(&table_name, deleted_paths).await
    }

    /// 切换 `scan_state`（0↔1）
    async fn switch_scan_state(&self) -> Result<()> {
        let current_state = self.get_cached_scan_state().await?;
        let new_state = 1 - current_state;

        self.insert_scan_state(new_state).await?;
        self.cached_scan_state.store(new_state, Ordering::Relaxed);

        debug!("Switched scan state: {} -> {}", current_state, new_state);

        Ok(())
    }

    /// 将临时表记录合并到主表（INSERT INTO SELECT），可排除指定路径
    async fn insert_temp_to_base_table(&self, excluded_paths: &[(String, String)]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::UnsupportedType("No temporary table available".to_string()))?;

        let base_table_name = get_scan_base_table_name(&self.job_id);

        debug!(
            "Inserting data from temporary table {} to base table {} (excluded: {} paths)",
            temp_table_name,
            base_table_name,
            excluded_paths.len()
        );

        if excluded_paths.is_empty() {
            // 增量扫描：全量插入（无额外开销）
            let insert_sql = format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                base_table_name, *FILE_SCAN_COLUMNS_LIST, *FILE_SCAN_COLUMNS_LIST, temp_table_name
            );
            self.execute(&insert_sql, &[]).await?;
        } else {
            // 增量拷贝：Memory 排除表 + NOT IN 过滤
            let exclude_table = format!("exclude_{}", uuid::Uuid::new_v4().simple());

            // 1. 创建 Memory 排除表
            self.execute(
                &format!("CREATE TABLE {exclude_table} (relative_path String, version_id String) ENGINE = Memory"),
                &[],
            )
            .await?;

            // 2. 写入排除路径
            let mut insert = self
                .sync_client
                .insert::<ExcludeRecord>(&exclude_table)
                .await
                .map_err(DatabaseError::ClickHouseError)?;
            for (path, vid) in excluded_paths {
                insert
                    .write(&ExcludeRecord {
                        relative_path: path.clone(),
                        version_id: vid.clone(),
                    })
                    .await
                    .map_err(DatabaseError::ClickHouseError)?;
            }
            insert.end().await.map_err(DatabaseError::ClickHouseError)?;

            // 3. 带过滤的 INSERT INTO SELECT
            let insert_sql = format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} \
                 WHERE (relative_path, version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {})",
                base_table_name, *FILE_SCAN_COLUMNS_LIST, *FILE_SCAN_COLUMNS_LIST, temp_table_name, exclude_table
            );
            self.execute(&insert_sql, &[]).await?;

            // 4. 清理排除表
            self.execute(&format!("DROP TABLE IF EXISTS {exclude_table}"), &[])
                .await?;
        }

        info!(
            "Successfully inserted records from {} to {} (excluded: {})",
            temp_table_name,
            base_table_name,
            excluded_paths.len()
        );
        Ok(())
    }

    /// 检测临时表中新增的文件（不存在于主表）
    async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = EntryEnum> + Send>> {
        let records = self
            .detect_items("new", |temp_table, base_table, strategy| {
                let (vc_join, vc_expr) = generate_version_count_join_sql!(base_table);
                strategy.build_detect_new_sql(temp_table, base_table, &vc_join, &vc_expr)
            })
            .await?;

        Ok(records_to_entry_iter(records))
    }

    async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (EntryEnum, ChangeKind)> + Send>> {
        // 分别查询三种变更（三类条件互斥），用 try_join! 并发执行以降低整体延迟
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

    /// 检测已删除或重命名的文件（old-state 记录 + `file_handle` 分组判定）
    async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
        let base_table_name = get_scan_base_table_name(&self.job_id);

        let current_state = self.query_scan_state().await?;
        debug!("During detect_deleted_items, current_state is {}", current_state);

        // 第一步：查询所有 old-state 记录（LIMIT 1 BY 替代 FINAL）
        let query = JoinStrategy::build_detect_deleted_sql(&base_table_name);

        let rows = self
            .sync_client
            .query(&query)
            .bind(1 - current_state)
            .fetch_all::<StorageEntryRecord>()
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to query deleted files: {e}")))?;

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
            const BATCH_SIZE: usize = 10_000;
            let unique_fh_list: Vec<&str> = unique_fh_set.iter().map(String::as_str).collect();

            // 每批 10K 个 fh，避免 SQL 过长
            for chunk in unique_fh_list.chunks(BATCH_SIZE) {
                let batch_query = JoinStrategy::build_batch_fh_query_sql(&base_table_name, chunk.len());

                let mut q = self.sync_client.query(&batch_query);
                for fh in chunk {
                    q = q.bind(*fh);
                }
                let batch_rows = q
                    .fetch_all::<StorageEntryRecord>()
                    .await
                    .map_err(|e| DatabaseError::QueryError(format!("Failed to batch query file_handle: {e}")))?;

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

    /// 获取指定表的记录总数（使用 FINAL 确保去重）
    async fn get_count(&self, table_name: &str) -> Result<u64> {
        let full_table_name = format!("{}_{}", table_name, sanitize_job_id(&self.job_id));
        let query = format!("SELECT COUNT() FROM {full_table_name} FINAL");

        let count = self
            .sync_client
            .query(&query)
            .fetch_one::<u64>()
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to get count from {full_table_name}: {e}")))?;

        Ok(count)
    }

    async fn query_storage_entry(
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>, tx: mpsc::Sender<EntryEnum>,
    ) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        debug!("[query_storage_entry] base table name: {}", base_table_name);

        let mut where_conditions = Vec::new();
        let mut ext_bind: Option<String> = None;

        if let Some(dir_val) = is_dir {
            where_conditions.push(format!("is_dir = {dir_val}"));
        }

        if let Some(symlink_val) = is_symlink {
            where_conditions.push(format!("is_symlink = {symlink_val}"));
        }

        if let Some(ext_val) = extension {
            where_conditions.push("ext ILIKE ?".to_string());
            ext_bind = Some(format!("%{ext_val}"));
        }

        let where_clause = if where_conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {} FROM {} FINAL {}",
            *FILE_SCAN_COLUMNS_LIST, base_table_name, where_clause
        );

        let mut q = self.sync_client.query(&query);
        if let Some(ext) = ext_bind {
            q = q.bind(ext);
        }
        let mut stream = q
            .fetch::<StorageEntryRecord>()
            .map_err(|e| DatabaseError::QueryError(format!("Failed to query storage entries: {e}")))?;

        while let Ok(Some(record)) = stream.next().await {
            debug!("query_storage_entry: the record {:?}", record);

            let entry_enum = record.to_entry_enum();
            debug!("query_storage_entry: {:?}", entry_enum);
            if let Err(err) = tx.send(entry_enum).await {
                return Err(DatabaseError::QueryError(format!(
                    "Failed to send storage entry: {err}"
                )));
            }
        }

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

        let mut insert = self
            .async_client
            .insert::<TarManifestRecord>(&table_name)
            .await
            .map_err(DatabaseError::ClickHouseError)?;

        for record in records {
            insert.write(record).await.map_err(DatabaseError::ClickHouseError)?;
        }

        insert.end().await.map_err(DatabaseError::ClickHouseError)?;

        info!(
            "Successfully inserted {} tar manifest records to {}",
            records.len(),
            table_name
        );
        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        self.check_table_exists(table_name).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::common::DeletionStatus;
    use crate::traits::StorageEntryRecord;

    /// 构造 StorageEntryRecord，只需指定关键差异字段
    fn make_record(relative_path: &str, file_handle: Option<&str>, ctime: i64) -> StorageEntryRecord {
        StorageEntryRecord {
            name: relative_path.rsplit('/').next().unwrap_or(relative_path).to_string(),
            relative_path: relative_path.to_string(),
            size: 1024,
            ext: None,
            ctime,
            mtime: ctime,
            atime: ctime,
            mode: Some(0o644),
            storage_type: "nas".to_string(),
            is_symlink: false,
            is_dir: false,
            is_regular_file: true,
            hard_links: 1,
            current_state: 0,
            uid: Some(1000),
            gid: Some(1000),
            ino: Some(12345),
            file_handle: file_handle.map(String::from),
            version_id: String::new(),
            tags: None,
            version_count: None,
        }
    }

    // ═══ 1.1 StorageEntryRecord 转换测试 ═══

    #[test]
    fn test_nas_entry_roundtrip() {
        let nas = data_mover::EntryEnum::NAS(data_mover::NASEntry {
            name: "test.txt".to_string(),
            relative_path: PathBuf::from("dir/test.txt"),
            extension: Some("txt".to_string()),
            is_dir: false,
            size: 2048,
            atime: 100,
            ctime: 200,
            mtime: 300,
            mode: 0o755,
            is_symlink: false,
            hard_links: Some(2),
            uid: Some(1000),
            gid: Some(1000),
            ino: Some(54321),
            file_handle: Some(bytes::Bytes::from_static(&[0xDE, 0xAD])),
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        });

        let record = StorageEntryRecord::from_entry_enum(&nas, 1);
        assert_eq!(record.storage_type, "nas");
        assert_eq!(record.file_handle, Some("dead".to_string()));
        assert_eq!(record.version_id, "");
        assert_eq!(record.current_state, 1);
        assert_eq!(record.size, 2048);

        let back = record.to_entry_enum();
        assert_eq!(back.get_name(), "test.txt");
        assert_eq!(back.get_size(), 2048);
        assert_eq!(back.get_mtime(), 300);
    }

    #[test]
    fn test_s3_entry_roundtrip() {
        let s3 = data_mover::EntryEnum::S3(data_mover::S3Entry {
            name: "obj.json".to_string(),
            relative_path: "bucket/obj.json".to_string(),
            extension: Some("json".to_string()),
            size: 512,
            mtime: 400,
            tags: None,
            version_id: Some("v1".to_string()),
            is_latest: true,
            is_delete_marker: false,
            version_count: Some(3),
            is_dir: false,
        });

        let record = StorageEntryRecord::from_entry_enum(&s3, 0);
        assert_eq!(record.storage_type, "s3");
        assert_eq!(record.version_id, "v1");
        assert_eq!(record.version_count, Some(3));

        let back = record.to_entry_enum();
        assert_eq!(back.get_version_id(), Some("v1"));
    }

    #[test]
    fn test_nas_entry_optional_fields_none() {
        let nas = data_mover::EntryEnum::NAS(data_mover::NASEntry {
            name: "a.txt".to_string(),
            relative_path: PathBuf::from("a.txt"),
            extension: None,
            is_dir: false,
            size: 0,
            atime: 0,
            ctime: 0,
            mtime: 0,
            mode: 0,
            is_symlink: false,
            hard_links: None,
            uid: None,
            gid: None,
            ino: None,
            file_handle: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        });

        let record = StorageEntryRecord::from_entry_enum(&nas, 0);
        assert_eq!(record.file_handle, None);

        let back = record.to_entry_enum();
        assert_eq!(back.get_file_handle(), None);
    }

    #[test]
    fn test_current_state_propagation() {
        let nas = data_mover::EntryEnum::NAS(data_mover::NASEntry {
            name: "a.txt".to_string(),
            relative_path: PathBuf::from("a.txt"),
            extension: None,
            is_dir: false,
            size: 100,
            atime: 0,
            ctime: 0,
            mtime: 0,
            mode: 0o644,
            is_symlink: false,
            hard_links: Some(1),
            uid: None,
            gid: None,
            ino: None,
            file_handle: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        });

        let r0 = StorageEntryRecord::from_entry_enum(&nas, 0);
        assert_eq!(r0.current_state, 0);

        let r1 = StorageEntryRecord::from_entry_enum(&nas, 1);
        assert_eq!(r1.current_state, 1);
    }

    // ═══ 1.2 file_handle 分组逻辑测试（优化 2 核心逻辑）═══

    #[test]
    fn test_classify_single_fh_is_deleted() {
        let fh_records = vec![make_record("a.txt", Some("abc"), 100)];
        let mut groups = HashMap::new();
        groups.insert("abc".to_string(), vec![make_record("a.txt", Some("abc"), 100)]);

        let result = classify_deletion_status(vec![], fh_records, &groups);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], DeletionStatus::Deleted(_)));
    }

    #[test]
    fn test_classify_two_fh_is_renamed() {
        let fh_records = vec![make_record("old.txt", Some("abc"), 100)];
        let mut groups = HashMap::new();
        groups.insert(
            "abc".to_string(),
            vec![
                make_record("old.txt", Some("abc"), 100),
                make_record("new.txt", Some("abc"), 200),
            ],
        );

        let result = classify_deletion_status(vec![], fh_records, &groups);
        assert_eq!(result.len(), 1);
        match &result[0] {
            DeletionStatus::Renamed(from, to) => {
                assert_eq!(from.get_ctime(), 100);
                assert_eq!(to.get_ctime(), 200);
            }
            DeletionStatus::Deleted(_) => panic!("Expected Renamed"),
        }
    }

    #[test]
    fn test_classify_renamed_reverses_ctime_order() {
        let fh_records = vec![make_record("old.txt", Some("abc"), 200)];
        let mut groups = HashMap::new();
        groups.insert(
            "abc".to_string(),
            vec![
                make_record("new.txt", Some("abc"), 200),
                make_record("old.txt", Some("abc"), 100),
            ],
        );

        let result = classify_deletion_status(vec![], fh_records, &groups);
        assert_eq!(result.len(), 1);
        match &result[0] {
            DeletionStatus::Renamed(from, to) => {
                // ctime 小的是 from，大的是 to，与 groups 中顺序无关
                assert_eq!(from.get_ctime(), 100);
                assert_eq!(to.get_ctime(), 200);
            }
            DeletionStatus::Deleted(_) => panic!("Expected Renamed"),
        }
    }

    #[test]
    fn test_classify_fh_not_in_groups() {
        let fh_records = vec![make_record("x.txt", Some("xyz"), 100)];
        let groups = HashMap::new(); // 空 groups

        let result = classify_deletion_status(vec![], fh_records, &groups);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], DeletionStatus::Deleted(_)));
    }

    #[test]
    fn test_classify_mixed() {
        let no_fh = vec![make_record("a.txt", None, 100), make_record("b.txt", None, 200)];
        let fh_records = vec![
            make_record("c.txt", Some("fh_a"), 100),
            make_record("d.txt", Some("fh_b"), 100),
            make_record("e.txt", Some("fh_c"), 100),
        ];
        let mut groups = HashMap::new();
        // fh_a: 1 条 → Deleted
        groups.insert("fh_a".to_string(), vec![make_record("c.txt", Some("fh_a"), 100)]);
        // fh_b: 2 条 → Renamed
        groups.insert(
            "fh_b".to_string(),
            vec![
                make_record("d.txt", Some("fh_b"), 100),
                make_record("d_new.txt", Some("fh_b"), 200),
            ],
        );
        // fh_c: 3 条 → 跳过 (error log)
        groups.insert(
            "fh_c".to_string(),
            vec![
                make_record("e1.txt", Some("fh_c"), 100),
                make_record("e2.txt", Some("fh_c"), 200),
                make_record("e3.txt", Some("fh_c"), 300),
            ],
        );

        let result = classify_deletion_status(no_fh, fh_records, &groups);
        // 2 Deleted (no_fh) + 1 Deleted (fh_a) + 1 Renamed (fh_b) + 0 (fh_c skipped)
        let deleted_count = result
            .iter()
            .filter(|s| matches!(s, DeletionStatus::Deleted(_)))
            .count();
        let renamed_count = result
            .iter()
            .filter(|s| matches!(s, DeletionStatus::Renamed(_, _)))
            .count();
        assert_eq!(deleted_count, 3);
        assert_eq!(renamed_count, 1);
    }

    // ═══ 1.4 JoinStrategy 测试（优化 7 核心逻辑）═══

    #[test]
    fn test_join_strategy_from_file_handle_status() {
        // 两表都无 fh，base 和 temp 都有数据 → Path
        assert!(matches!(
            JoinStrategy::from_file_handle_status(0, 0, 100, 50),
            JoinStrategy::Path
        ));
        // 有 fh → FileHandle
        assert!(matches!(
            JoinStrategy::from_file_handle_status(10, 5, 100, 50),
            JoinStrategy::FileHandle
        ));
        // base 空 → FileHandle
        assert!(matches!(
            JoinStrategy::from_file_handle_status(0, 0, 0, 50),
            JoinStrategy::FileHandle
        ));
        // temp 空 → FileHandle
        assert!(matches!(
            JoinStrategy::from_file_handle_status(0, 0, 100, 0),
            JoinStrategy::FileHandle
        ));
    }

    #[test]
    fn test_detect_new_sql_path_mode() {
        let (vc_join, vc_expr) = generate_version_count_join_sql!("base_y");
        let sql = JoinStrategy::Path.build_detect_new_sql("temp_x", "base_y", &vc_join, &vc_expr);
        assert!(sql.contains("NOT IN (SELECT relative_path, version_id FROM base_y)"));
        assert!(sql.contains("FROM temp_x"));
        assert!(!sql.contains("FINAL"));
    }

    #[test]
    fn test_detect_new_sql_fh_mode() {
        let (vc_join, vc_expr) = generate_version_count_join_sql!("base_y");
        let sql = JoinStrategy::FileHandle.build_detect_new_sql("temp_x", "base_y", &vc_join, &vc_expr);
        assert!(sql.contains("file_handle NOT IN"));
        assert!(!sql.contains("FINAL"));
    }

    /// 验证 NULL-safe 元数据等价比较的 SQL 形式：每个属性必须有 `IS NULL AND ... IS NULL` 兜底，
    /// 既不依赖 sentinel（避免把真实 mode=0 与 NULL 混淆），也覆盖 `ClickHouse` 三值逻辑下的 `NULL=NULL` 漏判。
    fn assert_null_safe_meta_clauses(sql: &str) {
        for col in ["mode", "uid", "gid"] {
            let clause = format!("(t.{col} IS NULL AND f.{col} IS NULL)");
            assert!(sql.contains(&clause), "missing NULL-safe clause for `{col}`:\n{sql}");
        }
        // 不应再出现历史 sentinel 形式
        assert!(
            !sql.contains("coalesce(t.mode, 0)"),
            "should no longer use coalesce sentinel:\n{sql}"
        );
    }

    #[test]
    fn test_detect_changed_sql_path_mode_data_only() {
        let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::DataOnly);
        assert!(sql.contains("LIMIT 1 BY (relative_path, version_id)"));
        // DataOnly 条件：内容变了 + 属性未变
        assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
        assert_null_safe_meta_clauses(&sql);
        assert!(!sql.contains("FINAL"));
    }

    #[test]
    fn test_detect_changed_sql_path_mode_metadata_only() {
        let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::MetadataOnly);
        // MetadataOnly 条件：内容未变 + 属性变了
        assert!(sql.contains("t.size = f.size AND t.mtime = f.mtime"));
        assert_null_safe_meta_clauses(&sql);
        // 必须出现取反形式
        assert!(sql.contains("NOT (t.mode = f.mode"));
    }

    #[test]
    fn test_detect_changed_sql_path_mode_both() {
        let sql = JoinStrategy::Path.build_detect_changed_sql("temp_x", "base_y", ChangeKind::Both);
        // Both 条件：内容和属性都变了
        assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
        assert_null_safe_meta_clauses(&sql);
        assert!(sql.contains("NOT (t.mode = f.mode"));
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_data_only() {
        let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::DataOnly);
        // 路径等值条件：仅对路径未变的条目判 Changed，避免把 rename+changed 误判为纯 Changed
        assert!(sql.contains("LIMIT 1 BY (file_handle, relative_path)"));
        assert!(sql.contains("t.file_handle = f.file_handle"));
        assert!(sql.contains("t.relative_path = f.relative_path"));
        assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
        assert_null_safe_meta_clauses(&sql);
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_metadata_only() {
        let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::MetadataOnly);
        assert!(sql.contains("t.size = f.size AND t.mtime = f.mtime"));
        assert_null_safe_meta_clauses(&sql);
        assert!(sql.contains("NOT (t.uid = f.uid"));
    }

    #[test]
    fn test_detect_changed_sql_fh_mode_both() {
        let sql = JoinStrategy::FileHandle.build_detect_changed_sql("temp_x", "base_y", ChangeKind::Both);
        assert!(sql.contains("t.size != f.size OR t.mtime != f.mtime"));
        assert_null_safe_meta_clauses(&sql);
        assert!(sql.contains("NOT (t.gid = f.gid"));
    }

    #[test]
    fn test_detect_deleted_sql() {
        let sql = JoinStrategy::build_detect_deleted_sql("base_y");
        assert!(sql.contains("FINAL"));
        assert!(sql.contains("current_state = ?"));
        assert!(!sql.contains("LIMIT 1 BY"));
    }

    #[test]
    fn test_batch_fh_query_sql() {
        let sql = JoinStrategy::build_batch_fh_query_sql("base_y", 3);
        assert!(sql.contains("file_handle IN (?, ?, ?)"));
        assert!(sql.contains("LIMIT 1 BY (relative_path, version_id)"));
        assert!(!sql.contains("FINAL"));
    }

    // ═══ 1.5 表名 & 宏测试 ═══

    #[test]
    fn test_table_names() {
        assert_eq!(get_scan_base_table_name("job1"), "base_job1");
        assert_eq!(get_scan_state_table_name("job1"), "state_job1");

        let temp1 = generate_scan_temp_table_name();
        let temp2 = generate_scan_temp_table_name();
        assert!(temp1.starts_with("temp_"));
        assert!(temp2.starts_with("temp_"));
        assert_ne!(temp1, temp2);
    }

    #[test]
    fn test_version_count_join_sql() {
        let (join_clause, select_expr) = generate_version_count_join_sql!("base_job1");
        assert!(join_clause.contains("base_job1"));
        assert!(join_clause.contains("LEFT JOIN"));
        assert!(join_clause.contains("GROUP BY relative_path"));
        assert!(select_expr.contains("CASE WHEN"));
        assert!(select_expr.contains("COALESCE"));
    }
}
