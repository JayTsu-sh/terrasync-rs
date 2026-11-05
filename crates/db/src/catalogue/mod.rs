//! `ClickHouse`数据库实现模块
//!
//! 提供与ClickHouse数据库交互的所有操作功能，包括表的创建、查询、插入和删除等。

// 标准库
use data_mover::model::BackendIdentity;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

// 外部crate
use async_trait::async_trait;
use clickhouse::Client;
use clickhouse::error::Error as ClickHouseError;
use data_mover::model::ObservedEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_adapter::{ChangeKind, StorageEntryMessage};
use tokio::sync::{Mutex, MutexGuard, mpsc};
use tracing::{debug, info, trace, warn};

// 内部模块
use crate::common::{
    CATALOGUE_COLUMNS as FILE_SCAN_COLUMNS_LIST,
    CATALOGUE_COLUMNS_WITH_T_PREFIX as FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX, DeletionStatus,
    FILE_SCAN_COLUMNS_LIST_WO_VERSIONCOUNT_WITH_T_PREFIX, classify_deletion_status,
};
use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
use crate::execution::base_client;
use crate::execution::{
    queries::{ScanExecute, check_scan_cancellation, execute_request, idempotent_insert_client},
    recovery::RequestSafety,
};
use crate::observation_record::{OBSERVATION_IDENTITY_JOIN_CLAUSE, ObservedEntryRecord};
use crate::scan::rows::NativeScanRecord;
use crate::traits::{
    CopyGenerationStatus, Database, IncrementalStorageEntryRecord, QueryResult, StorageEntryRecord, TarManifestRecord,
};
use crate::{
    INCREMENTAL_SCAN_TABLE_BASE_NAME, SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME,
    TAR_MANIFEST_TABLE_BASE_NAME, generate_scan_temp_table_name, generate_version_count_join_sql,
    get_incremental_scan_base_table_name, get_scan_base_table_name, get_scan_state_table_name,
    get_tar_manifest_table_name,
};
use storage_adapter::observed_scan::{ObservedItem, ObservedView, ObservedViews};
use utils::sanitize_job_id;

#[cfg(test)]
mod catalogue_tests;
mod copy_generation;
mod layout;
use crate::execution::read::QuorumRead;
use layout::{CatalogueEngine, ClusterCatalogue};

mod scan_resume;
mod scan_resume_statistics;

/// `ClickHouse`数据库实现
///
/// 提供与ClickHouse数据库交互的所有操作功能，包括表的创建、数据的插入、查询和删除等。
pub struct ClickHouseDatabase {
    /// ClickHouse同步客户端，用于执行同步操作如查询和简单插入
    sync_client: Client,
    catalogue_cluster: Option<ClusterCatalogue>,
    /// ClickHouse异步客户端，配置了异步插入功能，用于大批量数据插入
    async_client: Client,
    /// 任务ID，用于在多任务环境下表命名的隔离和区分
    job_id: String,
    /// 当前使用的临时扫描表名，可选值，创建临时表时设置，删除时清空
    pub scan_temp_table_name: Option<String>,
    /// 缓存的 `scan_state` 值（255 = 未缓存），避免每次 `batch_insert` 都查询数据库
    cached_scan_state: AtomicU8,
    /// 每次扫描实例只检查一次旧基线的身份格式，避免每批全表统计。
    resume_base_has_typed_handles: Option<bool>,
    native_dedup_ready: AtomicBool,
    /// 同一 `ClickHouse` 身份与 job 共享的 scan generation 生命周期
    generation_lifecycle: SharedGenerationLifecycle,
}

#[derive(Default)]
struct ScanGenerationLifecycle {
    working: Option<u8>,
    committed: bool,
    observation_publication_ambiguous: bool,
    copy_recovery: bool,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct GenerationLifecycleKey {
    dsn: String,
    database: String,
    username: String,
    job_id: String,
    cluster: Option<String>,
}

#[derive(Default)]
struct CatalogueLifecycle {
    state: Mutex<ScanGenerationLifecycle>,
    writes: Mutex<()>,
}

impl CatalogueLifecycle {
    async fn lock(&self) -> MutexGuard<'_, ScanGenerationLifecycle> {
        self.state.lock().await
    }
}

type SharedGenerationLifecycle = Arc<CatalogueLifecycle>;
type GenerationLifecycleRegistry = HashMap<GenerationLifecycleKey, Weak<CatalogueLifecycle>>;

static GENERATION_LIFECYCLES: OnceLock<StdMutex<GenerationLifecycleRegistry>> = OnceLock::new();

fn shared_generation_lifecycle(config: &ClickHouseConfig, job_id: &str) -> SharedGenerationLifecycle {
    let key = GenerationLifecycleKey {
        dsn: config.dsn.clone(),
        database: config.database.clone(),
        username: config.username.clone(),
        job_id: sanitize_job_id(job_id),
        cluster: config.cluster.clone(),
    };
    let registry = GENERATION_LIFECYCLES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, lifecycle| lifecycle.strong_count() > 0);
    if let Some(lifecycle) = registry.get(&key).and_then(Weak::upgrade) {
        return lifecycle;
    }
    let lifecycle = Arc::new(CatalogueLifecycle::default());
    registry.insert(key, Arc::downgrade(&lifecycle));
    lifecycle
}

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

/// Memory 排除表记录（优化 3）
#[derive(clickhouse::Row, Serialize, Deserialize)]
struct ExcludeRecord {
    relative_path: String,
    version_id: String,
}

/// JOIN/// JOIN 策略：path-based 或 file_handle-based
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
    ///
    /// 策略只取决于「是否存在 `file_handle`」，与表是否为空无关：
    /// 两侧都无 `file_handle`（local/S3）→ 必须用 Path（`relative_path+version_id`）；
    /// 任一侧有 `file_handle`（NFS/CIFS）→ 用 `FileHandle`。
    ///
    /// 注意：不得用 `base_total > 0` 之类的「表非空」作为前置条件——全量拷贝中断后
    /// 续传时 base 为空，但 local/S3 文件无 `file_handle`，若误入 `FileHandle` 分支，
    /// `NULL NOT IN (...)` 会求值为 NULL 导致新增文件漏判、被静默并入 base 而不拷贝。
    fn from_file_handle_status(temp_non_null: usize, base_non_null: usize) -> Self {
        if temp_non_null == 0 && base_non_null == 0 {
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
                "SELECT {columns}, {version_count_expr}, t.backend_kind,t.backend_id,t.entry_snapshot FROM {temp} as t \
                 {version_count_join} \
                 WHERE (t.relative_path, t.version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {base}) \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
            ),
            Self::FileHandle => format!(
                "SELECT {columns}, {version_count_expr}, t.backend_kind,t.backend_id,t.entry_snapshot FROM {temp} as t \
                 {version_count_join} \
                 WHERE t.file_handle NOT IN \
                 (SELECT file_handle FROM {base} WHERE file_handle IS NOT NULL) \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
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
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
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
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
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

/// 将 `StorageEntryRecord` 列表转换为 `ObservedEntry` 迭代器
fn records_to_entry_iter(records: Vec<StorageEntryRecord>) -> Result<Box<dyn Iterator<Item = ObservedEntry> + Send>> {
    let entries = records
        .into_iter()
        .map(|row| row.to_observation())
        .collect::<Result<Vec<_>>>()?;
    Ok(Box::new(entries.into_iter()))
}

fn observation_views(entry: &Arc<ObservedEntry>) -> ObservedViews<'_> {
    ObservedView::all(entry)
}

impl ClickHouseDatabase {
    fn native_insert_safety(&self) -> RequestSafety {
        if self.native_dedup_ready.load(Ordering::Acquire) {
            RequestSafety::IdempotentWrite
        } else {
            RequestSafety::UnconfirmedWrite
        }
    }

    /// 创建新的`ClickHouse`数据库实例
    ///
    /// # Errors
    /// 部署模式、集群名称或集群数据库标识符无效时返回配置错误；此时尚未发起网络请求。
    pub fn new(config: &ClickHouseConfig, job_id: &str) -> Result<Self> {
        Self::with_client(config, job_id, base_client(config)?)
    }

    pub(crate) fn with_client(config: &ClickHouseConfig, job_id: &str, client: Client) -> Result<Self> {
        let catalogue_cluster = ClusterCatalogue::from_config(config, job_id)?;
        let (sync_client, async_client) = if let Some(cluster) = &catalogue_cluster {
            let client = cluster.client(client);
            (client.clone(), client)
        } else {
            (
                client.clone(),
                client
                    .with_option("async_insert", "1")
                    .with_option("wait_for_async_insert", "1"),
            )
        };

        Ok(Self {
            catalogue_cluster,
            sync_client,
            async_client,
            job_id: job_id.to_string(),
            scan_temp_table_name: None,
            cached_scan_state: AtomicU8::new(255),
            resume_base_has_typed_handles: None,
            native_dedup_ready: AtomicBool::new(false),
            generation_lifecycle: shared_generation_lifecycle(config, job_id),
        })
    }

    /// 将 data-mover observation    /// 将 data-mover observation projection 写入当前 job 的同一张 base 大表。
    ///
    /// # Errors
    /// `ClickHouse` 拒绝 schema 或写入时返回数据库错误。
    pub async fn batch_insert_observations(&self, records: &[ObservedEntryRecord]) -> Result<()> {
        self.insert_observations(&get_scan_base_table_name(&self.job_id), records)
            .await
    }

    /// 将 observation projection 写入当前扫描临时表。
    ///
    /// # Errors
    /// 尚未创建临时表或 `ClickHouse` 拒绝写入时返回数据库错误。
    pub async fn batch_insert_temp_observations(&self, records: &[ObservedEntryRecord]) -> Result<()> {
        let table_name = self
            .scan_temp_table_name
            .as_ref()
            .ok_or_else(|| DatabaseError::TableNotFound("scan temporary table".to_string()))?;
        self.insert_observations(table_name, records).await
    }

    async fn insert_observations(&self, table_name: &str, records: &[ObservedEntryRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let _write = self.catalogue_write_guard().await;
        let client = self.catalogue_write_client(&self.async_client);
        let mut insert = client
            .insert::<ObservedEntryRecord>(table_name)
            .await
            .map_err(DatabaseError::from)?;
        for record in records {
            insert.write(record).await.map_err(DatabaseError::from)?;
        }
        insert.end().await.map_err(DatabaseError::from)
    }

    /// 用中立 identity key JOIN 临时观察与基线，返回查询投影发生变化的观察。
    ///
    /// # Errors
    /// 临时表不存在、查询失败、snapshot 异常或投影不一致时返回 typed error。
    pub async fn detect_changed_observations(&self, backend: &BackendIdentity) -> Result<Vec<ObservedEntry>> {
        let temp = self
            .scan_temp_table_name
            .as_ref()
            .ok_or_else(|| DatabaseError::TableNotFound("scan temporary table".to_string()))?;
        let base = get_scan_base_table_name(&self.job_id);
        let columns = "t.identity_key, t.relative_path, t.backend_kind, t.entry_kind, t.size, \
                       t.size_observed, t.modified_unix_nanos, t.current_state, t.entry_snapshot";
        let query = format!(
            "SELECT {columns} FROM {temp} t INNER JOIN {base} b ON {OBSERVATION_IDENTITY_JOIN_CLAUSE} \
             WHERE (t.relative_path != b.relative_path OR t.entry_kind != b.entry_kind \
                OR t.size != b.size OR t.size_observed != b.size_observed \
                OR NOT isNotDistinctFrom(t.modified_unix_nanos, b.modified_unix_nanos)) \
             ORDER BY t.identity_key"
        );
        self.sync_client
            .query(&query)
            .fetch_quorum_all::<ObservedEntryRecord>()
            .await?
            .into_iter()
            .map(|record| record.reconstruct(backend))
            .collect()
    }

    /// 创建与 base 表结构完全一致的 observation snapshot staging 表。
    ///
    /// # Errors
    /// base 表不存在或 `ClickHouse` 拒绝建表时返回数据库错误。
    pub async fn begin_observation_snapshot(&mut self) -> Result<u8> {
        if self.generation_lifecycle.lock().await.observation_publication_ambiguous {
            return Err(DatabaseError::TransactionError(
                "cannot begin observation snapshot while a previous EXCHANGE outcome is ambiguous".to_string(),
            ));
        }
        if self.scan_temp_table_name.is_some() {
            self.drop_scan_temporary_table().await?;
        }
        let generation = Database::begin_scan_generation(self).await?;
        let temp = generate_scan_temp_table_name();
        let base = get_scan_base_table_name(&self.job_id);
        if let Err(error) = self.create_catalogue_snapshot(&temp, &base).await {
            *self.generation_lifecycle.lock().await = ScanGenerationLifecycle::default();
            return Err(error);
        }
        self.scan_temp_table_name = Some(temp);
        Ok(generation)
    }

    /// 原子交换完整 observation snapshot，并返回消失 identity 的数量。
    ///
    /// # Errors
    /// staging 不存在、计数、交换或旧 snapshot 清理失败时返回数据库错误。
    pub async fn publish_observation_snapshot(&mut self) -> Result<u64> {
        let temp = self
            .scan_temp_table_name
            .clone()
            .ok_or_else(|| DatabaseError::TableNotFound("observation snapshot staging".to_string()))?;
        let base = get_scan_base_table_name(&self.job_id);
        let deleted = match self
            .sync_client
            .query(&format!(
                "SELECT count() FROM {base} WHERE identity_key != '' AND identity_key NOT IN \
                 (SELECT identity_key FROM {temp})"
            ))
            .fetch_quorum_one::<u64>()
            .await
        {
            Ok(deleted) => deleted,
            Err(error) => {
                if let Err(abort) = self.abort_observation_snapshot().await {
                    return Err(DatabaseError::ObservationAbortFailed {
                        source: Box::new(error),
                        abort: Box::new(abort),
                    });
                }
                return Err(error);
            }
        };
        if let Err(error) = self.exchange_catalogue_snapshot(&base, &temp).await {
            self.generation_lifecycle.lock().await.observation_publication_ambiguous = true;
            return Err(DatabaseError::ObservationExchangeAmbiguous {
                source: Box::new(error),
            });
        }
        if let Err(commit_error) = Database::commit_scan_generation(self).await {
            let Some(working) = self.generation_lifecycle.lock().await.working else {
                return Err(DatabaseError::ObservationGenerationLost {
                    source: Box::new(commit_error),
                });
            };
            match self.query_scan_state().await {
                Ok(Some(persisted)) if persisted == working => {
                    self.cached_scan_state.store(working, Ordering::Relaxed);
                    self.generation_lifecycle.lock().await.committed = true;
                    warn!("Recovered an ambiguous generation commit from persisted state: {commit_error}");
                }
                Ok(Some(_)) => {
                    if let Err(rollback_error) = self.exchange_catalogue_snapshot(&base, &temp).await {
                        self.generation_lifecycle.lock().await.observation_publication_ambiguous = true;
                        return Err(DatabaseError::ObservationRollbackFailed {
                            commit: Box::new(commit_error),
                            rollback: Box::new(rollback_error),
                        });
                    }
                    *self.generation_lifecycle.lock().await = ScanGenerationLifecycle::default();
                    if let Err(error) = self.drop_scan_temporary_table().await {
                        warn!("Rolled back observation publication but deferred staging cleanup for {temp}: {error}");
                    }
                    return Err(commit_error);
                }
                result @ (Ok(None) | Err(_)) => {
                    self.generation_lifecycle.lock().await.observation_publication_ambiguous = true;
                    return Err(DatabaseError::ObservationCommitAmbiguous {
                        commit: Box::new(commit_error),
                        verification: result.err().map(Box::new),
                    });
                }
            }
        }
        if let Err(error) = self.drop_catalogue_table(&temp).await {
            warn!("Committed observation snapshot but deferred retired table cleanup for {temp}: {error}");
        } else {
            self.scan_temp_table_name = None;
        }
        Ok(deleted)
    }

    /// 丢弃未完成的 observation snapshot staging。
    ///
    /// # Errors
    /// `ClickHouse` 拒绝清理时返回数据库错误。
    pub async fn abort_observation_snapshot(&mut self) -> Result<()> {
        if self.generation_lifecycle.lock().await.observation_publication_ambiguous {
            return Err(DatabaseError::TransactionError(
                "refusing to delete observation rollback table while EXCHANGE outcome is ambiguous".to_string(),
            ));
        }
        *self.generation_lifecycle.lock().await = ScanGenerationLifecycle::default();
        Database::drop_scan_temporary_table(self).await
    }

    /// 按持久 identity key 读取并仅通过 opaque codec 重建 observation。
    ///
    /// # Errors
    /// 查询失败、snapshot 异常或查询投影不一致时返回 typed error。
    pub async fn load_observation(
        &self, identity_key: &str, backend: &BackendIdentity,
    ) -> Result<Option<ObservedEntry>> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let query = format!(
            "SELECT identity_key, relative_path, backend_kind, entry_kind, size, size_observed, \
             modified_unix_nanos, current_state, entry_snapshot FROM {table_name} FINAL \
             WHERE identity_key = ? LIMIT 1"
        );
        let records = self
            .sync_client
            .query(&query)
            .bind(identity_key)
            .fetch_quorum_all::<ObservedEntryRecord>()
            .await?;
        records
            .into_iter()
            .next()
            .map(|value| value.reconstruct(backend))
            .transpose()
    }

    /// 执行SQL语句，支持参数化查询
    ///
    /// 注意：由于ClickHouse execute API的限制，当前无法获取受影响的行数
    pub(crate) async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        check_scan_cancellation()?;
        let _write = self.catalogue_write_guard().await;
        debug!("Executing ClickHouse statement: {}", sql);

        let client = self.catalogue_write_client(&self.sync_client);
        let mut query = client.query(sql);

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

        query.execute_scan().await?;

        Ok(QueryResult {
            rows: Vec::new(),
            affected_rows: 0, // ClickHouse execute返回()，无法获取affected_rows
            last_insert_id: None,
        })
    }

    /// 创建主扫描表（ReplacingMergeTree，按 `relative_path` + `version_id` 排序）
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            FILE_SCAN_COLUMNS_DEFINITION,
            "(relative_path, version_id)",
            CatalogueEngine::Replacing,
        )
        .await?;
        self.validate_scan_base_table().await
    }

    /// 检查已有快照键，拒绝续写旧表而保留所有记录。
    pub(super) async fn validate_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let sorting_key = self
            .sync_client
            .query("SELECT sorting_key FROM system.tables WHERE database=currentDatabase() AND name=?")
            .bind(self.catalogue_sorting_table(&table_name))
            .fetch_quorum_one::<String>()
            .await?;
        let normalized = sorting_key.replace([' ', '(', ')', '`'], "");
        if normalized.strip_prefix("tuple").unwrap_or(&normalized) != "relative_path,version_id" {
            return Err(DatabaseError::ConfigError(format!(
                "ClickHouse scan table {table_name} uses an incompatible sorting key; clear this scan job and run a full scan again"
            )));
        }
        Ok(())
    }

    /// 创建扫描状态表（id=1, `scan_state` 0/1）
    pub async fn create_scan_state_table(&self) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            "id UInt8, scan_state UInt8, baseline_committed Bool DEFAULT true",
            "id",
            CatalogueEngine::Replacing,
        )
        .await?;
        self.ensure_baseline_committed_column().await?;

        Ok(())
    }

    async fn ensure_baseline_committed_column(&self) -> Result<()> {
        let table = get_scan_state_table_name(&self.job_id);
        // 兼容旧状态行的已有基线；新任务在首次成功提交前显式写入 false。
        self.add_catalogue_column(&table, "baseline_committed Bool DEFAULT true")
            .await?;
        Ok(())
    }

    /// 建表不代表首次全量已完成，只有成功提交代次才发布基线。
    pub async fn has_committed_baseline(&self) -> Result<bool> {
        let table = get_scan_state_table_name(&self.job_id);
        if !self.check_table_exists(&table).await? {
            return Ok(false);
        }
        self.ensure_baseline_committed_column().await?;
        match self
            .sync_client
            .query(&format!("SELECT baseline_committed FROM {table} FINAL WHERE id=1"))
            .fetch_quorum_one::<bool>()
            .await
        {
            Ok(committed) => Ok(committed),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// 创建增量扫描表（按 `operation_type` + `relative_path` + `create_at` 排序）
    async fn create_incremental_scan_table(&self) -> Result<()> {
        let table_name = get_incremental_scan_base_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            FILE_INCREMENTAL_SCAN_COLUMNS_DEFINITION,
            "(operation_type, relative_path, create_at)",
            CatalogueEngine::Replacing,
        )
        .await?;

        Ok(())
    }

    /// 创建 tar manifest 表
    ///
    /// 记录每个 .tar 文件内部包含的条目清单
    async fn create_tar_manifest_table_impl(&self) -> Result<()> {
        let table_name = get_tar_manifest_table_name(&self.job_id);
        self.create_catalogue_table(
            &table_name,
            "tar_path String, entry_path String, size UInt64, ext Nullable(String), mtime Int64, mode Nullable(UInt32),
             storage_type String, is_dir Bool, is_symlink Bool, uid Nullable(UInt32), gid Nullable(UInt32),
             version_id String, tags Nullable(String)",
            "(tar_path, entry_path, version_id)",
            CatalogueEngine::Replacing,
        )
        .await?;

        Ok(())
    }

    /// 删除所有以指定前缀开头的表，返回已删除表名列表
    pub async fn drop_tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        // 转义 LIKE 通配符，防止 prefix 中含有 % 或 _ 匹配到计划外的表
        let escaped = prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let query = format!(
            "SELECT name FROM system.tables WHERE name LIKE '{escaped}%' ESCAPE '\\\\' AND database = currentDatabase()"
        );

        let table_names: Vec<String> = self.sync_client.query(&query).fetch_quorum_all::<String>().await?;

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
            .fetch_quorum_one()
            .await?;
        Ok(count > 0)
    }

    /// 查询 `scan_state` 表，返回 id=1 的 `scan_state` 值。
    ///
    /// `Ok(None)` 仅表示表存在但没有状态行；查询、连接等数据库错误会原样上抛。
    pub async fn query_scan_state(&self) -> Result<Option<u8>> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let query = format!("SELECT scan_state FROM {table_name} FINAL WHERE id = 1");

        match self.sync_client.query(&query).fetch_quorum_one::<u8>().await {
            Ok(scan_state) => Ok(Some(scan_state)),
            Err(DatabaseError::ClickHouseError(ClickHouseError::RowNotFound)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// 获取缓存的 `scan_state`，如果缓存为空（255）则查询数据库
    async fn get_cached_scan_state(&self) -> Result<u8> {
        let working = self.generation_lifecycle.lock().await.working;
        if let Some(working) = working {
            return Ok(working);
        }
        let cached = self.cached_scan_state.load(Ordering::Relaxed);
        if cached != 255 {
            return Ok(cached);
        }
        let state = self.query_scan_state().await?.unwrap_or(0);
        self.cached_scan_state.store(state, Ordering::Relaxed);
        Ok(state)
    }

    /// 插入或更新扫描状态（id=1，利用 `ReplacingMergeTree` 自动去重）
    pub async fn insert_scan_state(&self, scan_state: u8) -> Result<()> {
        self.ensure_baseline_committed_column().await?;
        self.insert_scan_state_with_baseline(scan_state, true).await
    }

    async fn insert_scan_state_with_baseline(&self, scan_state: u8, baseline_committed: bool) -> Result<()> {
        let _write = self.catalogue_write_guard().await;
        let table_name = get_scan_state_table_name(&self.job_id);
        let insert_sql = format!("INSERT INTO {table_name} (id, scan_state, baseline_committed) VALUES (?, ?, ?)");

        debug!("Inserting scan state: id=1, scan_state={}", scan_state);

        let client = idempotent_insert_client(&self.catalogue_write_client(&self.sync_client));
        execute_request(self.native_insert_safety(), || {
            let query = client
                .query(&insert_sql)
                .bind(1u8)
                .bind(scan_state)
                .bind(baseline_committed);
            async move { query.execute().await.map_err(DatabaseError::from) }
        })
        .await?;

        debug!("Inserted scan state record: id=1, scan_state={}", scan_state);
        Ok(())
    }

    /// 将上次未提交的 working 快照重新标记为 committed，作为同 generation 重试的基线。
    ///
    /// `ReplacingMergeTree` 可能已合并掉更早的 committed 物理行，不能通过删除 working 行恢复；
    /// 同步 mutation 原地回标可保留失败轮已安全写入的条目，并让重试正确识别之后的缺失项。
    async fn rebase_uncommitted_generation(&self, committed: u8, working: u8) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        if !self.table_exists(&base_table_name).await? {
            return Ok(());
        }
        self.execute(
            &format!(
                "ALTER TABLE {} UPDATE current_state = ? \
                 WHERE current_state = ? SETTINGS mutations_sync = 2",
                self.catalogue_storage_table(&base_table_name)?
            ),
            &[Value::from(committed), Value::from(working)],
        )
        .await?;
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

        let (temp_non_null, base_non_null) = self.check_file_handle_status(temp_table_name, &base_table_name).await?;

        let strategy = JoinStrategy::from_file_handle_status(temp_non_null, base_non_null);

        let query = query_builder(temp_table_name, &base_table_name, &strategy);
        trace!("Querying {}: {}", query_type, query);

        let rows = self
            .sync_client
            .query(&query)
            .fetch_quorum_all::<StorageEntryRecord>()
            .await?;

        debug!("Found {} {} files", rows.len(), query_type);
        Ok(rows)
    }

    /// 返回 (`temp_non_null`, `base_non_null`)：两表各自非 NULL `file_handle` 的数量，
    /// 用于决定 JOIN 策略（见 `JoinStrategy::from_file_handle_status`）。
    async fn check_file_handle_status(&self, temp_table_name: &str, base_table_name: &str) -> Result<(usize, usize)> {
        let query = format!(
            "SELECT t.non_null, b.non_null \
             FROM (SELECT if(countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle, '54534934')) = 0, count(file_handle), 0) AS non_null FROM {temp_table_name}) t \
             CROSS JOIN (SELECT if(countIf(file_handle IS NOT NULL AND NOT startsWith(file_handle, '54534934')) = 0, count(file_handle), 0) AS non_null FROM {base_table_name}) b"
        );

        debug!(
            "Checking file_handle status for temporary table '{}' and base table '{}'",
            temp_table_name, base_table_name
        );

        let (tn, bn): (u64, u64) = self.sync_client.query(&query).fetch_quorum_one().await?;

        // 旧驱动句柄与新的中立 identity key 不可混用；升级过渡代按路径比较。
        let (temp_non_null, base_non_null) = if tn == 0 || bn == 0 {
            (0, 0)
        } else {
            (tn as usize, bn as usize)
        };

        trace!(
            "file_handle status - temp non_null={}, base non_null={}",
            temp_non_null, base_non_null
        );

        Ok((temp_non_null, base_non_null))
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

        let delete_query = self.catalogue_delete_statement(table_name, &vec!["?"; deleted_paths.len()].join(", "))?;
        let paths = deleted_paths.iter().cloned().map(Value::from).collect::<Vec<_>>();
        self.execute(&delete_query, &paths).await?;

        info!("Successfully batch deleted {} base records", deleted_paths.len());
        Ok(())
    }

    /// 通用批量插入方法，支持同步和异步客户端模式
    async fn batch_insert<'a>(
        &self, table_name: &str, records: &'a [Arc<ObservedEntry>], use_async_client: bool,
    ) -> Result<()> {
        self.insert_views(
            table_name,
            Box::new(records.iter().flat_map(observation_views)),
            use_async_client,
        )
        .await
    }

    async fn insert_views<'a>(
        &self, table_name: &str, views: Box<dyn Iterator<Item = ObservedView<'a>> + Send + 'a>, use_async_client: bool,
    ) -> Result<()> {
        let mut views = views.peekable();
        if views.peek().is_none() {
            return Ok(());
        }
        let current_state = self.get_cached_scan_state().await?;
        let _write = self.catalogue_write_guard().await;
        debug!(
            "During batch inserting to table {}, current_state: {:?}",
            table_name, current_state
        );

        let mut record_count = 0;

        let client = if use_async_client {
            &self.async_client
        } else {
            &self.sync_client
        };
        let client = self.catalogue_write_client(client).with_validation(false);

        let mut insert = client
            .insert::<NativeScanRecord>(table_name)
            .await
            .map_err(DatabaseError::from)?;

        for view in views {
            let record = NativeScanRecord::capture(view, current_state)?;
            insert.write(&record).await.map_err(DatabaseError::from)?;
            record_count += 1;
        }

        insert.end().await.map_err(DatabaseError::from)?;

        info!("Successfully inserted {} records to table {}", record_count, table_name);
        Ok(())
    }
}

#[async_trait]
impl Database for ClickHouseDatabase {
    fn clone_box(&self) -> Box<dyn Database> {
        let new_db = Self {
            sync_client: self.sync_client.clone(),
            catalogue_cluster: self.catalogue_cluster.clone(),
            async_client: self.async_client.clone(),
            job_id: self.job_id.clone(),
            scan_temp_table_name: self.scan_temp_table_name.clone(),
            cached_scan_state: AtomicU8::new(self.cached_scan_state.load(Ordering::Relaxed)),
            resume_base_has_typed_handles: self.resume_base_has_typed_handles,
            native_dedup_ready: AtomicBool::new(self.native_dedup_ready.load(Ordering::Acquire)),
            generation_lifecycle: Arc::clone(&self.generation_lifecycle),
        };

        Box::new(new_db)
    }

    /// 初始化：创建主扫描表 + 状态表，仅在状态缺失时设置初始 `scan_state=0`
    async fn initialize(&self) -> Result<()> {
        self.create_table(SCAN_BASE_TABLE_BASE_NAME).await?;
        self.create_table(SCAN_STATE_TABLE_BASE_NAME).await?;
        if self.query_scan_state().await?.is_none() {
            self.insert_scan_state_with_baseline(0, false).await?;
        }
        Ok(())
    }

    /// 通过 SELECT 1 测试连接
    async fn ping(&self) -> Result<()> {
        self.sync_client.query("SELECT 1").fetch_quorum_one::<u8>().await?;

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
        let _write = self.catalogue_write_guard().await;

        let table_name = get_incremental_scan_base_table_name(&self.job_id);

        let records = messages
            .iter()
            .try_fold(Vec::new(), |mut records, message| -> Result<_> {
                records.extend(IncrementalStorageEntryRecord::from_message(message)?);
                Ok(records)
            })?;

        let record_count = records.len();

        let client = self.catalogue_write_client(&self.async_client);
        let mut insert = client
            .insert::<IncrementalStorageEntryRecord>(&table_name)
            .await
            .map_err(DatabaseError::from)?;

        for record in &records {
            trace!(
                "Inserting record: {:?} in incremental scan table {}",
                record, table_name
            );

            insert.write(record).await.map_err(DatabaseError::from)?;
        }

        insert.end().await.map_err(DatabaseError::from)?;

        info!(
            "Successfully inserted {} events to incremental scan table {}",
            record_count, table_name
        );
        Ok(())
    }

    /// 创建扫描临时表（MergeTree，自动生成唯一表名）
    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let temp_table_name = generate_scan_temp_table_name();
        self.create_catalogue_table(
            &temp_table_name,
            FILE_SCAN_COLUMNS_DEFINITION,
            "(relative_path, version_id, identity_key)",
            CatalogueEngine::MergeTree,
        )
        .await?;

        trace!(
            "ClickHouse scan temporary table '{}' created successfully",
            temp_table_name
        );
        self.scan_temp_table_name = Some(temp_table_name);
        Ok(())
    }

    /// 删除扫描临时表并清空 `scan_temp_table_name`
    async fn drop_scan_temporary_table(&mut self) -> Result<()> {
        if self.generation_lifecycle.lock().await.observation_publication_ambiguous {
            return Err(DatabaseError::TransactionError(
                "refusing to drop scan temporary table while EXCHANGE outcome is ambiguous".to_string(),
            ));
        }
        if let Some(temp_table_name) = &self.scan_temp_table_name {
            self.drop_catalogue_table(temp_table_name).await?;

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
        self.drop_catalogue_table(table_name).await?;
        Ok(())
    }

    /// 批量插入记录到临时表（async client）
    async fn batch_insert_temp_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::UnsupportedType("No temporary table available".to_string()))?;

        // 使用 async client，wait_for_async_insert=1 保证 end() 返回时数据已 flush
        self.batch_insert(temp_table_name, records, true).await
    }

    /// 批量插入记录到主表（async client）
    async fn batch_insert_base_record(&self, records: &[Arc<ObservedEntry>]) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, records, true).await
    }

    async fn batch_insert_completed_records(&self, records: &[ObservedItem]) -> Result<()> {
        let table = get_scan_base_table_name(&self.job_id);
        self.insert_views(&table, Box::new(records.iter().map(ObservedItem::view)), true)
            .await
    }

    /// 更新主表中的单条记录（利用 `ReplacingMergeTree` 插入覆盖）
    async fn update_base_record(&self, record: &Arc<ObservedEntry>) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);
        self.batch_insert(&base_table_name, &[Arc::clone(record)], false).await
    }

    /// 批量删除主表中的记录（按 `relative_path`）
    async fn batch_delete_base_record(&self, deleted_paths: &[String]) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
        self.batch_delete_record(&table_name, deleted_paths).await
    }

    async fn begin_copy_generation(&self, request_key: &str) -> Result<CopyGenerationStatus> {
        self.begin_catalogue_copy(request_key).await
    }

    async fn begin_scan_generation(&self) -> Result<u8> {
        self.invalidate_resume_state().await?;
        let mut lifecycle = self.generation_lifecycle.lock().await;
        if lifecycle.observation_publication_ambiguous {
            return Err(DatabaseError::TransactionError(
                "cannot begin scan generation while observation EXCHANGE outcome is ambiguous".to_string(),
            ));
        }
        *lifecycle = ScanGenerationLifecycle::default();
        let committed = self.query_scan_state().await?.ok_or_else(|| {
            DatabaseError::TransactionError("Cannot begin scan generation before initialization".to_string())
        })?;
        if committed > 1 {
            return Err(DatabaseError::ConversionError(format!(
                "Invalid committed scan generation {committed}; expected 0 or 1"
            )));
        }
        let working = 1 - committed;
        self.rebase_uncommitted_generation(committed, working).await?;
        lifecycle.working = Some(working);
        Ok(working)
    }

    async fn commit_scan_generation(&self) -> Result<()> {
        let mut lifecycle = self.generation_lifecycle.lock().await;
        let working = lifecycle
            .working
            .ok_or_else(|| DatabaseError::TransactionError("Cannot commit scan generation before begin".to_string()))?;
        if lifecycle.committed {
            return Ok(());
        }
        self.insert_scan_state(working).await?;
        self.cached_scan_state.store(working, Ordering::Relaxed);
        lifecycle.committed = true;
        lifecycle.copy_recovery = false;
        Ok(())
    }

    async fn abandon_scan_resume(&self) -> Result<()> {
        self.invalidate_resume_state().await
    }

    /// 将临时表记录合并到主表（INSERT INTO SELECT），可排除指定路径
    async fn insert_temp_to_base_table(&self, excluded_paths: &[(String, String)]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::UnsupportedType("No temporary table available".to_string()))?;

        let temp_table_name = self.catalogue_query_table(temp_table_name)?;
        let base_table_name = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;

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
                base_table_name,
                crate::scan::rows::OBSERVED_COLUMNS,
                crate::scan::rows::OBSERVED_COLUMNS,
                temp_table_name
            );
            self.execute(&insert_sql, &[]).await?;
        } else {
            // 增量拷贝：Memory 排除表 + NOT IN 过滤
            let exclude_table = format!("exclude_{}", uuid::Uuid::new_v4().simple());

            // 1. 创建 Memory 排除表
            self.create_catalogue_exclusions(&exclude_table).await?;

            // 2. 写入排除路径，与同作业其它 quorum 写入保持顺序。
            self.write_catalogue_exclusions(&exclude_table, excluded_paths).await?;

            // 3. 带过滤的 INSERT INTO SELECT
            let exclude_query_table = self.catalogue_query_table(&exclude_table)?;
            let insert_sql = format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} \
                 WHERE (relative_path, version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {})",
                base_table_name,
                crate::scan::rows::OBSERVED_COLUMNS,
                crate::scan::rows::OBSERVED_COLUMNS,
                temp_table_name,
                exclude_query_table
            );
            self.execute(&insert_sql, &[]).await?;

            // 4. 清理排除表
            self.drop_catalogue_table(&exclude_table).await?;
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
    async fn detect_new_items(&self) -> Result<Box<dyn Iterator<Item = ObservedEntry> + Send>> {
        if self.is_copy_recovery().await {
            return records_to_entry_iter(self.copy_replay_items().await?);
        }
        let records = self
            .detect_items("new", |temp_table, base_table, strategy| {
                let (vc_join, vc_expr) = generate_version_count_join_sql!(base_table);
                strategy.build_detect_new_sql(temp_table, base_table, &vc_join, &vc_expr)
            })
            .await?;

        records_to_entry_iter(records)
    }

    async fn detect_changed_items(&self) -> Result<Box<dyn Iterator<Item = (ObservedEntry, ChangeKind)> + Send>> {
        if self.is_copy_recovery().await {
            return Ok(Box::new(std::iter::empty()));
        }
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
            .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::DataOnly)))
            .chain(
                metadata_only
                    .into_iter()
                    .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::MetadataOnly))),
            )
            .chain(
                both.into_iter()
                    .map(|r| r.to_observation().map(|entry| (entry, ChangeKind::Both))),
            );
        let entries = iter.collect::<Result<Vec<_>>>()?;
        Ok(Box::new(entries.into_iter()))
    }

    /// 检测已删除或重命名的文件（old-state 记录 + `file_handle` 分组判定）
    async fn detect_deleted_items(&self) -> Result<Box<dyn Iterator<Item = DeletionStatus> + Send>> {
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
                let batch_rows = q.fetch_quorum_all::<StorageEntryRecord>().await?;

                for row in batch_rows {
                    if let Some(fh) = &row.file_handle {
                        fh_groups.entry(fh.clone()).or_default().push(row);
                    }
                }
            }
        }

        // 第四步：使用纯函数分类
        let deletion_statuses = classify_deletion_status(no_fh_records, fh_records, &fh_groups)?;

        debug!("Found {} deleted or renamed entries", deletion_statuses.len());

        Ok(Box::new(deletion_statuses.into_iter()))
    }

    /// 获取指定表的记录总数（使用 FINAL 确保去重）
    async fn get_count(&self, table_name: &str) -> Result<u64> {
        let full_table_name = format!("{}_{}", table_name, sanitize_job_id(&self.job_id));
        let query = format!("SELECT COUNT() FROM {full_table_name} FINAL");

        let count = self.sync_client.query(&query).fetch_quorum_one::<u64>().await?;

        Ok(count)
    }

    async fn query_storage_entry(
        &self, is_dir: Option<bool>, is_symlink: Option<bool>, extension: Option<String>,
        tx: mpsc::Sender<ObservedEntry>,
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
        self.send_catalogue_records(q, tx).await
    }

    async fn create_tar_manifest_table(&self) -> Result<()> {
        self.create_tar_manifest_table_impl().await
    }

    async fn batch_insert_tar_manifest(&self, records: &[TarManifestRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let _write = self.catalogue_write_guard().await;

        let table_name = get_tar_manifest_table_name(&self.job_id);

        let client = self.catalogue_write_client(&self.async_client);
        let mut insert = client
            .insert::<TarManifestRecord>(&table_name)
            .await
            .map_err(DatabaseError::from)?;

        for record in records {
            insert.write(record).await.map_err(DatabaseError::from)?;
        }

        insert.end().await.map_err(DatabaseError::from)?;

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
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use storage_adapter::ObservedEntryExt;

    use super::*;
    use crate::common::DeletionStatus;
    use crate::traits::StorageEntryRecord;

    /// 构造 StorageEntryRecord，只需指定关键差异字段
    fn make_record(relative_path: &str, file_handle: Option<&str>, ctime: i64) -> StorageEntryRecord {
        {
            let fixture_path: std::path::PathBuf = (relative_path.to_string()).into();
            let fixture_storage: String = "nas".to_string();
            let fixture_version: String = String::new();
            let fixture_handle: Option<String> = file_handle.map(String::from);
            let fixture_tags: Option<String> = None;
            let entry = crate::observed_fixtures::observation(
                if fixture_storage == "s3" {
                    data_mover::model::BackendKind::S3
                } else {
                    data_mover::model::BackendKind::Nfs
                },
                fixture_path,
                data_mover::model::EntryKind::File,
                1024,
                [ctime, ctime, ctime],
                Some(data_mover::model::OwnershipMode {
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }),
                fixture_handle.map(String::into_bytes),
                fixture_tags.map(|tags| {
                    serde_json::from_str::<Vec<storage_adapter::Tag>>(&tags)
                        .unwrap()
                        .into_iter()
                        .map(|tag| (tag.key, tag.value))
                        .collect()
                }),
                (!fixture_version.is_empty()).then_some((Some(fixture_version), true, false)),
            );
            StorageEntryRecord::capture(&entry, 0).unwrap()
        }
    }

    // ═══ 1.1 StorageEntryRecord 转换测试 ═══

    #[test]
    fn test_nas_entry_roundtrip() {
        let nas = {
            let fixture_path: std::path::PathBuf = PathBuf::from("dir/test.txt");
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::Nfs,
                fixture_path,
                data_mover::model::EntryKind::File,
                2048,
                [200, 300, 100],
                Some(data_mover::model::OwnershipMode {
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                }),
                (Some(bytes::Bytes::from_static(&[0xDE, 0xAD]))).map(|handle| handle.to_vec()),
                None,
                None,
            )
        };

        let record = StorageEntryRecord::capture(&nas, 1).unwrap();
        assert_eq!(record.storage_type, "nas");
        assert_eq!(record.file_handle, nas.get_file_handle().map(hex::encode));
        assert_eq!(record.version_id, "");
        assert_eq!(record.current_state, 1);
        assert_eq!(record.size, 2048);

        let back = record.to_observation().unwrap();
        assert_eq!(back.get_name(), "test.txt");
        assert_eq!(back.get_size(), 2048);
        assert_eq!(back.get_mtime(), 300);
    }

    #[test]
    fn test_s3_entry_roundtrip() {
        let s3 = {
            let fixture_path: std::path::PathBuf = ("bucket/obj.json".to_string()).into();
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::S3,
                fixture_path,
                data_mover::model::EntryKind::File,
                512,
                [0, 400, 0],
                None,
                None,
                None,
                Some((Some("v1".to_string()), true, false)),
            )
        };

        let record = StorageEntryRecord::capture(&s3, 0).unwrap();
        assert_eq!(record.storage_type, "s3");
        assert_eq!(record.version_id, "v1");
        assert_eq!(record.version_count, Some(1));
        assert_eq!(record.to_observation().unwrap(), s3);

        let back = record.to_observation().unwrap();
        assert_eq!(back.get_version_id(), Some("v1"));
    }

    #[test]
    fn test_nas_entry_optional_fields_none() {
        let nas = {
            let fixture_path: std::path::PathBuf = PathBuf::from("a.txt");
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::Nfs,
                fixture_path,
                data_mover::model::EntryKind::File,
                0,
                [0, 0, 0],
                None,
                None,
                None,
                None,
            )
        };

        let record = StorageEntryRecord::capture(&nas, 0).unwrap();
        assert_eq!(record.file_handle, None);
        assert_eq!(record.mode, None);

        let back = record.to_observation().unwrap();
        assert_eq!(back.get_file_handle(), None);
        assert_eq!(back.get_mode(), None);
    }

    #[test]
    fn test_current_state_propagation() {
        let nas = {
            let fixture_path: std::path::PathBuf = PathBuf::from("a.txt");
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::Nfs,
                fixture_path,
                data_mover::model::EntryKind::File,
                100,
                [0, 0, 0],
                Some(data_mover::model::OwnershipMode {
                    mode: 0o644,
                    uid: 0u32,
                    gid: 0u32,
                }),
                None,
                None,
                None,
            )
        };

        let r0 = StorageEntryRecord::capture(&nas, 0).unwrap();
        assert_eq!(r0.current_state, 0);

        let r1 = StorageEntryRecord::capture(&nas, 1).unwrap();
        assert_eq!(r1.current_state, 1);
    }

    // ═══ 1.2 file_handle 分组逻辑测试（优化 2 核心逻辑）═══

    #[test]
    fn test_classify_single_fh_is_deleted() {
        let fh_records = vec![make_record("a.txt", Some("abc"), 100)];
        let mut groups = HashMap::new();
        groups.insert(
            make_record("_", Some("abc"), 0).file_handle.unwrap(),
            vec![make_record("a.txt", Some("abc"), 100)],
        );

        let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], DeletionStatus::Deleted(_)));
    }

    #[test]
    fn test_classify_two_fh_is_renamed() {
        let fh_records = vec![make_record("old.txt", Some("abc"), 100)];
        let mut groups = HashMap::new();
        groups.insert(
            make_record("_", Some("abc"), 0).file_handle.unwrap(),
            vec![
                make_record("old.txt", Some("abc"), 100),
                make_record("new.txt", Some("abc"), 200),
            ],
        );

        let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
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
            make_record("_", Some("abc"), 0).file_handle.unwrap(),
            vec![
                make_record("new.txt", Some("abc"), 200),
                make_record("old.txt", Some("abc"), 100),
            ],
        );

        let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
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

        let result = classify_deletion_status(vec![], fh_records, &groups).unwrap();
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
        groups.insert(
            make_record("_", Some("fh_a"), 0).file_handle.unwrap(),
            vec![make_record("c.txt", Some("fh_a"), 100)],
        );
        // fh_b: 2 条 → Renamed
        groups.insert(
            make_record("_", Some("fh_b"), 0).file_handle.unwrap(),
            vec![
                make_record("d.txt", Some("fh_b"), 100),
                make_record("d_new.txt", Some("fh_b"), 200),
            ],
        );
        // fh_c: 3 条 → 跳过 (error log)
        groups.insert(
            make_record("_", Some("fh_c"), 0).file_handle.unwrap(),
            vec![
                make_record("e1.txt", Some("fh_c"), 100),
                make_record("e2.txt", Some("fh_c"), 200),
                make_record("e3.txt", Some("fh_c"), 300),
            ],
        );

        let result = classify_deletion_status(no_fh, fh_records, &groups).unwrap();
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
        // 两侧都无 fh（local/S3）→ Path
        assert!(matches!(
            JoinStrategy::from_file_handle_status(0, 0),
            JoinStrategy::Path
        ));
        // 任一侧有 fh（NFS/CIFS）→ FileHandle
        assert!(matches!(
            JoinStrategy::from_file_handle_status(10, 5),
            JoinStrategy::FileHandle
        ));
        assert!(matches!(
            JoinStrategy::from_file_handle_status(3, 0),
            JoinStrategy::FileHandle
        ));
        // 回归：两侧都无 fh 时，无论 base/temp 是否为空都必须用 Path。
        // 旧实现要求 base_total>0 才用 Path，导致全量中断后续传（base 空）误入
        // FileHandle，`NULL NOT IN (...)` 漏判 local 新增文件 → 静默不拷贝。
        assert!(matches!(
            JoinStrategy::from_file_handle_status(0, 0),
            JoinStrategy::Path
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
        assert!(sql.contains("FROM base_y WHERE file_handle IS NOT NULL"));
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
        assert!(!join_clause.contains("row_kind"));
        assert!(join_clause.contains("GROUP BY relative_path"));
        assert!(select_expr.contains("CASE WHEN"));
        assert!(select_expr.contains("COALESCE"));
    }

    #[tokio::test]
    async fn ambiguous_exchange_blocks_begin_and_destructive_abort() -> Result<()> {
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: "http://127.0.0.1:1".to_string(),
            dial_timeout: 1,
            read_timeout: 1,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        };
        let mut database = ClickHouseDatabase::new(&config, "ambiguous_exchange")?;
        let mut second_handle = ClickHouseDatabase::new(&config, "ambiguous_exchange")?;
        database.scan_temp_table_name = Some("rollback_table".to_string());
        database
            .generation_lifecycle
            .lock()
            .await
            .observation_publication_ambiguous = true;

        assert!(matches!(
            database.begin_observation_snapshot().await,
            Err(DatabaseError::TransactionError(_))
        ));
        assert!(matches!(
            database.abort_observation_snapshot().await,
            Err(DatabaseError::TransactionError(_))
        ));
        assert_eq!(database.scan_temp_table_name.as_deref(), Some("rollback_table"));
        assert!(matches!(
            second_handle.begin_observation_snapshot().await,
            Err(DatabaseError::TransactionError(_))
        ));
        assert!(matches!(
            Database::drop_scan_temporary_table(&mut second_handle).await,
            Err(DatabaseError::TransactionError(_))
        ));
        Ok(())
    }
}

mod native;

#[cfg(test)]
mod baseline_tests;
