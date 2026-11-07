//! 比对批次的暂存、分类、发布及清理。分类结果重新绑定输入观察，保留源端版本顺序。

use std::sync::Arc;

use data_mover::model::ObservedEntry;
use serde::Deserialize;
use serde_json::Value;
use task_model::migration::TargetCleanupPolicy;
use task_model::observation::{ObservedItem, ObservedView};
use task_model::{ChangeKind, EntryChange, MetadataTargetVersion};
use tracing::{debug, info};

use super::ClickHouseDatabase;
use super::versions::CatalogueVersions;
use crate::scan::rows::OBSERVED_COLUMNS;
use crate::schema::columns::{
    CATALOGUE_COLUMNS as FILE_SCAN_COLUMNS_LIST,
    CATALOGUE_COLUMNS_WITH_T_PREFIX as FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX,
};
use crate::{
    error::{DatabaseError, Result},
    execution::read::QuorumRead,
    get_scan_base_table_name,
};

#[cfg(test)]
#[path = "comparison/session_tests.rs"]
mod session_tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonMode {
    PublishObserved,
    AwaitCopyAcknowledgement,
}

/// 只携带分类所需的主键，不重新解码或复制输入观察。
struct CatalogueDifference {
    relative_path: String,
    version_id: String,
    kind: EntryChange,
    target_version: Option<String>,
}
pub struct CatalogueChange {
    pub item: ObservedItem,
    pub kind: EntryChange,
    pub metadata_target: MetadataTargetVersion,
}

#[derive(Deserialize, clickhouse::Row)]
struct DifferenceRow {
    relative_path: String,
    version_id: String,
    change_code: u8,
    target_version: Option<String>,
}

impl ClickHouseDatabase {
    async fn classify_batch(&self) -> Result<Vec<CatalogueDifference>> {
        let temporary = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("catalogue comparison staging".into()))?;
        let temporary = self.catalogue_query_table(temporary)?;
        let (force_copy, preserve_target, copy_only) = {
            let lifecycle = self.generation_lifecycle.lock().await;
            (
                lifecycle
                    .copy_generation
                    .as_ref()
                    .is_some_and(|generation| generation.plan.force_copy),
                lifecycle
                    .copy_generation
                    .as_ref()
                    .is_some_and(|generation| generation.plan.target_cleanup == TargetCleanupPolicy::Preserve),
                lifecycle.copy_generation.is_some(),
            )
        };
        let query = if force_copy || self.is_copy_recovery().await {
            let base = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
            format!(
                "SELECT t.relative_path,t.version_id,toUInt8(if(t.is_dir = 1 AND t.marker_state IS NULL AND f.marker_state IS NOT NULL,4,0)) AS change_code,CAST(NULL AS Nullable(String)) AS target_version FROM {temporary} t LEFT JOIN (SELECT relative_path,version_id,marker_state FROM {base} FINAL ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id) f USING (relative_path,version_id)"
            )
        } else {
            let base = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
            let strategy = if preserve_target {
                JoinStrategy::CopyPath
            } else {
                let (temporary_handles, baseline_handles) = self.check_file_handle_status(&temporary, &base).await?;
                JoinStrategy::from_file_handle_status(temporary_handles, baseline_handles)
            };
            let queries = [
                strategy.build_detect_new_sql(&temporary, &base, copy_only),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::DataOnly, false, copy_only),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::MetadataOnly, true, copy_only),
                strategy.build_detect_changed_sql(&temporary, &base, ChangeKind::Both, false, copy_only),
                strategy.build_detect_changed_sql(
                    &temporary,
                    &base,
                    ChangeKind::DirectoryMarkerRemoved,
                    false,
                    copy_only,
                ),
            ];
            queries
                .into_iter()
                .enumerate()
                .map(|(code, query)| {
                    let binding = if code == 2 { "copy_target_version" } else { "CAST(NULL AS Nullable(String))" };
                    format!("SELECT relative_path,version_id,toUInt8({code}) AS change_code,{binding} AS target_version FROM ({query})")
                })
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        };
        self.sync_client
            .query(&query)
            .fetch_quorum_all::<DifferenceRow>()
            .await?
            .into_iter()
            .map(|row| {
                let kind = match row.change_code {
                    0 => EntryChange::New,
                    1 => EntryChange::Changed(ChangeKind::DataOnly),
                    2 => EntryChange::Changed(ChangeKind::MetadataOnly),
                    3 => EntryChange::Changed(ChangeKind::Both),
                    4 => EntryChange::Changed(ChangeKind::DirectoryMarkerRemoved),
                    code => {
                        return Err(DatabaseError::ConversionError(format!(
                            "Unknown catalogue classification {code}"
                        )));
                    }
                };
                Ok(CatalogueDifference {
                    relative_path: row.relative_path,
                    version_id: row.version_id,
                    kind,
                    target_version: row.target_version,
                })
            })
            .collect()
    }
}

fn bind_input_order(
    entries: &[Arc<ObservedEntry>], differences: &mut Vec<CatalogueDifference>, versions: CatalogueVersions,
) -> Result<Vec<CatalogueChange>> {
    if differences.is_empty() {
        return Ok(Vec::new());
    }
    differences.sort_unstable_by(|left, right| {
        (left.relative_path.as_str(), left.version_id.as_str())
            .cmp(&(right.relative_path.as_str(), right.version_id.as_str()))
    });
    differences.dedup_by(|left, right| {
        left.relative_path == right.relative_path && left.version_id == right.version_id && left.kind == right.kind
    });
    if differences
        .windows(2)
        .any(|pair| pair[0].relative_path == pair[1].relative_path && pair[0].version_id == pair[1].version_id)
    {
        return Err(DatabaseError::ObservationProjectionMismatch {
            field: "conflicting catalogue classifications",
        });
    }
    let mut consumed = vec![usize::MAX; differences.len()];
    let mut changes: Vec<CatalogueChange> = Vec::with_capacity(differences.len());
    for entry in entries {
        let view = ObservedView {
            entry,
            version_index: entry.history().map(|_| 0),
        };
        let path = view.path_key();
        for view in ObservedView::all(entry).filter(|view| versions.includes(*view)) {
            let version = versions.key(view);
            if let Ok(index) = differences.binary_search_by(|row| {
                (row.relative_path.as_str(), row.version_id.as_str()).cmp(&(path.as_ref(), version))
            }) {
                if consumed[index] != usize::MAX {
                    let previous = changes[consumed[index]].item.view();
                    let same = match (previous.version(), view.version()) {
                        (Some(left), Some(right)) => left == right,
                        (None, None) => previous.entry == view.entry,
                        _ => false,
                    };
                    if !same {
                        return Err(DatabaseError::ObservationProjectionMismatch {
                            field: "conflicting source observations",
                        });
                    }
                    continue;
                }
                consumed[index] = changes.len();
                changes.push(CatalogueChange {
                    item: ObservedItem {
                        entry: Arc::clone(entry),
                        version_index: view.version_index,
                    },
                    kind: differences[index].kind,
                    metadata_target: if versions.key(view).is_empty() {
                        MetadataTargetVersion::Current
                    } else {
                        differences[index]
                            .target_version
                            .take()
                            .filter(|version| !version.is_empty())
                            .map_or(MetadataTargetVersion::Unconfirmed, MetadataTargetVersion::Bound)
                    },
                });
            }
        }
    }
    if consumed.contains(&usize::MAX) {
        return Err(DatabaseError::ObservationProjectionMismatch {
            field: "catalogue classification key",
        });
    }
    Ok(changes)
}

impl ClickHouseDatabase {
    /// Owns comparison staging and cleanup; results share the selected input observations.
    pub async fn compare_catalogue_batch(
        &self, entries: &[Arc<ObservedEntry>], mode: ComparisonMode,
    ) -> Result<Vec<CatalogueChange>> {
        let mut database = self.clone();
        database.create_scan_temporary_table().await?;
        let operation = async {
            database.batch_insert_temp_record(entries).await?;
            let mut differences = database.classify_batch().await?;
            let changes = bind_input_order(entries, &mut differences, self.catalogue_versions().await)?;
            // 复用分类查询拥有的主键；不再次复制观察中的路径／版本字符串。
            let excluded = match mode {
                ComparisonMode::PublishObserved => Vec::new(),
                ComparisonMode::AwaitCopyAcknowledgement => differences
                    .into_iter()
                    .map(|row| {
                        (
                            row.relative_path,
                            row.version_id,
                            row.kind == EntryChange::Changed(ChangeKind::MetadataOnly),
                        )
                    })
                    .collect(),
            };
            database.insert_temp_to_base_table(&excluded).await?;
            Ok(changes)
        }
        .await;
        let cleanup = database.drop_scan_temporary_table().await;
        match (operation, cleanup) {
            (Ok(changes), Ok(())) => Ok(changes),
            (Err(source), Ok(())) | (Ok(_), Err(source)) => Err(source),
            (Err(source), Err(cleanup)) => Err(DatabaseError::CatalogueBatchCleanupFailed {
                source: Box::new(source),
                cleanup: Box::new(cleanup),
            }),
        }
    }
}

/// JOIN 策略：path-based 或 file_handle-based
///
/// 决定增量扫描对比查询使用 `relative_path+version_id` 还是 `file_handle` 作为 JOIN 键。
/// 当两表的 `file_handle` 全为 NULL 时使用 Path 模式，否则使用 `FileHandle` 模式。
pub(super) enum JoinStrategy {
    /// 基于 `relative_path` + `version_id` 的对比
    Path,
    /// Preserve 时新路径必须产生复制工作；同路径的类型／稳定身份替换也不能复用旧确认。
    CopyPath,
    /// 基于 `file_handle` 的对比
    FileHandle,
}

impl JoinStrategy {
    fn confirmation_filter(data: bool, copy_only: bool) -> &'static str {
        match (data, copy_only) {
            (true, true) => "copy_data_confirmed AND copy_sequence>0 AND copy_execution_id!=''",
            (false, true) => "copy_path_confirmed AND copy_sequence>0 AND copy_execution_id!=''",
            (true, false) => "copy_data_confirmed",
            (false, false) => "copy_path_confirmed",
        }
    }

    /// 根据临时表和主表的 `file_handle` 统计信息决定 JOIN 策略
    ///
    /// 策略只取决于「是否存在 `file_handle`」，与表是否为空无关：
    /// 两侧都无 `file_handle`（local/S3）→ 必须用 Path（`relative_path+version_id`）；
    /// 任一侧有 `file_handle`（NFS/CIFS）→ 用 `FileHandle`。
    ///
    /// 注意：不得用 `base_total > 0` 之类的「表非空」作为前置条件——全量拷贝中断后
    /// 续传时 base 为空，但 local/S3 文件无 `file_handle`，若误入 `FileHandle` 分支，
    /// `NULL NOT IN (...)` 会求值为 NULL 导致新增文件漏判、被静默并入 base 而不拷贝。
    pub(super) fn from_file_handle_status(temp_non_null: usize, base_non_null: usize) -> Self {
        if temp_non_null == 0 && base_non_null == 0 {
            Self::Path
        } else {
            Self::FileHandle
        }
    }

    /// 构建 `detect_new` SQL：temp 中存在但 base 中不存在的记录
    ///
    pub(super) fn build_detect_new_sql(&self, temp: &str, base: &str, copy_only: bool) -> String {
        let confirmed = Self::confirmation_filter(true, copy_only);
        let columns = &*FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX;
        match self {
            Self::CopyPath => format!(
                "SELECT {columns} FROM {temp} as t WHERE (t.relative_path,t.version_id,ifNull(t.file_handle,''),t.entry_kind) NOT IN \
                 (SELECT relative_path,version_id,ifNull(file_handle,''),entry_kind FROM {base} FINAL WHERE {confirmed} \
                  AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {temp})) \
                 ORDER BY t.relative_path,t.history_segment,t.history_index,t.version_id"
            ),
            Self::Path => format!(
                "SELECT {columns} FROM {temp} as t \
                 WHERE (t.relative_path, t.version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {base} FINAL WHERE {confirmed}) \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
            ),
            Self::FileHandle => format!(
                "SELECT {columns} FROM {temp} as t \
                 WHERE t.file_handle NOT IN \
                 (SELECT file_handle FROM {base} FINAL WHERE {confirmed} AND file_handle IS NOT NULL) \
                 OR (t.relative_path,t.version_id) IN \
                 (SELECT p.relative_path,p.version_id FROM {temp} p \
                  JOIN (SELECT relative_path,version_id,file_handle FROM {base} FINAL \
                        WHERE {confirmed} AND (relative_path,version_id) IN \
                        (SELECT relative_path,version_id FROM {temp})) f \
                  ON p.relative_path=f.relative_path AND p.version_id=f.version_id \
                  WHERE NOT ifNull(p.file_handle=f.file_handle,p.file_handle IS NULL AND f.file_handle IS NULL)) \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
            ),
        }
    }

    /// 构建 `detect_changed` SQL：temp 与 base 都存在但存在变更
    ///
    /// 根据 `ChangeKind` 生成不同 WHERE 子句，三种 kind 互斥：
    /// - `DataOnly`：数据比较事实变了；元数据未变
    /// - `MetadataOnly`：大小、mtime、真实源版本及身份均未变；元数据变了
    /// - `Both`：内容和属性都变了
    pub(super) fn build_detect_changed_sql(
        &self, temp: &str, base: &str, kind: ChangeKind, include_binding: bool, copy_only: bool,
    ) -> String {
        let confirmed = Self::confirmation_filter(true, copy_only);
        let binding = if include_binding {
            ",f.copy_target_version AS copy_target_version"
        } else {
            ""
        };
        let t_columns = format!("{}{binding}", *FILE_SCAN_COLUMNS_LIST_WITH_T_PREFIX);
        let binding_column = if include_binding { ",copy_target_version" } else { "" };
        let marker_eq = "ifNull(t.marker_state = f.marker_state, (t.marker_state IS NULL AND f.marker_state IS NULL))";
        // PathScoped 身份可携带 ETag 等 backend 观察事实；null／非版本化对象覆盖后
        // 大小、mtime 和版本槽仍可能相同。复用已持久化的比较键，不额外读取或哈希数据。
        let data_changed = format!(
            "((t.is_dir = 0 AND (t.size != f.size OR t.mtime != f.mtime OR t.source_version_id != f.source_version_id OR t.identity_key != f.identity_key)) OR NOT {marker_eq})"
        );
        let data_unchanged = format!(
            "((t.is_dir = 1 OR (t.size = f.size AND t.mtime = f.mtime AND t.source_version_id = f.source_version_id AND t.identity_key = f.identity_key)) AND {marker_eq})"
        );
        // mode/uid/gid 在 S3 等协议中为 Nullable 且常常为 NULL；ClickHouse 三值逻辑下
        // `NULL = NULL` 返回 NULL（WHERE 视为 false），会让所有 ChangeKind 都不匹配。
        // 比较结果为 NULL 时，只有两侧均 NULL 才相等；单侧未知不能掩盖真实变更。
        // 此形式不依赖任何 sentinel，不会把真实值（如 mode=0）误等同于 NULL，
        // 也不依赖"两侧来自同一后端"这种隐含不变量。
        let mode_eq = "ifNull(t.mode = f.mode, (t.mode IS NULL AND f.mode IS NULL))";
        let uid_eq = "ifNull(t.uid = f.uid, (t.uid IS NULL AND f.uid IS NULL))";
        let gid_eq = "ifNull(t.gid = f.gid, (t.gid IS NULL AND f.gid IS NULL))";
        let meta_changed = format!(
            "(NOT {mode_eq} OR NOT {uid_eq} OR NOT {gid_eq} OR t.metadata_digests != f.metadata_digests OR (t.is_dir = 1 AND t.mtime != f.mtime) OR NOT f.copy_path_confirmed)"
        );
        let meta_unchanged = format!(
            "({mode_eq} AND {uid_eq} AND {gid_eq} AND t.metadata_digests = f.metadata_digests AND (t.is_dir = 0 OR t.mtime = f.mtime) AND f.copy_path_confirmed)"
        );

        let marker_removed = "(t.is_dir = 1 AND t.marker_state IS NULL AND f.marker_state IS NOT NULL)";
        let kind_filter = match kind {
            ChangeKind::DirectoryMarkerRemoved => marker_removed.into(),
            ChangeKind::DataOnly => format!("{data_changed} AND {meta_unchanged}"),
            ChangeKind::MetadataOnly => format!("{data_unchanged} AND {meta_changed}"),
            ChangeKind::Both => format!("{data_changed} AND {meta_changed}"),
        };

        let kind_filter = if kind == ChangeKind::DirectoryMarkerRemoved {
            kind_filter
        } else {
            format!("({kind_filter}) AND NOT {marker_removed}")
        };
        let identity_columns = if matches!(self, Self::CopyPath) {
            ",file_handle,entry_kind"
        } else {
            ""
        };
        let identity_match = if matches!(self, Self::CopyPath) {
            " AND ifNull(t.file_handle=f.file_handle,t.file_handle IS NULL AND f.file_handle IS NULL) AND t.entry_kind=f.entry_kind"
        } else {
            ""
        };
        match self {
            Self::Path | Self::CopyPath => format!(
                "SELECT {t_columns} FROM {temp} t \
                 JOIN (SELECT relative_path, version_id, source_version_id, identity_key, size, mtime, mode, uid, gid, is_dir, marker_state, metadata_digests,copy_path_confirmed{binding_column}{identity_columns} \
                       FROM {base} FINAL \
                       WHERE {confirmed} AND (relative_path, version_id) IN \
                             (SELECT relative_path, version_id FROM {temp}) \
                       ORDER BY relative_path, version_id \
                       LIMIT 1 BY (relative_path, version_id) \
                 ) f ON t.relative_path = f.relative_path AND t.version_id = f.version_id \
                 WHERE ({kind_filter}){identity_match} \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
            ),
            Self::FileHandle => format!(
                "SELECT {t_columns} FROM {temp} t \
                 JOIN (SELECT file_handle, relative_path, source_version_id, identity_key, size, mtime, mode, uid, gid, is_dir, marker_state, metadata_digests,copy_path_confirmed{binding_column} \
                       FROM {base} FINAL \
                       WHERE {confirmed} AND file_handle IN \
                             (SELECT file_handle FROM {temp} WHERE file_handle IS NOT NULL) \
                       ORDER BY file_handle, relative_path \
                       LIMIT 1 BY (file_handle, relative_path) \
                 ) f ON t.file_handle = f.file_handle \
                   AND t.relative_path = f.relative_path \
                 WHERE {kind_filter} \
                 ORDER BY t.relative_path, t.history_segment, t.history_index, t.version_id"
            ),
        }
    }

    /// 构建 `detect_deleted` SQL：base 中 old-state 的记录
    /// 使用 FINAL 确保 `ReplacingMergeTree` 去重后再按 `current_state` 过滤
    pub(super) fn build_detect_deleted_sql(base: &str, copy_only: bool) -> String {
        let confirmed = Self::confirmation_filter(false, copy_only);
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        format!(
            "SELECT {columns} FROM {base} FINAL \
             WHERE current_state = ? AND {confirmed} \
             ORDER BY relative_path, version_id"
        )
    }

    /// 构建批量 `file_handle` 查询 SQL（优化 2）
    pub(super) fn build_batch_fh_query_sql(base: &str, batch_size: usize, copy_only: bool) -> String {
        let origin = if copy_only {
            " AND copy_sequence>0 AND copy_execution_id!=''"
        } else {
            ""
        };
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        let placeholders = vec!["?"; batch_size].join(", ");
        format!(
            "SELECT {columns} FROM {base} FINAL \
             WHERE file_handle IN ({placeholders}) AND current_state = ? AND (? OR NOT copy_path_confirmed){origin} \
             ORDER BY file_handle, relative_path, version_id \
             LIMIT 1 BY (relative_path, version_id)"
        )
    }
}

impl ClickHouseDatabase {
    /// 将临时表记录合并到主表（INSERT INTO SELECT），可排除指定路径
    pub async fn insert_temp_to_base_table(&self, excluded_paths: &[(String, String, bool)]) -> Result<()> {
        let temp_table_name = self
            .scan_temp_table_name
            .as_deref()
            .ok_or_else(|| DatabaseError::TableNotFound("No temporary table available".to_string()))?;

        let temp_table_name = self.catalogue_query_table(temp_table_name)?;
        let base_table_name = self.catalogue_query_table(&get_scan_base_table_name(&self.job_id))?;
        let copy_identity = self
            .generation_lifecycle
            .lock()
            .await
            .copy_generation
            .as_ref()
            .map(|generation| (generation.request.execution_id.clone(), generation.sequence));
        let copying = copy_identity.is_some();
        let columns = if copying {
            format!("{OBSERVED_COLUMNS},copy_path_confirmed,copy_data_confirmed,copy_target_version")
        } else {
            OBSERVED_COLUMNS.to_string()
        };
        // 只继承同一路径已有的目标确认；新观察的 rename 路径在物理操作成功前保持未确认。
        let (projection, from) = if copying {
            let observed = OBSERVED_COLUMNS
                .split(',')
                .map(|name| format!("t.{name}"))
                .collect::<Vec<_>>()
                .join(",");
            let same = "ifNull(t.file_handle=f.file_handle,t.file_handle IS NULL AND f.file_handle IS NULL)";
            (
                format!(
                    "{observed},(f.copy_path_confirmed AND {same}),(f.copy_data_confirmed AND {same}),f.copy_target_version"
                ),
                format!(
                    "{temp_table_name} t LEFT JOIN (SELECT relative_path,version_id,file_handle,copy_path_confirmed,copy_data_confirmed,copy_target_version FROM {base_table_name} FINAL WHERE (relative_path,version_id) IN (SELECT relative_path,version_id FROM {temp_table_name})) f ON t.relative_path=f.relative_path AND t.version_id=f.version_id"
                ),
            )
        } else {
            (OBSERVED_COLUMNS.to_string(), format!("{temp_table_name} t"))
        };

        debug!(
            "Inserting data from temporary table {} to base table {} (excluded: {} paths)",
            temp_table_name,
            base_table_name,
            excluded_paths.len()
        );

        if excluded_paths.is_empty() {
            // 增量扫描：全量插入（无额外开销）
            let insert_sql = format!("INSERT INTO {base_table_name} ({columns}) SELECT {projection} FROM {from}");
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
            let mut insert_sql = format!(
                "INSERT INTO {base_table_name} ({columns}) SELECT {projection} FROM {from} \
                 WHERE (t.relative_path, t.version_id) NOT IN \
                 (SELECT relative_path, version_id FROM {exclude_query_table})"
            );
            if copying {
                // 可变目标开始覆盖前撤销旧就绪证明，保留原值供诊断；失败后源版本回退也必须重复制。
                // 与本批观察同一条 INSERT，避免增加逐项请求或确认；保留标记对象的决定不触碰旧证明。
                let (_, sequence) = copy_identity.as_ref().ok_or(DatabaseError::CopyExecutionUnavailable)?;
                let state = self.get_cached_scan_state().await?;
                let pending = OBSERVED_COLUMNS
                    .split(',')
                    .map(|name| match name {
                        "copy_execution_id" => "?".to_owned(),
                        "copy_sequence" => format!("toUInt64({sequence})"),
                        "current_state" => format!("toUInt8({state})"),
                        _ => format!("f.{name}"),
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                insert_sql = format!(
                    "{insert_sql} UNION ALL SELECT {pending},false,(f.copy_data_confirmed AND (f.relative_path,f.version_id) IN (SELECT relative_path,version_id FROM {exclude_query_table} WHERE data_unchanged)),f.copy_target_version FROM (SELECT {OBSERVED_COLUMNS},copy_data_confirmed,copy_target_version FROM {base_table_name} FINAL \
                     WHERE (relative_path,version_id) IN (SELECT relative_path,version_id FROM {exclude_query_table})) AS f \
                     WHERE NOT (f.is_dir AND f.marker_state IS NOT NULL AND (f.relative_path,f.version_id) IN \
                     (SELECT relative_path,version_id FROM {temp_table_name} WHERE is_dir AND marker_state IS NULL))"
                );
            }
            let parameters = copy_identity
                .as_ref()
                .map_or_else(Vec::new, |(execution, _)| vec![Value::from(execution.clone())]);
            self.execute(&insert_sql, &parameters).await?;

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
}
