//! 单机与集群共用的原生扫描比较：限定本批主键并复核后端身份归属。
use crate::execution::queries::check_scan_cancellation;
use crate::execution::read::QuorumRead;
use crate::scan::ScanFinalEvent;
use crate::scan::candidate_cache::ScanMetadataHash;
use crate::scan::rows::directory_marker_state;
use crate::{
    error::{DatabaseError, Result},
    schema::columns::FILE_SCAN_COLUMNS_LIST,
    schema::entry::EntrySummary as StorageEntryRecord,
};
use clickhouse::{Client, query::Query, sql::Bind};
use data_mover::model::ObservedEntry;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{fmt, sync::Arc};
use storage_adapter::{
    ChangeKind,
    observed_scan::{EntryStatistics, ObservedItem, ObservedScanEvent, ObservedView},
};
use tokio::task::yield_now;
use tracing::warn;

const SPARSE_KEY_BYTES: usize = 16_384;

struct BoundBytes(usize);
impl fmt::Write for BoundBytes {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

fn sparse_key_group_end(keys: &[impl Bind], start: usize) -> Result<usize> {
    let mut end = start;
    let mut bytes = 2_usize; // 数组的左右定界符。
    while end < keys.len() {
        let mut encoded = BoundBytes(0);
        keys[end].write(&mut encoded).map_err(DatabaseError::ConversionError)?;
        let next = encoded.0.saturating_add(usize::from(end > start));
        if bytes.saturating_add(next) > SPARSE_KEY_BYTES {
            break;
        }
        bytes += next;
        end += 1;
    }
    Ok(end)
}

pub(crate) struct NativeScanAccess<'a> {
    pub client: &'a Client,
    pub base: String,
    pub stage: String,
    pub events: String,
    pub scope: String,
    pub stage_scope: String,
    pub active: String,
    pub live: &'static str,
    pub final_clause: &'static str,
    pub typed_handles: bool,
}
pub(crate) struct ComparedBatch {
    pub events: Vec<ObservedScanEvent>,
    pub displaced: Vec<StorageEntryRecord>,
}
#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
struct Baseline {
    marker_state: Option<String>,
    relative_path: String,
    version_id: String,
    size: u64,
    mtime: i64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    is_dir: bool,
    identity_changed: bool,
    previous_generation: bool,
}

#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
struct CandidateReference {
    relative_path: String,
    version_id: String,
    current_state: u8,
    metadata_hash: Option<String>,
}

async fn verified_candidates(
    mut cached: Vec<StorageEntryRecord>, references: Vec<CandidateReference>,
) -> Result<Option<Vec<StorageEntryRecord>>> {
    check_scan_cancellation()?;
    let mut keys = FxHashSet::default();
    for chunk in cached.chunks(1024) {
        cache_checkpoint().await?;
        keys.extend(chunk.iter().map(|row| (&row.relative_path, &row.version_id)));
    }
    if keys.len() != cached.len() || references.len() != cached.len() {
        return Ok(None);
    }
    drop(keys);
    let mut current = FxHashMap::default();
    for chunk in references.chunks(1024) {
        cache_checkpoint().await?;
        current.extend(
            chunk
                .iter()
                .map(|row| ((row.relative_path.as_str(), row.version_id.as_str()), row)),
        );
    }
    if current.len() != cached.len() {
        return Ok(None);
    }
    for chunk in cached.chunks_mut(1024) {
        cache_checkpoint().await?;
        for row in chunk {
            let Some(reference) = current.get(&(row.relative_path.as_str(), row.version_id.as_str())) else {
                return Ok(None);
            };
            if reference.metadata_hash.as_deref() != Some(row.scan_metadata_hash().as_str()) {
                return Ok(None);
            }
            row.current_state = reference.current_state;
        }
    }
    Ok(Some(cached))
}

async fn cache_checkpoint() -> Result<()> {
    check_scan_cancellation()?;
    yield_now().await;
    check_scan_cancellation()
}
impl Baseline {
    fn change(&self, item: &ObservedItem) -> Option<ChangeKind> {
        if self.is_dir {
            let marker = directory_marker_state(&item.entry);
            return if self.marker_state.is_some() && marker.is_none() {
                Some(ChangeKind::DirectoryMarkerRemoved)
            } else {
                (self.marker_state != marker).then_some(ChangeKind::DataOnly)
            };
        }
        let view = item.view();
        let data = self.size != view.get_size() || self.mtime != view.get_mtime();
        // 与行投影使用同一组独立权限事实，保留未知值与真实零值的区别。
        let meta = self.mode != view.mode() || self.uid != view.uid() || self.gid != view.gid();
        match (data, meta) {
            (true, true) => Some(ChangeKind::Both),
            (true, false) => Some(ChangeKind::DataOnly),
            (false, true) => Some(ChangeKind::MetadataOnly),
            _ => None,
        }
    }
}
fn baseline_index(rows: &[Baseline]) -> FxHashMap<(&str, &str), &Baseline> {
    rows.iter()
        .map(|row| ((row.relative_path.as_str(), row.version_id.as_str()), row))
        .collect()
}
impl NativeScanAccess<'_> {
    fn read_query(&self, sql: &str) -> Query {
        // 分摊并行读取，避免固定副本承担全部比较；保留 client 的 quorum 与取消设置。
        self.client.query(sql).with_option("load_balancing", "random")
    }

    pub async fn compare(&self, entries: &[Arc<ObservedEntry>]) -> Result<ComparedBatch> {
        if entries.is_empty() {
            return Ok(ComparedBatch {
                events: Vec::new(),
                displaced: Vec::new(),
            });
        }
        let mut paths: Vec<_> = entries
            .iter()
            .flat_map(|entry| ObservedView::all(entry).map(|view| view.sql_path()))
            .collect();
        paths.sort_unstable();
        let by_handle = self.typed_handles
            && entries
                .iter()
                .any(|entry| ObservedItem::all(entry).any(|item| item.view().file_handle().is_some()));
        let rows = self
            .baselines(paths[0].as_ref(), paths[paths.len() - 1].as_ref(), by_handle)
            .await?;
        let by_path = baseline_index(&rows);
        let mut events = Vec::new();
        let mut absent = Vec::new();
        let mut path_only = Vec::new();
        let mut has_displaced = false;
        for entry in entries {
            for item in ObservedItem::all(entry) {
                let view = item.view();
                if let Some(old) = by_path.get(&(view.sql_path().as_ref(), view.version_id())) {
                    let changed_identity = by_handle && old.identity_changed;
                    if changed_identity {
                        has_displaced |= old.previous_generation;
                        if view.file_handle().is_some() {
                            absent.push(item);
                        }
                        continue;
                    }
                    if let Some(kind) = old.change(&item) {
                        events.push(ObservedScanEvent::Changed { item, kind });
                    }
                } else if by_handle {
                    if view.file_handle().is_some() {
                        absent.push(item);
                    } else {
                        path_only.push(item);
                    }
                } else {
                    path_only.push(item);
                }
            }
        }
        if !path_only.is_empty() {
            let checked = self
                .confirm_paths(&path_only, &paths[0], &paths[paths.len() - 1], false)
                .await?;
            let checked = baseline_index(&checked);
            let mut recovered = 0;
            for item in path_only {
                let view = item.view();
                if let Some(old) = checked.get(&(view.sql_path().as_ref(), view.version_id())) {
                    recovered += 1;
                    if let Some(kind) = old.change(&item) {
                        events.push(ObservedScanEvent::Changed { item, kind });
                    }
                } else {
                    events.push(ObservedScanEvent::New(item));
                }
            }
            if recovered > 0 {
                warn!(recovered, "Recovered baseline paths without stable identity handles");
            }
        }
        if !absent.is_empty() {
            // 身份归属需要整轮命名空间；批内只复核路径，避免每批扫描整张基线。
            let rechecked = self
                .confirm_paths(&absent, &paths[0], &paths[paths.len() - 1], true)
                .await?;
            let rechecked_by_path = baseline_index(&rechecked);
            let mut recovered = 0;
            for item in absent {
                let view = item.view();
                if let Some(old) = rechecked_by_path.get(&(view.sql_path().as_ref(), view.version_id())) {
                    if !old.identity_changed {
                        recovered += 1;
                        if let Some(kind) = old.change(&item) {
                            events.push(ObservedScanEvent::Changed { item, kind });
                        }
                        continue;
                    }
                    has_displaced |= old.previous_generation;
                }
                events.push(ObservedScanEvent::Candidate(item));
            }
            if recovered > 0 {
                warn!(recovered, "Recovered baseline paths during identity confirmation");
            }
        }
        let displaced = if has_displaced {
            let columns = FILE_SCAN_COLUMNS_LIST
                .split(',')
                .map(|name| format!("b.{}", name.trim()))
                .collect::<Vec<_>>()
                .join(",");
            self.read_query(&format!("SELECT {columns} FROM (SELECT {} FROM {} {} WHERE {} AND {} AND NOT ({}) AND relative_path BETWEEN ? AND ? AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {} WHERE {})) AS b INNER JOIN (SELECT relative_path,version_id,file_handle FROM {} WHERE {}) AS s USING(relative_path,version_id) WHERE b.file_handle IS NOT NULL AND startsWith(b.file_handle,'54534934') AND s.file_handle IS NOT NULL AND b.file_handle!=s.file_handle LIMIT 1 BY b.relative_path,b.version_id",*FILE_SCAN_COLUMNS_LIST,self.base,self.final_clause,self.scope,self.live,self.active,self.stage,self.stage_scope,self.stage,self.stage_scope)).bind(&paths[0]).bind(&paths[paths.len()-1]).fetch_quorum_all::<StorageEntryRecord>().await?
        } else {
            Vec::new()
        };
        Ok(ComparedBatch { events, displaced })
    }
    async fn confirm_paths(
        &self, missing: &[ObservedItem], first: &str, last: &str, by_handle: bool,
    ) -> Result<Vec<Baseline>> {
        // 稀疏身份变更只返回未匹配路径；大量变更复用 staging，避免大量分块查询。
        let direct_limit = if by_handle { 1024 } else { 256 };
        if missing.len() > direct_limit {
            return self.baselines(first, last, by_handle).await;
        }
        let identity_changed = self.identity_changed(by_handle);
        let keys: Vec<_> = missing
            .iter()
            .map(|item| {
                let view = item.view();
                (view.sql_path(), view.version_id())
            })
            .collect();
        let mut rows = Vec::new();
        let mut start = 0;
        while start < keys.len() {
            // 用 client 同一个 SQL 编码器计量实际转义字节，保持原上限并减少无效分块。
            let end = sparse_key_group_end(&keys, start)?;
            if end == start {
                return self.baselines(first, last, by_handle).await;
            }
            rows.extend(self.read_query(&format!("SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir,marker_state,({identity_changed}) AS identity_changed,NOT ({}) AS previous_generation FROM {} {} PREWHERE {} AND (relative_path,version_id) IN ? WHERE {} ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.active,self.base,self.final_clause,self.scope,self.live))
                .bind(&keys[start..end]).fetch_quorum_all::<Baseline>().await?);
            start = end;
        }
        Ok(rows)
    }
    fn identity_changed(&self, by_handle: bool) -> String {
        if by_handle {
            format!(
                "startsWith(ifNull(file_handle,''),'54534934') AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {} WHERE {} AND file_handle IS NOT NULL) AND (relative_path,version_id,file_handle) NOT IN (SELECT relative_path,version_id,file_handle FROM {} WHERE {} AND file_handle IS NOT NULL)",
                self.stage, self.stage_scope, self.stage, self.stage_scope
            )
        } else {
            "false".into()
        }
    }
    async fn baselines(&self, first: &str, last: &str, by_handle: bool) -> Result<Vec<Baseline>> {
        let key = "(relative_path,version_id)";
        let identity_changed = self.identity_changed(by_handle);
        // 动态 staging 集合保持 WHERE，保留索引剪枝；稀疏字面量集合可使用 PREWHERE。
        let sql = format!(
            "SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir,marker_state,({identity_changed}) AS identity_changed,NOT ({}) AS previous_generation FROM {} {} WHERE {} AND {} AND relative_path BETWEEN ? AND ? AND {key} IN (SELECT {key} FROM {} WHERE {}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",
            self.active, self.base, self.final_clause, self.scope, self.live, self.stage, self.stage_scope
        );
        Ok(self
            .read_query(&sql)
            .bind(first)
            .bind(last)
            .fetch_quorum_all::<Baseline>()
            .await?)
    }
    pub async fn final_events(&self, time: i64) -> Result<Vec<ScanFinalEvent>> {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        // 无删除的常见轮次只读去重所需列，避免解压整轮旧条目的全部元数据。
        let has_old = self
            .read_query(&format!(
                "SELECT count() FROM (SELECT 1 FROM {} {} WHERE {} AND {} AND NOT ({}) LIMIT 1)",
                self.base, self.final_clause, self.scope, self.live, self.active
            ))
            .fetch_quorum_one::<u64>()
            .await?
            != 0;
        let mut old = if has_old {
            // FINAL 后选出旧主键，再用 WHERE 键集合剪枝宽行；代次与墓碑仍在 FINAL 后复核。
            let keys = format!(
                "SELECT relative_path,version_id FROM {} {} WHERE {} AND {} AND NOT ({})",
                self.base, self.final_clause, self.scope, self.live, self.active
            );
            self.read_query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND NOT ({}) AND (relative_path,version_id) IN ({keys}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active))
                .fetch_quorum_all::<StorageEntryRecord>().await?
        } else {
            Vec::new()
        };
        let event_scope = format!("{} AND toUnixTimestamp64Nano(create_at)>={time}", self.scope);
        let displaced = self
            .read_query(&format!(
                "SELECT {columns} FROM {} FINAL WHERE {event_scope} AND operation_type='displaced_old'",
                self.events
            ))
            .fetch_quorum_all::<StorageEntryRecord>()
            .await?;
        let mut known: FxHashSet<_> = old
            .iter()
            .map(|row| {
                (
                    SmolStr::new(&row.relative_path),
                    SmolStr::new(&row.version_id),
                    row.file_handle.as_ref().map(SmolStr::new),
                )
            })
            .collect();
        old.extend(displaced.into_iter().filter(|row| {
            known.insert((
                SmolStr::new(&row.relative_path),
                SmolStr::new(&row.version_id),
                row.file_handle.as_ref().map(SmolStr::new),
            ))
        }));
        let candidates = self.candidate_rows(&event_scope).await?;
        let membership = format!(
            "SELECT file_handle FROM {} FINAL WHERE {event_scope} AND operation_type='candidate_new' AND file_handle IS NOT NULL",
            self.events
        );
        let typed_candidates = candidates.iter().any(|row| row.file_handle.is_some());
        let history_filter = match (typed_candidates, old.is_empty()) {
            (true, true) => Some(format!(
                "operation_type IN ('new','deleted','rename') AND file_handle IN ({membership})"
            )),
            (true, false) => Some(format!(
                "(operation_type IN ('new','deleted','rename') AND file_handle IN ({membership})) OR operation_type='rename'"
            )),
            (false, false) => Some("operation_type='rename'".to_owned()),
            (false, true) => None,
        };
        // 清理可能只移除了部分候选；已发布的 rename 必须独立于候选日志保留其来源。
        let history = if let Some(filter) = history_filter {
            self.read_query(&format!("SELECT operation_type,relative_path,version_id,file_handle,comment FROM {} FINAL WHERE {event_scope} AND ({filter})",self.events))
                .fetch_quorum_all::<FinalHistory>().await?
        } else {
            Vec::new()
        };
        let mut counts = FxHashMap::default();
        if typed_candidates {
            let originals: FxHashSet<_> = old
                .iter()
                .filter_map(|row| row.file_handle.as_deref())
                .chain(
                    history
                        .iter()
                        .filter(|row| matches!(row.operation_type.as_str(), "deleted" | "rename"))
                        .filter_map(|row| row.file_handle.as_deref()),
                )
                .collect();
            // 只有可能匹配旧身份的候选才需统计存活别名；历史决定补回部分删除的来源。
            if candidates.iter().any(|row| {
                row.file_handle
                    .as_deref()
                    .is_some_and(|handle| originals.contains(handle))
            }) {
                let rows = self.read_query(&format!("SELECT assumeNotNull(file_handle),count() FROM {} {} WHERE {} AND {} AND {} AND file_handle IN ({membership}) GROUP BY file_handle",self.base,self.final_clause,self.scope,self.live,self.active)).fetch_quorum_all::<(String,u64)>().await?;
                counts.extend(rows.into_iter().map(|(handle, count)| (SmolStr::new(handle), count)));
            }
        }
        resolve_final_events(old, candidates, &history, &counts)
    }

    async fn candidate_rows(&self, event_scope: &str) -> Result<Vec<StorageEntryRecord>> {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        let scope = format!("{event_scope} AND operation_type='candidate_new'");
        let keys = format!(
            "SELECT relative_path,version_id FROM {} FINAL WHERE {scope}",
            self.events
        );
        let cached = self
            .read_query(&format!(
                "SELECT {columns} FROM {} FINAL WHERE {scope} ORDER BY relative_path,version_id",
                self.events
            ))
            .fetch_quorum_all::<StorageEntryRecord>()
            .await?;
        if cached.is_empty() {
            return Ok(Vec::new());
        }
        // 只在 FINAL 前过滤不可变主键；NULL 摘要也须保留到去重之后。
        let current = format!(
            "SELECT relative_path,version_id,current_state,metadata_hash FROM {} {} PREWHERE {} AND (relative_path,version_id) IN ({keys}) WHERE {} AND {}",
            self.base, self.final_clause, self.scope, self.live, self.active
        );
        let references = self
            .read_query(&current)
            .fetch_quorum_all::<CandidateReference>()
            .await?;
        if let Some(rows) = verified_candidates(cached, references).await? {
            return Ok(rows);
        }
        // 缓存缺失、重复或内容已变化时，整批读取当前宽行，不能吞掉查询错误。
        self.read_query(&format!("SELECT {columns} FROM {} {} PREWHERE {} AND (relative_path,version_id) IN ({keys}) WHERE {} AND {} ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active))
            .fetch_quorum_all().await
    }
}

#[derive(Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(serde::Serialize))]
struct FinalHistory {
    operation_type: String,
    relative_path: String,
    version_id: String,
    file_handle: Option<String>,
    comment: Option<String>,
}
#[derive(Clone)]
enum RecordedDecision {
    Added,
    Renamed { from_path: String, from_version: String },
}
type PathKey = (SmolStr, SmolStr);
fn path_key(path: &str, version: &str) -> PathKey {
    (SmolStr::new(path), SmolStr::new(version))
}

/// 判定依赖整轮命名空间；删除日志和 rename 来源补回已移除的旧路径，使中断后判定不漂移。
fn resolve_final_events(
    old: Vec<StorageEntryRecord>, candidates: Vec<StorageEntryRecord>, history: &[FinalHistory],
    active_counts: &FxHashMap<SmolStr, u64>,
) -> Result<Vec<ScanFinalEvent>> {
    let mut originals: FxHashMap<SmolStr, FxHashSet<PathKey>> = FxHashMap::default();
    for row in &old {
        if let Some(handle) = &row.file_handle {
            originals
                .entry(SmolStr::new(handle))
                .or_default()
                .insert(path_key(&row.relative_path, &row.version_id));
        }
    }
    let mut decisions = FxHashMap::default();
    let mut renamed_sources = FxHashSet::default();
    for row in history {
        let key = path_key(&row.relative_path, &row.version_id);
        match row.operation_type.as_str() {
            "new" => {
                decisions.insert(key, RecordedDecision::Added);
            }
            "deleted" => {
                if let Some(handle) = &row.file_handle {
                    originals.entry(SmolStr::new(handle)).or_default().insert(key);
                }
            }
            "rename" => {
                let from_path = row
                    .comment
                    .as_ref()
                    .ok_or_else(|| DatabaseError::ConversionError("Recorded rename lacks source path".into()))?;
                let from_key = path_key(from_path, &row.version_id);
                if let Some(handle) = &row.file_handle {
                    originals
                        .entry(SmolStr::new(handle))
                        .or_default()
                        .insert(from_key.clone());
                }
                renamed_sources.insert(from_key);
                decisions.insert(
                    key,
                    RecordedDecision::Renamed {
                        from_path: from_path.clone(),
                        from_version: row.version_id.clone(),
                    },
                );
            }
            _ => {}
        }
    }
    let mut result = Vec::with_capacity(old.len() + candidates.len());
    for row in candidates {
        let key = path_key(&row.relative_path, &row.version_id);
        let inferred = row.file_handle.as_deref().and_then(|handle| {
            let originals = originals.get(handle)?;
            if originals.len() != 1 || active_counts.get(handle).copied() != Some(1) {
                return None;
            }
            originals
                .iter()
                .next()
                .map(|(path, version)| RecordedDecision::Renamed {
                    from_path: path.to_string(),
                    from_version: version.to_string(),
                })
        });
        // 已确认的决定优先；候选清理或部分删除不能把先前的 Added 重新判成 Rename。
        match decisions
            .get(&key)
            .cloned()
            .or(inferred)
            .unwrap_or(RecordedDecision::Added)
        {
            RecordedDecision::Added => result.push(ScanFinalEvent::Added(row)),
            RecordedDecision::Renamed {
                from_path,
                from_version,
            } => {
                renamed_sources.insert(path_key(&from_path, &from_version));
                result.push(ScanFinalEvent::Renamed {
                    from_path,
                    from_version,
                    to: row,
                });
            }
        }
    }
    result.extend(
        old.into_iter()
            .filter(|row| !renamed_sources.contains(&path_key(&row.relative_path, &row.version_id)))
            .map(ScanFinalEvent::Deleted),
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::read::tests::server;
    use crate::scan::rows::NativeScanRecord;
    use crate::scan::with_scan_cancellation;
    use clickhouse::Compression;
    use clickhouse::test::{Mock, handlers};
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, PermissionFacts, PrincipalIdentity,
        SourceIdentity, StoragePath,
    };
    use storage_adapter::observed_scan::ObservedView;
    use tokio_util::sync::CancellationToken;
    fn observation(path: &str, strength: IdentityStrength) -> Result<ObservedEntry> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "hardlink-fixture")?;
        let identity = SourceIdentity::new(backend, strength, b"same-object")?;
        Ok(ObservedEntry::new(
            StoragePath::new(path)?,
            EntryKind::File,
            Some(17),
            None,
            identity,
        )?)
    }
    fn baseline(entry: &ObservedEntry) -> Result<Baseline> {
        let row = NativeScanRecord::capture(
            ObservedView {
                entry,
                version_index: None,
            },
            0,
        )?
        .legacy();
        Ok(Baseline {
            marker_state: row.marker_state,
            relative_path: row.relative_path,
            version_id: row.version_id,
            size: row.size,
            mtime: row.mtime,
            mode: row.mode,
            uid: row.uid,
            gid: row.gid,
            is_dir: row.is_dir,
            identity_changed: false,
            previous_generation: true,
        })
    }
    #[test]
    fn unchanged_scan_compares_independent_permissions_as_persisted() -> Result<()> {
        for permissions in [
            PermissionFacts {
                owner: Some(PrincipalIdentity::Named("hdfs".into())),
                group: Some(PrincipalIdentity::Named("supergroup".into())),
                mode: Some(0o640),
            },
            PermissionFacts {
                owner: Some(PrincipalIdentity::Numeric(0)),
                group: None,
                mode: Some(0),
            },
            PermissionFacts {
                owner: None,
                group: Some(PrincipalIdentity::Numeric(0)),
                mode: None,
            },
        ] {
            let entry = Arc::new(
                observation("unchanged", IdentityStrength::StableWithinBackend)?
                    .with_permissions(Some(Box::new(permissions)))?,
            );
            let previous = baseline(&entry)?;
            assert_eq!(previous.change(&Arc::clone(&entry).into()), None);
        }
        Ok(())
    }

    #[test]
    fn changed_independent_mode_is_a_metadata_change() -> Result<()> {
        let original = observation("chmod", IdentityStrength::StableWithinBackend)?.with_permissions(Some(
            Box::new(PermissionFacts {
                mode: Some(0o640),
                ..Default::default()
            }),
        ))?;
        let previous = baseline(&original)?;
        let changed = Arc::new(original.with_permissions(Some(Box::new(PermissionFacts {
            mode: Some(0o600),
            ..Default::default()
        })))?);
        assert_eq!(previous.change(&changed.into()), Some(ChangeKind::MetadataOnly));
        Ok(())
    }

    fn access(client: &Client) -> NativeScanAccess<'_> {
        NativeScanAccess {
            client,
            base: "base".into(),
            stage: "stage".into(),
            events: "events".into(),
            scope: "1".into(),
            stage_scope: "1".into(),
            active: "current_state=0".into(),
            live: "1",
            final_clause: "",
            typed_handles: true,
        }
    }
    #[test]
    fn sparse_keys_use_actual_client_encoding_with_the_original_byte_limit() -> Result<()> {
        let ordinary: Vec<_> = (0..240)
            .map(|index| (format!("d000/d000/d000/d000/d000/file{index:06}.dat"), String::new()))
            .collect();
        assert_eq!(sparse_key_group_end(&ordinary, 0)?, ordinary.len());
        let escaped: Vec<_> = (0..400)
            .map(|index| {
                (
                    format!("目录/{}'\\\n{index}", "wide".repeat(20)),
                    "version'\\".to_owned(),
                )
            })
            .collect();
        let mut start = 0;
        while start < escaped.len() {
            let end = sparse_key_group_end(&escaped, start)?;
            assert!(end > start);
            let mut encoded = String::new();
            (&escaped[start..end])
                .write(&mut encoded)
                .map_err(DatabaseError::ConversionError)?;
            assert!(encoded.len() <= SPARSE_KEY_BYTES);
            if end < escaped.len() {
                encoded.clear();
                (&escaped[start..end + 1])
                    .write(&mut encoded)
                    .map_err(DatabaseError::ConversionError)?;
                assert!(encoded.len() > SPARSE_KEY_BYTES);
            }
            start = end;
        }
        let oversized = [("x".repeat(SPARSE_KEY_BYTES), String::new())];
        assert_eq!(sparse_key_group_end(&oversized, 0)?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn unchanged_round_does_not_fetch_unused_deletion_metadata() -> Result<()> {
        let mock = Mock::new();
        mock.add(handlers::provide([0_u64]));
        mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
        mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        assert!(access(&client).final_events(1).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn deletion_probe_keeps_missing_paths_and_displaced_history() -> Result<()> {
        for old_in_base in [true, false] {
            let mock = Mock::new();
            mock.add(handlers::provide([u64::from(old_in_base)]));
            if old_in_base {
                mock.add(handlers::provide([row("old")?]));
                mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
            } else {
                mock.add(handlers::provide([row("old")?]));
            }
            mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
            mock.add(handlers::provide(Vec::<FinalHistory>::new()));
            let client = Client::default().with_url(mock.url()).with_validation(false);
            let events = access(&client).final_events(1).await?;
            assert!(matches!(events.as_slice(), [ScanFinalEvent::Deleted(entry)] if entry.relative_path == "old"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn partially_cleaned_candidates_keep_recorded_rename_sources() -> Result<()> {
        for with_remaining_candidate in [false, true] {
            let old = row("old")?;
            let mut remaining = row("other")?;
            remaining.file_handle = Some("545349340000000000000001".into());
            let mock = Mock::new();
            mock.add(handlers::provide([0_u64]));
            mock.add(handlers::provide([old.clone()]));
            if with_remaining_candidate {
                mock.add(handlers::provide([remaining.clone()]));
                mock.add(handlers::provide([reference(&remaining)]));
            } else {
                mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
            }
            mock.add(handlers::provide([history(&row("renamed")?, "rename", Some("old"))]));
            let client = Client::default().with_url(mock.url()).with_validation(false);
            let events = access(&client).final_events(1).await?;
            if with_remaining_candidate {
                assert!(matches!(events.as_slice(), [ScanFinalEvent::Added(row)] if row.relative_path == "other"));
            } else {
                assert!(events.is_empty());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn deletion_probe_failure_is_not_an_empty_round() {
        let mock = Mock::new();
        mock.add(handlers::exception(7));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        assert!(access(&client).final_events(1).await.is_err());
    }

    #[tokio::test]
    async fn unordered_baseline_rows_do_not_create_false_new_entries() -> Result<()> {
        let entries = ["a", "b", "c"]
            .into_iter()
            .map(|path| observation(path, IdentityStrength::PathScoped).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let mock = Mock::new();
        mock.add(handlers::provide([
            baseline(&entries[1])?,
            baseline(&entries[0])?,
            baseline(&entries[2])?,
        ]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&entries).await?;
        assert!(
            result.events.is_empty(),
            "unchanged paths must match their baseline regardless of row delivery order"
        );
        Ok(())
    }
    #[tokio::test]
    async fn path_scoped_misses_are_confirmed_without_the_staging_set() -> Result<()> {
        let entry = Arc::new(observation("unchanged", IdentityStrength::PathScoped)?);
        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide([baseline(&entry)?]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&[entry]).await?;
        assert!(
            result.events.is_empty(),
            "a briefly missing path cannot become a durable false New event"
        );
        Ok(())
    }

    #[tokio::test]
    async fn path_confirmation_preserves_real_new_and_changed_entries() -> Result<()> {
        let entry = Arc::new(observation("changed", IdentityStrength::PathScoped)?);
        let mut old = baseline(&entry)?;
        old.size += 1;
        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide([old]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(std::slice::from_ref(&entry)).await?;
        assert!(matches!(result.events.as_slice(), [ObservedScanEvent::Changed { .. }]));

        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&[entry]).await?;
        assert!(matches!(result.events.as_slice(), [ObservedScanEvent::New(_)]));
        Ok(())
    }

    #[tokio::test]
    async fn matching_identity_rechecks_a_missing_path_before_creating_a_candidate() -> Result<()> {
        let entry = Arc::new(observation("unchanged", IdentityStrength::StableWithinBackend)?);
        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide([baseline(&entry)?]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&[entry]).await?;
        assert!(
            result.events.is_empty(),
            "a confirmed unchanged path must not become a new candidate"
        );
        Ok(())
    }
    #[tokio::test]
    async fn typed_misses_wait_for_round_wide_identity_resolution() -> Result<()> {
        let entry = Arc::new(observation("new", IdentityStrength::StableWithinBackend)?);
        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&[entry]).await?;
        assert!(matches!(result.events.as_slice(), [ObservedScanEvent::Candidate(_)]));
        Ok(())
    }

    #[tokio::test]
    async fn sparse_typed_confirmation_does_not_reload_unchanged_batch_paths() -> Result<()> {
        let unchanged = (0..1_000)
            .map(|index| observation(&format!("keep/{index:04}"), IdentityStrength::StableWithinBackend).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let baseline_rows = unchanged
            .iter()
            .map(|entry| baseline(entry))
            .collect::<Result<Vec<_>>>()?;
        let mut entries = unchanged;
        entries.extend(
            (0..300)
                .map(|index| {
                    observation(&format!("new/{index:04}"), IdentityStrength::StableWithinBackend).map(Arc::new)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let mock = Mock::new();
        mock.add(handlers::provide(baseline_rows));
        let confirmation = mock.add(handlers::record_ddl());
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&entries).await?;
        assert_eq!(result.events.len(), 300);
        assert!(
            result
                .events
                .iter()
                .all(|event| matches!(event, ObservedScanEvent::Candidate(_)))
        );
        let sql = confirmation.query().await;
        assert!(
            sql.contains(" PREWHERE "),
            "primary keys must filter before wide metadata reads"
        );
        assert!(sql.contains("new/0000"));
        assert!(
            !sql.contains("keep/0000"),
            "confirmation must not return the unchanged batch again"
        );
        assert!(
            sql.contains("startsWith(ifNull(file_handle"),
            "identity replacement must remain distinct from metadata changes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn large_new_candidate_sets_skip_unneeded_identity_counts() -> Result<()> {
        let candidates: Vec<_> = (0..513)
            .map(|index| {
                let mut entry = row(&format!("new/{index}"))?;
                entry.file_handle = Some(format!("54534934{index:064x}"));
                Ok(entry)
            })
            .collect::<Result<_>>()?;
        let mock = Mock::new();
        mock.add(handlers::provide([0_u64]));
        mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
        let references: Vec<_> = candidates.iter().map(reference).collect();
        mock.add(handlers::provide(candidates));
        mock.add(handlers::provide(references));
        mock.add(handlers::provide(Vec::<FinalHistory>::new()));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let events = access(&client).final_events(1).await?;
        assert_eq!(events.len(), 513);
        assert!(events.iter().all(|event| matches!(event, ScanFinalEvent::Added(_))));
        Ok(())
    }
    #[tokio::test]
    async fn large_rename_sets_still_count_all_live_aliases_once() -> Result<()> {
        let mut old = Vec::new();
        let mut candidates = Vec::new();
        let mut counts = Vec::new();
        for index in 0..513 {
            let handle = format!("54534934{index:064x}");
            let mut before = row(&format!("old/{index}"))?;
            before.file_handle = Some(handle.clone());
            let mut after = row(&format!("new/{index}"))?;
            after.file_handle = Some(handle.clone());
            old.push(before);
            candidates.push(after);
            counts.push((handle, 1_u64));
        }
        let mock = Mock::new();
        mock.add(handlers::provide([1_u64]));
        mock.add(handlers::provide(old));
        mock.add(handlers::provide(Vec::<StorageEntryRecord>::new()));
        let references: Vec<_> = candidates.iter().map(reference).collect();
        mock.add(handlers::provide(candidates));
        mock.add(handlers::provide(references));
        mock.add(handlers::provide(Vec::<FinalHistory>::new()));
        mock.add(handlers::provide(counts));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let events = access(&client).final_events(1).await?;
        assert_eq!(events.len(), 513);
        assert!(
            events
                .iter()
                .all(|event| matches!(event, ScanFinalEvent::Renamed { .. }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cache_hit_does_not_reread_baseline_metadata() -> Result<()> {
        let mock = Mock::new();
        let row = row("new")?;
        let mut current = reference(&row);
        current.current_state = 7;
        mock.add(handlers::provide([row]));
        mock.add(handlers::provide([current]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let rows = access(&client).candidate_rows("1").await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].relative_path, "new");
        assert_eq!(rows[0].current_state, 7);
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_or_duplicate_references_are_not_a_valid_cache() -> Result<()> {
        for duplicate in [false, true] {
            let first = row("new-a")?;
            let last = row("new-b")?;
            let references = if duplicate {
                vec![reference(&first), reference(&first)]
            } else {
                vec![reference(&first)]
            };
            assert!(verified_candidates(vec![first, last], references).await?.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_cached_keys_and_missing_or_changed_hashes_require_a_reread() -> Result<()> {
        let first = row("new")?;
        assert!(
            verified_candidates(vec![first.clone(), first.clone()], vec![reference(&first)])
                .await?
                .is_none()
        );
        for hash in [None, Some("different".into())] {
            let mut current = reference(&first);
            current.metadata_hash = hash;
            assert!(verified_candidates(vec![first.clone()], vec![current]).await?.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cache_keeps_latest_state_and_final_filters() -> Result<()> {
        let mock = Mock::new();
        mock.add(handlers::provide([row("new")?]));
        let query = mock.add(handlers::record_ddl());
        let mut latest = row("new")?;
        latest.size = 999;
        mock.add(handlers::provide([latest]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let mut access = access(&client);
        access.final_clause = "FINAL";
        assert_eq!(access.candidate_rows("1").await?[0].size, 999);
        let sql = query.query().await;
        assert!(sql.contains("SELECT relative_path,version_id,current_state,metadata_hash"));
        assert!(sql.contains("base FINAL PREWHERE"));
        assert!(sql.contains("WHERE 1 AND current_state=0"));
        assert!(!sql.contains("metadata_hash IS NOT NULL"));
        assert!(!sql.contains("LIMIT 1 BY"));
        Ok(())
    }

    #[tokio::test]
    async fn candidate_cache_query_failure_does_not_fall_back() -> Result<()> {
        let mock = Mock::new();
        mock.add(handlers::provide([row("new")?]));
        mock.add(handlers::exception(7));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        assert!(access(&client).candidate_rows("1").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_during_metadata_verification_is_not_a_cache_miss() -> Result<()> {
        let cached = (0..4096)
            .map(|index| row(&format!("new/{index}")))
            .collect::<Result<Vec<_>>>()?;
        let references = cached.iter().map(reference).collect();
        let cancel = CancellationToken::new();
        let validating = with_scan_cancellation(cancel.clone(), None, verified_candidates(cached, references));
        let cancelling = async {
            yield_now().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(validating, cancelling);
        assert!(matches!(result, Err(DatabaseError::ScanCancelled)));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn native_balanced_reads_preserve_quorum_and_leave_the_client_policy_unchanged() -> Result<()> {
        let (url, captured) = server(vec![
            (500, b"Code: 289. REPLICA_IS_NOT_IN_QUORUM".to_vec()),
            (200, Vec::new()),
            (200, 0_u64.to_le_bytes().to_vec()),
        ])
        .await;
        let client = Client::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(Compression::None)
            .with_option("select_sequential_consistency", "1")
            .with_option("load_balancing", "in_order");
        assert!(access(&client).baselines("a", "z", false).await?.is_empty());
        assert_eq!(client.query("SELECT 0").fetch_one::<u64>().await?, 0);
        let requests = captured.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| request.contains("select_sequential_consistency=1"))
        );
        assert!(
            requests[..2]
                .iter()
                .all(|request| request.contains("load_balancing=random"))
        );
        assert!(requests[2].contains("load_balancing=in_order"));
        Ok(())
    }

    fn row(path: &str) -> Result<StorageEntryRecord> {
        let entry = observation(path, IdentityStrength::StableWithinBackend)?;
        Ok(NativeScanRecord::capture(
            ObservedView {
                entry: &entry,
                version_index: None,
            },
            0,
        )?
        .legacy())
    }
    fn reference(row: &StorageEntryRecord) -> CandidateReference {
        CandidateReference {
            relative_path: row.relative_path.clone(),
            version_id: row.version_id.clone(),
            current_state: row.current_state,
            metadata_hash: Some(row.scan_metadata_hash()),
        }
    }
    fn counts(row: &StorageEntryRecord, count: u64) -> FxHashMap<SmolStr, u64> {
        row.file_handle
            .as_deref()
            .map(|key| (SmolStr::new(key), count))
            .into_iter()
            .collect()
    }
    fn history(row: &StorageEntryRecord, op: &str, from: Option<&str>) -> FinalHistory {
        FinalHistory {
            operation_type: op.into(),
            relative_path: row.relative_path.clone(),
            version_id: row.version_id.clone(),
            file_handle: row.file_handle.clone(),
            comment: from.map(str::to_owned),
        }
    }
    #[test]
    fn unambiguous_identity_renames_but_surviving_hardlink_makes_it_added_and_deleted() -> Result<()> {
        let old = row("old")?;
        let new = row("new")?;
        let renamed = resolve_final_events(vec![old.clone()], vec![new.clone()], &[], &counts(&new, 1))?;
        assert!(
            matches!(renamed.as_slice(),[ScanFinalEvent::Renamed{from_path,to,..}] if from_path=="old" && to.relative_path=="new")
        );
        let ambiguous = resolve_final_events(vec![old], vec![new.clone()], &[], &counts(&new, 2))?;
        assert_eq!(ambiguous.len(), 2);
        assert!(matches!(&ambiguous[0], ScanFinalEvent::Added(_)));
        assert!(matches!(&ambiguous[1], ScanFinalEvent::Deleted(_)));
        Ok(())
    }
    #[test]
    fn deleting_multiple_aliases_never_skips_them_or_invents_renames() -> Result<()> {
        for count in [2, 3, 4] {
            let old = (0..count)
                .map(|index| row(&format!("old{index}")))
                .collect::<Result<Vec<_>>>()?;
            let result = resolve_final_events(old, vec![], &[], &FxHashMap::default())?;
            assert_eq!(result.len(), count);
            assert!(result.iter().all(|event| matches!(event, ScanFinalEvent::Deleted(_))));
        }
        let new = row("new")?;
        let result = resolve_final_events(
            vec![row("old1")?, row("old2")?],
            vec![new.clone()],
            &[],
            &counts(&new, 1),
        )?;
        assert_eq!(result.len(), 3);
        assert!(
            result
                .iter()
                .all(|event| !matches!(event, ScanFinalEvent::Renamed { .. }))
        );
        Ok(())
    }
    #[test]
    fn finalization_replay_keeps_ambiguity_after_some_old_aliases_were_deleted() -> Result<()> {
        let first = row("old1")?;
        let second = row("old2")?;
        let new = row("new")?;
        let previous = [history(&first, "deleted", None), history(&new, "new", None)];
        let replay = resolve_final_events(vec![second], vec![new.clone()], &previous, &counts(&new, 1))?;
        assert!(matches!(
            replay.as_slice(),
            [ScanFinalEvent::Added(_), ScanFinalEvent::Deleted(_)]
        ));
        // 删除已经确认但 Added 还没写入时，也不能因旧路径只剩一个就改判 rename。
        let replay = resolve_final_events(vec![row("old2")?], vec![new.clone()], &previous[..1], &counts(&new, 1))?;
        assert!(matches!(
            replay.as_slice(),
            [ScanFinalEvent::Added(_), ScanFinalEvent::Deleted(_)]
        ));
        Ok(())
    }
    #[test]
    fn replay_after_rename_event_or_base_deletion_is_idempotent() -> Result<()> {
        let old = row("old")?;
        let new = row("new")?;
        let previous = [
            history(&new, "rename", Some("old")),
            history(&new, "rename", Some("old")),
        ];
        for remaining in [vec![old], vec![]] {
            let replay = resolve_final_events(remaining, vec![new.clone()], &previous, &counts(&new, 1))?;
            assert!(matches!(replay.as_slice(),[ScanFinalEvent::Renamed{from_path,..}] if from_path=="old"));
        }
        Ok(())
    }
    #[test]
    fn added_aliases_are_reported_without_a_disappearing_source() -> Result<()> {
        let new = row("added-link")?;
        let added = resolve_final_events(vec![], vec![new.clone()], &[], &counts(&new, 2))?;
        assert!(matches!(added.as_slice(), [ScanFinalEvent::Added(_)]));
        Ok(())
    }
}
