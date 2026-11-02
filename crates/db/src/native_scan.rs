//! 单机与集群共用的原生扫描比较：限定本批主键，保留后端身份补查和旧基线兼容。
use crate::clickhouse::replica_read::QuorumRead;
use crate::scan_store::ScanFinalEvent;
use crate::{
    common::FILE_SCAN_COLUMNS_LIST,
    error::{DatabaseError, Result},
    traits::StorageEntryRecord,
};
use clickhouse::Client;
use data_mover::model::ObservedEntry;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use smol_str::SmolStr;
use std::sync::Arc;
use storage_adapter::{
    ChangeKind,
    observed_scan::{EntryStatistics, ObservedItem, ObservedScanEvent},
};
use tracing::warn;

pub(crate) struct NativeScanAccess<'a> {
    pub client: &'a Client,
    pub base: String,
    pub stage: String,
    pub events: String,
    pub events_round: String,
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
impl Baseline {
    fn change(&self, item: &ObservedItem) -> Option<ChangeKind> {
        if self.is_dir {
            return None;
        }
        let view = item.view();
        let ownership = view.metadata().ownership_mode().value();
        let data = self.size != view.get_size() || self.mtime != view.get_mtime();
        let meta = self.mode != ownership.map(|o| o.mode)
            || self.uid != ownership.map(|o| o.uid)
            || self.gid != ownership.map(|o| o.gid);
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
    pub async fn compare(&self, entries: &[Arc<ObservedEntry>]) -> Result<ComparedBatch> {
        if entries.is_empty() {
            return Ok(ComparedBatch {
                events: Vec::new(),
                displaced: Vec::new(),
            });
        }
        let mut paths: Vec<_> = entries
            .iter()
            .flat_map(|entry| ObservedItem::all(entry).map(|item| item.view().path()))
            .collect();
        paths.sort_unstable();
        let by_handle = self.typed_handles
            && entries
                .iter()
                .any(|entry| ObservedItem::all(entry).any(|item| item.view().file_handle().is_some()));
        let rows = self
            .baselines(paths[0].as_str(), paths[paths.len() - 1].as_str(), by_handle)
            .await?;
        let by_path = baseline_index(&rows);
        let mut events = Vec::new();
        let mut absent = Vec::new();
        let mut path_only = Vec::new();
        let mut has_displaced = false;
        for entry in entries {
            for item in ObservedItem::all(entry) {
                let view = item.view();
                let path = view.path();
                if let Some(old) = by_path.get(&(path.as_str(), view.version_id())) {
                    let changed_identity = by_handle && old.identity_changed;
                    if changed_identity {
                        has_displaced |= old.previous_generation;
                        if let Some(handle) = view.file_handle() {
                            absent.push((item, handle));
                        }
                        continue;
                    }
                    if let Some(kind) = old.change(&item) {
                        events.push(ObservedScanEvent::Changed { item, kind });
                    }
                } else if by_handle {
                    if let Some(handle) = view.file_handle() {
                        absent.push((item, handle));
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
                .confirm_paths(&path_only, &paths[0], &paths[paths.len() - 1])
                .await?;
            let checked = baseline_index(&checked);
            let mut recovered = 0;
            for item in path_only {
                let view = item.view();
                let path = view.path();
                if let Some(old) = checked.get(&(path.as_str(), view.version_id())) {
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
            let client = self
                .client
                .clone()
                .with_option("use_skip_indexes", if entries.len() > 256 { "0" } else { "1" });
            // 身份集合从已持久化 staging 读取，避免百万级变更生成超长 SQL IN 字面量。
            let handles: Vec<_> = absent.iter().map(|(_, key)| key.as_str()).collect();
            let small = handles.len() <= 256;
            let membership = if small {
                "?".into()
            } else {
                format!(
                    "SELECT file_handle FROM {} WHERE {} AND file_handle IS NOT NULL",
                    self.stage, self.stage_scope
                )
            };
            // 小集合直接绑定；大集合从 staging 取，既限制返回量又避免超长 SQL。
            // 已覆盖的旧身份从本轮日志补回，判定不依赖原路径和目标落在同一批。
            let sql = format!(
                "SELECT DISTINCT handle FROM (SELECT assumeNotNull(file_handle) AS handle FROM {} {} WHERE {} AND {} AND file_handle IN ({membership}) UNION ALL SELECT assumeNotNull(file_handle) AS handle FROM {} FINAL WHERE {} AND operation_type='displaced_old' AND file_handle IN ({membership}))",
                self.base, self.final_clause, self.scope, self.live, self.events, self.events_round
            );
            let lookup = client.query(&sql);
            let lookup = if small {
                lookup.bind(&handles).bind(&handles)
            } else {
                lookup
            };
            let existing: FxHashSet<SmolStr> = lookup
                .fetch_quorum_all::<String>()
                .await?
                .into_iter()
                .map(SmolStr::new)
                .collect();
            // 路径首次未命中、身份却存在时，先复核路径，避免跨查询可见性差异制造假新增。
            let rechecked = if absent.iter().any(|(_, key)| existing.contains(key.as_str())) {
                self.baselines(paths[0].as_str(), paths[paths.len() - 1].as_str(), by_handle)
                    .await?
            } else {
                Vec::new()
            };
            let rechecked_by_path = baseline_index(&rechecked);
            let mut recovered = 0;
            for (item, key) in absent {
                let view = item.view();
                let path = view.path();
                if let Some(old) = rechecked_by_path.get(&(path.as_str(), view.version_id())) {
                    if !old.identity_changed {
                        recovered += 1;
                        if let Some(kind) = old.change(&item) {
                            events.push(ObservedScanEvent::Changed { item, kind });
                        }
                        continue;
                    }
                    has_displaced |= old.previous_generation;
                }
                events.push(if existing.contains(key.as_str()) {
                    ObservedScanEvent::Candidate(item)
                } else {
                    ObservedScanEvent::New(item)
                });
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
            self.client.query(&format!("SELECT {columns} FROM (SELECT {} FROM {} {} WHERE {} AND {} AND NOT ({}) AND relative_path BETWEEN ? AND ? AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {} WHERE {})) AS b INNER JOIN (SELECT relative_path,version_id,file_handle FROM {} WHERE {}) AS s USING(relative_path,version_id) WHERE b.file_handle IS NOT NULL AND startsWith(b.file_handle,'54534934') AND s.file_handle IS NOT NULL AND b.file_handle!=s.file_handle LIMIT 1 BY b.relative_path,b.version_id",*FILE_SCAN_COLUMNS_LIST,self.base,self.final_clause,self.scope,self.live,self.active,self.stage,self.stage_scope,self.stage,self.stage_scope)).bind(&paths[0]).bind(&paths[paths.len()-1]).fetch_quorum_all::<StorageEntryRecord>().await?
        } else {
            Vec::new()
        };
        Ok(ComparedBatch { events, displaced })
    }
    async fn confirm_paths(&self, missing: &[ObservedItem], first: &str, last: &str) -> Result<Vec<Baseline>> {
        // 大集合沿用已落库的键集合；小集合直接绑定，避免复核仍依赖暂存集合的可见性。
        if missing.len() > 256 {
            return self.baselines(first, last, false).await;
        }
        let keys: Vec<_> = missing
            .iter()
            .map(|item| {
                let view = item.view();
                (view.path(), view.version_id().to_owned())
            })
            .collect();
        let mut rows = Vec::new();
        let mut start = 0;
        while start < keys.len() {
            let mut end = start;
            let mut bytes = 0_usize;
            while end < keys.len() {
                // SQL 字符串转义最多翻倍，限制每次绑定的字节预算而非只限制条数。
                let next = keys[end]
                    .0
                    .len()
                    .saturating_add(keys[end].1.len())
                    .saturating_mul(2)
                    .saturating_add(16);
                if bytes.saturating_add(next) > 16_384 {
                    break;
                }
                bytes += next;
                end += 1;
            }
            if end == start {
                return self.baselines(first, last, false).await;
            }
            rows.extend(self.client.query(&format!("SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir,false AS identity_changed,NOT ({}) AS previous_generation FROM {} {} WHERE {} AND {} AND (relative_path,version_id) IN ? ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.active,self.base,self.final_clause,self.scope,self.live))
                .bind(&keys[start..end]).fetch_quorum_all::<Baseline>().await?);
            start = end;
        }
        Ok(rows)
    }
    async fn baselines(&self, first: &str, last: &str, by_handle: bool) -> Result<Vec<Baseline>> {
        let key = "(relative_path,version_id)";
        let identity_changed = if by_handle {
            format!(
                "startsWith(ifNull(file_handle,''),'54534934') AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {} WHERE {} AND file_handle IS NOT NULL) AND (relative_path,version_id,file_handle) NOT IN (SELECT relative_path,version_id,file_handle FROM {} WHERE {} AND file_handle IS NOT NULL)",
                self.stage, self.stage_scope, self.stage, self.stage_scope
            )
        } else {
            "false".into()
        };
        let sql = format!(
            "SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir,({identity_changed}) AS identity_changed,NOT ({}) AS previous_generation FROM {} {} WHERE {} AND {} AND relative_path BETWEEN ? AND ? AND {key} IN (SELECT {key} FROM {} WHERE {}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",
            self.active, self.base, self.final_clause, self.scope, self.live, self.stage, self.stage_scope
        );
        Ok(self
            .client
            .query(&sql)
            .bind(first)
            .bind(last)
            .fetch_quorum_all::<Baseline>()
            .await?)
    }
    pub async fn final_events(&self, time: i64) -> Result<Vec<ScanFinalEvent>> {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        // 无删除的常见轮次只读去重所需列，避免解压整轮旧条目的全部元数据。
        let has_old = self
            .client
            .query(&format!(
                "SELECT count() FROM (SELECT 1 FROM {} {} WHERE {} AND {} AND NOT ({}) LIMIT 1)",
                self.base, self.final_clause, self.scope, self.live, self.active
            ))
            .fetch_quorum_one::<u64>()
            .await?
            != 0;
        let mut old = if has_old {
            self.client.query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND NOT ({}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active))
                .fetch_quorum_all::<StorageEntryRecord>().await?
        } else {
            Vec::new()
        };
        let event_scope = format!("{} AND toUnixTimestamp64Nano(create_at)>={time}", self.scope);
        let displaced = self
            .client
            .query(&format!(
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
        let candidates=self.client.query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND {} AND (relative_path,version_id) IN (SELECT relative_path,version_id FROM {} FINAL WHERE {event_scope} AND operation_type='candidate_new') ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active,self.events)).fetch_quorum_all::<StorageEntryRecord>().await?;
        if candidates.is_empty() {
            return Ok(old.into_iter().map(ScanFinalEvent::Deleted).collect());
        }
        let handles: FxHashSet<SmolStr> = candidates
            .iter()
            .filter_map(|row| row.file_handle.as_deref())
            .map(SmolStr::new)
            .collect();
        let mut counts = FxHashMap::default();
        let mut history = Vec::new();
        let handles: Vec<_> = handles.iter().map(SmolStr::as_str).collect();
        // TSI4 摘要固定 72 字符；分块把绑定后的 SQL 限在默认查询长度预算内。
        for keys in handles.chunks(512) {
            let rows=self.client.query(&format!("SELECT assumeNotNull(file_handle),count() FROM {} {} WHERE {} AND {} AND {} AND file_handle IN ? GROUP BY file_handle",self.base,self.final_clause,self.scope,self.live,self.active)).bind(keys).fetch_quorum_all::<(String,u64)>().await?;
            counts.extend(rows.into_iter().map(|(handle, count)| (SmolStr::new(handle), count)));
            history.extend(self.client.query(&format!("SELECT operation_type,relative_path,version_id,file_handle,comment FROM {} FINAL WHERE {event_scope} AND operation_type IN ('new','deleted','rename') AND file_handle IN ?",self.events)).bind(keys).fetch_quorum_all::<FinalHistory>().await?);
        }
        resolve_final_events(old, candidates, &history, &counts)
    }
}

#[derive(Deserialize, clickhouse::Row)]
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
    use crate::native_scan_record::NativeScanRecord;
    use clickhouse::test::{Mock, handlers};
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
    };
    use storage_adapter::observed_scan::ObservedView;
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
    fn access(client: &Client) -> NativeScanAccess<'_> {
        NativeScanAccess {
            client,
            base: "base".into(),
            stage: "stage".into(),
            events: "events".into(),
            events_round: "1".into(),
            scope: "1".into(),
            stage_scope: "1".into(),
            active: "current_state=0".into(),
            live: "1",
            final_clause: "",
            typed_handles: true,
        }
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
            let client = Client::default().with_url(mock.url()).with_validation(false);
            let events = access(&client).final_events(1).await?;
            assert!(matches!(events.as_slice(), [ScanFinalEvent::Deleted(entry)] if entry.relative_path == "old"));
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
        let result = access(&client).compare(&[entry.clone()]).await?;
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
        let handle = ObservedView {
            entry: &entry,
            version_index: None,
        }
        .file_handle()
        .ok_or_else(|| DatabaseError::ConversionError("fixture lacks stable identity".into()))?;
        let mock = Mock::new();
        mock.add(handlers::provide(Vec::<Baseline>::new()));
        mock.add(handlers::provide([handle]));
        mock.add(handlers::provide([baseline(&entry)?]));
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let result = access(&client).compare(&[entry]).await?;
        assert!(
            result.events.is_empty(),
            "a confirmed unchanged path must not become a new candidate"
        );
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
