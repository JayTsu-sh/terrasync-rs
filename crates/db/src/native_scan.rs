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
        let rows = self
            .client
            .query(&sql)
            .bind(&paths[0])
            .bind(&paths[paths.len() - 1])
            .fetch_quorum_all::<Baseline>()
            .await?;
        let mut events = Vec::new();
        let mut absent = Vec::new();
        let mut has_displaced = false;
        for entry in entries {
            for item in ObservedItem::all(entry) {
                let view = item.view();
                let path = view.path();
                if let Ok(index) = rows.binary_search_by(|old| {
                    old.relative_path
                        .cmp(&path)
                        .then_with(|| old.version_id.as_str().cmp(view.version_id()))
                }) {
                    let changed_identity = by_handle && rows[index].identity_changed;
                    if changed_identity {
                        has_displaced |= rows[index].previous_generation;
                        if let Some(handle) = view.file_handle() {
                            absent.push((item, handle));
                        }
                        continue;
                    }
                    if let Some(kind) = rows[index].change(&item) {
                        events.push(ObservedScanEvent::Changed { item, kind });
                    }
                } else if by_handle {
                    if let Some(handle) = view.file_handle() {
                        absent.push((item, handle));
                    } else {
                        events.push(ObservedScanEvent::New(item));
                    }
                } else {
                    events.push(ObservedScanEvent::New(item));
                }
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
            events.extend(absent.into_iter().map(|(item, key)| {
                if existing.contains(key.as_str()) {
                    ObservedScanEvent::Candidate(item)
                } else {
                    ObservedScanEvent::New(item)
                }
            }));
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
    pub async fn final_events(&self, time: i64) -> Result<Vec<ScanFinalEvent>> {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        let mut old=self.client.query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND NOT ({}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active)).fetch_quorum_all::<StorageEntryRecord>().await?;
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
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
    };
    use storage_adapter::observed_scan::ObservedView;
    fn row(path: &str) -> Result<StorageEntryRecord> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "hardlink-fixture")?;
        let identity = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"same-object")?;
        let entry = ObservedEntry::new(StoragePath::new(path)?, EntryKind::File, Some(17), None, identity)?;
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
