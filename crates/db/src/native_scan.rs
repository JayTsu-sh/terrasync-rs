//! 单机与集群共用的原生扫描比较：限定本批主键，保留后端身份补查和旧基线兼容。
use crate::scan_store::ScanDeletion;
use crate::{common::FILE_SCAN_COLUMNS_LIST, error::Result, traits::StorageEntryRecord};
use clickhouse::Client;
use data_mover::model::ObservedEntry;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use storage_adapter::{
    ChangeKind,
    observed_scan::{EntryStatistics, ObservedItem, ObservedScanEvent},
};

pub(crate) struct NativeScanAccess<'a> {
    pub client: &'a Client,
    pub base: String,
    pub stage: String,
    pub scope: String,
    pub stage_scope: String,
    pub active: String,
    pub live: &'static str,
    pub final_clause: &'static str,
    pub typed_handles: bool,
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
    pub async fn compare(&self, entries: &[Arc<ObservedEntry>]) -> Result<Vec<ObservedScanEvent>> {
        if entries.is_empty() {
            return Ok(Vec::new());
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
        let key = if by_handle {
            "(relative_path,file_handle)"
        } else {
            "(relative_path,version_id)"
        };
        let sql = format!(
            "SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir FROM {} {} WHERE {} AND {} AND relative_path BETWEEN ? AND ? AND {key} IN (SELECT {key} FROM {} WHERE {}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",
            self.base, self.final_clause, self.scope, self.live, self.stage, self.stage_scope
        );
        let rows = self
            .client
            .query(&sql)
            .bind(&paths[0])
            .bind(&paths[paths.len() - 1])
            .fetch_all::<Baseline>()
            .await?;
        let mut events = Vec::new();
        let mut absent = Vec::new();
        for entry in entries {
            for item in ObservedItem::all(entry) {
                let view = item.view();
                let path = view.path();
                if let Ok(index) = rows.binary_search_by(|old| {
                    old.relative_path
                        .cmp(&path)
                        .then_with(|| old.version_id.as_str().cmp(view.version_id()))
                }) {
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
            let handles: Vec<_> = absent.iter().map(|(_, key)| key.as_str()).collect();
            let client = self
                .client
                .clone()
                .with_option("use_skip_indexes", if handles.len() > 256 { "0" } else { "1" });
            let existing: HashSet<String> = client
                .query(&format!(
                    "SELECT DISTINCT assumeNotNull(file_handle) FROM {} {} WHERE {} AND {} AND file_handle IN ?",
                    self.base, self.final_clause, self.scope, self.live
                ))
                .bind(&handles)
                .fetch_all()
                .await?
                .into_iter()
                .collect();
            events.extend(
                absent
                    .into_iter()
                    .filter(|(_, key)| !existing.contains(key))
                    .map(|(item, _)| ObservedScanEvent::New(item)),
            );
        }
        Ok(events)
    }
    pub async fn deleted(&self) -> Result<Vec<ScanDeletion>> {
        let columns = &*FILE_SCAN_COLUMNS_LIST;
        let old=self.client.query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND NOT ({}) ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live,self.active)).fetch_all::<StorageEntryRecord>().await?;
        let mut groups: HashMap<String, Vec<StorageEntryRecord>> = HashMap::new();
        let handles: Vec<_> = old
            .iter()
            .filter_map(|row| row.file_handle.as_deref())
            .filter(|key| key.starts_with("54534934"))
            .collect();
        for keys in handles.chunks(100_000) {
            let records=self.client.query(&format!("SELECT {columns} FROM {} {} WHERE {} AND {} AND file_handle IN ? ORDER BY relative_path,version_id LIMIT 1 BY relative_path,version_id",self.base,self.final_clause,self.scope,self.live)).bind(keys).fetch_all::<StorageEntryRecord>().await?;
            for record in records {
                if let Some(key) = &record.file_handle {
                    groups.entry(key.clone()).or_default().push(record);
                }
            }
        }
        let mut events = Vec::with_capacity(old.len());
        for row in old {
            let matches = row.file_handle.as_ref().and_then(|key| groups.get(key));
            match matches.map(Vec::as_slice) {
                Some([first, second]) => {
                    let target = if first.relative_path == row.relative_path {
                        second
                    } else {
                        first
                    };
                    events.push(ScanDeletion {
                        from: row.clone(),
                        to: Some(target.clone()),
                    });
                }
                Some(rows) if rows.len() > 2 => {
                    tracing::error!(path=%row.relative_path,"Ambiguous scan identity across multiple paths");
                }
                _ => events.push(ScanDeletion { from: row, to: None }),
            }
        }
        Ok(events)
    }
}
