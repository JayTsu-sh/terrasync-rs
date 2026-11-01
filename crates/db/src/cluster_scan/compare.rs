//! 每批排序并按主键裁剪共享 base；范围外的身份仅补查一次，保留 rename 语义。
use super::*;
use std::collections::HashSet;
use storage_adapter::ChangeKind;

#[derive(Deserialize, clickhouse::Row)]
struct BaselineRow {
    relative_path: String,
    version_id: String,
    size: u64,
    mtime: i64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    is_dir: bool,
}
impl BaselineRow {
    fn change_kind(&self, entry: &EntryEnum) -> Option<ChangeKind> {
        if self.is_dir {
            return None;
        }
        let data = self.size != entry.get_size() || self.mtime != entry.get_mtime();
        let meta = self.mode != entry.get_mode() || self.uid != entry.get_uid() || self.gid != entry.get_gid();
        match (data, meta) {
            (true, true) => Some(ChangeKind::Both),
            (true, false) => Some(ChangeKind::DataOnly),
            (false, true) => Some(ChangeKind::MetadataOnly),
            (false, false) => None,
        }
    }
}
impl ClusterScanStore {
    pub(crate) async fn detect_resume_batch_events(
        &self, entries: &[Arc<EntryEnum>],
    ) -> Result<Vec<StorageEntryMessage>> {
        let database = identifier(&self.database)?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut paths: Vec<_> = entries
            .iter()
            .map(|entry| entry.get_relative_path().to_string_lossy().into_owned())
            .collect();
        paths.sort_unstable();
        let by_handle = entries
            .iter()
            .filter_map(|entry| entry.get_file_handle())
            .next()
            .is_some()
            && entries
                .iter()
                .filter_map(|entry| entry.get_file_handle())
                .all(|handle| handle.starts_with(b"TSI4"));
        let key = if by_handle {
            "(relative_path,file_handle)"
        } else {
            "(relative_path,version_id)"
        };
        let rows=self.client.query(&format!("SELECT relative_path,version_id,size,mtime,mode,uid,gid,is_dir FROM {database}.base FINAL \
            WHERE job_id=? AND relative_path BETWEEN ? AND ? AND is_deleted=0 \
            AND {key} IN (SELECT {key} FROM {database}.scan_resume_batch WHERE job_id=? AND batch_id=?) ORDER BY relative_path,version_id"))
            .bind(&self.job_id).bind(&paths[0]).bind(&paths[paths.len()-1]).bind(&self.job_id).bind(self.batch_id)
            .fetch_all::<BaselineRow>().await?;
        let mut events = Vec::new();
        let mut absent = Vec::new();
        for entry in entries {
            let path = entry.get_relative_path().to_string_lossy();
            let version = entry.get_version_id().unwrap_or_default();
            if let Ok(index) = rows.binary_search_by(|old| {
                old.relative_path
                    .as_str()
                    .cmp(path.as_ref())
                    .then_with(|| old.version_id.as_str().cmp(version))
            }) {
                if let Some(kind) = rows[index].change_kind(entry) {
                    events.push(StorageEntryMessage::Changed {
                        entry: Arc::clone(entry),
                        kind,
                    });
                }
            } else if by_handle && !entry.get_is_dir() && entry.get_hard_links().unwrap_or(1) > 1 {
                // 新硬链接路径仍是新增；其他路径同 handle 不能把它吞掉。
                events.push(StorageEntryMessage::New(Arc::clone(entry)));
            } else if by_handle {
                if let Some(handle) = entry.get_file_handle() {
                    absent.push((Arc::clone(entry), hex::encode(handle)));
                }
            } else {
                events.push(StorageEntryMessage::New(Arc::clone(entry)));
            }
        }
        if !absent.is_empty() {
            let handles: Vec<_> = absent.iter().map(|(_, handle)| handle.as_str()).collect();
            let lookup_client = self
                .client
                .clone()
                .with_option("use_skip_indexes", if handles.len() > 256 { "0" } else { "1" });
            let existing:HashSet<String>=lookup_client.query(&format!("SELECT DISTINCT assumeNotNull(file_handle) FROM {database}.base FINAL WHERE job_id=? AND is_deleted=0 AND file_handle IN ?"))
                .bind(&self.job_id).bind(&handles).fetch_all::<String>().await?.into_iter().collect();
            events.extend(
                absent
                    .into_iter()
                    .filter(|(_, handle)| !existing.contains(handle))
                    .map(|(entry, _)| StorageEntryMessage::New(entry)),
            );
        }
        if entries.iter().any(|entry| entry.get_version_count().is_some()) {
            let counts:HashMap<String,u64>=self.client.query(&format!("SELECT relative_path,count() FROM {database}.base FINAL WHERE job_id=? AND relative_path BETWEEN ? AND ? AND is_deleted=0 GROUP BY relative_path"))
                .bind(&self.job_id).bind(&paths[0]).bind(&paths[paths.len()-1]).fetch_all::<(String,u64)>().await?.into_iter().collect();
            for event in &mut events {
                if let StorageEntryMessage::New(entry) = event {
                    if let EntryEnum::S3(object) = entry.as_ref() {
                        if let Some(total) = object.version_count {
                            let mut object = object.clone();
                            let existing = counts
                                .get(object.relative_path.as_str())
                                .copied()
                                .unwrap_or(0)
                                .min(u64::from(u32::MAX)) as u32;
                            object.version_count = Some(total.saturating_sub(existing));
                            *entry = Arc::new(EntryEnum::S3(object));
                        }
                    }
                }
            }
        }
        Ok(events)
    }
}
