//! 集群行封装：业务条目保持原有类型，写入时统一附加任务和轮次，禁止漏写 job_id。
use crate::scan::ScanResumeState;
use crate::scan::rows::{NativeScanRecord, serialize_snapshot};
use crate::traits::IncrementalStorageEntryRecord;
#[cfg(test)]
use crate::traits::StorageEntryRecord;
use data_mover::model::EntrySnapshot;
use serde::Serialize;

#[cfg(test)]
#[derive(Serialize, clickhouse::Row)]
pub(super) struct ClusterStorageEntryRecord {
    pub job_id: String,
    pub epoch: u64,
    pub ver: i64,
    pub batch_id: u64,
    pub name: String,
    pub relative_path: String,
    pub size: u64,
    pub ext: Option<String>,
    pub ctime: i64,
    pub mtime: i64,
    pub atime: i64,
    pub mode: Option<u32>,
    pub storage_type: String,
    pub is_symlink: bool,
    pub is_dir: bool,
    pub is_regular_file: bool,
    pub hard_links: u32,
    pub current_state: u8,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub ino: Option<u64>,
    pub file_handle: Option<String>,
    pub version_id: String,
    pub tags: Option<String>,
    pub version_count: Option<u32>,
}

#[cfg(test)]
impl ClusterStorageEntryRecord {
    pub fn new(job_id: String, epoch: u64, batch_id: u64, record: StorageEntryRecord) -> Self {
        Self {
            job_id,
            epoch,
            ver: epoch as i64,
            batch_id,
            name: record.name,
            relative_path: record.relative_path,
            size: record.size,
            ext: record.ext,
            ctime: record.ctime,
            mtime: record.mtime,
            atime: record.atime,
            mode: record.mode,
            storage_type: record.storage_type,
            is_symlink: record.is_symlink,
            is_dir: record.is_dir,
            is_regular_file: record.is_regular_file,
            hard_links: record.hard_links,
            current_state: record.current_state,
            uid: record.uid,
            gid: record.gid,
            ino: record.ino,
            file_handle: record.file_handle,
            version_id: record.version_id,
            tags: record.tags,
            version_count: record.version_count,
        }
    }
}

#[derive(Serialize, clickhouse::Row)]
pub(super) struct ClusterIncrementalStorageEntryRecord<'a> {
    pub job_id: &'a str,
    pub epoch: u64,
    pub ver: i64,
    pub operation_type: &'a str,
    pub name: &'a str,
    pub relative_path: &'a str,
    pub size: u64,
    pub ext: Option<&'a str>,
    pub ctime: i64,
    pub mtime: i64,
    pub atime: i64,
    pub mode: Option<u32>,
    pub storage_type: &'a str,
    pub is_symlink: bool,
    pub is_dir: bool,
    pub is_regular_file: bool,
    pub hard_links: u32,
    pub current_state: u8,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub ino: Option<u64>,
    pub file_handle: Option<&'a str>,
    pub version_id: &'a str,
    pub tags: Option<&'a str>,
    pub version_count: Option<u32>,
    pub create_at: i64,
    pub comment: Option<&'a str>,
}

impl<'a> ClusterIncrementalStorageEntryRecord<'a> {
    pub fn new(job_id: &'a str, epoch: u64, record: &'a IncrementalStorageEntryRecord) -> Self {
        Self {
            job_id,
            epoch,
            ver: epoch as i64,
            operation_type: &record.operation_type,
            name: &record.name,
            relative_path: &record.relative_path,
            size: record.size,
            ext: record.ext.as_deref(),
            ctime: record.ctime,
            mtime: record.mtime,
            atime: record.atime,
            mode: record.mode,
            storage_type: &record.storage_type,
            is_symlink: record.is_symlink,
            is_dir: record.is_dir,
            is_regular_file: record.is_regular_file,
            hard_links: record.hard_links,
            current_state: record.current_state,
            uid: record.uid,
            gid: record.gid,
            ino: record.ino,
            file_handle: record.file_handle.as_deref(),
            version_id: &record.version_id,
            tags: record.tags.as_deref(),
            version_count: record.version_count,
            create_at: record.create_at,
            comment: record.comment.as_deref(),
        }
    }
}

#[derive(Serialize, clickhouse::Row)]
pub(super) struct ClusterScanResumeState {
    pub job_id: String,
    pub id: u8,
    pub schema_version: u8,
    pub version: u64,
    pub request_key: String,
    pub generation: u8,
    pub incremental: bool,
    pub phase: u8,
    pub event_time: i64,
    pub observed_entries: u64,
    pub checkpoint: Vec<u8>,
    pub statistics: String,
    pub baseline_snapshot: String,
    pub checkpoint_snapshot: String,
}

impl ClusterScanResumeState {
    pub fn new(job_id: String, record: ScanResumeState) -> Self {
        Self {
            job_id,
            id: record.id,
            schema_version: record.schema_version,
            version: record.version,
            request_key: record.request_key,
            generation: record.generation,
            incremental: record.incremental,
            phase: record.phase,
            event_time: record.event_time,
            observed_entries: record.observed_entries,
            checkpoint: record.checkpoint,
            statistics: record.statistics,
            baseline_snapshot: record.baseline_snapshot,
            checkpoint_snapshot: record.checkpoint_snapshot,
        }
    }
}

#[derive(Serialize, clickhouse::Row)]
pub(super) struct ClusterNativeScanRecord {
    pub job_id: String,
    pub epoch: u64,
    pub ver: i64,
    pub batch_id: u64,
    pub name: String,
    pub relative_path: String,
    pub size: u64,
    pub ext: Option<String>,
    pub ctime: i64,
    pub mtime: i64,
    pub atime: i64,
    pub mode: Option<u32>,
    pub storage_type: &'static str,
    pub is_symlink: bool,
    pub is_dir: bool,
    pub is_regular_file: bool,
    pub hard_links: u32,
    pub current_state: u8,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub ino: Option<u64>,
    pub file_handle: Option<String>,
    pub version_id: String,
    pub tags: Option<String>,
    pub version_count: Option<u32>,
    pub identity_key: String,
    pub backend_kind: &'static str,
    pub entry_kind: &'static str,
    pub size_observed: bool,
    pub modified_unix_nanos: Option<i128>,
    #[serde(serialize_with = "serialize_snapshot")]
    pub entry_snapshot: EntrySnapshot,
    pub metadata_hash: Option<String>,
}
impl ClusterNativeScanRecord {
    pub fn new(job_id: String, epoch: u64, batch_id: u64, record: NativeScanRecord) -> Self {
        Self {
            job_id,
            epoch,
            ver: epoch as i64,
            batch_id,
            name: record.name,
            relative_path: record.relative_path,
            size: record.size,
            ext: record.ext,
            ctime: record.ctime,
            mtime: record.mtime,
            atime: record.atime,
            mode: record.mode,
            storage_type: record.storage_type,
            is_symlink: record.is_symlink,
            is_dir: record.is_dir,
            is_regular_file: record.is_regular_file,
            hard_links: record.hard_links,
            current_state: record.current_state,
            uid: record.uid,
            gid: record.gid,
            ino: record.ino,
            file_handle: record.file_handle,
            version_id: record.version_id,
            tags: record.tags,
            version_count: record.version_count,
            identity_key: record.identity_key,
            backend_kind: record.backend_kind,
            entry_kind: record.entry_kind,
            size_observed: record.size_observed,
            modified_unix_nanos: record.modified_unix_nanos,
            entry_snapshot: record.entry_snapshot,
            metadata_hash: record.metadata_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use serde_json::json;

    #[test]
    fn borrowed_event_row_preserves_every_owned_column() -> Result<()> {
        let record: IncrementalStorageEntryRecord = serde_json::from_value(json!({
            "operation_type":"rename", "name":"目标.dat", "relative_path":"nested/目标.dat", "size":37,
            "ext":"dat", "ctime":11, "mtime":12, "atime":13, "mode":420, "storage_type":"nas",
            "is_symlink":false, "is_dir":false, "is_regular_file":true, "hard_links":2,
            "current_state":1, "uid":1000, "gid":1001, "ino":42, "file_handle":"abcd",
            "version_id":"v1", "tags":"[]", "version_count":2, "create_at":17, "comment":"old/source.dat"
        }))?;
        let mut expected = serde_json::to_value(&record)?;
        expected["job_id"] = json!("job");
        expected["epoch"] = json!(19);
        expected["ver"] = json!(19);
        assert_eq!(
            serde_json::to_value(ClusterIncrementalStorageEntryRecord::new("job", 19, &record))?,
            expected
        );
        Ok(())
    }
}
