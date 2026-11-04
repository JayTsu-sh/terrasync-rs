//! 集群行封装：业务条目保持原有类型，写入时统一附加任务和轮次，禁止漏写 job_id。
use crate::clickhouse::scan_resume::ScanResumeState;
use crate::native_scan_record::{NativeScanRecord, serialize_snapshot};
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
pub(super) struct ClusterIncrementalStorageEntryRecord {
    pub job_id: String,
    pub epoch: u64,
    pub ver: i64,
    pub operation_type: String,
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
    pub create_at: i64,
    pub comment: Option<String>,
}

impl ClusterIncrementalStorageEntryRecord {
    pub fn new(job_id: String, epoch: u64, record: IncrementalStorageEntryRecord) -> Self {
        Self {
            job_id,
            epoch,
            ver: epoch as i64,
            operation_type: record.operation_type,
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
            create_at: record.create_at,
            comment: record.comment,
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
