//! 原生扫描的数据库行投影；只在 SQL 序列化处展开事实，运行时保留 ObservedEntry。
#[cfg(test)]
use crate::error::DatabaseError;
use crate::scan_store::ScanFinalEvent;
use crate::{
    error::Result,
    traits::{IncrementalStorageEntryRecord, StorageEntryRecord},
};
#[cfg(test)]
use data_mover::model::{
    BackendIdentity, IdentityStrength, RecordKind, SourceIdentity, SpecialFileKind, StoragePath, SymlinkTarget,
    SymlinkTargetEncoding, VersionRecord,
};
use data_mover::model::{
    BackendKind, EntryKind, EntrySnapshot, HistoryOrder, ObjectHistory, ObservedEntry, StorageTimestamp,
};
use rustc_hash::FxHashMap;
use serde::{Serialize, Serializer};
use smol_str::SmolStr;
use std::borrow::Cow;
#[cfg(test)]
use std::sync::Arc;
use storage_adapter::Tag;
#[cfg(test)]
use storage_adapter::observed_scan::ObservedItem;
use storage_adapter::observed_scan::{EntryStatistics, ObservedScanEvent, ObservedView, timestamp_nanos};

pub(crate) const OBSERVED_COLUMNS: &str = "name,relative_path,size,ext,ctime,mtime,atime,mode,storage_type,is_symlink,is_dir,is_regular_file,hard_links,current_state,uid,gid,ino,file_handle,version_id,tags,version_count,identity_key,backend_kind,entry_kind,size_observed,modified_unix_nanos,entry_snapshot,metadata_hash";

/// 比较集合只传输主键和身份；完整观察在事件持久化后直接写入 base。
#[derive(Serialize, clickhouse::Row)]
pub(crate) struct NativeScanKey<'a> {
    pub relative_path: Cow<'a, str>,
    pub version_id: &'a str,
    pub file_handle: Option<String>,
}
impl<'a> NativeScanKey<'a> {
    pub(crate) fn capture(view: ObservedView<'a>) -> Self {
        Self {
            relative_path: view.sql_path(),
            version_id: view.version_id(),
            file_handle: view.file_handle(),
        }
    }
}

// 公共查询列只投影一次；事件不编码随后会丢弃的原生 snapshot。
struct ScanRowFields {
    name: String,
    relative_path: String,
    size: u64,
    ext: Option<String>,
    ctime: i64,
    mtime: i64,
    atime: i64,
    mode: Option<u32>,
    storage_type: &'static str,
    is_symlink: bool,
    is_dir: bool,
    is_regular_file: bool,
    hard_links: u32,
    current_state: u8,
    uid: Option<u32>,
    gid: Option<u32>,
    ino: Option<u64>,
    file_handle: Option<String>,
    version_id: String,
    tags: Option<String>,
    version_count: Option<u32>,
}
impl ScanRowFields {
    fn capture(view: ObservedView<'_>, generation: u8) -> Result<Self> {
        let entry = view.entry;
        let ownership = view.metadata().ownership_mode().value();
        let timestamps = view.metadata().timestamps().value();
        let tags = view.metadata().tags().value().map(|tags| {
            tags.iter()
                .map(|tag| Tag {
                    key: tag.key().into(),
                    value: tag.value().into(),
                })
                .collect::<Vec<_>>()
        });
        Ok(Self {
            name: view.get_name().into(),
            relative_path: view.path(),
            size: view.get_size(),
            ext: view.get_extension().map(str::to_owned),
            ctime: timestamp_nanos(timestamps.and_then(|t| t.created)),
            mtime: view.get_mtime(),
            atime: timestamp_nanos(timestamps.and_then(|t| t.accessed)),
            mode: ownership.map(|o| o.mode),
            storage_type: match entry.backend_kind() {
                BackendKind::S3 => "s3",
                BackendKind::Hdfs => "hdfs",
                _ => "nas",
            }
            .into(),
            is_symlink: view.get_is_symlink(),
            is_dir: view.get_is_dir(),
            is_regular_file: entry.kind() == EntryKind::File,
            hard_links: 1,
            current_state: generation,
            uid: ownership.map(|o| o.uid),
            gid: ownership.map(|o| o.gid),
            ino: None,
            file_handle: view.file_handle(),
            version_id: view.version_id().into(),
            tags: tags.as_ref().map(serde_json::to_string).transpose()?,
            version_count: entry.history().map(|h| h.records().len().min(u32::MAX as usize) as u32),
        })
    }
    fn legacy(self) -> StorageEntryRecord {
        StorageEntryRecord {
            name: self.name,
            relative_path: self.relative_path,
            size: self.size,
            ext: self.ext,
            ctime: self.ctime,
            mtime: self.mtime,
            atime: self.atime,
            mode: self.mode,
            storage_type: self.storage_type.into(),
            is_symlink: self.is_symlink,
            is_dir: self.is_dir,
            is_regular_file: self.is_regular_file,
            hard_links: self.hard_links,
            current_state: self.current_state,
            uid: self.uid,
            gid: self.gid,
            ino: self.ino,
            file_handle: self.file_handle,
            version_id: self.version_id,
            tags: self.tags,
            version_count: self.version_count,
        }
    }
}

#[derive(Clone, Debug, Serialize, clickhouse::Row)]
pub(crate) struct NativeScanRecord {
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
pub(crate) fn serialize_snapshot<S: Serializer>(
    snapshot: &EntrySnapshot, serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    // RowBinary 的 Array(UInt8) 与字节串均为 varint 长度 + 原始字节。
    // 专用批量 insert 使用已验证的固定列布局，避免逐字节 serde 分派和类型检查。
    serializer.serialize_bytes(snapshot.as_bytes())
}
impl NativeScanRecord {
    pub(crate) fn capture(view: ObservedView<'_>, generation: u8) -> Result<Self> {
        let entry = view.entry;
        let fields = ScanRowFields::capture(view, generation)?;
        let kind = match entry.kind() {
            EntryKind::File => "file",
            EntryKind::Directory => "directory",
            EntryKind::Symlink => "symlink",
            EntryKind::Special(_) => "special",
        };
        let selected;
        let snapshot_entry = if let Some(record) = view.version() {
            // 每个 SQL 版本行仅保存自身 snapshot，避免长历史平方级复制。
            selected = ObservedEntry::new(
                entry.path().clone(),
                entry.kind(),
                record.size(),
                record.modified(),
                record.identity().clone(),
            )?
            .with_metadata(view.metadata().clone())
            .with_history(Some(Box::new(ObjectHistory::whole(
                vec![record.clone()],
                entry.history().map_or(HistoryOrder::Server, |h| h.order()),
            )?)))?;
            &selected
        } else {
            entry
        };
        let modified = snapshot_entry.modified().map(StorageTimestamp::unix_nanos);
        Ok(Self {
            name: fields.name,
            relative_path: fields.relative_path,
            size: fields.size,
            ext: fields.ext,
            ctime: fields.ctime,
            mtime: fields.mtime,
            atime: fields.atime,
            mode: fields.mode,
            storage_type: fields.storage_type,
            is_symlink: fields.is_symlink,
            is_dir: fields.is_dir,
            is_regular_file: fields.is_regular_file,
            hard_links: fields.hard_links,
            current_state: fields.current_state,
            uid: fields.uid,
            gid: fields.gid,
            ino: fields.ino,
            file_handle: fields.file_handle,
            version_id: fields.version_id,
            tags: fields.tags,
            version_count: fields.version_count,
            identity_key: hex::encode(snapshot_entry.identity_key().as_bytes()),
            backend_kind: entry.backend_kind().as_str().into(),
            entry_kind: kind.into(),
            size_observed: snapshot_entry.size().is_some(),
            modified_unix_nanos: modified,
            entry_snapshot: snapshot_entry.try_encode_snapshot()?,
            metadata_hash: None,
        })
    }
    #[cfg(test)]
    pub(crate) fn observed(&self, backend: &BackendIdentity) -> Result<ObservedItem> {
        let entry = ObservedEntry::decode_snapshot(self.entry_snapshot.as_bytes(), backend)?;
        let version_index = entry.history().map(|_| 0);
        let view = ObservedView {
            entry: &entry,
            version_index,
        };
        if hex::encode(entry.identity_key().as_bytes()) != self.identity_key
            || view.path() != self.relative_path
            || view.version_id() != self.version_id
            || view.get_size() != self.size
        {
            return Err(DatabaseError::ObservationProjectionMismatch {
                field: "identity/path/version/size",
            });
        }
        Ok(ObservedItem {
            entry: Arc::new(entry),
            version_index,
        })
    }
    #[cfg(test)]
    pub(crate) fn legacy(self) -> StorageEntryRecord {
        ScanRowFields {
            name: self.name,
            relative_path: self.relative_path,
            size: self.size,
            ext: self.ext,
            ctime: self.ctime,
            mtime: self.mtime,
            atime: self.atime,
            mode: self.mode,
            storage_type: self.storage_type,
            is_symlink: self.is_symlink,
            is_dir: self.is_dir,
            is_regular_file: self.is_regular_file,
            hard_links: self.hard_links,
            current_state: self.current_state,
            uid: self.uid,
            gid: self.gid,
            ino: self.ino,
            file_handle: self.file_handle,
            version_id: self.version_id,
            tags: self.tags,
            version_count: self.version_count,
        }
        .legacy()
    }
}

pub(crate) fn event_records(events: &[ObservedScanEvent], time: i64) -> Result<Vec<IncrementalStorageEntryRecord>> {
    let mut added: FxHashMap<SmolStr, u32> = FxHashMap::default();
    for event in events {
        if let ObservedScanEvent::New(item) = event
            && item.version_index.is_some()
        {
            let count = added.entry(SmolStr::new(item.view().path())).or_default();
            *count = count.saturating_add(1);
        }
    }
    events
        .iter()
        .map(|event| {
            let mut record = event_record(event, time)?;
            if matches!(event, ObservedScanEvent::New(_)) && record.version_count.is_some() {
                record.version_count = added.get(record.relative_path.as_str()).copied();
            }
            Ok(record)
        })
        .collect()
}

pub(crate) fn event_record(event: &ObservedScanEvent, event_time: i64) -> Result<IncrementalStorageEntryRecord> {
    let record = ScanRowFields::capture(event.item().view(), 0)?.legacy();
    Ok(incremental_record(
        record,
        event.operation(),
        match event {
            ObservedScanEvent::Renamed { from, .. } => Some(from.view().path()),
            _ => None,
        },
        event_time,
    ))
}

pub(crate) fn final_event_records(events: &[ScanFinalEvent], time: i64) -> Vec<IncrementalStorageEntryRecord> {
    let mut added: FxHashMap<SmolStr, u32> = FxHashMap::default();
    for event in events {
        if let ScanFinalEvent::Added(item) = event
            && item.version_count.is_some()
        {
            let count = added.entry(SmolStr::new(&item.relative_path)).or_default();
            *count = count.saturating_add(1);
        }
    }
    events
        .iter()
        .map(|event| {
            let mut record = event.item().clone();
            if matches!(event, ScanFinalEvent::Added(_)) && record.version_count.is_some() {
                record.version_count = added.get(record.relative_path.as_str()).copied();
            }
            incremental_record(record, event.operation(), event.previous_path(), time)
        })
        .collect()
}

pub(crate) fn incremental_record(
    record: StorageEntryRecord, operation: &str, comment: Option<String>, event_time: i64,
) -> IncrementalStorageEntryRecord {
    IncrementalStorageEntryRecord {
        operation_type: operation.into(),
        create_at: event_time,
        comment,
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

#[cfg(test)]
mod tests {
    use super::*;
    use data_mover::model::{
        AclMetadata, MetadataObservation, MetadataObservations, MetadataProvenance, OwnershipMode, TimePrecision,
    };
    #[test]
    fn native_row_roundtrips_identity_acl_and_metadata_without_projection_loss() -> Result<()> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "native-fixture")?;
        let identity = SourceIdentity::new(
            backend.clone(),
            IdentityStrength::StableWithinBackend,
            b"native-file-handle",
        )?;
        let metadata = MetadataObservations::new(
            MetadataObservation::Value {
                value: AclMetadata::new_posix(Some(vec![1, 2, 3]), None)?,
                provenance: MetadataProvenance::Inline,
            },
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::Value {
                value: OwnershipMode {
                    uid: 100,
                    gid: 200,
                    mode: 0o640,
                },
                provenance: MetadataProvenance::Inline,
            },
            MetadataObservation::NotRequested,
        )?;
        let entry = ObservedEntry::new(
            StoragePath::new("dir/file.bin")?,
            EntryKind::File,
            Some(17),
            Some(StorageTimestamp::new(1_000_000_001, TimePrecision::Nanoseconds)?),
            identity,
        )?
        .with_metadata(metadata);
        let record = NativeScanRecord::capture(
            ObservedView {
                entry: &entry,
                version_index: None,
            },
            1,
        )?;
        assert_eq!(record.mode, Some(0o640));
        assert_eq!(record.uid, Some(100));
        assert!(
            record
                .file_handle
                .as_ref()
                .is_some_and(|key| key.starts_with("54534934"))
        );
        assert_eq!(*record.observed(&backend)?.entry, entry);
        let mut corrupt = record.clone();
        corrupt.relative_path = "other".into();
        assert!(corrupt.observed(&backend).is_err());
        let other = BackendIdentity::new(BackendKind::Nfs, "other")?;
        assert!(record.observed(&other).is_err());
        Ok(())
    }
    #[test]
    fn event_projection_preserves_columns_and_exact_file_kind() -> Result<()> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "event-projection")?;
        for kind in [
            EntryKind::File,
            EntryKind::Directory,
            EntryKind::Symlink,
            EntryKind::Special(SpecialFileKind::BlockDevice),
            EntryKind::Special(SpecialFileKind::CharacterDevice),
            EntryKind::Special(SpecialFileKind::Fifo),
            EntryKind::Special(SpecialFileKind::Socket),
        ] {
            let identity = SourceIdentity::new(backend.clone(), IdentityStrength::StableWithinBackend, b"node")?;
            let entry = if kind == EntryKind::Symlink {
                ObservedEntry::new_symlink(
                    StoragePath::new("nested/node.dat")?,
                    None,
                    identity,
                    SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec())?,
                )?
            } else {
                ObservedEntry::new(StoragePath::new("nested/node.dat")?, kind, Some(17), None, identity)?
            };
            let entry = Arc::new(entry);
            let item = ObservedItem {
                entry,
                version_index: None,
            };
            let native = NativeScanRecord::capture(item.view(), 0)?;
            assert_eq!(native.is_regular_file, kind == EntryKind::File);
            let expected = incremental_record(native.legacy(), "new", None, 19);
            let actual = event_record(&ObservedScanEvent::New(item), 19)?;
            assert_eq!(serde_json::to_value(actual)?, serde_json::to_value(expected)?);
        }
        Ok(())
    }

    #[test]
    fn native_history_preserves_each_version_null_and_delete_marker() -> Result<()> {
        let backend = BackendIdentity::new(BackendKind::S3, "native-history")?;
        let identity = SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, b"key")?;
        let records = vec![
            VersionRecord::from_listing(
                "null",
                false,
                RecordKind::Object {
                    size: 17,
                    etag: "etag".into(),
                    modified: None,
                },
                identity.clone(),
            )?,
            VersionRecord::from_listing(
                "deleted",
                true,
                RecordKind::DeleteMarker { modified: None },
                identity.clone(),
            )?,
        ];
        let entry = ObservedEntry::new(StoragePath::new("key")?, EntryKind::File, None, None, identity)?
            .with_history(Some(Box::new(ObjectHistory::whole(records, HistoryOrder::Server)?)))?;
        let rows = ObservedView::all(&entry)
            .map(|view| NativeScanRecord::capture(view, 1))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].version_id, "null");
        assert_eq!(rows[0].size, 17);
        assert_eq!(rows[1].version_id, "deleted");
        assert_eq!(rows[0].version_count, Some(2));
        let final_rows = final_event_records(&[ScanFinalEvent::Added(rows[0].clone().legacy())], 1);
        assert_eq!(final_rows[0].version_count, Some(1));
        let marker = rows[1].observed(&backend)?;
        assert!(marker.view().version().is_some_and(VersionRecord::is_delete_marker));
        assert_eq!(marker.view().version_id(), "deleted");
        let entry = Arc::new(entry);
        let new = ObservedScanEvent::New(ObservedItem {
            entry,
            version_index: Some(1),
        });
        let events = event_records(&[new], 9)?;
        assert_eq!(
            events[0].version_count,
            Some(1),
            "incremental events count only newly observed versions"
        );
        Ok(())
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use clickhouse::{
        Client,
        test::{Mock, handlers},
    };
    use serde::Deserialize;
    use std::time::Instant;
    #[derive(Serialize, clickhouse::Row)]
    struct WriteRow {
        before: u64,
        #[serde(serialize_with = "serialize_snapshot")]
        entry_snapshot: EntrySnapshot,
        after: i128,
    }
    #[derive(Deserialize, clickhouse::Row)]
    struct ReadRow {
        before: u64,
        entry_snapshot: Vec<u8>,
        after: i128,
    }
    async fn exercise(count: u64) -> Result<()> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "wire-bulk")?;
        let identity = SourceIdentity::new(backend.clone(), IdentityStrength::StableWithinBackend, b"native-id")?;
        let entry = ObservedEntry::new(
            StoragePath::new("wire/entry")?,
            EntryKind::File,
            Some(17),
            None,
            identity,
        )?;
        let row = WriteRow {
            before: 127,
            entry_snapshot: entry.try_encode_snapshot()?,
            after: -9,
        };
        let mock = Mock::new();
        let recorded = mock.add(handlers::record::<ReadRow>());
        let client = Client::default().with_url(mock.url()).with_validation(false);
        let start = Instant::now();
        let mut insert = client.insert::<WriteRow>("snapshot_wire_fixture").await?;
        for _ in 0..count {
            insert.write(&row).await?;
        }
        insert.end().await?;
        eprintln!(
            "native snapshot RowBinary: {count} rows in {:.3}s",
            start.elapsed().as_secs_f64()
        );
        let rows: Vec<ReadRow> = recorded.collect().await;
        assert_eq!(rows.len(), count as usize);
        for read in rows {
            assert_eq!((read.before, read.after), (127, -9));
            assert_eq!(read.entry_snapshot, row.entry_snapshot.as_bytes());
            assert_eq!(ObservedEntry::decode_snapshot(&read.entry_snapshot, &backend)?, entry);
        }
        Ok(())
    }
    #[tokio::test]
    async fn snapshot_rowbinary_matches_array_u8_wire_layout() -> Result<()> {
        exercise(3).await
    }
    #[tokio::test]
    #[ignore = "manual CPU probe for native snapshot serialization"]
    async fn snapshot_serialization_cpu_probe() -> Result<()> {
        exercise(200_000).await
    }
}
