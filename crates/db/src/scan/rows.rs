//! 原生扫描的数据库行投影；只在 SQL 序列化处展开事实，运行时保留 ObservedEntry。
#[cfg(test)]
use crate::error::DatabaseError;
use crate::scan::ScanFinalEvent;
use crate::schema::identity::encoded_file_handle;
use crate::{
    error::Result, schema::entry::EntrySummary as StorageEntryRecord, schema::records::IncrementalStorageEntryRecord,
};
use blake3::Hasher;
use data_mover::model::ObjectTag;
#[cfg(test)]
use data_mover::model::{
    BackendIdentity, HistoryOrder, IdentityStrength, ObjectHistory, RecordKind, SourceIdentity, SpecialFileKind,
    StoragePath, SymlinkTarget, SymlinkTargetEncoding, VersionRecord,
};
use data_mover::model::{BackendKind, EntryKind, EntrySnapshot, ObservedEntry, StorageTimestamp};
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use task_model::observation::ObservedItem;
use task_model::observation::{EntryStatistics, ObservedScanEvent, ObservedView, timestamp_nanos};

/// 只摘要原生标记对象的比较事实，不重新编码或复制 opaque snapshot。
pub(crate) fn directory_marker_state(entry: &ObservedEntry) -> Option<String> {
    let marker = entry.directory_marker_object()?;
    let mut hash = Hasher::new();
    hash.update(marker.identity_key().as_bytes());
    hash.update(
        &marker
            .modified()
            .map(StorageTimestamp::unix_nanos)
            .unwrap_or_default()
            .to_le_bytes(),
    );
    if let Some(tags) = marker.metadata().tags().value() {
        hash.update(&[1]);
        for tag in tags {
            for part in [tag.key(), tag.value()] {
                hash.update(&(part.len() as u64).to_le_bytes());
                hash.update(part.as_bytes());
            }
        }
    }
    Some(hash.finalize().to_hex().to_string())
}

pub(crate) const OBSERVED_COLUMNS: &str = "name,relative_path,size,ext,ctime,mtime,atime,mode,storage_type,is_symlink,is_dir,is_regular_file,hard_links,current_state,uid,gid,ino,file_handle,version_id,tags,marker_state,identity_key,backend_kind,backend_id,entry_kind,size_observed,modified_unix_nanos,entry_snapshot,metadata_hash,history_segment,history_index";

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
            relative_path: view.path_key(),
            version_id: view.version_id(),
            file_handle: encoded_file_handle(view.entry),
        }
    }
}

struct BorrowedTags<'a>(&'a [ObjectTag]);
impl Serialize for BorrowedTags<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct TagRef<'a> {
            key: &'a str,
            value: &'a str,
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for tag in self.0 {
            sequence.serialize_element(&TagRef {
                key: tag.key(),
                value: tag.value(),
            })?;
        }
        sequence.end()
    }
}

// 公共查询列只投影一次；事件不编码随后会丢弃的原生 snapshot。
struct ScanRowFields<'a> {
    name: &'a str,
    relative_path: Cow<'a, str>,
    size: u64,
    ext: Option<&'a str>,
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
    version_id: &'a str,
    tags: Option<String>,
    marker_state: Option<String>,
}
impl<'a> ScanRowFields<'a> {
    fn capture(view: ObservedView<'a>, generation: u8) -> Result<Self> {
        let entry = view.entry;
        let timestamps = view.metadata().timestamps().value();
        let tags = view
            .metadata()
            .tags()
            .value()
            .map(|tags| serde_json::to_string(&BorrowedTags(tags)))
            .transpose()?;
        let path = Path::new(entry.path().as_str());
        Ok(Self {
            name: path.file_name().and_then(|name| name.to_str()).unwrap_or_default(),
            relative_path: view.path_key(),
            size: view.get_size(),
            ext: path.extension().and_then(|ext| ext.to_str()),
            ctime: timestamp_nanos(timestamps.and_then(|t| t.created)),
            mtime: view.get_mtime(),
            atime: timestamp_nanos(timestamps.and_then(|t| t.accessed)),
            mode: view.mode(),
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
            uid: view.uid(),
            gid: view.gid(),
            ino: None,
            file_handle: encoded_file_handle(view.entry),
            version_id: view.version_id(),
            tags,
            marker_state: directory_marker_state(entry),
        })
    }
    fn legacy(self) -> StorageEntryRecord {
        StorageEntryRecord {
            name: self.name.into(),
            relative_path: self.relative_path.into_owned(),
            size: self.size,
            ext: self.ext.map(str::to_owned),
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
            version_id: self.version_id.into(),
            tags: self.tags,
            marker_state: self.marker_state,
        }
    }
}

pub(crate) fn capture_summary(view: ObservedView<'_>, generation: u8) -> Result<StorageEntryRecord> {
    Ok(ScanRowFields::capture(view, generation)?.legacy())
}

#[derive(Clone, Debug, Serialize, clickhouse::Row)]
pub(crate) struct NativeScanRecord<'a> {
    pub name: &'a str,
    pub relative_path: Cow<'a, str>,
    pub size: u64,
    pub ext: Option<&'a str>,
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
    pub version_id: &'a str,
    pub tags: Option<String>,
    pub marker_state: Option<String>,
    pub identity_key: String,
    pub backend_kind: &'static str,
    pub backend_id: &'a str,
    pub entry_kind: &'static str,
    pub size_observed: bool,
    pub modified_unix_nanos: Option<i128>,
    #[serde(serialize_with = "serialize_snapshot")]
    pub entry_snapshot: EntrySnapshot,
    pub metadata_hash: Option<String>,
    pub history_segment: u32,
    pub history_index: u32,
}
/// 每次请求拥有有界的编码缓冲；重试复用相同字节和去重 token。
pub(crate) struct NativeWriteBatches<'a> {
    views: Box<dyn Iterator<Item = ObservedView<'a>> + Send + 'a>,
    generation: u8,
}
impl<'a> NativeWriteBatches<'a> {
    pub(crate) fn new(entries: &'a [Arc<ObservedEntry>], generation: u8) -> Self {
        Self {
            views: Box::new(entries.iter().flat_map(|entry| ObservedView::all(entry))),
            generation,
        }
    }
}
impl<'a> Iterator for NativeWriteBatches<'a> {
    type Item = Result<Vec<NativeScanRecord<'a>>>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut rows = Vec::new();
        let mut retained_bytes = 0usize;
        while rows.len() < 1024 && retained_bytes < 1024 * 1024 {
            let Some(view) = self.views.next() else {
                break;
            };
            let row = match NativeScanRecord::capture(view, self.generation) {
                Ok(row) => row,
                Err(error) => return Some(Err(error)),
            };
            retained_bytes = retained_bytes
                .saturating_add(row.entry_snapshot.as_bytes().len())
                .saturating_add(row.tags.as_ref().map_or(0, String::len))
                .saturating_add(row.identity_key.len())
                .saturating_add(row.file_handle.as_ref().map_or(0, String::len));
            rows.push(row);
        }
        (!rows.is_empty()).then_some(Ok(rows))
    }
}

pub(crate) fn serialize_snapshot<S: Serializer>(
    snapshot: &EntrySnapshot, serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    // RowBinary 的 Array(UInt8) 与字节串均为 varint 长度 + 原始字节。
    // 专用批量 insert 使用已验证的固定列布局，避免逐字节 serde 分派和类型检查。
    serializer.serialize_bytes(snapshot.as_bytes())
}
impl<'a> NativeScanRecord<'a> {
    pub(crate) fn capture(view: ObservedView<'a>, generation: u8) -> Result<Self> {
        let entry = view.entry;
        let fields = ScanRowFields::capture(view, generation)?;
        let kind = match entry.kind() {
            EntryKind::File => "file",
            EntryKind::Directory => "directory",
            EntryKind::Symlink => "symlink",
            EntryKind::Special(_) => "special",
        };
        let snapshot = match view.version_index {
            Some(index) => entry.try_encode_version_snapshot(index)?,
            None => entry.try_encode_snapshot()?,
        };
        let identity = view
            .version()
            .map_or(entry.source_identity(), |version| version.identity());
        let size = view.version().map_or(entry.size(), |version| version.size());
        let modified = view
            .version()
            .map_or(entry.modified(), |version| version.modified())
            .map(StorageTimestamp::unix_nanos);
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
            marker_state: fields.marker_state,
            identity_key: hex::encode(identity.identity_key().as_bytes()),
            backend_kind: entry.backend_kind().as_str().into(),
            backend_id: entry.source_identity().backend().stable_id(),
            entry_kind: kind.into(),
            size_observed: size.is_some(),
            modified_unix_nanos: modified,
            entry_snapshot: snapshot,
            metadata_hash: None,
            history_segment: entry.history().map_or(0, |history| history.segment()),
            history_index: view.version_index.unwrap_or_default() as u32,
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
            || view.path_key().as_ref() != self.relative_path
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
            marker_state: self.marker_state,
        }
        .legacy()
    }
}

pub(crate) fn event_records(events: &[ObservedScanEvent], time: i64) -> Result<Vec<IncrementalStorageEntryRecord>> {
    events.iter().map(|event| event_record(event, time)).collect()
}

pub(crate) fn event_record(event: &ObservedScanEvent, event_time: i64) -> Result<IncrementalStorageEntryRecord> {
    let record = ScanRowFields::capture(event.item().view(), 0)?.legacy();
    Ok(incremental_record(
        record,
        event.operation(),
        match event {
            ObservedScanEvent::Renamed { from, .. } => Some(from.view().path_key().into_owned()),
            _ => None,
        },
        event_time,
    ))
}

pub(crate) fn final_event_records(events: &[ScanFinalEvent], time: i64) -> Vec<IncrementalStorageEntryRecord> {
    events
        .iter()
        .map(|event| incremental_record(event.item().clone(), event.operation(), event.previous_path(), time))
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
        marker_state: record.marker_state,
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
    fn native_row_keeps_mode_and_named_facts_without_fabricating_numeric_owners() -> Result<()> {
        use data_mover::model::{PermissionFacts, PrincipalIdentity};
        let backend = BackendIdentity::new(BackendKind::Hdfs, "named-permissions")?;
        let facts = PermissionFacts {
            owner: Some(PrincipalIdentity::Named("alice".into())),
            group: None,
            mode: Some(0o640),
        };
        let entry = ObservedEntry::new(
            StoragePath::new("file")?,
            EntryKind::File,
            Some(3),
            None,
            SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, b"file-with-mode")?,
        )?
        .with_permissions(Some(Box::new(facts.clone())))?;
        let record = NativeScanRecord::capture(
            ObservedView {
                entry: &entry,
                version_index: None,
            },
            1,
        )?;
        assert_eq!(record.mode, Some(0o640));
        assert_eq!((record.uid, record.gid), (None, None));
        assert_eq!(record.observed(&backend)?.entry.permissions(), Some(&facts));
        Ok(())
    }
    #[test]
    fn native_row_preserves_independently_mapped_nfs_principals() -> Result<()> {
        use data_mover::model::{PermissionFacts, PrincipalIdentity};
        let backend = BackendIdentity::new(BackendKind::Nfs, "partial-permissions")?;
        for (uid, gid) in [(Some(100), None), (None, Some(200))] {
            let entry = ObservedEntry::new(
                StoragePath::new("file")?,
                EntryKind::File,
                Some(3),
                None,
                SourceIdentity::new(backend.clone(), IdentityStrength::StableWithinBackend, b"handle")?,
            )?
            .with_permissions(Some(Box::new(PermissionFacts {
                owner: uid.map(PrincipalIdentity::Numeric),
                group: gid.map(PrincipalIdentity::Numeric),
                mode: Some(0o640),
            })))?;
            let record = NativeScanRecord::capture(
                ObservedView {
                    entry: &entry,
                    version_index: None,
                },
                1,
            )?;
            assert_eq!(record.mode, Some(0o640));
            assert_eq!((record.uid, record.gid), (uid, gid));
            assert_eq!(record.observed(&backend)?.entry.permissions(), entry.permissions());
        }
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
        let final_rows = final_event_records(&[ScanFinalEvent::Added(rows[0].clone().legacy())], 1);
        assert_eq!(final_rows.len(), 1);
        assert_eq!(final_rows[0].version_id, "null");
        let marker = rows[1].observed(&backend)?;
        assert!(marker.view().version().is_some_and(VersionRecord::is_delete_marker));
        assert_eq!(marker.view().version_id(), "deleted");
        let entry = Arc::new(entry);
        let new = ObservedScanEvent::New(ObservedItem {
            entry,
            version_index: Some(1),
        });
        let events = event_records(&[new], 9)?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version_id, "deleted");
        assert_eq!(events[0].size, 0);
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
