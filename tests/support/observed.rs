//! 原生观察测试工厂；生产实现和测试均不依赖后端专属条目类型。
#![allow(clippy::unwrap_used, clippy::too_many_arguments, dead_code)]
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, HistoryOrder, IdentityStrength, MetadataObservation, MetadataObservations,
    MetadataProvenance, ObjectHistory, ObjectTag, ObservedEntry, OwnershipMode, RecordKind, SourceIdentity,
    StoragePath, StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding, TimePrecision, TimestampMetadata,
    VersionRecord,
};
use std::path::PathBuf;

pub fn directory_marker(path: &str, tags: Vec<(String, String)>) -> ObservedEntry {
    let directory = observation(
        BackendKind::S3,
        path.into(),
        EntryKind::Directory,
        0,
        [0; 3],
        None,
        None,
        None,
        None,
    );
    let marker = observation(
        BackendKind::S3,
        format!("{path}/").into(),
        EntryKind::File,
        0,
        [123; 3],
        None,
        None,
        Some(tags),
        None,
    );
    directory.with_directory_marker_object(Some(Box::new(marker))).unwrap()
}

fn value<T>(value: T) -> MetadataObservation<T> {
    MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::Inline,
    }
}
pub fn observation(
    backend: BackendKind, path: PathBuf, kind: EntryKind, size: u64, times: [i64; 3], ownership: Option<OwnershipMode>,
    stable: Option<Vec<u8>>, tags: Option<Vec<(String, String)>>, version: Option<(Option<String>, bool, bool)>,
) -> ObservedEntry {
    let path = StoragePath::new(path.to_str().unwrap()).unwrap();
    let backend = BackendIdentity::new(backend, "terrasync-fixture").unwrap();
    let strength = if version.is_some() {
        IdentityStrength::VersionScoped
    } else if stable.is_some() {
        IdentityStrength::StableWithinBackend
    } else {
        IdentityStrength::PathScoped
    };
    let mut identity_bytes = stable.unwrap_or_else(|| path.as_str().as_bytes().to_vec());
    if let Some((id, _, _)) = &version {
        identity_bytes.push(0);
        identity_bytes.extend_from_slice(id.as_deref().unwrap_or("null").as_bytes());
    }
    let identity = SourceIdentity::new(backend, strength, identity_bytes).unwrap();
    let timestamp = |n| StorageTimestamp::new(i128::from(n), TimePrecision::Nanoseconds).unwrap();
    let metadata = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        tags.map_or(MetadataObservation::NotRequested, |tags| {
            value(
                tags.into_iter()
                    .map(|(key, value)| ObjectTag::new(key, value).unwrap())
                    .collect(),
            )
        }),
        ownership.map_or(MetadataObservation::NotRequested, value),
        value(TimestampMetadata {
            created: Some(timestamp(times[0])),
            modified: Some(timestamp(times[1])),
            accessed: Some(timestamp(times[2])),
        }),
    )
    .unwrap();
    let entry = if kind == EntryKind::Symlink {
        ObservedEntry::new_symlink(
            path,
            Some(timestamp(times[1])),
            identity.clone(),
            SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec()).unwrap(),
        )
        .unwrap()
    } else {
        ObservedEntry::new(path, kind, Some(size), Some(timestamp(times[1])), identity.clone()).unwrap()
    }
    .with_metadata(metadata.clone());
    if let Some((id, latest, deleted)) = version {
        let record = VersionRecord::new(
            id,
            latest,
            if deleted {
                RecordKind::DeleteMarker {
                    modified: Some(timestamp(times[1])),
                }
            } else {
                RecordKind::Object {
                    size,
                    etag: "fixture".into(),
                    modified: Some(timestamp(times[1])),
                }
            },
            identity,
            Some(Box::new(metadata)),
        )
        .unwrap();
        entry
            .with_history(Some(Box::new(
                ObjectHistory::whole(vec![record], HistoryOrder::Server).unwrap(),
            )))
            .unwrap()
    } else {
        entry
    }
}

pub fn with_mode(entry: ObservedEntry, mode: Option<u32>) -> ObservedEntry {
    let mut metadata = entry.metadata().clone();
    metadata.set_ownership_mode(mode.map_or(MetadataObservation::NotRequested, |mode| {
        value(OwnershipMode {
            mode,
            uid: metadata.ownership_mode().value().map_or(0, |v| v.uid),
            gid: metadata.ownership_mode().value().map_or(0, |v| v.gid),
        })
    }));
    entry.with_metadata(metadata)
}
pub fn with_times(entry: ObservedEntry, accessed: Option<i64>, modified: Option<i64>) -> ObservedEntry {
    let mut metadata = entry.metadata().clone();
    let mut times = metadata.timestamps().value().copied().unwrap_or(TimestampMetadata {
        accessed: None,
        modified: None,
        created: None,
    });
    let stamp = |n| StorageTimestamp::new(i128::from(n), TimePrecision::Nanoseconds).unwrap();
    if let Some(n) = accessed {
        times.accessed = Some(stamp(n));
    }
    if let Some(n) = modified {
        times.modified = Some(stamp(n));
    }
    metadata.set_timestamps(value(times));
    entry.with_metadata(metadata)
}
pub fn with_size(entry: ObservedEntry, size: u64) -> ObservedEntry {
    ObservedEntry::new(
        entry.path().clone(),
        entry.kind(),
        Some(size),
        entry.modified(),
        entry.source_identity().clone(),
    )
    .unwrap()
    .with_metadata(entry.metadata().clone())
}
