use data_mover::model::{EntryKind, ObservedEntry, StorageTimestamp};
use serde::{Deserialize, Serialize};

use crate::error::{DatabaseError, Result};

/// 同一张扫描大表中的中立查询列与 data-mover 所有的 opaque snapshot。
#[derive(Clone, Debug, Deserialize, Serialize, clickhouse::Row)]
pub struct ObservedEntryRecord {
    pub identity_key: String,
    pub relative_path: String,
    pub backend_kind: String,
    pub entry_kind: String,
    pub size: u64,
    pub size_observed: bool,
    pub modified_unix_nanos: Option<i128>,
    pub current_state: u8,
    pub entry_snapshot: Vec<u8>,
}

impl ObservedEntryRecord {
    #[must_use]
    pub fn capture(entry: &ObservedEntry, current_state: u8) -> Self {
        Self {
            identity_key: hex::encode(entry.identity_key().as_bytes()),
            relative_path: entry.path().as_str().to_string(),
            backend_kind: entry.backend_kind().as_str().to_string(),
            entry_kind: entry_kind_name(entry.kind()).to_string(),
            size: entry.size().unwrap_or_default(),
            size_observed: entry.size().is_some(),
            modified_unix_nanos: entry.modified().map(StorageTimestamp::unix_nanos),
            current_state,
            entry_snapshot: entry.encode_snapshot().as_bytes().to_vec(),
        }
    }

    /// 只用已持久化 snapshot 重建，并核对同一行的查询投影。
    ///
    /// # Errors
    /// snapshot 损坏、版本未知或查询列与 snapshot 不一致时返回 typed error。
    pub fn reconstruct(&self) -> Result<ObservedEntry> {
        let entry = ObservedEntry::decode_snapshot(&self.entry_snapshot)?;
        self.validate_projection(&entry)?;
        Ok(entry)
    }

    fn validate_projection(&self, entry: &ObservedEntry) -> Result<()> {
        let checks = [
            (
                self.identity_key == hex::encode(entry.identity_key().as_bytes()),
                "identity_key",
            ),
            (self.relative_path == entry.path().as_str(), "relative_path"),
            (self.backend_kind == entry.backend_kind().as_str(), "backend_kind"),
            (self.entry_kind == entry_kind_name(entry.kind()), "entry_kind"),
            (
                self.size_observed == entry.size().is_some()
                    && (!self.size_observed || Some(self.size) == entry.size()),
                "size",
            ),
            (
                self.modified_unix_nanos == entry.modified().map(StorageTimestamp::unix_nanos),
                "modified_unix_nanos",
            ),
        ];
        checks
            .into_iter()
            .find_map(|(matches, field)| (!matches).then_some(field))
            .map_or(Ok(()), |field| {
                Err(DatabaseError::ObservationProjectionMismatch { field })
            })
    }
}

fn entry_kind_name(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Directory => "directory",
        EntryKind::Symlink => "symlink",
        EntryKind::Special(_) => "special",
    }
}

/// 新旧观察行按持久中立 identity key 关联时使用的固定同表 JOIN 条件。
pub const OBSERVATION_IDENTITY_JOIN_CLAUSE: &str = "t.identity_key = b.identity_key";

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
    };

    use super::{OBSERVATION_IDENTITY_JOIN_CLAUSE, ObservedEntryRecord};
    use crate::clickhouse::FILE_SCAN_COLUMNS_DEFINITION;

    fn observed(path: &str, stable_id: &[u8]) -> ObservedEntry {
        let backend = BackendIdentity::new(BackendKind::Local, "fixture").unwrap();
        let source = SourceIdentity::new(backend, IdentityStrength::PathScoped, stable_id).unwrap();
        ObservedEntry::new(StoragePath::new(path).unwrap(), EntryKind::File, Some(17), None, source).unwrap()
    }

    #[test]
    fn opaque_snapshot_round_trips_with_consistent_query_projection() {
        let entry = observed("nested/file.bin", b"nested/file.bin");
        let record = ObservedEntryRecord::capture(&entry, 7);

        let rebuilt = record.reconstruct().unwrap();

        assert_eq!(rebuilt, entry);
        assert_eq!(record.relative_path, "nested/file.bin");
        assert_eq!(record.size, 17);
        assert!(record.size_observed);
        assert_eq!(record.identity_key.len(), 64);
    }

    #[test]
    fn malformed_snapshot_is_a_typed_codec_failure() {
        let mut record = ObservedEntryRecord::capture(&observed("file", b"file"), 1);
        record.entry_snapshot[0] ^= 0xff;

        let error = record.reconstruct().unwrap_err();

        assert!(matches!(error, crate::DatabaseError::SnapshotDecode(_)));
    }

    #[test]
    fn snapshot_that_disagrees_with_query_columns_is_rejected() {
        let mut record = ObservedEntryRecord::capture(&observed("file", b"file"), 1);
        record.relative_path = "other".to_string();

        let error = record.reconstruct().unwrap_err();

        assert!(matches!(
            error,
            crate::DatabaseError::ObservationProjectionMismatch { .. }
        ));
    }

    #[test]
    fn incremental_join_uses_only_persisted_identity_key() {
        assert_eq!(OBSERVATION_IDENTITY_JOIN_CLAUSE, "t.identity_key = b.identity_key");
    }

    #[test]
    fn observation_columns_live_in_the_existing_scan_table_definition() {
        for column in [
            "identity_key String",
            "backend_kind String",
            "entry_kind String",
            "size_observed Bool",
            "modified_unix_nanos Nullable(Int128)",
            "entry_snapshot Array(UInt8)",
            "INDEX identity_key_idx identity_key TYPE bloom_filter",
        ] {
            assert!(FILE_SCAN_COLUMNS_DEFINITION.contains(column));
        }
    }
}
