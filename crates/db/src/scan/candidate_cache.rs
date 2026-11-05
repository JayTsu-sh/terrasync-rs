//! 候选事件缓存的元数据摘要；扫描代次由当前基线单独提供。
use blake3::Hasher;
use clickhouse::Client;
use rustc_hash::{FxHashMap, FxHashSet};
use storage_adapter::observed_scan::ObservedScanEvent;
use tokio::task::yield_now;

use crate::{
    error::Result,
    execution::queries::check_scan_cancellation,
    execution::read::QuorumRead,
    scan::rows::NativeScanRecord,
    traits::{IncrementalStorageEntryRecord, StorageEntryRecord},
};

pub(crate) async fn missing_hash_columns(client: &Client, tables: &[String]) -> Result<Vec<String>> {
    client.query("SELECT table FROM system.columns WHERE database=currentDatabase() AND table IN ? GROUP BY table HAVING countIf(name='metadata_hash')=0")
        .bind(tables).fetch_quorum_all().await
}

pub(crate) trait ScanMetadataHash {
    fn scan_metadata_hash(&self) -> String;
}

/// 仅候选日志可能复用的主键需要摘要；普通基线写入置空以安全失效。
#[derive(Default)]
pub(crate) struct CandidateMetadataCache {
    versions: FxHashMap<String, FxHashSet<String>>,
}

impl CandidateMetadataCache {
    pub(crate) async fn new(events: &[ObservedScanEvent]) -> Result<Self> {
        check_scan_cancellation()?;
        let mut cache = Self::default();
        for chunk in events.chunks(1024) {
            yield_now().await;
            check_scan_cancellation()?;
            for event in chunk {
                if let ObservedScanEvent::Candidate(item) = event {
                    let view = item.view();
                    cache
                        .versions
                        .entry(view.sql_path().into_owned())
                        .or_default()
                        .insert(view.version_id().into());
                }
            }
        }
        Ok(cache)
    }

    pub(crate) fn apply(&self, row: &mut NativeScanRecord) {
        let candidate = self
            .versions
            .get(row.relative_path.as_str())
            .is_some_and(|versions| versions.contains(row.version_id.as_str()));
        row.metadata_hash = candidate.then(|| row.scan_metadata_hash());
    }
}

trait HashField {
    fn hash_field(&self, hash: &mut Hasher);
}

impl HashField for str {
    fn hash_field(&self, hash: &mut Hasher) {
        hash.update(&(self.len() as u64).to_le_bytes());
        hash.update(self.as_bytes());
    }
}
impl HashField for String {
    fn hash_field(&self, hash: &mut Hasher) {
        self.as_str().hash_field(hash);
    }
}
impl<T: HashField> HashField for Option<T> {
    fn hash_field(&self, hash: &mut Hasher) {
        hash.update(&[u8::from(self.is_some())]);
        if let Some(value) = self {
            value.hash_field(hash);
        }
    }
}
impl HashField for bool {
    fn hash_field(&self, hash: &mut Hasher) {
        hash.update(&[u8::from(*self)]);
    }
}
macro_rules! integer_fields {
    ($($integer:ty),+) => {$ (
        impl HashField for $integer {
            fn hash_field(&self, hash: &mut Hasher) {
                hash.update(&self.to_le_bytes());
            }
        }
    )+};
}
integer_fields!(u32, u64, i64);

macro_rules! metadata_hash {
    ($($record:ty),+) => {$ (
        impl ScanMetadataHash for $record {
            fn scan_metadata_hash(&self) -> String {
                let mut hash = Hasher::new();
                hash.update(b"terrasync scan event metadata v1\0");
                // 仅排除 current_state；命中后必须使用当前基线的代次。
                self.name.hash_field(&mut hash);
                self.relative_path.hash_field(&mut hash);
                self.size.hash_field(&mut hash);
                self.ext.hash_field(&mut hash);
                self.ctime.hash_field(&mut hash);
                self.mtime.hash_field(&mut hash);
                self.atime.hash_field(&mut hash);
                self.mode.hash_field(&mut hash);
                self.storage_type.hash_field(&mut hash);
                self.is_symlink.hash_field(&mut hash);
                self.is_dir.hash_field(&mut hash);
                self.is_regular_file.hash_field(&mut hash);
                self.hard_links.hash_field(&mut hash);
                self.uid.hash_field(&mut hash);
                self.gid.hash_field(&mut hash);
                self.ino.hash_field(&mut hash);
                self.file_handle.hash_field(&mut hash);
                self.version_id.hash_field(&mut hash);
                self.tags.hash_field(&mut hash);
                self.version_count.hash_field(&mut hash);
                hash.finalize().to_hex().to_string()
            }
        }
    )+};
}
metadata_hash!(NativeScanRecord, StorageEntryRecord, IncrementalStorageEntryRecord);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseError;
    use crate::scan::rows::incremental_record;
    use crate::scan::with_scan_cancellation;
    use data_mover::model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
    };
    use std::sync::Arc;
    use storage_adapter::observed_scan::{ObservedItem, ObservedView};
    use tokio_util::sync::CancellationToken;

    fn observation() -> Result<Arc<ObservedEntry>> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "metadata-hash")?;
        let identity = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"file-id")?;
        Ok(Arc::new(ObservedEntry::new(
            StoragePath::new("a.txt")?,
            EntryKind::File,
            Some(17),
            None,
            identity,
        )?))
    }

    fn native() -> Result<NativeScanRecord> {
        let entry = observation()?;
        NativeScanRecord::capture(
            ObservedView {
                entry: &entry,
                version_index: None,
            },
            1,
        )
    }

    #[test]
    fn ordinary_observations_do_not_compute_or_retain_a_digest() -> Result<()> {
        let mut row = native()?;
        assert!(row.metadata_hash.is_none());
        row.metadata_hash = Some(row.scan_metadata_hash());
        CandidateMetadataCache::default().apply(&mut row);
        assert!(row.metadata_hash.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn only_candidate_paths_and_versions_get_a_digest() -> Result<()> {
        let candidate = ObservedScanEvent::Candidate(ObservedItem {
            entry: observation()?,
            version_index: None,
        });
        let cache = CandidateMetadataCache::new(&[candidate]).await?;
        let mut row = native()?;
        cache.apply(&mut row);
        assert_eq!(row.metadata_hash, Some(row.scan_metadata_hash()));
        row.version_id = "other-version".into();
        cache.apply(&mut row);
        assert!(row.metadata_hash.is_none());
        row.version_id.clear();
        row.relative_path = "other-path".into();
        cache.apply(&mut row);
        assert!(row.metadata_hash.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn public_new_events_do_not_enable_candidate_caching() -> Result<()> {
        let event = ObservedScanEvent::New(ObservedItem {
            entry: observation()?,
            version_index: None,
        });
        let mut row = native()?;
        CandidateMetadataCache::new(&[event]).await?.apply(&mut row);
        assert!(row.metadata_hash.is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn building_candidate_keys_yields_to_cancellation() -> Result<()> {
        let entry = observation()?;
        let events: Vec<_> = (0..16_384)
            .map(|_| {
                ObservedScanEvent::Candidate(ObservedItem {
                    entry: Arc::clone(&entry),
                    version_index: None,
                })
            })
            .collect();
        let cancel = CancellationToken::new();
        let building = with_scan_cancellation(cancel.clone(), None, CandidateMetadataCache::new(&events));
        let cancelling = async {
            yield_now().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(building, cancelling);
        assert!(matches!(result, Err(DatabaseError::ScanCancelled)));
        Ok(())
    }

    #[test]
    fn native_and_event_hashes_agree_across_generation_changes() -> Result<()> {
        let mut native = native()?;
        native.metadata_hash = Some(native.scan_metadata_hash());
        let digest = native.metadata_hash.clone();
        let mut row = native.legacy();
        assert_eq!(digest.as_deref(), Some(row.scan_metadata_hash().as_str()));
        row.current_state = 0;
        let event = incremental_record(row, "candidate_new", None, 999);
        assert_eq!(digest, Some(event.scan_metadata_hash()));
        Ok(())
    }

    #[test]
    fn data_metadata_identity_and_version_changes_invalidate_the_cache() -> Result<()> {
        let row = native()?.legacy();
        let digest = row.scan_metadata_hash();
        let mutations: [fn(&mut StorageEntryRecord); 10] = [
            |row| row.size += 1,
            |row| row.mtime += 1,
            |row| row.ctime += 1,
            |row| row.atime += 1,
            |row| row.mode = Some(0),
            |row| row.uid = Some(999),
            |row| row.tags = Some("tag".into()),
            |row| row.file_handle = Some("replacement".into()),
            |row| row.version_id = "new-version".into(),
            |row| row.version_count = Some(2),
        ];
        for change in mutations {
            let mut changed = row.clone();
            change(&mut changed);
            assert_ne!(digest, changed.scan_metadata_hash());
        }
        Ok(())
    }

    #[test]
    fn field_boundaries_and_nullable_values_have_distinct_encodings() -> Result<()> {
        let mut first = native()?.legacy();
        first.name = "ab".into();
        first.relative_path = "c".into();
        let mut second = first.clone();
        second.name = "a".into();
        second.relative_path = "bc".into();
        assert_ne!(first.scan_metadata_hash(), second.scan_metadata_hash());
        second = first.clone();
        second.tags = Some(String::new());
        assert_ne!(first.scan_metadata_hash(), second.scan_metadata_hash());
        second = first.clone();
        second.mode = Some(0);
        assert_ne!(first.scan_metadata_hash(), second.scan_metadata_hash());
        Ok(())
    }
}
