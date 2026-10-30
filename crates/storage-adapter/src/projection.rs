use crate::{error::operation, *};
use bytes::Bytes;
use data_mover::{
    model::{observation::PrivateBackendEntryFacts, *},
    storage::{InlineOwnership, SourceDescriptor},
};
use std::path::PathBuf;

pub(crate) fn observation_plan(tags: bool) -> ObservationPlan {
    ObservationPlan::default()
        .with_timestamps(ObservationMode::InlineOnly)
        .with_ownership_mode(ObservationMode::InlineOnly)
        .with_tags(if tags {
            ObservationMode::BestEffort
        } else {
            ObservationMode::Omit
        })
}
pub(crate) fn nanos(value: Option<StorageTimestamp>) -> i64 {
    value.map_or(0, |v| {
        v.unix_nanos().clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    })
}
impl ConnectedStorage {
    pub(crate) async fn project_descriptor(&self, descriptor: SourceDescriptor, tags: bool) -> Result<EntryEnum> {
        let role = self.roles.metadata()?;
        let plan = observation_plan(tags);
        let metadata = match role.observe_listed(&descriptor, plan) {
            Some(result) => result?,
            None => role.observe(&descriptor.path, plan).await?,
        };
        let modified = metadata
            .timestamps()
            .value()
            .and_then(|v| v.modified)
            .or(descriptor.inline_timestamps.and_then(|v| v.modified));
        let entry = ObservedEntry::new(
            descriptor.path.clone(),
            descriptor.kind,
            descriptor.size,
            modified,
            descriptor.source_identity.clone(),
        )?
        .with_metadata(metadata);
        let mut projected = project(&entry);
        match &mut projected {
            EntryEnum::NAS(nas) => {
                if self.roles.kind() != BackendKind::Cifs
                    && let Some(mode) = descriptor.inline_mode
                {
                    nas.mode = Some(mode);
                }
                if let Some(InlineOwnership::Known(OwnershipMode { uid, gid, .. })) = descriptor.inline_ownership {
                    nas.uid = Some(uid);
                    nas.gid = Some(gid);
                }
            }
            _ => {}
        }
        self.project_named_permissions(&descriptor, &mut projected).await?;
        Ok(projected)
    }

    async fn project_named_permissions(&self, descriptor: &SourceDescriptor, entry: &mut EntryEnum) -> Result<()> {
        let EntryEnum::HDFS(entry) = entry else { return Ok(()) };
        let permissions = self.roles.metadata()?.permissions(descriptor).await?;
        entry.mode = permissions.mode;
        if let Some(data_mover::storage::PrincipalIdentity::Named(owner)) = permissions.owner {
            entry.owner = owner;
        }
        if let Some(data_mover::storage::PrincipalIdentity::Named(group)) = permissions.group {
            entry.group = group;
        }
        Ok(())
    }

    pub(crate) async fn project_history(
        &self, observed: &ObservedEntry, histories: &mut HistoryProjection,
    ) -> Result<Vec<EntryEnum>> {
        let mut entries = histories.push(observed)?;
        if let PrivateBackendEntryFacts::Hdfs(facts) = observed.backend_facts() {
            // Forward opaque backend facts to their role; HDFS answers permissions from the
            // listing facts without a second stat or a numeric principal mapping.
            let descriptor = SourceDescriptor::new(
                observed.path().clone(),
                observed.kind(),
                observed.size(),
                observed.source_identity().clone(),
            )
            .with_backend_fact(Bytes::copy_from_slice(facts));
            for entry in &mut entries {
                self.project_named_permissions(&descriptor, entry).await?;
            }
        }
        Ok(entries)
    }
    pub(crate) async fn observe(&self, value: &std::path::Path) -> Result<ObservedEntry> {
        let p = crate::connected::path(value)?;
        let descriptor = self.roles.read_source()?.describe(&p).await?;
        let metadata = self
            .roles
            .metadata()?
            .observe_bound(&p, &descriptor.source_identity, observation_plan(true))
            .await?;
        let mut entry = ObservedEntry::new(
            p,
            descriptor.kind,
            descriptor.size,
            descriptor.inline_timestamps.and_then(|v| v.modified),
            descriptor.source_identity,
        )?;
        if descriptor.kind == EntryKind::Symlink {
            entry = ObservedEntry::new_symlink(
                entry.path().clone(),
                entry.modified(),
                entry.source_identity().clone(),
                self.roles.namespace()?.read_link(entry.path()).await?,
            )?;
        }
        Ok(entry.with_metadata(metadata))
    }
}
pub(crate) fn project(entry: &ObservedEntry) -> EntryEnum {
    let p = PathBuf::from(entry.path().as_str());
    let name = p
        .file_name()
        .map(|v| v.to_string_lossy().into_owned())
        .unwrap_or_default();
    let extension = p.extension().map(|v| v.to_string_lossy().into_owned());
    let timestamps = entry.metadata().timestamps().value();
    let mtime = nanos(timestamps.and_then(|v| v.modified).or(entry.modified()));
    let atime = nanos(timestamps.and_then(|v| v.accessed));
    let ownership = entry.metadata().ownership_mode().value();
    let mode = ownership.map(|v| v.mode);
    if entry.backend_kind() == data_mover::model::BackendKind::S3 {
        EntryEnum::S3(S3Entry {
            name,
            relative_path: entry.path().as_str().into(),
            extension,
            size: entry.size().unwrap_or(0),
            mtime,
            tags: entry.metadata().tags().value().map(|tags| {
                tags.iter()
                    .map(|v| Tag {
                        key: v.key().into(),
                        value: v.value().into(),
                    })
                    .collect()
            }),
            version_id: entry.source_version().and_then(|v| v.version_id().map(str::to_owned)),
            is_latest: true,
            is_delete_marker: false,
            version_count: None,
            is_dir: entry.kind() == EntryKind::Directory,
        })
    } else if entry.backend_kind() == data_mover::model::BackendKind::Hdfs {
        EntryEnum::HDFS(HDFSEntry {
            name,
            relative_path: p,
            extension,
            size: entry.size().unwrap_or(0),
            mtime,
            atime,
            mode,
            is_dir: entry.kind() == EntryKind::Directory,
            owner: String::new(),
            group: String::new(),
            replication: None,
            block_size: None,
        })
    } else {
        EntryEnum::NAS(NASEntry {
            name,
            relative_path: p,
            extension,
            size: entry.size().unwrap_or(0),
            mtime,
            atime,
            ctime: nanos(timestamps.and_then(|v| v.created)),
            mode,
            is_dir: entry.kind() == EntryKind::Directory,
            is_symlink: entry.kind() == EntryKind::Symlink,
            hard_links: None,
            uid: ownership.map(|v| v.uid),
            gid: ownership.map(|v| v.gid),
            ino: None,
            file_handle: if entry.backend_kind() == data_mover::model::BackendKind::Local {
                None
            } else {
                entry.source_identity().stable_bytes().map(|_| {
                    let mut key = b"TSI4".to_vec();
                    key.extend_from_slice(entry.identity_key().as_bytes());
                    Bytes::from(key)
                })
            },
            acl: entry.metadata().acl().value().cloned(),
            owner: None,
            owner_group: None,
            xattrs: entry.metadata().xattrs().value().map(|v| {
                v.iter()
                    .map(|a| (String::from_utf8_lossy(a.name()).into_owned(), a.value().to_vec()))
                    .collect()
            }),
        })
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
struct WireAcl {
    encoding: u8,
    access: Option<Vec<u8>>,
    default: Option<Vec<u8>>,
}
pub(crate) fn encode_acl(value: &AclMetadata) -> Result<Vec<u8>> {
    let wire = WireAcl {
        encoding: match value.encoding() {
            AclEncoding::Posix => 0,
            AclEncoding::NfsV4 => 1,
            AclEncoding::WindowsSecurityDescriptor => 2,
        },
        access: value.access().map(<[u8]>::to_vec),
        default: value.default_acl().map(<[u8]>::to_vec),
    };
    bincode_encode(&wire)
}
fn bincode_encode(value: &WireAcl) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(operation)
}
pub(crate) fn decode_acl(bytes: &[u8]) -> Result<AclMetadata> {
    let wire: WireAcl = serde_json::from_slice(bytes).map_err(operation)?;
    match wire.encoding {
        0 => Ok(AclMetadata::new_posix(wire.access, wire.default)?),
        1 => Ok(AclMetadata::new(AclEncoding::NfsV4, wire.access.unwrap_or_default())?),
        2 => Ok(AclMetadata::new(
            AclEncoding::WindowsSecurityDescriptor,
            wire.access.unwrap_or_default(),
        )?),
        _ => Err(operation("unknown ACL encoding")),
    }
}

/// 将上游按 key 交付的对象历史展开为 TerraSync 既有的逐版本消息。
pub(crate) fn project_all(entry: &ObservedEntry) -> Vec<EntryEnum> {
    let base = project(entry);
    let (EntryEnum::S3(base), Some(history)) = (&base, entry.history()) else {
        return vec![base];
    };
    let source_path = entry.history_source_path().ok().flatten();
    history
        .records()
        .iter()
        .map(|record| {
            let mut projected = base.clone();
            if let Some(path) = &source_path {
                projected.relative_path = path.as_str().into();
            }
            projected.version_id = Some(record.version_id().unwrap_or("null").to_owned());
            projected.is_latest = record.is_latest();
            projected.is_delete_marker = record.is_delete_marker();
            projected.version_count = Some(history.records().len() as u32);
            projected.mtime = nanos(record.modified());
            projected.size = record.size().unwrap_or(0);
            projected.tags = record.metadata().and_then(|m| m.tags().value()).map(|tags| {
                tags.iter()
                    .map(|tag| Tag {
                        key: tag.key().into(),
                        value: tag.value().into(),
                    })
                    .collect()
            });
            EntryEnum::S3(projected)
        })
        .collect()
}

/// 旧产品逐版本协议需要完整计数；只在最后一段到达后交出该 key。
#[derive(Default)]
pub(crate) struct HistoryProjection {
    path: Option<String>,
    next_segment: u32,
    entries: Vec<EntryEnum>,
}
impl HistoryProjection {
    pub fn push(&mut self, entry: &ObservedEntry) -> Result<Vec<EntryEnum>> {
        let Some(history) = entry.history() else {
            if self.path.is_some() {
                return Err(operation("interrupted object history"));
            }
            return Ok(project_all(entry));
        };
        if self.path.is_none() {
            self.path = Some(entry.path().as_str().into());
            self.next_segment = 0;
        }
        if self.path.as_deref() != Some(entry.path().as_str()) || history.segment() != self.next_segment {
            return Err(operation("out-of-order object history"));
        }
        self.entries.extend(project_all(entry));
        self.next_segment = self.next_segment.saturating_add(1);
        if !history.is_complete() {
            return Ok(Vec::new());
        }
        let count = u32::try_from(self.entries.len()).map_err(operation)?;
        for entry in &mut self.entries {
            entry.set_version_count(count);
        }
        self.path = None;
        Ok(std::mem::take(&mut self.entries))
    }
    pub fn is_complete(&self) -> bool {
        self.path.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
    #[test]
    fn unobserved_posix_mode_remains_unknown() -> TestResult {
        let backend = BackendIdentity::new(BackendKind::Cifs, "unknown-permissions")?;
        let identity = SourceIdentity::new(backend, IdentityStrength::PathScoped, b"dir")?;
        let entry = ObservedEntry::new(StoragePath::new("dir")?, EntryKind::Directory, None, None, identity)?;
        assert_eq!(project(&entry).get_mode(), None);
        Ok(())
    }

    #[test]
    fn history_segments_preserve_null_versions_markers_and_total_count() -> TestResult {
        let backend = BackendIdentity::new(BackendKind::S3, "history-fixture")?;
        let identity = SourceIdentity::new(backend, IdentityStrength::VersionScoped, b"key")?;
        let records = vec![
            VersionRecord::new(
                None,
                false,
                RecordKind::Object {
                    size: 3,
                    etag: "etag".into(),
                    modified: None,
                },
                identity.clone(),
                None,
            )?,
            VersionRecord::new(
                Some("marker".into()),
                true,
                RecordKind::DeleteMarker { modified: None },
                identity.clone(),
                None,
            )?,
        ];
        let entry = ObservedEntry::new(StoragePath::new("key")?, EntryKind::File, None, None, identity)?
            .with_history(Some(Box::new(ObjectHistory::whole(records, HistoryOrder::Server)?)))?;
        let mut projection = HistoryProjection::default();
        let mut entries = Vec::new();
        for segment in entry.into_segments(1) {
            entries.extend(projection.push(&segment)?);
        }
        assert!(projection.is_complete());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].get_version_id(), Some("null"));
        assert!(entries[1].get_is_delete_marker());
        assert!(entries.iter().all(|entry| entry.get_version_count() == Some(2)));
        Ok(())
    }
}
