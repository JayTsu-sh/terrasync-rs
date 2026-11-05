//! 元数据观察与 opaque 编码；运行时直接交付原生观察，不投影为后端条目。
use crate::{ConnectedStorage, Result, StorageError, error::operation};
use data_mover::model::*;
use data_mover::storage::SourceDescriptor;

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

impl ConnectedStorage {
    pub(crate) async fn project_descriptor(&self, descriptor: SourceDescriptor, tags: bool) -> Result<ObservedEntry> {
        let metadata_role = self.roles.metadata()?;
        let plan = observation_plan(tags);
        let metadata = match metadata_role.observe_listed(&descriptor, plan) {
            Some(result) => result?,
            None => metadata_role.observe(&descriptor.path, plan).await?,
        };
        let modified = metadata
            .timestamps()
            .value()
            .and_then(|times| times.modified)
            .or(descriptor.inline_timestamps.and_then(|times| times.modified));
        let mut entry = if descriptor.kind == EntryKind::Symlink {
            let target = self.roles.namespace()?.read_link(&descriptor.path).await?;
            ObservedEntry::new_symlink(descriptor.path, modified, descriptor.source_identity, target)?
        } else {
            ObservedEntry::new(
                descriptor.path,
                descriptor.kind,
                descriptor.size,
                modified,
                descriptor.source_identity,
            )?
        }
        .with_permissions(descriptor.permissions)?
        .with_metadata(metadata)
        .with_rename_evidence(descriptor.rename_evidence);
        if let Some(facts) = descriptor.backend_fact {
            entry = entry.with_backend_fact_bytes(facts.to_vec())?;
        }
        Ok(entry)
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
    serde_json::to_vec(value).map_err(StorageError::from)
}
pub(crate) fn decode_acl(bytes: &[u8]) -> Result<AclMetadata> {
    let wire: WireAcl = serde_json::from_slice(bytes).map_err(StorageError::from)?;
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

/// 验证原生历史分段顺序，不积累整段历史，也不复制逐版本条目。
#[derive(Default)]
pub(crate) struct HistorySequence {
    path: Option<StoragePath>,
    next_segment: u32,
}
impl HistorySequence {
    pub fn push(&mut self, entry: ObservedEntry) -> Result<ObservedEntry> {
        if let Some(history) = entry.history() {
            if self.path.is_none() {
                self.path = Some(entry.path().clone());
                self.next_segment = 0;
            }
            if self.path.as_ref() != Some(entry.path()) || history.segment() != self.next_segment {
                return Err(operation("out-of-order object history"));
            }
            self.next_segment = self
                .next_segment
                .checked_add(1)
                .ok_or_else(|| operation("history segment overflow"))?;
            if history.is_complete() {
                self.path = None;
            }
        } else if self.path.is_some() {
            return Err(operation("interrupted object history"));
        }
        Ok(entry)
    }
    pub fn is_complete(&self) -> bool {
        self.path.is_none()
    }
}
