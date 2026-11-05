//! 元数据观察与 opaque 编码；运行时直接交付原生观察，不投影为后端条目。
use crate::{Result, StorageContext, StorageError, error::operation};
use data_mover::model::*;
use data_mover::storage::SourceDescriptor;
use data_mover::storage::StorageRoleFailure;

/// 必须复制的事实失败后不能继续发布观察或把缺失事实解释成 Omit。
pub(crate) fn validate_required_tags(entry: &ObservedEntry) -> Result<()> {
    fn check(path: &StoragePath, facts: &MetadataObservations) -> Result<()> {
        let (class, transience) = match facts.tags() {
            MetadataObservation::Value { .. } | MetadataObservation::NotApplicable => return Ok(()),
            MetadataObservation::Unsupported => (FailureClass::Unsupported, Transience::Permanent),
            MetadataObservation::Failed { class, transience } => (*class, *transience),
            MetadataObservation::NotRequested => (FailureClass::InvalidInput, Transience::Permanent),
        };
        Err(StorageRoleFailure::Entry(EntryOperationFailure::new(
            path.clone(),
            Operation::Metadata,
            class,
            transience,
            "required copy tags were not observed",
        )?)
        .into())
    }
    if let Some(history) = entry.history() {
        for record in history.records().iter().filter(|record| !record.is_delete_marker()) {
            if let Some(metadata) = record.metadata() {
                check(entry.path(), metadata)?;
            } else {
                check(entry.path(), &MetadataObservations::default())?;
            }
        }
    } else if let Some(marker) = entry.directory_marker_object() {
        check(marker.path(), marker.metadata())?;
    } else if entry.kind() != EntryKind::Directory {
        check(entry.path(), entry.metadata())?;
    }
    Ok(())
}

pub(crate) fn observation_plan(tags: bool) -> ObservationPlan {
    ObservationPlan::default()
        .with_timestamps(ObservationMode::InlineOnly)
        .with_ownership_mode(ObservationMode::InlineOnly)
        .with_tags(if tags {
            ObservationMode::Required
        } else {
            ObservationMode::Omit
        })
}

impl StorageContext {
    pub(crate) fn tags_required(&self, include_tags: bool) -> Result<bool> {
        Ok(include_tags
            && !matches!(
                self.roles
                    .metadata()?
                    .observation_capabilities()
                    .support(MetadataFamily::Tags),
                ObservationSupport::Unsupported | ObservationSupport::NotApplicable
            ))
    }
    pub(crate) async fn project_descriptor(&self, descriptor: SourceDescriptor, tags: bool) -> Result<ObservedEntry> {
        let metadata_role = self.roles.metadata()?;
        let plan = observation_plan(self.tags_required(tags)?);
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
    pub fn check(&mut self, entry: &ObservedEntry, tags: bool) -> Result<()> {
        if tags {
            validate_required_tags(entry)?;
        }
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
        Ok(())
    }
    pub fn is_complete(&self) -> bool {
        self.path.is_none()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod required_tags_tests {
    use super::*;
    use crate::observed_fixtures::{directory_marker, observation};

    #[test]
    fn failed_required_tags_block_the_object_and_keep_the_typed_cause() {
        let entry = directory_marker("folder", Vec::new());
        assert!(validate_required_tags(&entry).is_ok());
        let marker = entry.directory_marker_object().unwrap();
        let facts = MetadataObservations::new(
            MetadataObservation::NotApplicable,
            MetadataObservation::NotApplicable,
            MetadataObservation::Failed {
                class: FailureClass::Timeout,
                transience: Transience::Transient,
            },
            MetadataObservation::NotApplicable,
            MetadataObservation::NotRequested,
        )
        .unwrap();
        let marker = marker.clone().with_metadata(facts);
        let entry = entry.with_directory_marker_object(Some(Box::new(marker))).unwrap();
        match validate_required_tags(&entry) {
            Err(StorageError::Role(StorageRoleFailure::Entry(error))) => {
                assert_eq!(error.path().as_str(), "folder/");
                assert_eq!(error.class(), FailureClass::Timeout);
                assert_eq!(error.transience(), Transience::Transient);
            }
            result => panic!("expected the typed tag failure, got {result:?}"),
        }
    }

    #[test]
    fn absent_required_tags_cannot_admit_a_history_version() {
        let entry = observation(
            BackendKind::S3,
            "file".into(),
            EntryKind::File,
            1,
            [123; 3],
            None,
            None,
            None,
            Some((Some("v1".into()), true, false)),
        );
        assert!(validate_required_tags(&entry).is_err());
        let structural = directory_marker("folder", Vec::new())
            .with_directory_marker_object(None)
            .unwrap();
        assert!(validate_required_tags(&structural).is_ok());
        assert_eq!(
            observation_plan(true).mode(MetadataFamily::Tags),
            ObservationMode::Required
        );
    }
}
