//! 将存储层的有类型错误映射为版本诊断上下文，不在这里重试或决定删源。

use data_mover::metadata::RefusalCause;
use data_mover::model::BackendKind;
use data_mover::transfer::{TransferPhase, TransferSide};

use crate::error::StorageError;

pub(crate) struct VersionFailureContext {
    pub phase: String,
    pub backend: BackendKind,
    pub metadata_only: bool,
}

impl VersionFailureContext {
    pub fn for_error(error: &StorageError, source: BackendKind, destination: BackendKind) -> Self {
        let (phase, backend, metadata_only) = match error {
            StorageError::PublishedDataMetadata { source: error, .. } => {
                return Self::for_error(error, source, destination);
            }
            StorageError::HistoricalPublicationUnconfirmed { .. } => {
                ("historical_publication_unconfirmed".into(), destination, false)
            }
            StorageError::TargetVersionUnconfirmed { .. } => ("target_version_binding".into(), destination, true),
            StorageError::TransferAndMetadata { data, .. } => return Self::for_error(data, source, destination),
            StorageError::SourceMetadataAcquisitionFailed { .. } => ("source_metadata".into(), source, true),
            StorageError::MetadataPlan(error) => {
                let backend = match error.cause() {
                    RefusalCause::SourceCannotObserve
                    | RefusalCause::SourceDidNotObserve
                    | RefusalCause::SourceObservationFailed { .. } => source,
                    _ => destination,
                };
                ("metadata_plan".into(), backend, true)
            }
            StorageError::MetadataApplication(_) => ("target_metadata_apply".into(), destination, true),
            StorageError::MetadataVerification(_) => ("target_metadata_verify".into(), destination, true),
            StorageError::Transfer(error)
                if error.phase() == TransferPhase::Metadata && error.published_data().is_some() =>
            {
                ("target_metadata_apply".into(), destination, true)
            }
            StorageError::Transfer(error) => (
                format!("{:?}", error.phase()),
                if error.side() == TransferSide::Source {
                    source
                } else {
                    destination
                },
                false,
            ),
            _ => ("data_copy".into(), destination, false),
        };
        Self {
            phase,
            backend,
            metadata_only,
        }
    }
}
