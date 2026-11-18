//! 将存储层的有类型错误映射为版本诊断上下文，不在这里重试或决定删源。

use data_mover::metadata::RefusalCause;
use data_mover::model::BackendKind;
use data_mover::transfer::{TransferPhase, TransferSide};

use task_model::observation::ObservedView;

use crate::error::StorageError;
use crate::error::storage::Result;

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

/// 历史数据缺少确认的目标版本时不能按普通属性错误继续发布后续版本。
pub(super) fn confirmed_target_version(
    result: Result<Option<String>>, view: ObservedView<'_>, keep_history: bool, metadata_only: bool,
) -> Result<Option<String>> {
    let result = result.map(|version| version.filter(|value| !value.is_empty()));
    if !keep_history || view.version().is_none() {
        return result;
    }
    match result {
        Ok(Some(version)) => Ok(Some(version)),
        Ok(None) => Err(unconfirmed(view, metadata_only)?),
        Err(error)
            if !metadata_only
                && error
                    .published_data()
                    .is_some_and(|data| data.destination_version.as_deref().is_none_or(str::is_empty)) =>
        {
            Err(StorageError::TransferAndMetadata {
                data: Box::new(unconfirmed(view, false)?),
                metadata: Box::new(error),
            })
        }
        Err(error) => Err(error),
    }
}

fn unconfirmed(view: ObservedView<'_>, metadata_only: bool) -> Result<StorageError> {
    let path = view.source_path()?;
    let source_version = view.version_id().to_owned();
    Ok(if metadata_only {
        StorageError::TargetVersionUnconfirmed { path, source_version }
    } else {
        StorageError::HistoricalPublicationUnconfirmed { path, source_version }
    })
}
