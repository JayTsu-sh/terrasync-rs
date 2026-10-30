use data_mover::error::StorageError as NativeStorageError;
use std::error::Error as StdError;
use tokio::task::JoinError;

use crate::{MismatchDataField, MismatchMetaField, TransferMode};
use data_mover::{
    metadata::{ApplicationFailureKind, MetadataApplicationFailure, MetadataApplicationReport, MetadataPlanError},
    model::{BackendKind, EntryOperationFailure, FailureClass, MetadataFamily, ModelValueError, StoragePath},
    probes::ProbeFailure,
    storage::{
        BackendConnectError, CapabilityUnavailable, CreateDirectoryAllFailure, DeleteTreeTerminalFailure,
        SourceQosStats, SourceQosValueError, StorageRoleFailure,
    },
    tar_pack::TarFailure,
    transfer::{DirectPatchError, MoveFailure, TransferFailure, TransferPhase, TransferSide, TransferValueError},
    traversal::{FailureScope, ObservationFailure, TraversalConfigurationError, TraversalTerminalFailure},
};
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{backend:?} backend does not support {mode:?} transfer mode: {reason}")]
    UnsupportedTransferMode {
        backend: BackendKind,
        mode: TransferMode,
        reason: &'static str,
    },
    #[error(transparent)]
    DirectPatch(#[from] DirectPatchError),
    #[error(transparent)]
    Upstream(#[from] NativeStorageError),
    #[error("Storage operation error: {0}")]
    OperationError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),
    #[error("transfer cancelled by caller")]
    Cancelled,
    #[error("Data mismatch: {0:?}")]
    MismatchData(Vec<MismatchDataField>),
    #[error("Metadata mismatch: {0:?}")]
    MismatchMeta(Vec<MismatchMetaField>),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TaskJoinError(#[from] JoinError),
    #[error(transparent)]
    Role(#[from] StorageRoleFailure),
    #[error(transparent)]
    MetadataPlan(#[from] MetadataPlanError),
    #[error(transparent)]
    SourceQos(#[from] SourceQosValueError),
    #[error(transparent)]
    ModelValue(#[from] ModelValueError),
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error(transparent)]
    BackendConnect(#[from] BackendConnectError),
    #[error(transparent)]
    TransferValue(#[from] TransferValueError),
    #[error(transparent)]
    Traversal(#[from] TraversalTerminalFailure),
    #[error(transparent)]
    TraversalConfiguration(#[from] TraversalConfigurationError),
    #[error("Scan failed at {path} ({scope:?}, delivered={delivered:?}): {source}")]
    TraversalEntry {
        path: StoragePath,
        scope: FailureScope,
        delivered: Option<u64>,
        #[source]
        source: EntryOperationFailure,
    },
    #[error("Scan observation failed: {0:?}")]
    TraversalObservation(Box<ObservationFailure>),
    #[error(transparent)]
    DeleteTraversal(#[from] DeleteTreeTerminalFailure),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Probe(#[from] ProbeFailure),
    #[error(transparent)]
    CreateDirectory(#[from] CreateDirectoryAllFailure),
    #[error(transparent)]
    MetadataApplication(#[from] MetadataApplicationFailure),
    #[error(transparent)]
    IntegerConversion(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    Transfer(Box<TransferFailure>),
    #[error(transparent)]
    StreamTransfer(Box<StreamTransferDiagnostic>),
    #[error(transparent)]
    Move(Box<MoveFailure>),
    #[error(transparent)]
    Archive(Box<TarFailure>),
}
impl StorageError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::FileNotFound(_) | Self::DirectoryNotFound(_))
            || matches!(self, Self::Role(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
            || matches!(self, Self::Role(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    }
}
impl From<TransferFailure> for StorageError {
    fn from(value: TransferFailure) -> Self {
        Self::Transfer(Box::new(value))
    }
}
pub type Result<T> = std::result::Result<T, StorageError>;
pub(crate) fn operation(error: impl Into<String>) -> StorageError {
    StorageError::OperationError(error.into())
}

impl From<MoveFailure> for StorageError {
    fn from(value: MoveFailure) -> Self {
        Self::Move(Box::new(value))
    }
}
impl From<TarFailure> for StorageError {
    fn from(value: TarFailure) -> Self {
        Self::Archive(Box::new(value))
    }
}

/// stage 清理会消耗上游失败；先复制公开的结构化证据，清理失败也保留在同一诊断中。
#[derive(Debug, thiserror::Error)]
#[error(
    "stream transfer failed in {phase:?}/{side:?}: role={role:?}, metadata={metadata_failures:?}, refusal={refusal:?}, cleanup={cleanup_failure:?}"
)]
pub struct StreamTransferDiagnostic {
    pub phase: TransferPhase,
    pub side: TransferSide,
    pub final_destination_changed: bool,
    pub source_qos: SourceQosStats,
    #[source]
    pub role: Option<StorageRoleFailure>,
    pub metadata_report: Option<MetadataApplicationReport>,
    pub metadata_failures: Vec<(MetadataFamily, ApplicationFailureKind, Option<StorageRoleFailure>)>,
    pub refusal: Option<MetadataPlanError>,
    pub cleanup_failure: Option<StorageRoleFailure>,
}

impl StreamTransferDiagnostic {
    pub(crate) fn capture(failure: &TransferFailure) -> Self {
        let metadata = failure.metadata_failure();
        let role = StdError::source(failure)
            .and_then(|source| source.downcast_ref::<StorageRoleFailure>())
            .or_else(|| metadata.and_then(|failure| failure.storage_error()))
            .cloned();
        Self {
            phase: failure.phase(),
            side: failure.side(),
            final_destination_changed: failure.final_destination_changed(),
            source_qos: failure.source_qos(),
            role,
            metadata_report: metadata.map(|failure| failure.report().clone()),
            metadata_failures: metadata.map_or_else(Vec::new, |failure| {
                failure
                    .failures()
                    .iter()
                    .map(|family| (family.family(), family.kind(), family.storage_error().cloned()))
                    .collect()
            }),
            refusal: failure.metadata_refusal(),
            cleanup_failure: None,
        }
    }
}
