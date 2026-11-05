use data_mover::error::StorageError as NativeStorageError;
use task_model::TaskModelError;
use tokio::task::JoinError;

use crate::{MismatchDataField, MismatchMetaField, RemoteScanFailure, TransferMode};
use data_mover::{
    metadata::{MetadataApplicationFailure, MetadataPlanError},
    model::{BackendKind, EntryOperationFailure, FailureClass, ModelValueError, StoragePath},
    probes::ProbeFailure,
    storage::{
        BackendConnectError, CapabilityUnavailable, CreateDirectoryAllFailure, DeleteTreeTerminalFailure,
        SourceQosValueError, StorageRoleFailure,
    },
    tar_pack::TarFailure,
    transfer::{DirectPatchError, MoveFailure, TransferFailure, TransferValueError},
    traversal::{
        FailureScope, ObservationFailure, ScanSetupError, TraversalConfigurationError, TraversalTerminalFailure,
    },
};
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("incomplete object history")]
    IncompleteHistory,
    #[error(transparent)]
    TaskModel(#[from] TaskModelError),
    #[error(transparent)]
    ScanSetup(#[from] ScanSetupError),
    #[error(transparent)]
    RemoteScan(#[from] Box<RemoteScanFailure>),
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
        source: Box<EntryOperationFailure>,
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
            || matches!(self, Self::DirectPatch(error) if error.is_cancelled())
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

impl From<RemoteScanFailure> for StorageError {
    fn from(failure: RemoteScanFailure) -> Self {
        Self::RemoteScan(Box::new(failure))
    }
}
