use data_mover::error::StorageError as NativeStorageError;
use data_mover::metadata::MetadataVerificationError;
use data_mover::model::MetadataObservationFailure;
use data_mover::qos::SourceQosSetupError;
use data_mover::storage::EntryObservationFailure;
use task_model::TaskModelError;
use tokio::task::JoinError;

use crate::integrity_check::{MismatchDataField, MismatchMetaField};
use data_mover::traversal::RemoteScanFailure;
use data_mover::{
    metadata::{MetadataApplicationFailure, MetadataPlanError},
    model::{BackendKind, EntryOperationFailure, FailureClass, ModelValueError, StoragePath},
    probes::ProbeFailure,
    storage::{
        BackendConnectError, CapabilityUnavailable, CreateDirectoryAllFailure, DeleteTreeTerminalFailure,
        StorageRoleFailure,
    },
    tar_pack::TarFailure,
    transfer::{DirectPatchError, TransferFailure, TransferValueError},
    traversal::{
        FailureScope, ObservationFailure, ScanSetupError, TraversalConfigurationError, TraversalTerminalFailure,
    },
};
use task_model::TransferMode;
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    MetadataVerification(#[from] MetadataVerificationError),
    #[error("目标发布状态与复制结果不一致，禁止删源: {path}")]
    TargetPublicationChanged { path: StoragePath },
    #[error("源端元数据获取失败，数据已保留且禁止删源: {path} version={version:?}, failures={failures:?}")]
    SourceMetadataAcquisitionFailed {
        path: StoragePath,
        version: Option<String>,
        failures: Vec<MetadataObservationFailure>,
    },
    #[error("源端版本永久清理不可用，已保留所有历史: {path}")]
    PermanentVersionCleanupUnavailable { path: StoragePath },
    #[error(transparent)]
    Observation(EntryObservationFailure),
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
    SourceQos(#[from] SourceQosSetupError),
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

// 原生聚合操作的已知原因保持原有边界分类，不能因一次封装改变缺失／取消语义。
impl From<EntryObservationFailure> for StorageError {
    fn from(failure: EntryObservationFailure) -> Self {
        match failure {
            EntryObservationFailure::Role(error) => Self::Role(error),
            EntryObservationFailure::Capability(error) => Self::Capability(error),
            EntryObservationFailure::ModelValue(error) => Self::ModelValue(error),
            other @ EntryObservationFailure::UnsupportedDescriptor { .. } => Self::Observation(other),
        }
    }
}
