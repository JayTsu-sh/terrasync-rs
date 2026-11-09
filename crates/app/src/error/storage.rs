use std::iter;

use data_mover::error::StorageError as NativeStorageError;
use data_mover::integrity::IntegritySide as NativeIntegritySide;
use data_mover::metadata::MetadataVerificationError;
use data_mover::model::MetadataFamily;
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
        NamespaceReadFailure, PurgeFailure, StorageRoleFailure,
    },
    tar_pack::TarFailure,
    transfer::{DirectPatchError, TransferFailure, TransferOutcome, TransferValueError},
    traversal::{
        FailureScope, ObservationFailure, ScanSetupError, TraversalConfigurationError, TraversalTerminalFailure,
    },
};
use task_model::TransferMode;
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    NamespaceRead(#[from] NamespaceReadFailure),
    #[error(transparent)]
    MetadataVerification(#[from] MetadataVerificationError),
    #[error("目标发布状态与复制结果不一致，禁止删源: {path}")]
    TargetPublicationChanged { path: StoragePath },
    #[error("目标端持久化确认失败，保留源端: {path}: {source}; {}", role_diagnostic(.source))]
    TargetPersistenceUnconfirmed {
        path: StoragePath,
        #[source]
        source: StorageRoleFailure,
    },
    #[error("删源前数据全量校验未完成，保留源端: {path}, side={side:?}: {source}; {}", role_diagnostic(.source))]
    SourceCleanupContentUnverified {
        path: StoragePath,
        side: NativeIntegritySide,
        #[source]
        source: StorageRoleFailure,
    },
    #[error("源端受保护清理失败，保留未确认删除的源项: {path}: {source}; {}", role_diagnostic(.source.source.as_ref()))]
    SourcePurgeRefused {
        path: StoragePath,
        #[source]
        source: PurgeFailure,
    },
    #[error("复制后源端元数据发生变化，保留源端: {path}, families={families:?}")]
    SourceMetadataChanged {
        path: StoragePath,
        families: Vec<MetadataFamily>,
    },
    #[error("源端元数据获取失败，禁止删源: {path} version={version:?}, failures={failures:?}")]
    SourceMetadataAcquisitionFailed {
        path: StoragePath,
        version: Option<String>,
        failures: Vec<MetadataObservationFailure>,
    },
    #[error("复制失败: {data}; 元数据处理同时失败: {metadata}")]
    TransferAndMetadata {
        #[source]
        data: Box<StorageError>,
        metadata: Box<StorageError>,
    },
    #[error("数据已发布，但元数据未就绪，禁止删源: {source}")]
    PublishedDataMetadata {
        data: Box<TransferOutcome>,
        #[source]
        source: Box<StorageError>,
    },
    #[error("缺少实际目标版本映射，保留数据且不重新发布旧版本: {path} source_version={source_version}")]
    TargetVersionUnconfirmed { path: StoragePath, source_version: String },
    #[error(
        "历史数据发布结果未确认，保留所有源和目标版本，禁止重放旧数据及继续发布后续版本: {path} source_version={source_version}"
    )]
    HistoricalPublicationUnconfirmed { path: StoragePath, source_version: String },
    #[error("源端版本永久清理不可用，已保留所有历史: {path}")]
    PermanentVersionCleanupUnavailable { path: StoragePath },
    #[error("无法重新确认版本化源的当前对象，不能认定全部历史已删除，保留源历史: {path}")]
    SourceHistoryCurrentUnconfirmed { path: StoragePath },
    #[error("目录标记对象的目标就绪核验尚未接通，保留源对象: {path}")]
    DirectoryMarkerCleanupUnavailable { path: StoragePath },
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
    #[error("stream producer failed: {source}; preceding stream failure: {preceding}")]
    StreamProducerFailed {
        #[source]
        source: JoinError,
        preceding: Box<StorageError>,
    },
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
    pub(crate) fn published_data(&self) -> Option<&TransferOutcome> {
        match self {
            Self::PublishedDataMetadata { data, .. } => Some(data),
            Self::Transfer(error) => error.published_data(),
            Self::TransferAndMetadata { data, .. } => data.published_data(),
            _ => None,
        }
    }
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::FileNotFound(_) | Self::DirectoryNotFound(_))
            || matches!(self, Self::Role(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
            || matches!(self, Self::NamespaceRead(NamespaceReadFailure::Cancelled { .. }))
            || matches!(self,
                Self::Role(StorageRoleFailure::Entry(error))
                | Self::TargetPersistenceUnconfirmed { source: StorageRoleFailure::Entry(error), .. }
                | Self::SourceCleanupContentUnverified { source: StorageRoleFailure::Entry(error), .. }
                if error.class() == FailureClass::Cancelled)
            || matches!(self,
                Self::SourceCleanupContentUnverified { source: StorageRoleFailure::Session(error), .. }
                if error.class() == FailureClass::Cancelled)
            || matches!(self, Self::SourcePurgeRefused { source, .. } if
            iter::once(source.source.as_ref()).chain(source.additional.iter().map(|(_, cause)| cause))
            .any(|cause| match cause {
                StorageRoleFailure::Entry(cause) => cause.class() == FailureClass::Cancelled,
                StorageRoleFailure::Session(cause) => cause.class() == FailureClass::Cancelled,
            }))
            || matches!(self, Self::DirectPatch(error) if error.is_cancelled())
    }
}
impl From<TransferFailure> for StorageError {
    fn from(value: TransferFailure) -> Self {
        Self::Transfer(Box::new(value))
    }
}
// role 的 Display 只展示类别；这里显式选用契约已脱敏的诊断供任务日志及 callback 使用。
fn role_diagnostic(source: &StorageRoleFailure) -> &str {
    match source {
        StorageRoleFailure::Entry(error) => error.diagnostic(),
        StorageRoleFailure::Session(error) => error.diagnostic(),
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
