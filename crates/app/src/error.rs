use data_mover::storage::{CapabilityUnavailable, StorageRoleFailure};
// 标准库
use std::fmt::{Display, Formatter};
use std::fs::TryLockError;
use std::num::TryFromIntError;
use std::path::{Path, PathBuf};
use std::time::SystemTimeError;

// 外部crate
use data_mover::error::StorageError as DataMoverStorageError;
use data_mover::model::{BackendKind, SnapshotDecodeError};
use data_mover::transfer::{TransferFailure, TransferValueError};
use data_mover::traversal::TraversalTerminalFailure;
use db::error::DatabaseError;
#[cfg(feature = "license")]
use licensing::error::LicenseError;
use serde_json::Error as JsonError;
pub mod storage;
use crate::broadcast::BroadcastError;
use crate::callback::CallbackEndpointError;
use std::sync::Arc;
pub use storage::StorageError;
use task_model::StorageEntryMessage;
use thiserror::Error;
use tokio::task::JoinError;
use transport::error::TransportError;
use transport::message::{RemoteTransferFailure, SessionNdx};
use utils::error::UtilsError;

// 内部模块
// 无

/// 本机复制失败所在的任务步骤；原始存储原因由 source 保留。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStep {
    ReadSourceLink,
    CreateDestinationLink,
    Transfer,
    SourceCleanup,
}

impl Display for CopyStep {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ReadSourceLink => "read source symlink",
            Self::CreateDestinationLink => "create destination symlink",
            Self::Transfer => "transfer file",
            Self::SourceCleanup => "clean up source entry",
        })
    }
}

/// 应用程序的专用错误枚举
#[derive(Error, Debug)]
pub enum AppError {
    #[error("复制失败: {source}; 源端元数据同时获取失败: {metadata}")]
    CopyAndSourceMetadataFailed {
        #[source]
        source: Box<AppError>,
        metadata: Box<StorageError>,
    },
    #[error("复制启动失败: {startup}; 原生扫描收束同时失败: {source}")]
    CopyStartupAndTraversalFailed {
        #[source]
        source: Box<AppError>,
        startup: Box<AppError>,
    },
    #[error("目录目标身份状态超出预算: path={path:?}, count_limit={count_limit}, byte_limit={byte_limit}")]
    CopyDirectoryBudget {
        path: PathBuf,
        count_limit: usize,
        byte_limit: usize,
    },
    #[error("复制 worker 失败，目标写结果未知: path={path:?}, phase={phase}: {source}")]
    CopyWorkerFailed {
        path: PathBuf,
        phase: &'static str,
        #[source]
        source: JoinError,
    },
    #[error("受控复制工作协议无效: {reason}")]
    CopyWorkProtocol { reason: &'static str },
    #[error("根目录元数据失败 backend={backend:?} phase={phase}: {source}")]
    CopyRootMetadataFailure {
        backend: BackendKind,
        phase: &'static str,
        #[source]
        source: Arc<StorageError>,
    },
    #[error("迁移失败: {source}; 根目录元数据同时失败: {metadata}")]
    MigrationAndRootMetadataFailed {
        #[source]
        source: Box<AppError>,
        metadata: Box<AppError>,
    },
    #[error("迁移执行因子任务失败而停止: {source}")]
    CopyExecutionStopped {
        #[source]
        source: Arc<AppError>,
    },
    #[error("迁移执行失败: {source}; 数据库同时不可用: {database}")]
    CopyExecutionAndDatabaseFailed {
        #[source]
        source: Box<AppError>,
        database: DatabaseError,
    },
    #[error("迁移执行失败: {source}; 源清理同时失败: {cleanup}")]
    MigrationAndSourceCleanupFailed {
        #[source]
        source: Box<AppError>,
        cleanup: Box<AppError>,
    },
    #[error("历史版本分段顺序无效: {path:?}, expected={expected}, actual={actual}")]
    CopyHistoryOrder { path: PathBuf, expected: u32, actual: u32 },
    #[error("历史版本没有完整结束: {path:?}, last_version={last_version:?}")]
    CopyHistoryIncomplete {
        path: PathBuf,
        last_version: Option<String>,
    },
    #[error("活跃历史 entry 超出有界协议: {path:?}, limit={limit}")]
    CopyHistoryCapacity { path: PathBuf, limit: usize },
    #[error("历史版本分段编号超出范围: {path:?}")]
    CopyHistorySegmentOverflow { path: PathBuf },
    #[error("历史版本选择无效: {path:?}, version_index={version_index:?}")]
    CopyHistorySelection {
        path: PathBuf,
        version_index: Option<usize>,
    },
    #[error("历史分段内版本顺序无效: {path:?}, segment={segment}, previous={previous}, actual={actual}")]
    CopyHistoryVersionOrder {
        path: PathBuf,
        segment: u32,
        previous: usize,
        actual: usize,
    },
    #[error("历史 entry 的版本字节总量超出统计范围: {path:?}")]
    CopyHistoryBytesOverflow { path: PathBuf },
    #[error(transparent)]
    MigrationPolicy(#[from] task_model::TaskModelError),
    #[error("迁移失败: {source}; 代次收尾也失败: {finalization}")]
    MigrationFinalizationFailed {
        #[source]
        source: Box<AppError>,
        finalization: DatabaseError,
    },
    #[error("迁移失败: {source}; 迁移明细报告同时失败: {reporting}")]
    MigrationReportingFailed {
        #[source]
        source: Box<AppError>,
        reporting: Box<AppError>,
    },
    #[error("双进程模式尚未提供目标就绪后受保护删除源端的能力；已拒绝任务，未启动复制")]
    RemoteSourceCleanupUnavailable,
    #[error("Diagnostic processing stopped: {source}")]
    DiagnosticJournalStopped {
        #[source]
        source: Arc<AppError>,
    },
    #[error("Diagnostic database unavailable after bounded recovery: {source}")]
    DiagnosticDatabaseUnavailable {
        #[source]
        source: Arc<DatabaseError>,
    },
    #[error("Unresolved diagnostics could not be retained or acknowledged within the delivery budget")]
    DiagnosticDeliveryUnavailable,
    #[error("Invalid archived diagnostic record: {field}")]
    DiagnosticArchiveInvalid { field: &'static str },
    #[error("{source}")]
    ConsumerDisconnected {
        #[source]
        source: Box<BroadcastError<StorageEntryMessage>>,
    },
    #[error("Invalid progress callback endpoint: {0}")]
    CallbackEndpoint(#[from] CallbackEndpointError),
    #[error("{source}")]
    ReportedCopyFailure {
        #[source]
        source: Box<AppError>,
        data_failed: bool,
    },
    #[error("Failed to {step} {path:?}: {source}")]
    CopyEntryFailed {
        step: CopyStep,
        path: PathBuf,
        #[source]
        source: Box<AppError>,
    },
    #[error("Scan storage failure: {source}; database also failed while stopping: {database}")]
    ScanStorageAndDatabaseFailure {
        #[source]
        source: Box<StorageError>,
        database: Box<DatabaseError>,
    },
    #[error("同一数据库作业 {job_id} 已有执行")]
    JobExecutionBusy { job_id: String },
    #[error("无法获取数据库作业 {job_id} 的执行锁: {source}")]
    JobExecutionLock { job_id: String, source: TryLockError },
    /// 中立 traversal 的 session/runtime 终态失败。
    #[error("Traversal terminal failure: {0}")]
    TraversalTerminal(#[from] TraversalTerminalFailure),

    /// 传输请求或身份值无效，保留上游校验错误。
    #[error("Transfer value error: {0}")]
    TransferValue(#[from] TransferValueError),

    /// 扫描过滤条件解析失败，保留上游错误类型。
    #[error("Scan filter error: {0}")]
    ScanFilter(#[from] DataMoverStorageError),

    /// 扫描选项无法编码为检查点请求标识。
    #[error("Scan request encoding error: {0}")]
    ScanRequestEncoding(#[from] JsonError),

    /// 系统时间早于 Unix epoch，无法生成扫描事件时间。
    #[error("Scan clock error: {0}")]
    ScanClock(#[from] SystemTimeError),

    /// 扫描事件时间超出持久化整数范围。
    #[error("Scan timestamp conversion error: {0}")]
    ScanTimestampConversion(#[from] TryFromIntError),

    #[error("Cannot lock copy job {job_id}: {source}")]
    CopyJobLock {
        job_id: String,
        #[source]
        source: TryLockError,
    },

    /// 扫描 job 锁获取失败，保留锁竞争或底层 I/O 原因。
    #[error("Cannot lock scan job {job_id}: {source}")]
    ScanJobLock {
        job_id: String,
        #[source]
        source: TryLockError,
    },

    /// traversal completion evidence 与本地 transfer queue 实际消费数量不一致。
    #[error(
        "Traversal count mismatch: consumed {consumed_entries} entries/{consumed_failures} failures, reported {reported_entries} entries/{reported_failures} failures"
    )]
    TraversalCountMismatch {
        consumed_entries: u64,
        reported_entries: u64,
        consumed_failures: u64,
        reported_failures: u64,
    },

    /// traversal 有 entry 级失败，因此快照不完整。
    #[error("Observation scan incomplete after {entry_failures} entry failures")]
    ObservationScanIncomplete { entry_failures: u64 },

    /// traversal completion evidence 与实际投影条目数不一致。
    #[error("Observation scan count mismatch: projected {projected}, traversal reported {reported}")]
    ObservationScanCountMismatch { projected: u64, reported: u64 },

    /// traversal 在完整证据前被取消。
    #[error("Observation scan cancelled")]
    ObservationScanCancelled,

    /// remote advertisement 在完整 traversal evidence 前被取消。
    #[error("Remote advertisement cancelled after {observed_entries} entries and {entry_failures} entry failures")]
    RemoteAdvertisementCancelled { observed_entries: u64, entry_failures: u64 },

    /// remote advertisement 实际投影数与 traversal completion evidence 不一致。
    #[error(
        "Remote advertisement count mismatch: projected {projected_entries} entries/{projected_failures} failures, reported {reported_entries} entries/{reported_failures} failures"
    )]
    RemoteAdvertisementCountMismatch {
        projected_entries: u64,
        projected_failures: u64,
        reported_entries: u64,
        reported_failures: u64,
    },

    /// remote advertisement wire 违反分页、NDX、snapshot 或 terminal 合同。
    #[error("Remote advertisement protocol error: {reason}")]
    RemoteAdvertisementProtocol { reason: String },

    /// 广告中的 opaque snapshot 无法解码，保留 codec 原始错误。
    #[error("Remote advertisement snapshot {ndx:?} decode failed: {source}")]
    RemoteAdvertisementSnapshotDecode {
        ndx: SessionNdx,
        #[source]
        source: SnapshotDecodeError,
    },

    /// 对端以失败 terminal 结束 remote advertisement。
    #[error("Remote advertisement peer failed: {diagnostic}")]
    RemoteAdvertisementPeerFailed { diagnostic: String },

    /// remote expert transfer 收到错误 NDX、offset、chunk 或消息顺序。
    #[error("Remote expert transfer {ndx} protocol error: {reason}")]
    RemoteExpertTransferProtocol { ndx: u64, reason: String },

    /// data-mover expert half 的 typed failure；保留 stage 清理/恢复所有权。
    #[error("Remote expert transfer {ndx}: {source}")]
    RemoteExpertTransferFailure {
        ndx: u64,
        #[source]
        source: TransferFailure,
    },

    /// peer 通过结构化 terminal 报告的 expert transfer 失败。
    #[error("Remote expert peer transfer {ndx} failed: {failure:?}")]
    RemoteExpertPeerFailure { ndx: u64, failure: RemoteTransferFailure },

    /// peer 在完成 evidence 前取消了 expert transfer。
    #[error("Remote expert peer transfer {ndx} was cancelled")]
    RemoteExpertPeerCancelled { ndx: u64 },

    /// 远端 payload 中断，同时保留目标端 unpublished stage 的清理/恢复所有权。
    #[error("Remote expert transfer {ndx} payload interrupted: {peer}; destination: {destination}")]
    RemoteExpertPayloadInterrupted {
        ndx: u64,
        peer: Box<AppError>,
        #[source]
        destination: TransferFailure,
    },
    /// 存储错误
    #[error("{0}")]
    StorageError(#[from] StorageError),

    /// 数据库错误
    #[error("{0}")]
    DatabaseError(#[from] DatabaseError),

    /// 工具错误
    #[error("{0}")]
    UtilsError(#[from] UtilsError),

    /// License 验证错误
    #[cfg(feature = "license")]
    #[error("{0}")]
    LicenseError(#[from] LicenseError),

    /// Transport 层错误
    #[error("{0}")]
    TransportError(#[from] TransportError),
    #[error("Channel closed")]
    CopyQueueClosed,

    /// IO错误
    /// 当IO操作失败时触发
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    /// 扫描错误
    #[error("Scan error: {0}")]
    ScanError(String),

    /// 复制错误
    #[error("Copy error: {0}")]
    CopyError(String),

    /// 单文件落盘阶段失败，并保留原始 typed error chain。
    #[error("File commit {stage}: {source}")]
    FileCommitStage {
        stage: &'static str,
        #[source]
        source: Box<AppError>,
    },

    /// Remote Sender negotiated session 的阶段化失败，保留原始 typed error chain。
    #[error("Remote Sender session {stage}: {source}")]
    SenderSessionStage {
        stage: &'static str,
        #[source]
        source: Box<AppError>,
    },

    /// 配置错误
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// 任务执行错误
    #[error("Task join error: {0}")]
    TaskJoinError(#[from] JoinError),

    /// 消费者错误
    /// 当消费者操作失败时触发
    #[error("Consumer error: {0}")]
    ConsumerError(String),

    #[error("Required database scan worker stopped before acknowledgement")]
    ScanWorkerStopped,

    /// 路径不存在错误
    /// 当指定路径不存在时触发
    #[error("Path not exist error: {0}")]
    PathNotExistError(String),

    /// 完整性检查错误
    /// 当文件完整性检查失败时触发
    #[error("Integrity check error: {0}")]
    IntegrityCheckError(String),

    /// 打包错误
    /// 当目录打包为 tar 文件失败时触发
    #[error("Pack error for {path}: {reason}")]
    PackError { path: String, reason: String },

    /// 断点续传错误
    /// 当字节级续传状态读写或解析失败时触发
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    /// 非法相对路径（Sender 提供的相对路径为绝对路径或含 `..` 组件）
    /// 拒绝以该路径驱动目标端写操作，防止路径穿越
    #[error("Unsafe relative path: {path:?}")]
    UnsafeRelativePath { path: PathBuf },
}

/// 应用程序的结果类型别名
pub type Result<T> = std::result::Result<T, AppError>;

impl AppError {
    /// 同一次根观察由 Arc 身份标识；组合错误的另一分支也可能已携带该失败。
    pub(crate) fn contains_copy_root_failure(&self, observed: &Arc<StorageError>) -> bool {
        match self {
            Self::CopyRootMetadataFailure { source, .. } => Arc::ptr_eq(source, observed),
            Self::MigrationAndRootMetadataFailed { source, metadata } => {
                source.contains_copy_root_failure(observed) || metadata.contains_copy_root_failure(observed)
            }
            Self::MigrationAndSourceCleanupFailed { source, cleanup } => {
                source.contains_copy_root_failure(observed) || cleanup.contains_copy_root_failure(observed)
            }
            Self::MigrationReportingFailed { source, reporting } => {
                source.contains_copy_root_failure(observed) || reporting.contains_copy_root_failure(observed)
            }
            Self::CopyExecutionStopped { source } | Self::DiagnosticJournalStopped { source } => {
                source.contains_copy_root_failure(observed)
            }
            Self::CopyExecutionAndDatabaseFailed { source, .. }
            | Self::MigrationFinalizationFailed { source, .. }
            | Self::ReportedCopyFailure { source, .. }
            | Self::CopyEntryFailed { source, .. }
            | Self::FileCommitStage { source, .. }
            | Self::SenderSessionStage { source, .. } => source.contains_copy_root_failure(observed),
            Self::RemoteExpertPayloadInterrupted { peer, .. } => peer.contains_copy_root_failure(observed),
            _ => false,
        }
    }

    pub(crate) fn source_cleanup_entry(&self) -> Option<(&Path, &Self)> {
        match self {
            Self::MigrationAndSourceCleanupFailed { source, cleanup } => {
                cleanup.source_cleanup_entry().or_else(|| source.source_cleanup_entry())
            }
            Self::CopyExecutionStopped { source } => source.source_cleanup_entry(),
            Self::MigrationAndRootMetadataFailed { source, .. } => source.source_cleanup_entry(),
            Self::CopyEntryFailed {
                step: CopyStep::SourceCleanup,
                path,
                source,
            } => Some((path, source)),
            Self::MigrationFinalizationFailed { source, .. }
            | Self::MigrationReportingFailed { source, .. }
            | Self::CopyExecutionAndDatabaseFailed { source, .. } => source.source_cleanup_entry(),
            _ => None,
        }
    }
    pub(crate) fn is_reported_copy_failure(&self) -> bool {
        match self {
            Self::CopyExecutionStopped { source } => source.is_reported_copy_failure(),
            Self::ReportedCopyFailure { .. } => true,
            Self::CopyEntryFailed { source, .. } => source.is_reported_copy_failure(),
            _ => false,
        }
    }

    pub(crate) fn with_reported_copy_failure(self, metadata_only: bool) -> Self {
        let data_failed = !metadata_only
            && self
                .storage_error()
                .is_none_or(|error| error.published_data().is_none());
        Self::ReportedCopyFailure {
            source: Box::new(self),
            data_failed,
        }
    }

    #[cfg(test)]
    pub(crate) fn reported_copy_data_failed(&self) -> bool {
        match self {
            Self::CopyExecutionStopped { source } => source.reported_copy_data_failed(),
            Self::ReportedCopyFailure { data_failed, .. } => *data_failed,
            Self::CopyEntryFailed { source, .. } => source.reported_copy_data_failed(),
            _ => false,
        }
    }
    pub(crate) fn copy_entry_failure(step: CopyStep, path: &Path, source: impl Into<Self>) -> Self {
        Self::CopyEntryFailed {
            step,
            path: path.to_path_buf(),
            source: Box::new(source.into()),
        }
    }

    pub(crate) fn stops_event_pipeline(&self) -> bool {
        match self {
            Self::CopyExecutionStopped { source } => source.stops_event_pipeline(),
            Self::ConsumerDisconnected { .. } => true,
            Self::MigrationAndRootMetadataFailed { source, metadata } => {
                source.stops_event_pipeline() || metadata.stops_event_pipeline()
            }
            Self::CopyEntryFailed { source, .. } | Self::ReportedCopyFailure { source, .. } => {
                source.stops_event_pipeline()
            }
            _ => false,
        }
    }

    pub fn storage_error(&self) -> Option<&StorageError> {
        match self {
            Self::MigrationAndSourceCleanupFailed { source, cleanup } => {
                source.storage_error().or_else(|| cleanup.storage_error())
            }
            Self::MigrationAndRootMetadataFailed { source, metadata } => {
                source.storage_error().or_else(|| metadata.storage_error())
            }
            Self::CopyRootMetadataFailure { source, .. } => Some(source),
            Self::CopyExecutionStopped { source } => source.storage_error(),
            Self::CopyEntryFailed { source, .. }
            | Self::ReportedCopyFailure { source, .. }
            | Self::CopyExecutionAndDatabaseFailed { source, .. } => source.storage_error(),
            Self::CopyAndSourceMetadataFailed { source, .. } => source.storage_error(),
            Self::StorageError(error) => Some(error),
            Self::ScanStorageAndDatabaseFailure { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<StorageRoleFailure> for AppError {
    fn from(source: StorageRoleFailure) -> Self {
        Self::StorageError(source.into())
    }
}

impl From<BroadcastError<StorageEntryMessage>> for AppError {
    fn from(source: BroadcastError<StorageEntryMessage>) -> Self {
        Self::ConsumerDisconnected {
            source: Box::new(source),
        }
    }
}
impl From<CapabilityUnavailable> for AppError {
    fn from(source: CapabilityUnavailable) -> Self {
        Self::StorageError(source.into())
    }
}
