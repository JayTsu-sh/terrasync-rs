// 标准库
use std::path::PathBuf;

// 外部crate
use thiserror::Error;

// 内部模块
// 无

/// 应用程序的专用错误枚举
#[derive(Error, Debug)]
pub enum AppError {
    /// 中立 traversal 的 session/runtime 终态失败。
    #[error("Traversal terminal failure: {0}")]
    TraversalTerminal(#[from] data_mover::traversal::TraversalTerminalFailure),

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
        source: data_mover::transfer::TransferFailure,
    },

    /// peer 通过结构化 terminal 报告的 expert transfer 失败。
    #[error("Remote expert peer transfer {ndx} failed: {failure:?}")]
    RemoteExpertPeerFailure {
        ndx: u64,
        failure: transport::message::RemoteTransferFailure,
    },

    /// peer 在完成 evidence 前取消了 expert transfer。
    #[error("Remote expert peer transfer {ndx} was cancelled")]
    RemoteExpertPeerCancelled { ndx: u64 },

    /// FinalDestination 已发布，但应用恢复完成事件未能持久化。
    #[error("Remote expert transfer {ndx} recovery completion persistence failed: {source}")]
    RemoteExpertRecoveryCompletion {
        ndx: u64,
        source_qos: data_mover::storage::SourceQosStats,
        #[source]
        source: Box<AppError>,
    },

    /// 远端 payload 中断，同时保留目标端 unpublished stage 的清理/恢复所有权。
    #[error("Remote expert transfer {ndx} payload interrupted: {peer}; destination: {destination}")]
    RemoteExpertPayloadInterrupted {
        ndx: u64,
        peer: Box<AppError>,
        #[source]
        destination: data_mover::transfer::TransferFailure,
    },
    /// 存储错误
    #[error("{0}")]
    StorageError(#[from] data_mover::error::StorageError),

    /// 数据库错误
    #[error("{0}")]
    DatabaseError(#[from] db::error::DatabaseError),

    /// 工具错误
    #[error("{0}")]
    UtilsError(#[from] utils::error::UtilsError),

    /// License 验证错误
    #[cfg(feature = "license")]
    #[error("{0}")]
    LicenseError(#[from] licensing::error::LicenseError),

    /// Transport 层错误
    #[error("{0}")]
    TransportError(#[from] transport::error::TransportError),

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
    TaskJoinError(#[from] tokio::task::JoinError),

    /// 消费者错误
    /// 当消费者操作失败时触发
    #[error("Consumer error: {0}")]
    ConsumerError(String),

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
