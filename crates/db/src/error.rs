//! 数据库错误处理模块
//!
//! 该模块定义了数据库操作中可能发生的各种错误类型，使用thiserror库实现：
//! 1. 统一的错误类型枚举
//! 2. 从各种底层错误类型的自动转换
//! 3. 友好的错误信息
//! 4. 结果类型别名

// 标准库
// 无

// 外部crate
use data_mover::model::SnapshotDecodeError;
use thiserror::Error;

// 内部模块
// 无

/// 数据库操作的错误类型定义
/// 使用thiserror库实现错误处理和转换
#[derive(Error, Debug)]
pub enum DatabaseError {
    /// data-mover 拒绝数据库中损坏或未知版本的 opaque observation snapshot。
    #[error("Observation snapshot codec error: {0}")]
    SnapshotDecode(#[from] SnapshotDecodeError),

    /// 可查询列与同一行的 opaque snapshot 不一致。
    #[error("Observation query projection mismatch in field {field}")]
    ObservationProjectionMismatch { field: &'static str },

    /// 已完成的 attempt 不能再次取得 recovery claim 并重新执行 payload。
    #[error("Recovery attempt is already completed")]
    RecoveryAttemptCompleted,
    /// `ClickHouse` 数据库相关错误
    /// 从 `clickhouse` 库的错误类型自动转换
    #[error("ClickHouse error: {0}")]
    ClickHouseError(#[from] clickhouse::error::Error),

    /// 配置错误
    /// 当数据库配置无效或不完整时触发
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// 数据库禁用错误
    /// 当尝试使用已禁用的数据库时触发
    #[error("Database disabled error")]
    DatabaseDisabledError(),

    /// 不支持的数据库类型
    /// 当尝试使用未实现的数据库类型时触发
    #[error("Unsupported database type: {0}")]
    UnsupportedType(String),

    /// 表不存在错误
    /// 当尝试访问不存在的表时触发
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// 数据库连接错误
    /// 无法建立或保持数据库连接时触发
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// SQL查询错误
    /// 执行SQL查询时发生错误时触发
    #[error("Query error: {0}")]
    QueryError(String),

    /// IO操作错误
    /// 文件或网络IO操作失败时触发
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// UUID生成或解析错误
    /// 处理UUID值时发生错误时触发
    #[error("UUID error: {0}")]
    UuidError(#[from] uuid::Error),

    /// 异步任务执行错误
    /// 当异步任务执行失败时触发
    #[error("Task join error: {0}")]
    TaskJoinError(#[from] tokio::task::JoinError),

    /// 事务错误
    /// 事务处理过程中发生错误时触发
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// 并发错误
    /// 并发数据库操作时发生错误时触发
    #[error("Concurrency error: {0}")]
    ConcurrencyError(String),

    /// 数据转换错误
    /// 数据转换过程中发生错误时触发
    #[error("Data conversion error: {0}")]
    ConversionError(String),

    /// 工具层错误
    /// 从utils crate传播的错误，保留完整类型信息
    #[error("Utils error: {0}")]
    UtilsError(#[from] utils::error::UtilsError),
}

/// 数据库操作的结果类型别名
/// 封装了 `DatabaseError` 作为错误类型
pub type Result<T> = std::result::Result<T, DatabaseError>;
