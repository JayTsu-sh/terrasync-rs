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
use hyper_util::client::legacy::Error as HyperClientError;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("scan cancellation deadline expired; remote request completion is unconfirmed")]
pub(crate) struct ScanCancellationDeadline;

// 内部模块
// 无

/// 数据库操作的错误类型定义
/// 使用thiserror库实现错误处理和转换
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("ClickHouse probe could not reach all nodes required by scan snapshot publication")]
    ClusterProbeIncomplete,
    #[error("ClickHouse connectivity monitor ended unexpectedly")]
    ConnectivityMonitorStopped,
    #[error("ClickHouse scan request failed: {source}")]
    ScanRequestFailed {
        #[source]
        source: Arc<DatabaseError>,
    },
    #[error("ClickHouse unavailable after {recovery_window:?} recovery: {source}; termination: {termination:?}")]
    ScanDatabaseUnavailable {
        recovery_window: Duration,
        #[source]
        source: Arc<DatabaseError>,
        termination: Option<Box<DatabaseError>>,
    },
    #[error("ClickHouse health probe timed out: {source}")]
    ProbeTimeout {
        #[source]
        source: tokio::time::error::Elapsed,
    },
    #[error("Scan database operation cancelled before starting another request")]
    ScanCancelled,
    #[error("System clock is before the epoch: {0}")]
    SystemClock(#[from] std::time::SystemTimeError),
    #[error("Integer conversion failed: {0}")]
    IntegerConversion(#[from] std::num::TryFromIntError),
    #[error("Invalid native scan observation: {0}")]
    ObservationModel(#[from] data_mover::model::ModelValueError),
    #[error("Observation tag serialization failed: {0}")]
    ObservationJson(#[from] serde_json::Error),
    /// data-mover 拒绝数据库中损坏或未知版本的 opaque observation snapshot。
    #[error("Observation snapshot codec error: {0}")]
    SnapshotDecode(#[from] SnapshotDecodeError),

    /// 可查询列与同一行的 opaque snapshot 不一致。
    #[error("Observation query projection mismatch in field {field}")]
    ObservationProjectionMismatch { field: &'static str },

    #[error("Catalogue batch failed: {source}; staging cleanup also failed: {cleanup}")]
    CatalogueBatchCleanupFailed {
        #[source]
        source: Box<DatabaseError>,
        cleanup: Box<DatabaseError>,
    },

    /// 查询失败后中止 staging 也失败，保留两次失败的类型。
    #[error("Observation publication failed: {source}; staging abort also failed: {abort}")]
    ObservationAbortFailed {
        #[source]
        source: Box<DatabaseError>,
        abort: Box<DatabaseError>,
    },

    /// EXCHANGE 返回错误，无法确认 snapshot 是否已经交换。
    #[error("Observation snapshot EXCHANGE outcome is ambiguous: {source}")]
    ObservationExchangeAmbiguous {
        #[source]
        source: Box<DatabaseError>,
    },

    /// generation 提交失败后工作代状态丢失，保留提交错误。
    #[error("Observation publication lost its working generation after commit failed: {source}")]
    ObservationGenerationLost {
        #[source]
        source: Box<DatabaseError>,
    },

    /// generation 提交及反向交换均失败，保留提交和回滚错误。
    #[error("Generation commit failed ({commit}); snapshot rollback also failed ({rollback})")]
    ObservationRollbackFailed {
        #[source]
        commit: Box<DatabaseError>,
        rollback: Box<DatabaseError>,
    },

    /// 无法确认 generation 提交结果；检查状态失败时也保留该错误。
    #[error("Generation commit outcome is ambiguous after snapshot exchange: {commit}; state query: {verification:?}")]
    ObservationCommitAmbiguous {
        #[source]
        commit: Box<DatabaseError>,
        verification: Option<Box<DatabaseError>>,
    },

    /// `ClickHouse` 数据库相关错误
    /// 从 `clickhouse` 库的错误类型自动转换
    #[error("ClickHouse error: {0}")]
    ClickHouseError(#[source] clickhouse::error::Error),

    #[error("ClickHouse request termination could not be confirmed; last acknowledged checkpoint retained: {source}")]
    ScanTerminationUnconfirmed {
        #[source]
        source: clickhouse::error::Error,
    },

    #[error("ClickHouse response was lost; request outcome is unconfirmed: {source}")]
    RequestOutcomeUnconfirmed {
        #[source]
        source: clickhouse::error::Error,
    },

    /// 配置错误
    /// 当数据库配置无效或不完整时触发
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// 数据库禁用错误
    /// 当尝试使用已禁用的数据库时触发
    #[error("Database disabled error")]
    DatabaseDisabledError(),

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

impl From<clickhouse::error::Error> for DatabaseError {
    fn from(source: clickhouse::error::Error) -> Self {
        let mut cause: Option<&(dyn StdError + 'static)> = Some(&source);
        while let Some(error) = cause {
            if error.is::<ScanCancellationDeadline>() {
                return Self::ScanTerminationUnconfirmed { source };
            }
            // io::Error 的 source 可能跳过自定义内层，SDK Other 也未声明 source。
            cause = if let Some(io) = error.downcast_ref::<std::io::Error>() {
                io.get_ref().map(|inner| inner as &(dyn StdError + 'static))
            } else if let Some(clickhouse::error::Error::Other(inner)) =
                error.downcast_ref::<clickhouse::error::Error>()
            {
                Some(inner.as_ref())
            } else {
                error.source()
            };
        }
        let unconfirmed = match &source {
            clickhouse::error::Error::Network(error) => !error
                .downcast_ref::<HyperClientError>()
                .is_some_and(HyperClientError::is_connect),
            clickhouse::error::Error::TimedOut => true,
            clickhouse::error::Error::Other(error) => error.is::<std::io::Error>(),
            _ => false,
        };
        if unconfirmed {
            Self::RequestOutcomeUnconfirmed { source }
        } else {
            Self::ClickHouseError(source)
        }
    }
}

impl DatabaseError {
    /// 日志只暴露稳定分类和服务端数字码，原始响应可能含 SQL 或绑定值，留在 typed cause 中。
    pub(crate) fn diagnostic_class(&self) -> (&'static str, Option<u32>) {
        match self {
            Self::ScanRequestFailed { source } | Self::ScanDatabaseUnavailable { source, .. } => {
                source.diagnostic_class()
            }
            Self::ScanTerminationUnconfirmed { .. } => ("termination_unconfirmed", None),
            Self::RequestOutcomeUnconfirmed { .. } => ("request_unconfirmed", None),
            Self::ProbeTimeout { .. } => ("probe_timeout", None),
            Self::ClusterProbeIncomplete => ("cluster_unavailable", None),
            Self::ConnectivityMonitorStopped => ("monitor_stopped", None),
            Self::ScanCancelled => ("cancelled", None),
            Self::ClickHouseError(clickhouse::error::Error::BadResponse(message)) => {
                ("server_response", server_error_code(message))
            }
            Self::ClickHouseError(_) => ("client_error", None),
            _ => ("database_error", None),
        }
    }
    pub(crate) fn request_was_not_sent(&self) -> bool {
        match self {
            Self::ScanRequestFailed { source } => source.request_was_not_sent(),
            Self::ClickHouseError(clickhouse::error::Error::Network(error)) => error
                .downcast_ref::<HyperClientError>()
                .is_some_and(HyperClientError::is_connect),
            _ => false,
        }
    }
    pub(crate) fn is_transient_connectivity_failure(&self) -> bool {
        match self {
            Self::ProbeTimeout { .. } | Self::ClusterProbeIncomplete => true,
            Self::ScanRequestFailed { source } | Self::ScanDatabaseUnavailable { source, .. } => {
                source.is_transient_connectivity_failure()
            }
            Self::RequestOutcomeUnconfirmed { source } | Self::ClickHouseError(source) => match source {
                clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut => true,
                clickhouse::error::Error::Other(error) => error.is::<std::io::Error>(),
                clickhouse::error::Error::BadResponse(message) => matches!(
                    server_error_code(message),
                    Some(198 | 209 | 210 | 254 | 265 | 279 | 285 | 286 | 289 | 297)
                ),
                _ => false,
            },
            _ => false,
        }
    }
    pub fn is_confirmed_scan_cancellation(&self) -> bool {
        match self {
            Self::ScanCancelled => true,
            Self::ClickHouseError(clickhouse::error::Error::BadResponse(message)) => {
                let code = server_error_code(message);
                // 服务端明确确认 QUERY_WAS_CANCELLED / QUERY_WAS_CANCELLED_BY_CLIENT。
                matches!(code, Some(394 | 735))
            }
            _ => false,
        }
    }
}

fn server_error_code(message: &str) -> Option<u32> {
    message
        .trim_start()
        .strip_prefix("Code:")
        .and_then(|text| text.trim_start().split('.').next())
        .and_then(|text| text.parse().ok())
}
