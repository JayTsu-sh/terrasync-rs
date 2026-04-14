// 标准库
// 无

// 外部crate
use thiserror::Error;

// 内部模块
// 无

/// 应用程序的专用错误枚举
#[derive(Error, Debug)]
pub enum AppError {
    /// 存储错误
    #[error("{0}")]
    StorageV2Error(#[from] storage_v2::error::StorageError),

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
}

/// 应用程序的结果类型别名
pub type Result<T> = std::result::Result<T, AppError>;
