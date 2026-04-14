//! 工具库错误处理模块
//!
//! 该模块定义了工具库中使用的统一错误类型和结果类型，使用thiserror库实现：
//! 1. 统一的错误类型枚举
//! 2. 从各种底层错误类型的自动转换
//! 3. 友好的错误信息
//! 4. 结果类型别名

// 标准库
// 无

// 外部crate
use thiserror::Error;

// 内部模块
// 无

/// 工具库的专用错误枚举
///
/// 该枚举包含了工具库中可能遇到的所有错误类型，并提供了友好的错误信息。
#[derive(Error, Debug)]
pub enum UtilsError {
    /// 配置错误
    ///
    /// 当配置文件解析或加载失败时触发，从config库的ConfigError自动转换。
    #[error("Config error: {0}")]
    ConfigError(#[from] config::ConfigError),

    /// 互斥锁中毒错误
    ///
    /// 当互斥锁被另一个线程在持有锁时panic导致中毒时触发。
    #[error("Mutex poisoned error")]
    MutexPoisoned,

    /// IO错误
    ///
    /// 当文件或网络IO操作失败时触发，从标准库的io::Error自动转换。
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// 日志设置错误
    ///
    /// 当设置日志记录器失败时触发，从log库的SetLoggerError自动转换。
    #[error("Logger error: {0}")]
    LoggerError(#[from] log::SetLoggerError),

    /// Tracing设置错误
    ///
    /// 当设置tracing记录器失败时触发，从tracing库的SetGlobalDefaultError自动转换。
    #[error("Tracing error: {0}")]
    TracingError(#[from] tracing::subscriber::SetGlobalDefaultError),

    /// 无效的日志级别
    ///
    /// 当提供的日志级别字符串无效时触发。
    #[error("Invalid log level: {0}")]
    InvalidLogLevel(String),

    /// 配置转换错误
    ///
    /// 当配置值转换为特定类型失败时触发。
    #[error("Config conversion error: {0}")]
    ConfigConversionError(String),

    /// 加密错误
    ///
    /// 当密码加密或解密失败时触发。
    #[error("Crypto error: {0}")]
    CryptoError(String),

    /// 其他错误
    ///
    /// 用于无法归类到其他错误类型的通用错误。
    #[error("Other error: {0}")]
    Other(String),
}

/// 工具库的结果类型别名
///
/// 封装了UtilsError作为错误类型，方便在工具库中使用。
pub type Result<T> = std::result::Result<T, UtilsError>;

/// 从PoisonError转换为UtilsError
///
/// 当互斥锁中毒时自动转换为UtilsError::MutexPoisoned。
impl<T> From<std::sync::PoisonError<T>> for UtilsError {
    fn from(_err: std::sync::PoisonError<T>) -> Self {
        UtilsError::MutexPoisoned
    }
}

/// 从String转换为UtilsError
///
/// 方便将字符串直接转换为错误类型。
impl From<String> for UtilsError {
    fn from(err: String) -> Self {
        UtilsError::Other(err)
    }
}

/// 从&str转换为UtilsError
///
/// 方便将字符串字面量直接转换为错误类型。
impl From<&str> for UtilsError {
    fn from(err: &str) -> Self {
        UtilsError::Other(err.to_string())
    }
}

/// 从Box<dyn Error>转换为UtilsError
///
/// 方便将各种错误类型转换为统一的UtilsError。
impl From<Box<dyn std::error::Error + Send + Sync>> for UtilsError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        UtilsError::Other(err.to_string())
    }
}
