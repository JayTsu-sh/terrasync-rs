//! CLI错误处理模块
//!
//! 该模块定义了CLI模块的错误类型和结果类型，使用thiserror库实现：
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

/// CLI的专用错误枚举
///
/// 该枚举包含了CLI模块中可能遇到的所有错误类型，并提供了友好的错误信息。
#[derive(Error, Debug)]
pub enum CliError {
    /// 应用错误
    ///
    /// 当应用核心逻辑产生错误时触发，从 `app::error::AppError` 自动转换。
    #[error("{0}")]
    AppError(#[from] app::error::AppError),

    /// 工具错误
    ///
    /// 当工具库产生错误时触发，从 `utils::error::UtilsError` 自动转换。
    #[error("{0}")]
    UtilsError(#[from] utils::error::UtilsError),

    /// Clap命令行解析错误
    ///
    /// 当命令行参数解析失败时触发，从 `clap::Error` 自动转换。
    #[error("{0}")]
    ClapError(#[from] clap::Error),

    /// 命令执行错误
    ///
    /// 当命令执行过程中发生错误时触发。
    #[error("Command execution error: {0}")]
    ExecutionError(String),

    /// 参数错误
    ///
    /// 当命令行参数验证失败时触发。
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    /// 配置合并错误
    ///
    /// 当合并配置文件或命令行参数时发生错误时触发。
    #[error("Config merge error: {0}")]
    ConfigMergeError(String),

    /// License 验证错误
    ///
    /// 当 License 验证、激活或读取失败时触发。
    #[cfg(feature = "license")]
    #[error("{0}")]
    LicenseError(#[from] licensing::error::LicenseError),

    /// Transport 层错误
    #[error("{0}")]
    TransportError(#[from] transport::error::TransportError),

    /// 存储层错误
    #[error("{0}")]
    StorageError(#[from] storage_v2::error::StorageError),

    /// IO错误
    ///
    /// 当文件或网络IO操作失败时触发，从标准库的 `io::Error` 自动转换。
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    /// Web GUI 错误
    #[cfg(feature = "gui")]
    #[error("{0}")]
    WebError(#[from] web::error::WebError),
}

/// CLI的结果类型别名
///
/// 封装了CliError作为错误类型，方便在CLI模块中使用。
pub type Result<T> = std::result::Result<T, CliError>;
