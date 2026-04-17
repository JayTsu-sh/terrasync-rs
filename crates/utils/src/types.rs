//! 工具库通用类型定义模块
//!
//! 该模块定义了工具库中使用的通用类型，包括：
//! 1. 日志级别枚举
//! 2. 其他通用类型（当前文件中只有LogLevel）

// 标准库
use std::str::FromStr;

// 外部crate
use serde::{Deserialize, Serialize};

// 内部模块
use crate::error::{Result, UtilsError};

/// 日志级别枚举
///
/// 该枚举定义了应用程序支持的日志级别，从低到高分别为：
/// - Trace: 最详细的日志，用于调试
/// - Debug: 调试信息，用于开发阶段
/// - Info: 普通信息，用于生产环境
/// - Warn: 警告信息，表示潜在问题
/// - Error: 错误信息，表示严重问题
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LogLevel {
    /// 跟踪级别 - 最详细的日志，包含所有调试信息
    #[serde(rename = "trace")]
    Trace,
    /// 调试级别 - 用于开发阶段的详细调试信息
    #[serde(rename = "debug")]
    Debug,
    /// 信息级别 - 普通的信息日志，用于生产环境
    #[serde(rename = "info")]
    Info,
    /// 警告级别 - 表示潜在问题，但不影响程序运行
    #[serde(rename = "warn")]
    Warn,
    /// 错误级别 - 表示严重问题，可能影响程序运行
    #[serde(rename = "error")]
    Error,
}

/// 实现 `Display` trait，方便将 `LogLevel` 转换为字符串
impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match *self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        write!(f, "{s}")
    }
}

/// 实现 `FromStr` trait，方便从字符串转换为 `LogLevel`
impl FromStr for LogLevel {
    type Err = UtilsError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(UtilsError::InvalidLogLevel(format!("Invalid log level: {s}"))),
        }
    }
}

/// 将 job ID 规范化为 ClickHouse/文件系统安全的标识符
///
/// 仅保留 `[a-zA-Z0-9_]`，其余字符一律替换为 `_`。
/// 适用于 `ClickHouse` 表名后缀和 job 目录名。
pub fn sanitize_job_id(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}
