#![cfg_attr(feature = "nightly", feature(backtrace))]

//! 工具库模块
//!
//! 该模块提供了应用程序通用的工具功能，包括：
//! 1. 应用配置管理
//! 2. 错误处理
//! 3. 日志记录
//! 4. 通用类型定义

// 标准库
// 无

// 外部crate
// 无

// 内部模块

/// 通用异步 mpsc 通道接收器封装
pub mod async_receiver;

/// 应用配置管理模块
/// 负责加载、解析和提供应用配置
pub mod app_config;

/// 加密工具模块
/// 提供密码加密和解密功能
pub mod crypto;

/// 错误处理模块
/// 定义通用错误类型和错误转换
pub mod error;

/// 日志记录模块
/// 提供统一的日志初始化和配置
pub mod logger;

/// 通用类型定义模块
/// 包含应用中使用的通用类型和工具函数
pub mod types;

/// 公共 API 的 prelude 模块
///
/// 用户可以通过 `use utils::prelude::*` 来导入最常用的类型，
/// 简化应用开发过程中的导入语句
/// 顶层便捷重导出
pub use types::sanitize_job_id;

pub mod prelude {
    /// 应用配置相关类型
    pub use crate::app_config::AppConfig;
    /// 异步通道接收器封装
    pub use crate::async_receiver::AsyncReceiver;
    /// 加密工具相关类型
    pub use crate::crypto::CryptoUtil;
    /// 错误处理相关类型
    pub use crate::error::{Result, UtilsError};
    /// 日志记录相关函数
    pub use crate::logger::{init_test_logging, reload_log_level, setup_logging};
    /// 通用类型定义
    pub use crate::types::{LogLevel, sanitize_job_id};
}
