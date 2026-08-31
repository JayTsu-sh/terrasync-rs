//! 应用核心功能模块
//!
//! 该模块包含了应用的核心功能实现，包括文件扫描、复制、消费者管理等功能。
//! 主要提供了文件同步、增量扫描和复制的核心逻辑。

/// Windows ACL（访问控制列表）处理模块
///
/// 提供Windows平台下文件和目录的访问控制列表管理功能
#[cfg(windows)]
pub mod ace;

pub mod broadcast;

/// 双进程模式 Receiver 落盘任务模块
///
/// 串行消费 `DiskCommitMsg`，用 data-mover 3 段流式 API 写入 `.part`，
/// 提交时读回 hash 校验后原子 rename
pub mod disk_commit;
mod file_commit;
mod file_copy;
pub mod storage_factory;

/// 字节级断点续传状态模块
///
/// 为多块大文件记录已落盘 offset 区间，支持中断后从未完成位置续传
pub mod byte_resume;

/// 配置管理模块
///
/// 处理应用的配置初始化和管理
pub mod config;

/// 消费者管理模块
///
/// 负责管理不同类型的消费者，如存储条目消费者和统计消费者
pub mod consumer;

pub mod observation_scan;
/// Sync 编排器模块
///
/// SyncOrchestrator 封装 sync/incremental_sync 的核心逻辑
pub mod orchestrator;

/// 双进程远端同步（Sender 侧）
///
/// 将 run_sync_remote 的各阶段逻辑提取为独立函数，降低单函数复杂度
pub mod remote_sync;

/// 双进程远端同步 Receiver 会话
///
/// 封装一次已协商会话的生命周期；当前版本保留既有事件循环作为内部实现，
/// 后续重构可在不扩大调用方 interface 的前提下逐步迁移会话状态。
pub mod remote_receiver_session;

/// Receiver 侧逻辑
///
/// 从 transport 接收消息，在目标存储上执行写入
pub mod receiver;

/// Sender 侧逻辑
///
/// 从 walkdir 接收 entry，通过 transport 发送给 Receiver
pub mod sender;

pub mod integrity_check;
/// 文件复制模块
///
/// 提供文件复制和增量复制的核心功能
pub mod sync;

/// 目录遍历模块
///
/// 负责递归遍历目录结构，生成存储条目
pub mod dir_walker;

/// 错误处理模块
///
/// 定义应用的错误类型和结果类型
pub mod error;

/// 文件扫描模块
///
/// 提供全量扫描和增量扫描功能
pub mod scan;

///// 文件删除模块
/// 提供删除指定路径下所有文件和目录的功能
pub mod rm;

/// 流式 tar 打包模块
/// 将日期目录打包为 .tar 文件写入目标端
pub mod tar_pack;

/// 公共 API 的 prelude 模块
///
/// 用户可以通过 `use app::prelude::*` 来导入最常用的类型，
/// 简化应用开发过程中的导入语句
pub mod prelude {
    /// `QoS` 带宽解析函数
    pub use data_mover::qos::parse_bandwidth_string;

    /// Windows ACL相关功能（仅Windows平台可用）
    #[cfg(windows)]
    pub use crate::ace::{copy_aces, list_security_info};
    /// 配置类型
    pub use crate::config::{ConsumerConfig, ScanConfig, SyncJobConfig};
    /// 消费者相关类型
    pub use crate::consumer::{Consumer, ConsumerManager, DatabaseConsumer, StatisticConsumer};
    /// 错误处理类型
    pub use crate::error::{AppError, Result};
    /// 完整性检查
    pub use crate::integrity_check::integrity_check;
    /// Sync 编排器
    pub use crate::orchestrator::SyncOrchestrator;
    /// 文件删除功能
    pub use crate::rm::rm;
    /// 文件扫描功能
    pub use crate::scan::{ScanType, determine_scan_type, scan};
    /// 文件复制功能
    pub use crate::sync::{incremental_sync, sync};
}
