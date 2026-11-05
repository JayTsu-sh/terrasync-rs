//! 存储编排使用的公共选项和后端配置类型。

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use data_mover::transfer::TransferPolicy;
use task_model::TransferMode;
use tokio_util::sync::CancellationToken;

use crate::filter::FilterExpression;
use crate::qos::QosManager;
use data_mover::storage::HdfsConfig;

#[derive(Debug, Clone)]
pub struct WalkOptions {
    /// 单个远程存储操作的等待上限；独立于自动恢复，不包含消费者背压。
    pub operation_timeout: Option<std::time::Duration>,
    /// 调用方显式选择的故障恢复窗口；None 直接报告失败，不自动重连。
    pub recovery_window: Option<std::time::Duration>,
    pub depth: Option<usize>,
    pub match_expressions: Option<FilterExpression>,
    pub exclude_expressions: Option<FilterExpression>,
    pub concurrency: usize,
    pub include_tags: bool,
    pub packaged: bool,
    pub package_depth: usize,
}

impl Default for WalkOptions {
    fn default() -> Self {
        Self {
            operation_timeout: None,
            recovery_window: None,
            depth: None,
            match_expressions: None,
            exclude_expressions: None,
            concurrency: 1,
            include_tags: false,
            packaged: false,
            package_depth: 0,
        }
    }
}

#[derive(Clone, Default)]
pub struct CopyOptions {
    pub transfer_mode: TransferMode,
    pub qos: Option<QosManager>,
    pub enable_integrity_check: bool,
    pub is_source_reserved: bool,
    pub bytes_counter: Option<Arc<AtomicU64>>,
    pub cancel: Option<CancellationToken>,
}

#[derive(Clone, Default)]
pub struct TarPackOptions {
    pub qos: Option<QosManager>,
    pub bytes_counter: Option<Arc<AtomicU64>>,
}

/// 后端专属的创建配置。
///
/// 仅当后端有不属于存储地址或公共创建选项的显式配置时，才增加对应变体。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum BackendConfig {
    #[default]
    Default,
    Hdfs(HdfsConfig),
}

/// 统一存储工厂使用的类型化选项。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateStorageOptions {
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
    pub backend: BackendConfig,
}

impl CreateStorageOptions {
    #[must_use]
    pub const fn new(block_size: Option<u64>, ensure_dir: bool) -> Self {
        Self {
            block_size,
            ensure_dir,
            backend: BackendConfig::Default,
        }
    }
}

pub(crate) const fn transfer_policy(mode: TransferMode) -> TransferPolicy {
    match mode {
        TransferMode::Checkpoint => TransferPolicy::Checkpointed,
        TransferMode::Atomic => TransferPolicy::AtomicReplace,
        TransferMode::Direct => TransferPolicy::Direct,
    }
}
