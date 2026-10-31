//! 存储编排使用的公共选项和后端配置类型。

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use data_mover::transfer::TransferPolicy;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::filter::FilterExpression;
use crate::qos::QosManager;
use data_mover::storage::HdfsConfig;

#[derive(Debug, Clone)]
pub struct WalkOptions {
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

/// sync 的显式写入模式，原样传递给上游，不按文件大小或后端静默降级。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferMode {
    Checkpoint,
    #[default]
    Atomic,
    Direct,
}

impl TransferMode {
    pub const fn from_flags(resume: bool, in_place: bool) -> Self {
        if resume {
            Self::Checkpoint
        } else if in_place {
            Self::Atomic
        } else {
            Self::Direct
        }
    }

    pub const fn policy(self) -> TransferPolicy {
        match self {
            Self::Checkpoint => TransferPolicy::Checkpointed,
            Self::Atomic => TransferPolicy::AtomicReplace,
            Self::Direct => TransferPolicy::Direct,
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
