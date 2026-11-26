//! 单进程复制，负责从源存储读取并写入目标存储。

use crate::copy_bytes::CopyBytes;
use crate::file_copy::local::copy_file_for_job;
use crate::file_copy::namespace as copy_namespace;
// 标准库
use crate::error::StorageError;
use crate::file_copy::{CopiedPublication, CopyOptions, prepare_directory};
use std::sync::Arc;
use task_model::ObservedEntryExt;
use task_model::TransferMode;

// 外部 crate
use crate::storage_factory::StorageContext;
use data_mover::model::ObservedEntry;
use data_mover::qos::QosManager;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

// 内部模块
use crate::error::{AppError, CopyStep, Result};

mod in_process;
pub(crate) use in_process::{
    LocalCopyCompletion, LocalCopyExecutor, LocalCopyOutcome, LocalCopyPhase, LocalCopyPhaseProof, LocalCopyReceiver,
    LocalCopyWork,
};

/// Receiver 运行时配置
#[allow(clippy::struct_excessive_bools)] // 按用途分组的开关集合，重构为枚举会失去可读性
pub struct ReceiverConfig {
    pub job_id: String,
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    /// 任务目录（字节级断点续传状态存于 `<job_dir>/byte_resume/`）；为空则禁用续传
    pub job_dir: String,
    /// 显式选定的写入模式。
    pub transfer_mode: TransferMode,
    /// 当前任务的取消信号；独立调用方可以不绑定任务。
    pub cancel: Option<CancellationToken>,
}

/// 在 Receiver 侧执行完整的 entry 复制（对应原 `process_entry`）
///
/// 根据 entry 类型（目录/符号链接/文件）dispatch 到不同的存储操作。
/// 单进程模式下 Receiver 拥有 src+dest storage，可以直接调用 `copy_file`。
pub(crate) async fn process_entry_on_receiver(
    entry: &Arc<ObservedEntry>, src_storage: &Arc<StorageContext>, dest_storage: &Arc<StorageContext>,
    config: &ReceiverConfig, qos_manager: Option<QosManager>, bytes_counter: Option<Arc<CopyBytes>>,
) -> Result<Option<CopiedPublication>> {
    let relative_path = entry.get_relative_path();
    let span = info_span!("recv_entry", path = %relative_path.display());

    async {
        if config.cancel.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(StorageError::Cancelled.into());
        }
        if prepare_directory(entry, dest_storage).await? {
            Ok(None)
        } else if entry.get_is_symlink() {
            let target_path = copy_namespace::read_symlink(src_storage.storage(), entry)
                .await
                .map_err(|e| AppError::copy_entry_failure(CopyStep::ReadSourceLink, relative_path, e))?;
            copy_namespace::create_symlink(dest_storage.storage(), entry, &target_path, config.cancel.as_ref())
                .await
                .map_err(|e| AppError::copy_entry_failure(CopyStep::CreateDestinationLink, relative_path, e))?;
            Ok(None)
        } else {
            copy_file_for_job(
                src_storage,
                dest_storage,
                entry,
                CopyOptions {
                    transfer_mode: config.transfer_mode,
                    qos: qos_manager,
                    enable_integrity_check: config.enable_integrity_check,
                    bytes_counter,
                    cancel: config.cancel.clone(),
                },
                &config.job_id,
                &config.job_dir,
                |error| AppError::copy_entry_failure(CopyStep::Transfer, relative_path, error),
            )
            .await
        }
    }
    .instrument(span)
    .await
}

#[cfg(test)]
#[path = "receiver/copy_failure_tests.rs"]
mod copy_failure_tests;
