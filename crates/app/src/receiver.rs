//! 单进程复制，负责从源存储读取并写入目标存储。

use crate::file_copy::namespace as copy_namespace;
use data_mover::model::BackendKind;
// 标准库
use crate::error::StorageError;
use crate::file_copy::{CopiedPublication, CopyOptions, HdfsCopyContext, copy_file, prepare_directory};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use task_model::ObservedEntryExt;
use task_model::TransferMode;

// 外部 crate
use crate::storage_factory::StorageContext;
use data_mover::model::ObservedEntry;
use data_mover::qos::QosManager;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::message::{ReceiverMsg, SenderMsg};
use transport::traits::ReceiverTransport;

// 内部模块
use crate::error::{AppError, CopyStep, Result};
use crate::sync::{CommitRecorder, ResumeOpts, copy_file_with_resume, should_resume};

mod in_process;
pub(crate) use in_process::{LocalCopyExecutor, LocalCopyReceiver};

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

/// 单进程模式的 Receiver 主 task（R1 + R2 合一）
///
/// 从 transport 接收 `SenderMsg`，调用任务复制用例与原生存储角色执行写入。
/// 每完成一个 entry 后通过 transport 发送 `ReceiverMsg::EntrySuccess` 或 `EntryError`。
///
/// 单进程模式下收到 `CopyEntry` 消息时，Receiver 有 src+dest storage，
/// 直接调用 `process_entry_on_receiver()` 完成完整复制。
pub async fn receiver_task(
    transport: &(dyn ReceiverTransport + 'static), src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>,
    config: &ReceiverConfig, qos_manager: Option<QosManager>, bytes_tracker: Option<Arc<AtomicU64>>,
) -> Result<()> {
    info!("[Receiver] Started");

    while let Some(msg) = transport.recv().await {
        match msg {
            // ── 单进程模式：命令模式 ──
            SenderMsg::CopyEntry { entry } => {
                trace!("[Receiver] CopyEntry: {:?}", entry.get_relative_path());

                match process_entry_on_receiver(
                    &entry,
                    &src_storage,
                    &dest_storage,
                    config,
                    qos_manager.clone(),
                    bytes_tracker.clone(),
                )
                .await
                {
                    Ok(_) => {
                        if let Err(e) = transport.send(ReceiverMsg::EntrySuccess { entry }).await {
                            warn!("[Receiver] Failed to send EntrySuccess: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("[Receiver] CopyEntry failed for {:?}: {}", entry.get_relative_path(), e);
                        if let Err(send_err) = transport
                            .send(ReceiverMsg::EntryError {
                                entry,
                                reason: format!("{e}"),
                            })
                            .await
                        {
                            warn!("[Receiver] Failed to send EntryError: {}", send_err);
                        }
                    }
                }
            }

            // ── 单进程模式：tar 打包结果（Sender 完成打包后通知 Receiver） ──
            SenderMsg::TarPacked {
                tar_entry,
                manifest_entries,
            } => {
                trace!("[Receiver] TarPacked: {:?}", tar_entry.get_relative_path());
                if let Err(e) = transport
                    .send(ReceiverMsg::TarSuccess {
                        tar_entry,
                        manifest_entries,
                    })
                    .await
                {
                    warn!("[Receiver] Failed to send TarSuccess: {}", e);
                }
            }

            // ── 控制消息 ──
            SenderMsg::TransferDone => {
                info!("[Receiver] Received TransferDone, shutting down");
                if let Err(e) = transport.send(ReceiverMsg::AllDone).await {
                    warn!("[Receiver] Failed to send AllDone: {}", e);
                }
                break;
            }

            SenderMsg::EntryError { path, reason, .. } => {
                error!("[Receiver] Sender reported error for {}: {}", path.display(), reason);
            }

            // 双进程模式的消息（Phase 3 实现）
            _ => {
                debug!("[Receiver] Ignoring unhandled message type (dual-process mode, Phase 3)");
            }
        }
    }

    info!("[Receiver] Completed");
    Ok(())
}

/// 在 Receiver 侧执行完整的 entry 复制（对应原 `process_entry`）
///
/// 根据 entry 类型（目录/符号链接/文件）dispatch 到不同的存储操作。
/// 单进程模式下 Receiver 拥有 src+dest storage，可以直接调用 `copy_file`。
pub(crate) async fn process_entry_on_receiver(
    entry: &Arc<ObservedEntry>, src_storage: &Arc<StorageContext>, dest_storage: &Arc<StorageContext>,
    config: &ReceiverConfig, qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
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
            copy_namespace::create_symlink(dest_storage.storage(), entry, &target_path)
                .await
                .map_err(|e| AppError::copy_entry_failure(CopyStep::CreateDestinationLink, relative_path, e))?;
            Ok(None)
        } else {
            // 所有文件使用同一显式策略；检查点及原子发布由上游管理。
            let resume = ResumeOpts {
                job_id: config.job_id.clone(),
                job_dir: config.job_dir.clone(),
                transfer_mode: config.transfer_mode,
                cancel: config.cancel.clone(),
            };
            let published = if dest_storage.as_ref().storage().kind() == BackendKind::Hdfs {
                let hdfs = HdfsCopyContext::from_job(&config.job_id, config.transfer_mode)?;
                let recorder = if config.transfer_mode == TransferMode::Checkpoint {
                    Some(CommitRecorder::start(&config.job_dir, entry, src_storage.block_size()).await?)
                } else {
                    None
                };
                let hdfs = recorder
                    .as_ref()
                    .map_or(hdfs.clone(), |recorder| hdfs.with_commit_callback(recorder.callback()));
                let result = copy_file(
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
                    Some(&hdfs),
                )
                .await;
                if let Some(recorder) = recorder {
                    recorder.finish(result).await?
                } else {
                    result?
                }
            } else if should_resume(&resume, entry, src_storage, dest_storage) {
                copy_file_with_resume(
                    entry,
                    src_storage,
                    dest_storage,
                    CopyOptions {
                        transfer_mode: config.transfer_mode,
                        qos: qos_manager,
                        enable_integrity_check: config.enable_integrity_check,
                        bytes_counter,
                        cancel: config.cancel.clone(),
                    },
                    &config.job_dir,
                )
                .await?
            } else {
                copy_file(
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
                    None,
                )
                .await
                .map_err(|e| AppError::copy_entry_failure(CopyStep::Transfer, relative_path, e))?
            };

            // 普通传输和检查点传输都在发布前应用协商后的元数据。
            Ok(published)
        }
    }
    .instrument(span)
    .await
}

#[cfg(test)]
#[path = "receiver/copy_failure_tests.rs"]
mod copy_failure_tests;
