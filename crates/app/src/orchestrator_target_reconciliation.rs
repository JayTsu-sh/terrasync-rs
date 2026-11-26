//! 复制完成后的目标清理与 rename；任务调度和通知生命周期仍由父模块负责。
use crate::copy_bytes::CopyBytes;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

use data_mover::{
    model::{BackendKind, ObservedEntry},
    qos::QosManager,
};
use db::{ClickHouseDatabase, DeletionStatus};
use task_model::{
    ChangeKind, EntryChange, ErrorEvent, ObservedEntryExt, SourcePresence, StorageEntryMessage, TargetDeletionOutcome,
    TargetRetention, TargetRetentionReason, TransferMode,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::SyncOrchestrator;
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::local::CommitRecorder;
use crate::file_copy::{CopyOptions, HdfsCopyContext, copy_file};
use crate::file_copy::{metadata as copy_metadata, namespace as copy_namespace};
use crate::rename::{RenameOptions, RenameOutcome, rename_or_copy};
use crate::storage_factory::StorageContext;
use crate::sync::{ResumeOpts, process_entry};
use crate::target_absence::{SourceAbsenceCheck, preserve};

async fn publish_target_deletion(
    broadcaster: &BroadcastForwarder<StorageEntryMessage>, entry: &Arc<ObservedEntry>, source: SourcePresence,
    outcome: TargetDeletionOutcome, reason: Option<String>,
) -> Result<()> {
    if entry.get_is_regular_file() && entry.history().is_none() {
        broadcaster
            .broadcast(StorageEntryMessage::TargetDeletion {
                entry: Arc::clone(entry),
                source,
                outcome,
                reason,
            })
            .await?;
    } else if outcome.confirms_absence() {
        broadcaster
            .broadcast(StorageEntryMessage::Deleted(Arc::clone(entry)))
            .await?;
    }
    Ok(())
}

/// 目标清理复用既有缺失证明和错误传播，逐项结果随原广播批处理。
async fn reconcile_target_deletion(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, is_source_reserved: bool,
    events: &BroadcastForwarder<StorageEntryMessage>, source_absence: &SourceAbsenceCheck,
) -> Result<Option<AppError>> {
    // 搬迁不将源缺失反向删除目标；rename 仍需推进，不能跳过整个阶段。
    if !is_source_reserved {
        let reason = "源端删除迁移保留目标；本轮未观察到该源项，未核验其存在性";
        events
            .broadcast(StorageEntryMessage::TargetPreserved {
                retention: Box::new(TargetRetention::entry(
                    entry.path(),
                    TargetRetentionReason::SourceCleanupPolicy,
                )),
                reason: reason.into(),
            })
            .await?;
        publish_target_deletion(
            events,
            entry,
            SourcePresence::Unknown,
            TargetDeletionOutcome::Preserved,
            Some(reason.into()),
        )
        .await?;
        return Ok(None);
    }
    let source_presence = match source_absence.confirm(source, entry, events).await {
        Ok(presence) => presence,
        Err(error) => {
            if !source_absence.cancellation().is_cancelled() {
                let _ = publish_target_deletion(
                    events,
                    entry,
                    SourcePresence::Unknown,
                    TargetDeletionOutcome::Failed,
                    Some(error.to_string()),
                )
                .await;
            }
            return Err(error);
        }
    };
    if source_presence != SourcePresence::Absent {
        publish_target_deletion(
            events,
            entry,
            source_presence,
            TargetDeletionOutcome::Preserved,
            Some("源端缺失未获确认，保留目标".into()),
        )
        .await?;
        return Ok(None);
    }
    // 仅删除本条目或空目录；未核验的后代不能据此授权删除。
    source_absence.check_active()?;
    let outcome = match copy_namespace::delete_file(destination.storage(), entry).await {
        Ok(()) => TargetDeletionOutcome::Deleted,
        Err(error) if error.is_not_found() => TargetDeletionOutcome::AlreadyAbsent,
        Err(error) => {
            preserve(
                events,
                entry,
                SourcePresence::Absent,
                TargetRetentionReason::StaleEvidence,
                format!("源路径已确认缺失，但目标删除未获成功确认；保留尚存部分，删除结果可能未知: {error}"),
            )
            .await?;
            publish_target_deletion(
                events,
                entry,
                source_presence,
                TargetDeletionOutcome::Failed,
                Some(error.to_string()),
            )
            .await?;
            error!("Phase B: Failed to delete {:?}: {}", entry.get_relative_path(), error);
            events
                .broadcast(StorageEntryMessage::Error {
                    event: ErrorEvent::Delete,
                    path: entry.get_relative_path().to_path_buf(),
                    entry: Some(Arc::clone(entry)),
                    reason: error.to_string(),
                })
                .await?;
            return Ok(Some(AppError::from(error).with_reported_copy_failure(false)));
        }
    };
    publish_target_deletion(events, entry, source_presence, outcome, None).await?;
    Ok(None)
}

impl SyncOrchestrator {
    /// rename 后的内容与元数据必须完成，才能确认该条目的新状态。
    #[allow(clippy::too_many_arguments)]
    async fn sync_renamed_file_changes(
        kind: ChangeKind, entry: &Arc<ObservedEntry>, src: &StorageContext, dest: &StorageContext,
        qos: Option<QosManager>, integrity: bool, bytes: Option<Arc<CopyBytes>>, job_id: &str, job_dir: &str,
        transfer_mode: TransferMode, cancel: CancellationToken,
    ) -> Result<()> {
        let mut metadata_applied = false;
        if kind != ChangeKind::MetadataOnly {
            let hdfs = if dest.storage().kind() == BackendKind::Hdfs {
                Some(HdfsCopyContext::from_job(job_id, transfer_mode)?)
            } else {
                None
            };
            let recorder = if hdfs.is_some() && transfer_mode == TransferMode::Checkpoint {
                Some(CommitRecorder::start(job_dir, entry, src.block_size()).await?)
            } else {
                None
            };
            let hdfs = hdfs.map(|context| {
                recorder.as_ref().map_or(context.clone(), |recorder| {
                    context.with_commit_callback(recorder.callback())
                })
            });
            let result = copy_file(
                src,
                dest,
                entry,
                CopyOptions {
                    transfer_mode,
                    qos,
                    enable_integrity_check: integrity,
                    bytes_counter: bytes,
                    cancel: Some(cancel.clone()),
                },
                hdfs.as_ref(),
            )
            .await;
            if let Some(recorder) = recorder {
                recorder.finish(result).await?;
            } else {
                result?;
            }
            metadata_applied = true;
        }
        if !metadata_applied {
            copy_metadata::set_entry_metadata(dest.storage(), entry, Some(cancel)).await?;
        }
        Ok(())
    }

    /// DB 写入由 [`DatabaseConsumer`](crate::consumer::DatabaseConsumer) 在收到 `Deleted` / `Renamed` /
    /// `Error` 消息后自动完成（走和 handler workers 同一套批量重试逻辑）；本函数只负责
    /// 「物理操作 + 广播结果消息」。保留首个类型化错误，处理完当前阶段后返回。
    ///
    /// # 两遍处理设计
    ///
    /// **第一遍**：收集所有 `DeletionStatus`，从中提取「目录名发生变化的 rename」
    /// 构建 `renamed_dirs` 映射（`old_dir_path` → `new_dir_path`）。
    ///
    /// **第二遍**：顺序执行物理操作。对每条 `Renamed(from, to)` 判断是否跳过：
    /// - `from.name != to.name`：目录/文件本身改了名字 → 必须调用 `rename_or_copy`
    /// - `from.name == to.name` 且 `from.parent` 在 `renamed_dirs` 中且映射到 `to.parent`：
    ///   父目录的 NFS RENAME 已传递性地移动了这个子条目 → 跳过物理操作
    /// - `from.name == to.name` 但父目录未在 `renamed_dirs` 中（跨父目录 move）：
    ///   没有父目录 rename 代为处理 → 必须调用 `rename_or_copy`
    ///
    /// 对于 rename 成功后检测到内容或元数据也同时变化的普通文件，额外执行内容拷贝
    /// 或元数据更新（处理「rename + data/metadata changed」复合场景）。
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_deletions_and_renames(
        db: &ClickHouseDatabase, src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>,
        is_source_reserved: bool, broadcaster: &BroadcastForwarder<StorageEntryMessage>, enable_integrity_check: bool,
        qos_manager: Option<QosManager>, bytes_counter: Option<Arc<CopyBytes>>, job_id: &str, job_dir: &str,
        transfer_mode: TransferMode, source_absence: SourceAbsenceCheck,
    ) -> Result<()> {
        source_absence.check_active()?;
        let deletion_iter = db.detect_deleted_items().await?;
        let mut failure: Option<AppError> = None;

        // 第一遍：收集目录 rename 计划及待确认状态。
        // 映射同时保存目标路径和实际移动确认，计划存在不代表子树已经移动。
        // 收录所有 is_dir=true 的 Renamed 条目（包括同名跨父目录 move 如 d3/d3_4 → d4/d3_4）。
        // 用于判断某条目的父目录是否正在被 rename，若是则子条目无需单独执行物理操作。
        let mut statuses: Vec<DeletionStatus> = deletion_iter.collect();
        // 先移动父目录，再清理旧子项／空父目录；避免正常目录移除被非空检查误报失败。
        // 仅重排已经物化的候选，不新增按全部扫描条目增长的状态。
        statuses.sort_unstable_by(|left, right| reconciliation_order(left).cmp(&reconciliation_order(right)));
        let mut renamed_dirs: HashMap<PathBuf, (PathBuf, bool)> = statuses
            .iter()
            .filter_map(|s| {
                if let DeletionStatus::Renamed(from, to) = s
                    && from.get_is_dir()
                {
                    return Some((
                        from.get_relative_path().to_path_buf(),
                        (to.get_relative_path().to_path_buf(), false),
                    ));
                }
                None
            })
            .collect();

        // 无句柄的源历史按 path/version 连续返回；同一 key 的保留决定只交付一次。
        let mut last_preserved_path = None;
        // 第二遍：按顺序执行物理操作
        let processing = async {
            for status in statuses {
                source_absence.check_active()?;
                match status {
                    DeletionStatus::Deleted(entry) => {
                        if last_preserved_path.as_ref() == Some(entry.path()) {
                            continue;
                        }
                        last_preserved_path = Some(entry.path().clone());
                        if let Some(error) = reconcile_target_deletion(
                            &src_storage,
                            &dest_storage,
                            &Arc::new(entry),
                            is_source_reserved,
                            broadcaster,
                            &source_absence,
                        )
                        .await?
                        {
                            failure.get_or_insert(error);
                        }
                    }
                    DeletionStatus::Renamed(from, to) => {
                        let from_arc = Arc::new(*from);
                        let to_arc = Arc::new(*to);
                        let old_source_absent = source_absence.confirm(&src_storage, &from_arc, broadcaster).await?
                            == SourcePresence::Absent;
                        source_absence.check_active()?;
                        // 父目录正在被 rename 时，NFS RENAME 已传递性地移动了子条目，无需重复操作。
                        // from_parent 分配仅在同名情况下进行，避免对名字不同的 rename 做无效分配。
                        let skip_physical = from_arc.get_name() == to_arc.get_name() && {
                            let from_parent = from_arc.get_relative_path().parent().map(PathBuf::from);
                            let to_parent = to_arc.get_relative_path().parent().map(PathBuf::from);
                            match (from_parent, to_parent) {
                                (Some(fp), Some(tp)) => renamed_dirs
                                    .get(&fp)
                                    .is_some_and(|(path, completed)| *completed && path == &tp),
                                _ => false,
                            }
                        };

                        let dest_op_result = if !old_source_absent {
                            // 同一身份也可能是硬链接；旧源仍在或证据不足时复制新路径并保留旧事实。
                            process_entry(
                                &to_arc,
                                src_storage.clone(),
                                dest_storage.clone(),
                                enable_integrity_check,
                                false,
                                true,
                                qos_manager.clone(),
                                bytes_counter.clone(),
                                broadcaster,
                                ResumeOpts {
                                    job_id: job_id.into(),
                                    job_dir: job_dir.into(),
                                    transfer_mode,
                                    cancel: Some(source_absence.cancellation().clone()),
                                },
                            )
                            .await
                            .map(|()| RenameOutcome::Copied)
                        } else if skip_physical {
                            Ok(RenameOutcome::Moved)
                        } else {
                            rename_or_copy(
                                from_arc.clone(),
                                to_arc.clone(),
                                src_storage.clone(),
                                dest_storage.clone(),
                                RenameOptions {
                                    enable_integrity_check,
                                    enable_acl: false,
                                    resume: ResumeOpts {
                                        job_id: job_id.into(),
                                        job_dir: job_dir.into(),
                                        transfer_mode,
                                        cancel: Some(source_absence.cancellation().clone()),
                                    },
                                    qos: qos_manager.clone(),
                                    bytes: bytes_counter.clone(),
                                    events: broadcaster,
                                },
                            )
                            .await
                        };

                        match dest_op_result {
                            Ok(outcome) => {
                                if outcome == RenameOutcome::Moved
                                    && let Some((_, completed)) = renamed_dirs.get_mut(from_arc.get_relative_path())
                                {
                                    *completed = true;
                                }
                                // Phase A 不再把路径已变的文件判为 Changed（Fix 2a），
                                // 此处补足 rename+changed 文件的内容/元数据同步，并广播 Changed 消息供消费者统计。
                                if old_source_absent
                                    && from_arc.get_is_regular_file()
                                    && let Some(kind) = ChangeKind::from_entry_diff(&from_arc, &to_arc)
                                {
                                    let result = if outcome == RenameOutcome::Moved {
                                        Self::sync_renamed_file_changes(
                                            kind,
                                            &to_arc,
                                            &src_storage,
                                            &dest_storage,
                                            qos_manager.clone(),
                                            enable_integrity_check,
                                            bytes_counter.clone(),
                                            job_id,
                                            job_dir,
                                            transfer_mode,
                                            source_absence.cancellation().clone(),
                                        )
                                        .await
                                    } else {
                                        // 回退已经复制当前源内容并完成属性，无需重复写入。
                                        Ok(())
                                    };
                                    if let Err(error) = result {
                                        broadcaster
                                            .broadcast(StorageEntryMessage::CopyRenameFailed {
                                                from: from_arc,
                                                to: to_arc,
                                                change: Some(kind),
                                                reason: format!("目标已移动，后续内容或属性更新未完成: {error}"),
                                            })
                                            .await?;
                                        failure.get_or_insert_with(|| {
                                            error.with_reported_copy_failure(kind == ChangeKind::MetadataOnly)
                                        });
                                        continue;
                                    }
                                    broadcaster
                                        .broadcast(StorageEntryMessage::Changed {
                                            entry: to_arc.clone().into(),
                                            kind,
                                        })
                                        .await?;
                                }

                                if old_source_absent {
                                    let paths = (from_arc, to_arc);
                                    let event = match outcome {
                                        RenameOutcome::Moved => StorageEntryMessage::Renamed(paths),
                                        RenameOutcome::Copied => StorageEntryMessage::CopyRenameRecovered(paths),
                                    };
                                    broadcaster.broadcast(event).await?;
                                } else {
                                    broadcaster.broadcast(StorageEntryMessage::New(to_arc.into())).await?;
                                }
                            }
                            Err(e) => {
                                if source_absence.cancellation().is_cancelled()
                                    && e.storage_error().is_some_and(StorageError::is_cancelled)
                                {
                                    return Err(e);
                                }
                                error!(
                                    "Phase B: Failed to rename {:?} → {:?}: {}",
                                    from_arc.get_relative_path(),
                                    to_arc.get_relative_path(),
                                    e
                                );
                                let reason = e.to_string();
                                let event = if old_source_absent {
                                    StorageEntryMessage::CopyRenameFailed {
                                        from: from_arc,
                                        to: to_arc,
                                        change: None,
                                        reason,
                                    }
                                } else {
                                    StorageEntryMessage::CopyChangeFailed {
                                        entry: to_arc.into(),
                                        change: EntryChange::New,
                                        reason,
                                    }
                                };
                                broadcaster.broadcast(event).await?;
                                failure.get_or_insert_with(|| e.with_reported_copy_failure(false));
                            }
                        }
                    }
                }
            }
            db.mark_copy_target_reconciliation_completed()
                .await
                .map_err(AppError::from)
        }
        .await;
        match processing {
            Err(error) if source_absence.cancellation().is_cancelled() => Err(failure.unwrap_or(error)),
            Err(error) => Err(error),
            Ok(()) => failure.map_or(Ok(()), Err),
        }
    }
}

fn reconciliation_order(status: &DeletionStatus) -> (u8, usize, &str) {
    match status {
        DeletionStatus::Renamed(from, _) => (0, from.path().as_str().split('/').count(), from.path().as_str()),
        DeletionStatus::Deleted(entry) => (
            1,
            usize::MAX - entry.path().as_str().split('/').count(),
            entry.path().as_str(),
        ),
    }
}
