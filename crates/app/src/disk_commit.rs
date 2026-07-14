//! 双进程模式 Receiver 的落盘任务：串行消费 `DiskCommitMsg`，
//! 对全量文件用 data-mover 3 段 API（`resume_prepare` / `write_chunk_stream` /
//! `commit_chunk_stream`）流式写入 `.part`，`FileCommit` 时读回 `.part` hash
//! 校验后原子 rename。目录/符号链接即时完成。

// 标准库
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

// 外部 crate
use data_mover::{CommitCallback, DataChunk, EntryEnum, StorageEnum, StreamHandle};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use transport::message::{DcAck, DiskCommitMsg, FileOutcome, ReceiverMsg, SessionConfig};

// 内部模块
use crate::byte_resume::part_path_for;
use crate::error::{AppError, Result};
use crate::receiver::ReceiverProgress;

/// 当前正在流式写入的文件上下文。
struct ActiveFile {
    /// 目标端文件 entry（提交 rename 的目标 + `set_metadata` 依据）
    entry: Arc<EntryEnum>,
    /// 向 `write_chunk_stream` 内部 channel 转发数据块的发送端
    tx_inner: mpsc::Sender<DataChunk>,
    /// 后台写任务句柄，channel 关闭后收尾并返回结果
    write_join: JoinHandle<data_mover::error::Result<()>>,
    /// 提交时用于 rename 的 stream handle（write 任务持有其 clone）
    handle: StreamHandle,
    /// `.part` 临时文件的目标端相对路径
    part_path: PathBuf,
    /// 文件总大小（提交时 `set_file_len` + hash 校验用）
    size: u64,
}

/// 双进程 Receiver 落盘任务。串行消费 `DiskCommitMsg`，每完成一个 entry
/// 通过 `ack_tx` 发送 `ReceiverMsg::EntrySuccess` / `EntryError`。
pub async fn disk_commit_task(
    dest: Arc<StorageEnum>, session: SessionConfig, mut rx: mpsc::Receiver<DiskCommitMsg>,
    ack_tx: mpsc::UnboundedSender<DcAck>, progress: Arc<ReceiverProgress>,
) -> Result<()> {
    let mut active: Option<ActiveFile> = None;

    while let Some(msg) = rx.recv().await {
        match msg {
            DiskCommitMsg::CreateDir { entry } => {
                if let Err(e) = dest.create_dir_all(&entry).await {
                    warn!("[dc] create_dir {:?}: {}", entry.get_relative_path(), e);
                }
                let _ = dest.set_entry_metadata(&entry).await;
                progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntrySuccess { entry }));
            }
            DiskCommitMsg::CreateSymlink { entry, target } => match dest.create_symlink(&entry, &target).await {
                Ok(()) => {
                    progress.files_transferred.fetch_add(1, Ordering::Relaxed);
                    let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntrySuccess { entry }));
                }
                Err(e) => {
                    let _ = ack_tx.send(DcAck::Entry(ReceiverMsg::EntryError {
                        entry,
                        reason: format!("{e}"),
                    }));
                }
            },
            DiskCommitMsg::FileBegin { ndx, entry } => {
                let part_path = part_path_for(entry.get_relative_path());
                match StorageEnum::resume_prepare(&dest, &entry, &part_path, false).await {
                    Ok((_missing, handle)) => {
                        let (tx_inner, rx_inner) = mpsc::channel::<DataChunk>(8);
                        let dest2 = dest.clone();
                        let entry2 = entry.clone();
                        let handle2 = handle.clone();
                        let write_join = tokio::spawn(async move {
                            StorageEnum::write_chunk_stream(&dest2, &entry2, rx_inner, &handle2, None, noop_commit())
                                .await
                        });
                        let size = entry.get_size();
                        active = Some(ActiveFile {
                            entry,
                            tx_inner,
                            write_join,
                            handle,
                            part_path,
                            size,
                        });
                    }
                    Err(e) => {
                        error!("[dc] resume_prepare {:?}: {}", entry.get_relative_path(), e);
                        let _ = ack_tx.send(DcAck::FileOutcome {
                            ndx,
                            outcome: FileOutcome::HardError(format!("{e}")),
                        });
                    }
                }
            }
            DiskCommitMsg::FileChunk { entry, chunk } => {
                // 保留 active：后续 FileCommit 仍需 join 写任务并上报那唯一的真实 outcome。
                // 写任务已死时每个残余 chunk 都会 send 失败，故降为 debug 避免刷屏。
                if let Some(a) = active.as_ref()
                    && a.tx_inner.send(chunk).await.is_err()
                {
                    debug!("[dc] write channel closed early for {:?}", entry.get_relative_path());
                }
            }
            DiskCommitMsg::FileCommit {
                ndx,
                entry,
                source_hash,
            } => {
                if let Some(a) = active.take() {
                    finalize_file(&dest, &session, ndx, a, source_hash, &ack_tx, &progress).await;
                } else {
                    // FileBegin 失败时已上报过唯一的 outcome，这里不再重复上报，只记日志。
                    warn!("[dc] FileCommit without active stream: {:?}", entry.get_relative_path());
                }
            }
            DiskCommitMsg::AbortFile => {
                // Sender 读源失败并已发出 EntryError，丢弃当前正在写入的文件：
                // 关闭写 channel → 等写任务收尾 → 删除 .part。不再发 ack，避免与 Sender 重复信号。
                if let Some(a) = active.take() {
                    let ActiveFile {
                        entry,
                        tx_inner,
                        write_join,
                        part_path,
                        ..
                    } = a;
                    drop(tx_inner);
                    let _ = write_join.await;
                    remove_part(&dest, &entry, &part_path).await;
                }
            }
            DiskCommitMsg::Shutdown => break,
            // delta / tar 变体不走 disk_commit task（见 receiver 路由）
            _ => {}
        }
    }
    Ok(())
}

/// 提交单个文件：关闭写 channel → 等写任务收尾 → 读回 `.part` hash 校验 →
/// `commit_chunk_stream`（原子 rename）→ `set_metadata` → 上报 outcome。
///
/// 任一步失败：删除 `.part` 并上报 `FileOutcome`（hash 不符为 `HashMismatch`，可 redo；
/// 其余为 `HardError`，不可 redo）；是否触发 redo 由 Receiver 主 task 统一决策，本函数
/// 不再直接发终态 `ReceiverMsg`。
async fn finalize_file(
    dest: &Arc<StorageEnum>, session: &SessionConfig, ndx: i32, a: ActiveFile, source_hash: Option<String>,
    ack_tx: &mpsc::UnboundedSender<DcAck>, progress: &Arc<ReceiverProgress>,
) {
    let ActiveFile {
        entry,
        tx_inner,
        write_join,
        handle,
        part_path,
        size,
    } = a;
    // 关闭 channel → write_chunk_stream 收尾
    drop(tx_inner);

    // 等写任务结束：JoinError 与 StorageError 都经 #[from] 收敛为 AppError
    let write_result: Result<()> = match write_join.await {
        Ok(inner) => inner.map_err(AppError::from),
        Err(join_err) => Err(AppError::from(join_err)),
    };
    if let Err(e) = write_result {
        error!("[dc] write {:?}: {}", entry.get_relative_path(), e);
        remove_part(dest, &entry, &part_path).await;
        let _ = ack_tx.send(DcAck::FileOutcome {
            ndx,
            outcome: FileOutcome::HardError(format!("{e}")),
        });
        return;
    }

    // 读回 .part hash 校验（verify-before-rename）
    if session.enable_integrity_check
        && let Some(expected) = source_hash.as_ref()
    {
        match dest.compute_hash(&part_path, size).await {
            Ok(actual) if &actual == expected => {}
            Ok(actual) => {
                error!(
                    "[dc] hash mismatch {:?}: {} != {}",
                    entry.get_relative_path(),
                    actual,
                    expected
                );
                remove_part(dest, &entry, &part_path).await;
                let _ = ack_tx.send(DcAck::FileOutcome {
                    ndx,
                    outcome: FileOutcome::HashMismatch,
                });
                return;
            }
            Err(e) => {
                remove_part(dest, &entry, &part_path).await;
                let _ = ack_tx.send(DcAck::FileOutcome {
                    ndx,
                    outcome: FileOutcome::HardError(format!("hash read-back: {e}")),
                });
                return;
            }
        }
    }

    // size 断言（commit 前，独立于 hash 校验的防线）：`commit_chunk_stream` 内部会用
    // `set_file_len` 把 `.part` 强制补齐/截断到声明大小再 rename，若不在此拦截，截断
    // 的 `.part` 会被静默补零后当作"大小正确"提交（size 对、内容错）；这层拦截 hash
    // 校验关闭、或 hash 基于同一份被截断数据计算而"自洽"通过（同源失明）的场景。
    match dest.get_metadata(&part_path).await {
        Ok(meta) if meta.get_size() == size => {}
        Ok(meta) => {
            error!(
                "[dc] size mismatch {:?}: part={} expected={}",
                entry.get_relative_path(),
                meta.get_size(),
                size
            );
            remove_part(dest, &entry, &part_path).await;
            let _ = ack_tx.send(DcAck::FileOutcome {
                ndx,
                outcome: FileOutcome::SizeMismatch,
            });
            return;
        }
        Err(e) => {
            remove_part(dest, &entry, &part_path).await;
            let _ = ack_tx.send(DcAck::FileOutcome {
                ndx,
                outcome: FileOutcome::HardError(format!("part size read-back: {e}")),
            });
            return;
        }
    }

    // 原子 rename：.part → 最终文件
    if let Err(e) = StorageEnum::commit_chunk_stream(dest, &entry, size, handle).await {
        error!("[dc] commit {:?}: {}", entry.get_relative_path(), e);
        remove_part(dest, &entry, &part_path).await;
        let _ = ack_tx.send(DcAck::FileOutcome {
            ndx,
            outcome: FileOutcome::HardError(format!("{e}")),
        });
        return;
    }

    let _ = dest.set_entry_metadata(&entry).await;
    progress.files_transferred.fetch_add(1, Ordering::Relaxed);
    progress.bytes_transferred.fetch_add(size, Ordering::Relaxed);
    let _ = ack_tx.send(DcAck::FileOutcome {
        ndx,
        outcome: FileOutcome::Success,
    });
}

/// 删除残留的 `.part` 文件。data-mover 的删除 API 以 `EntryEnum` 为参，
/// 故用原 entry 派生一个指向 `.part` 相对路径的临时 entry 再删。失败忽略
/// （清理是尽力而为，`.part` 后缀不会污染最终结果）。
async fn remove_part(dest: &StorageEnum, entry: &EntryEnum, part_path: &Path) {
    let part_entry = part_entry_of(entry, part_path);
    let _ = dest.delete_file(&part_entry).await;
}

/// 由原 entry 克隆出一个 `relative_path` 指向 `.part` 的 entry（仅用于删除 `.part`）。
fn part_entry_of(entry: &EntryEnum, part_path: &Path) -> EntryEnum {
    let mut part = entry.clone();
    match &mut part {
        EntryEnum::NAS(e) => e.relative_path = part_path.to_path_buf(),
        EntryEnum::S3(e) => e.relative_path = part_path.to_string_lossy().into_owned(),
    }
    part
}

/// `disk_commit` task 不做逐 chunk 进度回调，提交回调为空操作。
fn noop_commit() -> CommitCallback {
    Arc::new(|_off, _len| {})
}
