//! 双进程 Receiver 的落盘命令分派器。
//!
//! 文件的全量与差分写入均由 `FileCommitter` 管理。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use data_mover::model::ObservedEntry;
use data_mover::transfer::write_delete_marker;
use sync_delta::DeltaToken;
use task_model::observation::ObservedItem;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transport::chunk::DataChunk;
use transport::message::SessionConfig;

use crate::error::{Result, StorageError};
use crate::file_commit::{FileCommitter, FileTransferMode};
use crate::remote_receiver_session::ReceiverProgress;
use crate::storage_factory::StorageContext;

/// SPSC channel 消息，用于 Receiver 主 task 与 disk-commit task 之间的通信
#[derive(Debug)]
pub(crate) enum DiskCommitMsg {
    WriteDeleteMarker {
        ndx: i32,
        entry: ObservedItem,
    },

    // ── 全量文件传输（3 段流式驱动） ──
    /// 文件开始：Receiver 据此 resume_prepare + 起 write_chunk_stream
    FileBegin {
        ndx: i32,
        entry: ObservedItem,
    },
    /// 按活动文件的会话索引核验后转交数据块，避免每块持有完整观察。
    FileChunk {
        ndx: i32,
        chunk: DataChunk,
    },
    /// 文件传输结束，提交：读回 .part hash 校验 + set_metadata + ACL + 原子 rename
    FileCommit {
        ndx: i32,
        entry: Arc<ObservedEntry>,
        source_hash: Option<String>,
    },

    // ── 增量传输 (delta，issue #54 阶段 3 接入) ──
    /// 开始 delta 重建：Receiver 据此 resume_prepare + 起 write_chunk_stream（与 `FileBegin`
    /// 同构）。`ndx`：`DeltaMatch`/`DeltaData` 消息本身不带 ndx（dc task 严格串行，同一时刻
    /// 只有一个 `ActiveFile`），basis 读失败等 mid-stream 错误需要据此上报 outcome
    DeltaBegin {
        ndx: i32,
        entry: Arc<ObservedEntry>,
        block_size: u32,
    },
    /// Delta: 原始数据
    DeltaData {
        entry: Arc<ObservedEntry>,
        data: Bytes,
    },
    /// Delta: 引用目标端 basis file 的 block
    DeltaMatch {
        entry: Arc<ObservedEntry>,
        block_index: u32,
    },
    /// Delta 完成：读回 .part hash 校验 + 原子 rename（与 `FileCommit` 同构）
    DeltaCommit {
        ndx: i32,
        entry: Arc<ObservedEntry>,
        source_hash: Option<String>,
    },

    // ── 控制 ──
    /// 中止当前正在写入的文件（Sender 读源失败）：丢弃 `.part`，不发 ack
    AbortFile,
    /// 关闭 disk-commit task
    Shutdown,
}

/// disk-commit task 向 Receiver 主 task 汇报的结果
///
/// 全量与差分文件写入均交付 `FileOutcome` 与 `ndx`，
/// 由会话决定发送 `Success`、`Redo` 或 `Error`。
#[derive(Debug)]
pub(crate) struct DcAck {
    pub(crate) ndx: i32,
    pub(crate) outcome: FileOutcome,
}

/// 文件传输校验结果，用于 Receiver 主 task 的 redo 决策
#[derive(Debug)]
pub(crate) enum FileOutcome {
    /// 校验通过（或未启用完整性校验）
    Success,
    /// hash 校验失败，可 redo（每 ndx 至多重试一次）
    HashMismatch,
    /// 落盘实际字节数与 entry 声明大小不符，可 redo（每 ndx 至多重试一次）。独立于 hash
    /// 校验的防线：拦截 hash 校验关闭、或 hash 基于同一份被截断数据计算而"自洽"通过
    /// （即 hash 无法识别的截断损坏）的场景（issue #53）
    SizeMismatch,
    /// 硬错误（basis 读失败 / 写盘失败 / `resume_prepare` 失败等，非 hash 类），不可 redo
    HardError(String),
}

/// 串行分派 Receiver 落盘命令，并将文件命令交给单一生命周期所有者。
pub(crate) async fn disk_commit_task(
    dest: Arc<StorageContext>, session: SessionConfig, mut rx: mpsc::Receiver<DiskCommitMsg>,
    ack_tx: mpsc::UnboundedSender<DcAck>, progress: Arc<ReceiverProgress>, cancel: CancellationToken,
) -> Result<()> {
    let mut files = FileCommitter::new(dest.clone(), session, ack_tx.clone(), progress.clone(), cancel.clone());

    while let Some(msg) = rx.recv().await {
        match msg {
            DiskCommitMsg::WriteDeleteMarker { ndx, entry } => {
                let result = async {
                    let path = entry.view().source_path().map_err(StorageError::from)?;
                    write_delete_marker(dest.storage().clone(), path, cancel.clone())
                        .await
                        .map_err(StorageError::from)?;
                    Ok::<_, StorageError>(())
                }
                .await;
                let outcome = match result {
                    Ok(()) => {
                        progress.files_transferred.fetch_add(1, Ordering::Relaxed);
                        FileOutcome::Success
                    }
                    Err(error) => FileOutcome::HardError(error.to_string()),
                };
                let _ = ack_tx.send(DcAck { ndx, outcome });
            }
            DiskCommitMsg::FileBegin { ndx, entry } => files.begin_full(ndx, entry).await,
            DiskCommitMsg::FileChunk { ndx, chunk } => files.push_full(ndx, chunk).await,
            DiskCommitMsg::FileCommit {
                ndx,
                entry,
                source_hash,
            } => files.commit(ndx, &entry, source_hash, FileTransferMode::Full).await,
            DiskCommitMsg::DeltaBegin { ndx, entry, block_size } => {
                files.begin_delta(ndx, entry, block_size).await;
            }
            DiskCommitMsg::DeltaMatch { entry, block_index } => {
                files.push_delta(&entry, DeltaToken::Match { block_index }).await;
            }
            DiskCommitMsg::DeltaData { entry, data } => {
                files.push_delta(&entry, DeltaToken::Data(data)).await;
            }
            DiskCommitMsg::DeltaCommit {
                ndx,
                entry,
                source_hash,
            } => files.commit(ndx, &entry, source_hash, FileTransferMode::Delta).await,
            DiskCommitMsg::AbortFile => files.abort().await,
            DiskCommitMsg::Shutdown => {
                files.shutdown().await;
                break;
            }
        }
    }
    files.shutdown().await;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests;
