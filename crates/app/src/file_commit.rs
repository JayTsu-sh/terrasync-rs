//! Receiver 单文件落盘生命周期：消费 full/delta 文件命令，
//! 通过存储适配层驱动 expert destination 的 prepare、write、complete 生命周期。
//! 接收完成后先校验长度及源摘要，再由引擎读回校验并发布；目录和符号链接即时完成。
//! delta token 经 Reconstructor 重建，basis 区间通过源角色读取。

// 标准库
use std::sync::Arc;
use std::sync::atomic::Ordering;

// 外部 crate
use bytes::{Bytes, BytesMut};
use storage_adapter::error::Result as StorageResult;
use storage_adapter::{DataChunk, EntryEnum, StorageEnum, StreamChunk, StreamHandle};
use sync_delta::DeltaToken;
use sync_delta::reconstruct::{ReconstructStep, Reconstructor};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use transport::message::{DcAck, FileOutcome, SessionConfig};

// 内部模块
use crate::error::{AppError, Result};
use crate::receiver::{ReceiverProgress, validate_relative_path};

/// 当前正在流式写入的文件上下文。
struct ActiveFile {
    /// Receiver 请求索引，在完整落盘生命周期中保持权威关联。
    ndx: i32,
    /// 目标端文件 entry（最终路径及暂存元数据计划的依据）
    entry: Arc<EntryEnum>,
    /// 向 `write_chunk_stream` 内部 channel 转发数据块的发送端
    tx_inner: mpsc::Sender<StreamChunk>,
    /// 后台写任务句柄，channel 关闭后收尾并返回结果
    write_join: JoinHandle<StorageResult<()>>,
    /// expert destination 的会话状态（write 任务持有其 clone）
    handle: StreamHandle,
    /// 声明的文件总大小（输入长度及摘要校验用）
    size: u64,
    /// 权威 transfer mode；delta 状态只存在于 delta variant 中。
    mode: ActiveFileMode,
}

enum ActiveFileMode {
    Full,
    Delta(DeltaCtx),
}

enum FileCommitState {
    Idle,
    FullActive(ActiveFile),
    DeltaActive(ActiveFile),
    Committing,
    Failed,
    Completed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileTransferMode {
    Full,
    Delta,
}

impl ActiveFileMode {
    fn transfer_mode(&self) -> FileTransferMode {
        match self {
            Self::Full => FileTransferMode::Full,
            Self::Delta(_) => FileTransferMode::Delta,
        }
    }
}

/// 模块内部保留 typed error；仅在 `DcAck` seam 转换为 wire outcome。
enum CommitOutcome {
    Success,
    HashMismatch,
    SizeMismatch,
    HardError(AppError),
}

/// delta 重建期间的可变状态。
struct DeltaCtx {
    reconstructor: Reconstructor,
    /// 已写入输出流的字节数：下一个 `DataChunk` 的 offset（与 basis 偏移无关，
    /// 由 token 到达顺序单调递增）
    write_pos: u64,
    pending_basis: Option<(u64, u32, u64)>,
}

/// 持有 disk-commit 接受的单个文件完整落盘生命周期。
/// 外层 task 只分派命令；本模块决定文件是否 active 以及如何 settle。
pub(crate) struct FileCommitter {
    dest: Arc<StorageEnum>,
    session: SessionConfig,
    ack_tx: mpsc::UnboundedSender<DcAck>,
    progress: Arc<ReceiverProgress>,
    state: FileCommitState,
}

impl FileCommitter {
    pub(crate) fn new(
        dest: Arc<StorageEnum>, session: SessionConfig, ack_tx: mpsc::UnboundedSender<DcAck>,
        progress: Arc<ReceiverProgress>,
    ) -> Self {
        Self {
            dest,
            session,
            ack_tx,
            progress,
            state: FileCommitState::Idle,
        }
    }

    pub(crate) async fn begin_full(&mut self, ndx: i32, entry: Arc<EntryEnum>) {
        self.begin(ndx, entry, None).await;
    }

    pub(crate) async fn begin_delta(&mut self, ndx: i32, entry: Arc<EntryEnum>, block_size: u32) {
        self.begin(ndx, entry, Some(block_size)).await;
    }

    async fn begin(&mut self, ndx: i32, entry: Arc<EntryEnum>, delta_block_size: Option<u32>) {
        if matches!(
            self.state,
            FileCommitState::FullActive(_) | FileCommitState::DeltaActive(_) | FileCommitState::Committing
        ) {
            warn!(
                "[dc] rejecting begin for {:?} while another file is active",
                entry.get_relative_path()
            );
            return;
        }
        if let Err(error) = validate_relative_path(entry.get_relative_path()) {
            warn!(
                "[dc] rejecting unsafe relative path {:?}: {error}",
                entry.get_relative_path()
            );
            self.state = FileCommitState::Failed;
            self.report(ndx, CommitOutcome::HardError(error));
            return;
        }

        let basis_size = if delta_block_size.is_some() {
            match self.dest.get_metadata(entry.get_relative_path()).await {
                Ok(basis) => basis.get_size(),
                Err(error) if error.is_not_found() => entry.get_size(),
                Err(error) => {
                    self.state = FileCommitState::Failed;
                    self.report(ndx, stage_error("basis describe", error.into()));
                    return;
                }
            }
        } else {
            0
        };
        match StorageEnum::prepare_stream_with_mode(&self.dest, &entry, self.session.transfer_mode).await {
            Ok(handle) => {
                let (tx_inner, rx_inner) = mpsc::channel::<StreamChunk>(8);
                let writer_handle = handle.clone();
                let write_join =
                    tokio::spawn(async move { StorageEnum::write_planned_stream(rx_inner, &writer_handle).await });
                let size = entry.get_size();
                let mode = delta_block_size.map_or(ActiveFileMode::Full, |block_size| {
                    ActiveFileMode::Delta(DeltaCtx {
                        reconstructor: Reconstructor::new(block_size, basis_size),
                        write_pos: 0,
                        pending_basis: None,
                    })
                });
                let active = ActiveFile {
                    ndx,
                    entry,
                    tx_inner,
                    write_join,
                    handle,
                    size,
                    mode,
                };
                self.state = match active.mode {
                    ActiveFileMode::Full => FileCommitState::FullActive(active),
                    ActiveFileMode::Delta(_) => FileCommitState::DeltaActive(active),
                };
            }
            Err(error) => {
                error!("[dc] prepare_stream {:?}: {error}", entry.get_relative_path());
                self.state = FileCommitState::Failed;
                self.report(ndx, CommitOutcome::HardError(AppError::from(error)));
            }
        }
    }

    pub(crate) async fn push_full(&self, entry: &EntryEnum, chunk: DataChunk) {
        let FileCommitState::FullActive(active) = &self.state else {
            debug!("[dc] full chunk without active stream: {:?}", entry.get_relative_path());
            return;
        };
        if active.entry.get_relative_path() != entry.get_relative_path() {
            debug!("[dc] rejecting full chunk for non-matching active stream");
            return;
        }
        if active.tx_inner.send(StreamChunk::from(chunk)).await.is_err() {
            debug!("[dc] write channel closed early for {:?}", entry.get_relative_path());
        }
    }

    pub(crate) async fn push_delta(&mut self, entry: &EntryEnum, token: DeltaToken) {
        let FileCommitState::DeltaActive(active) = &mut self.state else {
            debug!(
                "[dc] delta token for {:?} without active stream",
                entry.get_relative_path()
            );
            return;
        };
        if active.entry.get_relative_path() != entry.get_relative_path() {
            debug!("[dc] rejecting delta token for non-matching active stream");
            return;
        }
        let ndx = active.ndx;
        let ActiveFileMode::Delta(delta) = &mut active.mode else {
            debug!(
                "[dc] delta token for {:?} on non-delta active stream",
                entry.get_relative_path()
            );
            return;
        };

        let step = delta.reconstructor.push(&token);
        if let ReconstructStep::NeedBasis { offset, len } = step {
            if let Some((start, pending_len, _)) = &mut delta.pending_basis
                && *start + u64::from(*pending_len) == offset
                && u64::from(*pending_len) + u64::from(len) <= 2 * 1024 * 1024
            {
                *pending_len += len;
                delta.write_pos += u64::from(len);
                return;
            }
        }
        if let Err(error) = flush_basis(&self.dest, active).await {
            let previous = std::mem::replace(&mut self.state, FileCommitState::Failed);
            if let FileCommitState::DeltaActive(active) = previous {
                settle_and_remove(&self.dest, active).await;
            }
            self.report(ndx, stage_error("basis read", error));
            return;
        }
        let ActiveFileMode::Delta(delta) = &mut active.mode else {
            return;
        };
        match step {
            ReconstructStep::NeedBasis { offset, len } => {
                delta.pending_basis = Some((offset, len, delta.write_pos));
                delta.write_pos += u64::from(len);
            }
            ReconstructStep::Ready(data) => {
                let offset = delta.write_pos;
                delta.write_pos += data.len() as u64;
                let _ = active
                    .tx_inner
                    .send(StreamChunk::from(DataChunk { offset, data }))
                    .await;
            }
        }
    }

    pub(crate) async fn commit(
        &mut self, ndx: i32, entry: &EntryEnum, source_hash: Option<String>, expected_mode: FileTransferMode,
    ) {
        let active = match &self.state {
            FileCommitState::FullActive(active) | FileCommitState::DeltaActive(active) => active,
            _ => {
                warn!("[dc] commit without active stream: {:?}", entry.get_relative_path());
                return;
            }
        };
        let mode_matches = active.mode.transfer_mode() == expected_mode;
        if active.ndx != ndx || active.entry.get_relative_path() != entry.get_relative_path() || !mode_matches {
            warn!(
                "[dc] rejecting commit for non-matching active stream: {:?}",
                entry.get_relative_path()
            );
            return;
        }
        if let FileCommitState::DeltaActive(active) = &mut self.state
            && let Err(error) = flush_basis(&self.dest, active).await
        {
            let previous = std::mem::replace(&mut self.state, FileCommitState::Failed);
            if let FileCommitState::DeltaActive(active) = previous {
                settle_and_remove(&self.dest, active).await;
            }
            self.report(ndx, stage_error("basis read", error));
            return;
        }
        let previous = std::mem::replace(&mut self.state, FileCommitState::Committing);
        let active = match previous {
            FileCommitState::FullActive(active) | FileCommitState::DeltaActive(active) => active,
            _ => unreachable!("validated active state changed before commit"),
        };
        let outcome = finalize_file(&self.dest, &self.session, active, source_hash, &self.progress).await;
        self.state = match &outcome {
            CommitOutcome::Success => FileCommitState::Completed,
            _ => FileCommitState::Failed,
        };
        self.report(ndx, outcome);
    }

    pub(crate) async fn abort(&mut self) {
        match std::mem::replace(&mut self.state, FileCommitState::Idle) {
            FileCommitState::FullActive(active) | FileCommitState::DeltaActive(active) => {
                settle_and_remove(&self.dest, active).await;
            }
            _ => {}
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        self.abort().await;
    }

    fn report(&self, ndx: i32, outcome: CommitOutcome) {
        let outcome = match outcome {
            CommitOutcome::Success => FileOutcome::Success,
            CommitOutcome::HashMismatch => FileOutcome::HashMismatch,
            CommitOutcome::SizeMismatch => FileOutcome::SizeMismatch,
            CommitOutcome::HardError(error) => FileOutcome::HardError(error.to_string()),
        };
        let _ = self.ack_tx.send(DcAck::FileOutcome { ndx, outcome });
    }
}

/// 关闭输入后校验长度和源摘要，再由 expert 引擎校验目标并发布。
/// 失败由会话清理其拥有的 stage，上报 outcome 后由 Receiver 决定是否 redo。
async fn finalize_file(
    dest: &Arc<StorageEnum>, session: &SessionConfig, a: ActiveFile, source_hash: Option<String>,
    progress: &Arc<ReceiverProgress>,
) -> CommitOutcome {
    let ActiveFile {
        entry,
        tx_inner,
        write_join,
        handle,
        size,
        ..
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
        let _ = handle.discard().await;
        let received = handle.received_size().await;
        if received > size || (handle.input_ended().await && received < size) {
            return CommitOutcome::SizeMismatch;
        }
        return CommitOutcome::HardError(e);
    }

    // 数据长度及源端摘要先校验；目标端读回校验由 expert complete 负责。
    if handle.received_size().await != size {
        let _ = handle.discard().await;
        return CommitOutcome::SizeMismatch;
    }
    if session.enable_integrity_check
        && let Some(expected) = source_hash.as_ref()
        && &handle.received_hash().await != expected
    {
        let _ = handle.discard().await;
        return CommitOutcome::HashMismatch;
    }

    // 引擎完成读回校验与发布。
    if let Err(e) = StorageEnum::commit_chunk_stream(dest, &entry, size, handle).await {
        error!("[dc] commit {:?}: {}", entry.get_relative_path(), e);
        return CommitOutcome::HardError(AppError::from(e));
    }

    progress.files_transferred.fetch_add(1, Ordering::Relaxed);
    progress.bytes_transferred.fetch_add(size, Ordering::Relaxed);
    CommitOutcome::Success
}

/// 消费一个 delta token（`Match`/`Data`）：推进 `Reconstructor` 状态机，按需读 basis
/// block，生成 `DataChunk` 转发给 `write_chunk_stream` 的 channel。
///
/// `entry` 取自消息本身（而非 `active` 内部，因为 `active` 可能为 `None`/非 delta 时
/// 仍需用于日志），实际 basis 读用 `active.entry`（`DeltaBegin` 时已固定，权威来源）。
/// basis 读失败视为该文件的 `HardError`，中止当前 `ActiveFile`（清理当前 stage，语义同
/// `AbortFile`）——中止后残余 token 因 `active` 已为 `None` 被静默丢弃，与 `FileChunk`
/// 的"写 channel 已关闭"降噪处理同构。
async fn settle_and_remove(_dest: &StorageEnum, active: ActiveFile) {
    let ActiveFile {
        tx_inner,
        write_join,
        handle,
        ..
    } = active;
    drop(tx_inner);
    let _ = write_join.await;
    let _ = handle.discard().await;
}

fn stage_error(stage: &'static str, source: AppError) -> CommitOutcome {
    CommitOutcome::HardError(AppError::FileCommitStage {
        stage,
        source: Box::new(source),
    })
}

/// 读取 basis file（目标端当前已有文件）的 `[offset, offset + len)` 区间，用于 `Match`
/// token 重建。经 `read_chunk_stream` 公共入口（`intervals=Some`）读取——避免绕过已修复
/// 的 NFS 短读处理（issue #54 阶段 3）。
async fn read_basis_block(dest: &Arc<StorageEnum>, entry: &Arc<EntryEnum>, offset: u64, len: u32) -> Result<Bytes> {
    let (mut rx, handle) = StorageEnum::read_chunk_stream(
        dest,
        entry,
        Some(vec![(offset, offset + u64::from(len))]),
        None,
        false,
        1,
    );
    let mut buf = BytesMut::with_capacity(len as usize);
    while let Some(chunk) = rx.recv().await {
        buf.extend_from_slice(&chunk.data);
    }
    match handle.await {
        Ok(Ok(_)) => Ok(buf.freeze()),
        Ok(Err(e)) => Err(AppError::from(e)),
        Err(join_err) => Err(AppError::from(join_err)),
    }
}

/// 连续的 basis 引用合并为有界读取，避免大文件逐块反复打开和描述目标。
async fn flush_basis(dest: &Arc<StorageEnum>, active: &mut ActiveFile) -> Result<()> {
    let ActiveFileMode::Delta(delta) = &mut active.mode else {
        return Ok(());
    };
    let Some((offset, len, write_offset)) = delta.pending_basis.take() else {
        return Ok(());
    };
    let data = read_basis_block(dest, &active.entry, offset, len).await?;
    active
        .tx_inner
        .send(StreamChunk {
            chunk: DataChunk {
                offset: write_offset,
                data,
            },
            unchanged_basis: offset == write_offset,
        })
        .await
        .map_err(|_| AppError::CopyError("Delta destination writer closed".into()))?;
    Ok(())
}
