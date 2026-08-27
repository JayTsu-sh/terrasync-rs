//! Receiver 单文件落盘生命周期：消费 full/delta 文件命令，
//! 对全量文件用 data-mover 3 段 API（`resume_prepare` / `write_chunk_stream` /
//! `commit_chunk_stream`）流式写入 `.part`，`FileCommit` 时读回 `.part` hash
//! 校验后原子 rename。目录/符号链接即时完成。delta 文件复用同一套三段式：逐 token
//! 经 `Reconstructor` 推算出待写字节（`Data` token 直接可写，`Match` token 按需
//! `read_chunk_stream` 区间读 basis file），写入同一个 `write_chunk_stream` channel。

// 标准库
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

// 外部 crate
use bytes::{Bytes, BytesMut};
use data_mover::{CommitCallback, DataChunk, EntryEnum, StorageEnum, StreamHandle};
use sync_delta::DeltaToken;
use sync_delta::reconstruct::{ReconstructStep, Reconstructor};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use transport::message::{DcAck, FileOutcome, SessionConfig};

// 内部模块
use crate::byte_resume::part_path_for;
use crate::error::{AppError, Result};
use crate::receiver::{ReceiverProgress, validate_relative_path};

/// 当前正在流式写入的文件上下文。
struct ActiveFile {
    /// Receiver 请求索引，在完整落盘生命周期中保持权威关联。
    ndx: i32,
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

        let part_path = part_path_for(entry.get_relative_path());
        match StorageEnum::resume_prepare(&self.dest, &entry, &part_path, false).await {
            Ok((_missing, handle)) => {
                let (tx_inner, rx_inner) = mpsc::channel::<DataChunk>(8);
                let dest = self.dest.clone();
                let writer_entry = entry.clone();
                let writer_handle = handle.clone();
                let write_join = tokio::spawn(async move {
                    StorageEnum::write_chunk_stream(&dest, &writer_entry, rx_inner, &writer_handle, None, noop_commit())
                        .await
                });
                let size = entry.get_size();
                let mode = delta_block_size.map_or(ActiveFileMode::Full, |block_size| {
                    ActiveFileMode::Delta(DeltaCtx {
                        reconstructor: Reconstructor::new(block_size, size),
                        write_pos: 0,
                    })
                });
                let active = ActiveFile {
                    ndx,
                    entry,
                    tx_inner,
                    write_join,
                    handle,
                    part_path,
                    size,
                    mode,
                };
                self.state = match active.mode {
                    ActiveFileMode::Full => FileCommitState::FullActive(active),
                    ActiveFileMode::Delta(_) => FileCommitState::DeltaActive(active),
                };
            }
            Err(error) => {
                error!("[dc] resume_prepare {:?}: {error}", entry.get_relative_path());
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
        if active.tx_inner.send(chunk).await.is_err() {
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

        let data = match delta.reconstructor.push(&token) {
            ReconstructStep::Ready(bytes) => bytes,
            ReconstructStep::NeedBasis { len: 0, .. } => Bytes::new(),
            ReconstructStep::NeedBasis { offset, len } => {
                match read_basis_block(&self.dest, &active.entry, offset, len).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        error!("[dc] delta basis read {:?}: {error}", active.entry.get_relative_path());
                        if let FileCommitState::DeltaActive(active) =
                            std::mem::replace(&mut self.state, FileCommitState::Failed)
                        {
                            settle_and_remove(&self.dest, active).await;
                        }
                        self.report(ndx, stage_error("basis read", error));
                        return;
                    }
                }
            }
        };

        let write_offset = delta.write_pos;
        delta.write_pos += data.len() as u64;
        if active
            .tx_inner
            .send(DataChunk {
                offset: write_offset,
                data,
            })
            .await
            .is_err()
        {
            debug!(
                "[dc] delta write channel closed early for {:?}",
                active.entry.get_relative_path()
            );
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

/// 提交单个文件：关闭写 channel → 等写任务收尾 → 读回 `.part` hash 校验 →
/// `commit_chunk_stream`（原子 rename）→ `set_metadata` → 上报 outcome。
///
/// 任一步失败：删除 `.part` 并上报 `FileOutcome`（hash 不符为 `HashMismatch`，可 redo；
/// 其余为 `HardError`，不可 redo）；是否触发 redo 由 Receiver 主 task 统一决策，本函数
/// 不再直接发终态 `ReceiverMsg`。
async fn finalize_file(
    dest: &Arc<StorageEnum>, session: &SessionConfig, a: ActiveFile, source_hash: Option<String>,
    progress: &Arc<ReceiverProgress>,
) -> CommitOutcome {
    let ActiveFile {
        entry,
        tx_inner,
        write_join,
        handle,
        part_path,
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
        remove_part(dest, &entry, &part_path).await;
        return CommitOutcome::HardError(e);
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
                return CommitOutcome::HashMismatch;
            }
            Err(e) => {
                remove_part(dest, &entry, &part_path).await;
                return stage_error("hash read-back", AppError::from(e));
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
            return CommitOutcome::SizeMismatch;
        }
        Err(e) => {
            remove_part(dest, &entry, &part_path).await;
            return stage_error("part size read-back", AppError::from(e));
        }
    }

    // 原子 rename：.part → 最终文件
    if let Err(e) = StorageEnum::commit_chunk_stream(dest, &entry, size, handle).await {
        error!("[dc] commit {:?}: {}", entry.get_relative_path(), e);
        remove_part(dest, &entry, &part_path).await;
        return CommitOutcome::HardError(AppError::from(e));
    }

    if let Err(error) = dest.set_entry_metadata(&entry).await {
        return stage_error("metadata commit", AppError::from(error));
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
/// basis 读失败视为该文件的 `HardError`，中止当前 `ActiveFile`（丢弃 `.part`，语义同
/// `AbortFile`）——中止后残余 token 因 `active` 已为 `None` 被静默丢弃，与 `FileChunk`
/// 的"写 channel 已关闭"降噪处理同构。
async fn settle_and_remove(dest: &StorageEnum, active: ActiveFile) {
    let ActiveFile {
        entry,
        tx_inner,
        write_join,
        part_path,
        ..
    } = active;
    drop(tx_inner);
    let _ = write_join.await;
    remove_part(dest, &entry, &part_path).await;
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
        EntryEnum::HDFS(e) => e.relative_path = part_path.to_path_buf(),
    }
    part
}

/// `disk_commit` task 不做逐 chunk 进度回调，提交回调为空操作。
fn noop_commit() -> CommitCallback {
    Arc::new(|_off, _len| {})
}
