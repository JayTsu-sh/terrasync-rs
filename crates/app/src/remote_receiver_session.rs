//! 已协商的远端 Receiver 会话。
//!
//! 该模块提供生产调用方与 characterization tests 共用的高层 seam。握手、鉴权和
//! `SessionConfig` 接收仍由外层负责；磁盘写入仍位于 disk-commit 内部 seam 后面。
//! 当前实现委托既有事件循环，后续 tickets 将在此 interface 后逐步收拢会话状态。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use data_mover::dir_tree::DirPageResult;
use data_mover::{DataChunk, EntryEnum, StorageEnum};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};
use transport::message::{
    BlockSignature, DcAck, DestIndex, DiskCommitMsg, FeatureFlags, FileOutcome, NdxTable, ProgressSnapshot,
    ReceiverMsg, SessionConfig, TransferDecision,
};
use transport::quic::credit::{ReceiverCreditOutcome, ReceiverCreditState};
use transport::traits::ReceiverTransport;

use crate::byte_resume::is_part_file;
use crate::error::Result;
use crate::receiver::{ReceiverProgress, recv_file_list_and_data_phase, validate_relative_path};

/// 一次会话的请求与终态账本。
///
/// 所有结束判断都通过该类型完成，避免消息分支各自复制计数与 `TransferDone` 条件。
#[derive(Debug, Default)]
struct TransferLedger {
    requested: u64,
    completed: u64,
    transfer_done_seen: bool,
    terminated_ndx: HashSet<i32>,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum ActiveTransfer {
    #[default]
    Idle,
    Full,
    Delta {
        ndx: i32,
    },
}

impl TransferLedger {
    fn record_request(&mut self) {
        self.requested += 1;
    }

    /// 记录带 ndx 的首次终态；重复终态不改变完成计数。
    fn record_terminal(&mut self, ndx: i32) -> bool {
        if !self.terminated_ndx.insert(ndx) {
            return false;
        }
        self.completed += 1;
        true
    }

    /// 记录协议中不携带 ndx 的 entry 终态（例如符号链接）。
    fn record_unindexed_terminal(&mut self) {
        self.completed += 1;
    }

    fn observe_transfer_done(&mut self) {
        self.transfer_done_seen = true;
    }

    fn is_complete(&self) -> bool {
        self.transfer_done_seen && self.completed >= self.requested
    }

    fn counts(&self) -> (u64, u64) {
        (self.completed, self.requested)
    }
}

/// Receiver session 当前已迁移的协调状态。
#[derive(Debug, Default)]
pub(crate) struct RemoteSessionState {
    ledger: TransferLedger,
    ndx_table: NdxTable,
    created_dirs: Vec<Arc<EntryEnum>>,
    active_transfer: ActiveTransfer,
    credit: ReceiverCreditState,
    attempts: HashMap<i32, u8>,
}

impl RemoteSessionState {
    pub(crate) fn ingest_page(&mut self, page: &data_mover::dir_tree::DirPageResult) {
        self.ndx_table.ingest_page(page);
    }

    pub(crate) fn indexed_entry(&self, ndx: i32) -> Option<&Arc<EntryEnum>> {
        self.ndx_table.get(ndx)
    }

    pub(crate) fn indexed_len(&self) -> usize {
        self.ndx_table.len()
    }

    /// 处理不需要 delta implementation 的基础分类。返回该分类是否已完成处理。
    pub(crate) async fn handle_base_classification(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &StorageEnum, progress: &ReceiverProgress,
        ndx: i32, entry: &Arc<EntryEnum>, decision: TransferDecision,
    ) -> bool {
        match decision {
            TransferDecision::FullTransfer => {
                let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
                self.ledger.record_request();
                true
            }
            TransferDecision::MetadataOnly => {
                let _ = dest.set_entry_metadata(entry).await;
                progress
                    .metadata_only
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::Classified {
                        entry: entry.clone(),
                        decision,
                    })
                    .await;
                true
            }
            TransferDecision::Skip => {
                progress
                    .files_skipped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::Classified {
                        entry: entry.clone(),
                        decision,
                    })
                    .await;
                true
            }
            TransferDecision::DeltaTransfer | TransferDecision::Deleted => false,
        }
    }

    /// 处理 changed entry 的 delta eligibility、basis signature 与 full fallback。
    pub(crate) async fn handle_delta_classification(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &Arc<StorageEnum>, features: &FeatureFlags,
        delta_size_threshold: u64, ndx: i32, entry: &Arc<EntryEnum>, decision: TransferDecision,
    ) -> bool {
        if decision != TransferDecision::DeltaTransfer {
            return false;
        }

        if !features.delta {
            let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
            self.ledger.record_request();
            return true;
        }

        let size = entry.get_size();
        if size > delta_size_threshold {
            info!(
                "[Receiver Remote] {:?} size {} exceeds delta_size_threshold {} bytes, downgrading to full transfer",
                entry.get_relative_path(),
                size,
                delta_size_threshold
            );
            let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
            self.ledger.record_request();
            return true;
        }

        let block_size = sync_delta::calculate_block_size(size);
        let (mut basis_rx, basis_handle) = StorageEnum::read_chunk_stream(dest, entry, None, None, false, 8);
        let mut sig_calc = sync_delta::signature::SignatureCalculator::new(block_size);
        while let Some(chunk) = basis_rx.recv().await {
            sig_calc.push(&chunk.data);
        }
        let basis_read_result = match basis_handle.await {
            Ok(inner) => inner.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };

        match basis_read_result {
            Ok(_) => {
                let signatures = sig_calc
                    .finish()
                    .into_iter()
                    .map(|signature| BlockSignature {
                        rolling: signature.rolling,
                        strong: signature.strong,
                    })
                    .collect();
                let _ = transport
                    .send(ReceiverMsg::DeltaTransferRequest {
                        ndx,
                        block_size,
                        signatures,
                    })
                    .await;
            }
            Err(error) => {
                warn!(
                    "[Receiver Remote] 读取 basis file {:?} 失败: {}, 降级全量传输",
                    entry.get_relative_path(),
                    error
                );
                let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
            }
        }
        self.ledger.record_request();
        true
    }

    /// 处理一个文件页中的子目录与目标端 orphan，并登记会话结束时需要恢复 metadata 的目录。
    pub(crate) async fn handle_directory_lifecycle(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &StorageEnum, page: &DirPageResult,
        dest_index: &mut DestIndex, delete_target: bool,
    ) {
        for subdir in &page.subdirs {
            dest_index.mark_matched(&subdir.entry);
            if let Err(error) = validate_relative_path(subdir.entry.get_relative_path()) {
                warn!("[Receiver Remote] Rejecting unsafe subdir path: {}", error);
                let _ = transport
                    .send(ReceiverMsg::EntryError {
                        entry: subdir.entry.clone(),
                        reason: format!("{error}"),
                    })
                    .await;
                continue;
            }
            if let Err(error) = dest.create_dir_all(&subdir.entry).await {
                warn!(
                    "[Receiver Remote] create_dir {:?}: {}",
                    subdir.entry.get_relative_path(),
                    error
                );
            }
            self.created_dirs.push(subdir.entry.clone());
        }

        if !delete_target {
            return;
        }
        for orphan in dest_index.orphaned_entries() {
            let path = orphan.get_relative_path();
            if is_part_file(path) {
                debug!("[Receiver Remote] Skipping in-progress part file: {:?}", path);
                continue;
            }
            let result = if orphan.get_is_dir() {
                info!("[Receiver Remote] Deleting orphaned dir: {:?}", path);
                dest.delete_dir_all(orphan).await
            } else {
                info!("[Receiver Remote] Deleting orphaned file: {:?}", path);
                dest.delete_file(orphan).await
            };
            match result {
                Ok(()) => {
                    let _ = transport
                        .send(ReceiverMsg::Classified {
                            entry: orphan.clone(),
                            decision: TransferDecision::Deleted,
                        })
                        .await;
                }
                Err(error) => {
                    warn!("[Receiver Remote] delete {:?}: {}", path, error);
                    let _ = transport
                        .send(ReceiverMsg::EntryError {
                            entry: orphan.clone(),
                            reason: format!("{error}"),
                        })
                        .await;
                }
            }
        }
    }

    /// 在所有文件 durable outcome 完成后恢复目录 metadata。
    pub(crate) async fn finalize_directories(&self, dest: &StorageEnum) {
        for entry in &self.created_dirs {
            if let Err(error) = dest.set_entry_metadata(entry).await {
                warn!(
                    "[Receiver Remote] 回写目录元数据 {:?} 失败: {}",
                    entry.get_relative_path(),
                    error
                );
            }
        }
    }

    pub(crate) async fn begin_full(&mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: Arc<EntryEnum>) -> bool {
        if self.active_transfer != ActiveTransfer::Idle {
            warn!("[Receiver Remote] rejecting FileBegin while another transfer is active");
            return false;
        }
        self.active_transfer = ActiveTransfer::Full;
        let _ = dc_tx.send(DiskCommitMsg::FileBegin { ndx, entry }).await;
        true
    }

    async fn push_full_chunk(&self, dc_tx: &Sender<DiskCommitMsg>, entry: Arc<EntryEnum>, chunk: DataChunk) -> bool {
        if self.active_transfer != ActiveTransfer::Full {
            warn!("[Receiver Remote] rejecting FileData without an active full transfer");
            return false;
        }
        dc_tx.send(DiskCommitMsg::FileChunk { entry, chunk }).await.is_ok()
    }

    /// Accepts one full-data chunk at the bounded disk-commit seam and returns
    /// credit only after that enqueue succeeds.
    pub(crate) async fn accept_full_chunk(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dc_tx: &Sender<DiskCommitMsg>, entry: Arc<EntryEnum>,
        chunk: DataChunk,
    ) -> bool {
        let bytes = chunk.data.len() as u64;
        if !self.push_full_chunk(dc_tx, entry, chunk).await {
            return false;
        }
        self.record_data_consumed(transport, bytes).await;
        true
    }

    /// 提交 active full transfer；返回 false 表示该 `EndOfFile` 应由 delta 路径处理。
    pub(crate) async fn commit_full(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<EntryEnum>, source_hash: &Option<String>,
    ) -> bool {
        if self.active_transfer != ActiveTransfer::Full {
            return false;
        }
        self.active_transfer = ActiveTransfer::Idle;
        let _ = dc_tx
            .send(DiskCommitMsg::FileCommit {
                ndx,
                entry: entry.clone(),
                source_hash: source_hash.clone(),
            })
            .await;
        true
    }

    /// Lazily starts the delta stream for `ndx`. Repeated events for the same
    /// entry are idempotent; interleaved or full-transfer events are rejected.
    async fn ensure_delta_active(&mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<EntryEnum>) -> bool {
        match self.active_transfer {
            ActiveTransfer::Idle => {
                let block_size = sync_delta::calculate_block_size(entry.get_size());
                let _ = dc_tx
                    .send(DiskCommitMsg::DeltaBegin {
                        ndx,
                        entry: entry.clone(),
                        block_size,
                    })
                    .await;
                self.active_transfer = ActiveTransfer::Delta { ndx };
                true
            }
            ActiveTransfer::Delta { ndx: active_ndx } if active_ndx == ndx => true,
            ActiveTransfer::Full => {
                warn!("[Receiver Remote] rejecting delta event while a full transfer is active");
                false
            }
            ActiveTransfer::Delta { ndx: active_ndx } => {
                warn!(
                    "[Receiver Remote] rejecting delta event for ndx {} while ndx {} is active",
                    ndx, active_ndx
                );
                false
            }
        }
    }

    pub(crate) async fn push_delta_match(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<EntryEnum>, block_index: u32,
    ) -> bool {
        if !self.ensure_delta_active(dc_tx, ndx, entry).await {
            return false;
        }
        let _ = dc_tx
            .send(DiskCommitMsg::DeltaMatch {
                entry: entry.clone(),
                block_index,
            })
            .await;
        true
    }

    pub(crate) async fn push_delta_data(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<EntryEnum>, data: bytes::Bytes,
    ) -> bool {
        if !self.ensure_delta_active(dc_tx, ndx, entry).await {
            return false;
        }
        let _ = dc_tx
            .send(DiskCommitMsg::DeltaData {
                entry: entry.clone(),
                data,
            })
            .await;
        true
    }

    /// Commits a delta stream, lazily beginning it when it contains zero tokens.
    pub(crate) async fn commit_delta(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<EntryEnum>, source_hash: &Option<String>,
    ) -> bool {
        if !self.ensure_delta_active(dc_tx, ndx, entry).await {
            return false;
        }
        self.active_transfer = ActiveTransfer::Idle;
        let _ = dc_tx
            .send(DiskCommitMsg::DeltaCommit {
                ndx,
                entry: entry.clone(),
                source_hash: source_hash.clone(),
            })
            .await;
        true
    }

    pub(crate) async fn abort_active_file(&mut self, dc_tx: &Sender<DiskCommitMsg>) {
        self.active_transfer = ActiveTransfer::Idle;
        let _ = dc_tx.send(DiskCommitMsg::AbortFile).await;
    }

    pub(crate) async fn handle_source_failure(&mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: Option<i32>) -> bool {
        self.abort_active_file(dc_tx).await;
        match ndx {
            Some(ndx) => self.record_terminal(ndx) && self.is_complete(),
            None => self.complete_unindexed(),
        }
    }

    /// Stops disk commit, drains every durable outcome, then restores directory metadata.
    pub(crate) async fn shutdown_disk_commit(
        &mut self, dc_tx: Sender<DiskCommitMsg>, dc_join: tokio::task::JoinHandle<Result<()>>,
        ack_rx: &mut tokio::sync::mpsc::UnboundedReceiver<DcAck>, transport: &(dyn ReceiverTransport + 'static),
        progress: &ReceiverProgress, dest: &StorageEnum,
    ) -> Result<()> {
        let _ = dc_tx.send(DiskCommitMsg::Shutdown).await;
        drop(dc_tx);
        dc_join
            .await
            .map_err(|error| crate::error::AppError::CopyError(format!("disk-commit task join: {error}")))??;
        while let Ok(ack) = ack_rx.try_recv() {
            match ack {
                DcAck::Entry(ack) => {
                    let _ = self.handle_entry_outcome(transport, ack).await;
                }
                DcAck::FileOutcome { ndx, outcome } => {
                    let _ = self.handle_file_outcome(transport, progress, ndx, outcome).await;
                }
            }
        }
        self.finalize_directories(dest).await;
        Ok(())
    }

    /// Records bytes accepted by the disk-commit seam and emits an exact delayed
    /// credit grant once the configured threshold is reached.
    pub(crate) async fn record_data_consumed(&mut self, transport: &(dyn ReceiverTransport + 'static), bytes: u64) {
        if let Ok(ReceiverCreditOutcome::Grant(message)) = self.credit.record_accepted(bytes) {
            // Grant delivery remains best-effort. A broken connection is handled by
            // the surrounding receive loop and terminates the session deterministically.
            let _ = transport.send(message).await;
        }
    }

    pub(crate) async fn handle_file_outcome(
        &mut self, transport: &(dyn ReceiverTransport + 'static), progress: &ReceiverProgress, ndx: i32,
        outcome: FileOutcome,
    ) -> bool {
        let (ack, terminal) = match outcome {
            FileOutcome::Success => (ReceiverMsg::Success { ndx }, true),
            FileOutcome::HashMismatch => self.redo_or_error(ndx, "hash mismatch"),
            FileOutcome::SizeMismatch => self.redo_or_error(ndx, "size mismatch"),
            FileOutcome::HardError(reason) => (ReceiverMsg::Error { ndx, reason }, true),
        };
        if !terminal {
            let _ = transport.send(ack).await;
            return false;
        }
        if !self.record_terminal(ndx) {
            debug!(
                "[Receiver Remote] ndx {} already terminated; dropping duplicate outcome",
                ndx
            );
            return false;
        }
        if matches!(ack, ReceiverMsg::Error { .. }) {
            progress.error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let _ = transport.send(ack).await;
        self.is_complete()
    }

    pub(crate) async fn handle_entry_outcome(
        &mut self, transport: &(dyn ReceiverTransport + 'static), ack: ReceiverMsg,
    ) -> bool {
        let _ = transport.send(ack).await;
        self.ledger.record_unindexed_terminal();
        self.ledger.is_complete()
    }

    fn redo_or_error(&mut self, ndx: i32, reason: &str) -> (ReceiverMsg, bool) {
        let count = self.attempts.entry(ndx).or_insert(0);
        *count += 1;
        if *count == 1 {
            (ReceiverMsg::Redo { ndx }, false)
        } else {
            (
                ReceiverMsg::Error {
                    ndx,
                    reason: reason.into(),
                },
                true,
            )
        }
    }

    fn record_terminal(&mut self, ndx: i32) -> bool {
        self.ledger.record_terminal(ndx)
    }

    pub(crate) fn complete_unindexed(&mut self) -> bool {
        self.ledger.record_unindexed_terminal();
        self.ledger.is_complete()
    }

    pub(crate) fn observe_transfer_done(&mut self) -> bool {
        self.ledger.observe_transfer_done();
        self.ledger.is_complete()
    }

    fn is_complete(&self) -> bool {
        self.ledger.is_complete()
    }

    pub(crate) fn counts(&self) -> (u64, u64) {
        self.ledger.counts()
    }
}

/// 一次已完成协议协商与鉴权的远端 Receiver 会话。
pub struct RemoteReceiverSession {
    dest_storage: Arc<StorageEnum>,
    session_config: SessionConfig,
    negotiated_features: FeatureFlags,
    delta_size_threshold: u64,
    progress: Arc<ReceiverProgress>,
}

impl RemoteReceiverSession {
    pub fn new(
        dest_storage: Arc<StorageEnum>, session_config: SessionConfig, negotiated_features: FeatureFlags,
        delta_size_threshold: u64, progress: Arc<ReceiverProgress>,
    ) -> Self {
        Self {
            dest_storage,
            session_config,
            negotiated_features,
            delta_size_threshold,
            progress,
        }
    }

    /// 运行会话直到 Sender 宣告传输结束且所有已请求 entry 均获得 durable outcome。
    pub async fn run(
        &mut self, transport: &(dyn ReceiverTransport + 'static), progress_rx: Receiver<ProgressSnapshot>,
    ) -> Result<()> {
        let mut state = RemoteSessionState::default();
        recv_file_list_and_data_phase(
            transport,
            &self.dest_storage,
            &self.session_config,
            &self.negotiated_features,
            self.delta_size_threshold,
            &self.progress,
            progress_rx,
            &mut state,
        )
        .await
    }

    /// Emits the terminal snapshot only after the session's durable shutdown completed.
    pub async fn finish(&self, transport: &(dyn ReceiverTransport + 'static), start_time: std::time::Instant) {
        let _ = transport
            .send(ReceiverMsg::Progress(self.progress.snapshot(start_time)))
            .await;
        let _ = transport.send(ReceiverMsg::AllDone).await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use data_mover::dir_tree::{DirPageResult, NdxEntry, NdxEvent};
    use data_mover::{DataChunk, NASEntry, create_storage};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use transport::in_process::create_in_process_pair;
    use transport::message::{ReceiverMsg, SenderMsg, TransferDecision};
    use transport::traits::SenderTransport;

    use super::*;

    fn session_config() -> SessionConfig {
        SessionConfig {
            src_path: String::new(),
            qos: None,
            peak_qos_rate: 1.0,
            iops: None,
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            block_size: None,
            delete_target: false,
            delta_size_threshold: None,
        }
    }

    fn progress_snapshot() -> ProgressSnapshot {
        ProgressSnapshot {
            receiver_id: "characterization".into(),
            files_transferred: 0,
            dirs_created: 0,
            bytes_transferred: 0,
            files_skipped: 0,
            metadata_only: 0,
            error_count: 0,
            elapsed_secs: 0.0,
            speed_bytes_per_sec: 0.0,
        }
    }

    fn nas_entry(name: &str, relative_path: PathBuf, is_dir: bool) -> Arc<EntryEnum> {
        Arc::new(EntryEnum::NAS(NASEntry {
            name: name.into(),
            relative_path,
            extension: None,
            is_dir,
            size: 0,
            atime: 0,
            ctime: 0,
            mtime: 0,
            mode: if is_dir { 0o755 } else { 0o644 },
            is_symlink: false,
            hard_links: None,
            uid: None,
            gid: None,
            ino: None,
            file_handle: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        }))
    }

    #[tokio::test]
    async fn session_interleaves_progress_and_waits_for_durable_outcome_after_transfer_done() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = Bytes::from_static(b"session seam characterization");
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();

        let src_storage = create_storage(src_dir.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let walk = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walk.next().await {
            Some(NdxEvent::Page(page)) => page,
            other => panic!("expected one file page, got {other:?}"),
        };
        let ndx = page.files[0].ndx;
        let entry = page.files[0].entry.clone();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let progress = Arc::new(ReceiverProgress::new());
        let (progress_tx, progress_rx) = mpsc::channel(1);
        progress_tx.send(progress_snapshot()).await.unwrap();
        drop(progress_tx);

        let mut session = RemoteReceiverSession::new(
            dest_storage,
            session_config(),
            FeatureFlags::current(),
            u64::MAX,
            progress,
        );
        let session_handle = tokio::spawn(async move { session.run(&receiver_transport, progress_rx).await });

        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();
        let mut saw_progress = false;
        let mut saw_request = false;
        while !saw_progress || !saw_request {
            match sender_transport.recv().await {
                Some(ReceiverMsg::Progress(snapshot)) => {
                    saw_progress = true;
                    assert_eq!(snapshot.receiver_id, "characterization");
                }
                Some(ReceiverMsg::TransferRequest {
                    ndx: requested,
                    decision,
                }) => {
                    assert_eq!(requested, ndx);
                    assert_eq!(decision, TransferDecision::FullTransfer);
                    saw_request = true;
                }
                other => panic!("expected progress or transfer request, got {other:?}"),
            }
        }

        sender_transport.send(SenderMsg::FileListDone).await.unwrap();
        assert!(matches!(sender_transport.recv().await, Some(ReceiverMsg::RequestsDone)));
        sender_transport
            .send(SenderMsg::FileBegin {
                ndx,
                entry: entry.clone(),
            })
            .await
            .unwrap();

        // Control-stream completion may overtake data. The session must stay alive until disk commit
        // reports the requested file's durable outcome.
        sender_transport.send(SenderMsg::TransferDone).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !session_handle.is_finished(),
            "TransferDone must not end a session with pending data"
        );

        sender_transport
            .send(SenderMsg::FileData {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: content.clone(),
                },
            })
            .await
            .unwrap();
        sender_transport
            .send(SenderMsg::EndOfFile {
                ndx,
                entry,
                source_hash: None,
            })
            .await
            .unwrap();

        assert!(
            matches!(sender_transport.recv().await, Some(ReceiverMsg::Success { ndx: completed }) if completed == ndx)
        );
        tokio::time::timeout(Duration::from_secs(2), session_handle)
            .await
            .expect("session should finish after durable outcome")
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), content);
    }

    #[tokio::test]
    async fn session_with_no_requested_transfers_finishes_after_transfer_done() {
        let dest_dir = tempdir().unwrap();
        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let (_progress_tx, progress_rx) = mpsc::channel(1);
        let mut session = RemoteReceiverSession::new(
            dest_storage,
            session_config(),
            FeatureFlags::current(),
            u64::MAX,
            Arc::new(ReceiverProgress::new()),
        );
        let session_handle = tokio::spawn(async move { session.run(&receiver_transport, progress_rx).await });

        sender_transport.send(SenderMsg::FileListDone).await.unwrap();
        assert!(matches!(sender_transport.recv().await, Some(ReceiverMsg::RequestsDone)));
        sender_transport.send(SenderMsg::TransferDone).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), session_handle)
            .await
            .expect("empty session should finish after TransferDone")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn mixed_page_base_classifications_share_session_state_and_observable_outputs() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("full.txt"), b"new").unwrap();
        fs::write(src_dir.path().join("meta.txt"), b"same meta content").unwrap();
        fs::write(src_dir.path().join("skip.txt"), b"same skip content").unwrap();
        fs::write(dest_dir.path().join("meta.txt"), b"same meta content").unwrap();
        fs::write(dest_dir.path().join("skip.txt"), b"same skip content").unwrap();
        fs::set_permissions(dest_dir.path().join("meta.txt"), fs::Permissions::from_mode(0o600)).unwrap();

        let src_storage = create_storage(src_dir.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let walk = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walk.next().await {
            Some(NdxEvent::Page(page)) => page,
            other => panic!("expected mixed file page, got {other:?}"),
        };
        assert_eq!(page.files.len(), 3);

        let dest_storage = create_storage(dest_dir.path().to_str().unwrap(), None, true)
            .await
            .unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let progress = ReceiverProgress::new();
        let mut state = RemoteSessionState::default();
        state.ingest_page(&page);

        for indexed in &page.files {
            let decision = match indexed.entry.get_name() {
                "full.txt" => TransferDecision::FullTransfer,
                "meta.txt" => TransferDecision::MetadataOnly,
                "skip.txt" => TransferDecision::Skip,
                other => panic!("unexpected entry {other}"),
            };
            assert!(
                state
                    .handle_base_classification(
                        &receiver_transport,
                        &dest_storage,
                        &progress,
                        indexed.ndx,
                        &indexed.entry,
                        decision,
                    )
                    .await
            );
        }

        let mut saw_full = false;
        let mut saw_metadata = false;
        let mut saw_skip = false;
        for _ in 0..3 {
            match sender_transport.recv().await {
                Some(ReceiverMsg::TransferRequest {
                    decision: TransferDecision::FullTransfer,
                    ..
                }) => saw_full = true,
                Some(ReceiverMsg::Classified {
                    decision: TransferDecision::MetadataOnly,
                    ..
                }) => saw_metadata = true,
                Some(ReceiverMsg::Classified {
                    decision: TransferDecision::Skip,
                    ..
                }) => saw_skip = true,
                other => panic!("unexpected base classification output: {other:?}"),
            }
        }

        assert!(saw_full && saw_metadata && saw_skip);
        assert_eq!(state.indexed_len(), 3);
        assert_eq!(state.counts(), (0, 1));
        assert_eq!(progress.metadata_only.load(Ordering::Relaxed), 1);
        assert_eq!(progress.files_skipped.load(Ordering::Relaxed), 1);
        assert_eq!(
            fs::metadata(dest_dir.path().join("meta.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        assert_eq!(
            fs::read(dest_dir.path().join("skip.txt")).unwrap(),
            b"same skip content"
        );
    }

    #[tokio::test]
    async fn delta_basis_read_failure_falls_back_to_full_request() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("changed.txt"), b"changed content").unwrap();
        let src_storage = create_storage(src_dir.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let walk = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walk.next().await {
            Some(NdxEvent::Page(page)) => page,
            other => panic!("expected changed file page, got {other:?}"),
        };
        let indexed = &page.files[0];
        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();

        assert!(
            state
                .handle_delta_classification(
                    &receiver_transport,
                    &dest_storage,
                    &FeatureFlags::current(),
                    u64::MAX,
                    indexed.ndx,
                    &indexed.entry,
                    TransferDecision::DeltaTransfer,
                )
                .await
        );
        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::TransferRequest {
                ndx,
                decision: TransferDecision::DeltaTransfer,
            }) if ndx == indexed.ndx
        ));
        assert_eq!(state.counts(), (0, 1));
    }

    #[tokio::test]
    async fn unsafe_subdirectory_is_rejected_before_storage_mutation() {
        let dest_dir = tempdir().unwrap();
        let dest_storage = create_storage(dest_dir.path().to_str().unwrap(), None, true)
            .await
            .unwrap();
        let escape_name = format!("terrasync-unsafe-{}", std::process::id());
        let escape_path = PathBuf::from("..").join(&escape_name);
        let page = DirPageResult {
            dir_path: String::new(),
            ndx_start: 0,
            files: vec![],
            subdirs: vec![NdxEntry {
                ndx: 0,
                entry: nas_entry(&escape_name, escape_path, true),
            }],
            gap_ndx: -1,
        };
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();
        let mut dest_index = DestIndex::new();

        state
            .handle_directory_lifecycle(&receiver_transport, &dest_storage, &page, &mut dest_index, false)
            .await;

        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::EntryError { .. })
        ));
        assert!(!dest_dir.path().parent().unwrap().join(escape_name).exists());
    }

    #[tokio::test]
    async fn orphan_part_file_is_preserved_when_target_deletion_is_enabled() {
        let dest_dir = tempdir().unwrap();
        let part_name = "keep.txt.terrasync-part";
        fs::write(dest_dir.path().join(part_name), b"partial").unwrap();
        let dest_storage = create_storage(dest_dir.path().to_str().unwrap(), None, true)
            .await
            .unwrap();
        let page = DirPageResult {
            dir_path: String::new(),
            ndx_start: 0,
            files: vec![],
            subdirs: vec![],
            gap_ndx: -1,
        };
        let (_sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();
        let mut dest_index = DestIndex::new();
        dest_index.insert(nas_entry(part_name, PathBuf::from(part_name), false));

        state
            .handle_directory_lifecycle(&receiver_transport, &dest_storage, &page, &mut dest_index, true)
            .await;

        assert!(dest_dir.path().join(part_name).exists());
    }

    #[tokio::test]
    async fn full_state_routes_valid_sequence_and_rejects_malformed_order() {
        let entry = nas_entry("file.txt", PathBuf::from("file.txt"), false);
        let (dc_tx, mut dc_rx) = mpsc::channel(8);
        let mut state = RemoteSessionState::default();

        assert!(
            !state
                .push_full_chunk(
                    &dc_tx,
                    entry.clone(),
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"invalid"),
                    },
                )
                .await
        );
        assert!(dc_rx.try_recv().is_err());

        assert!(state.begin_full(&dc_tx, 7, entry.clone()).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::FileBegin { ndx: 7, .. })
        ));
        assert!(!state.begin_full(&dc_tx, 8, entry.clone()).await);
        assert!(dc_rx.try_recv().is_err());

        assert!(
            state
                .push_full_chunk(
                    &dc_tx,
                    entry.clone(),
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"valid"),
                    },
                )
                .await
        );
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileChunk { .. })));
        assert!(state.commit_full(&dc_tx, 7, &entry, &None).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::FileCommit { ndx: 7, .. })
        ));
        assert!(!state.commit_full(&dc_tx, 7, &entry, &None).await);

        state.abort_active_file(&dc_tx).await;
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::AbortFile)));
    }

    #[tokio::test]
    async fn delta_state_routes_tokens_and_rejects_interleaved_indexes() {
        let entry = nas_entry("file.txt", PathBuf::from("file.txt"), false);
        let other = nas_entry("other.txt", PathBuf::from("other.txt"), false);
        let (dc_tx, mut dc_rx) = mpsc::channel(8);
        let mut state = RemoteSessionState::default();

        assert!(state.push_delta_match(&dc_tx, 7, &entry, 3).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaBegin { ndx: 7, .. })
        ));
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaMatch { block_index: 3, .. })
        ));

        assert!(
            !state
                .push_delta_data(&dc_tx, 8, &other, Bytes::from_static(b"interleaved"))
                .await
        );
        assert!(dc_rx.try_recv().is_err());

        assert!(state.commit_delta(&dc_tx, 7, &entry, &None).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaCommit { ndx: 7, .. })
        ));
    }

    #[tokio::test]
    async fn delta_state_lazily_begins_zero_token_commit() {
        let entry = nas_entry("empty.txt", PathBuf::from("empty.txt"), false);
        let (dc_tx, mut dc_rx) = mpsc::channel(8);
        let mut state = RemoteSessionState::default();

        assert!(state.commit_delta(&dc_tx, 9, &entry, &None).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaBegin { ndx: 9, .. })
        ));
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaCommit { ndx: 9, .. })
        ));

        assert!(state.begin_full(&dc_tx, 10, entry.clone()).await);
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileBegin { .. })));
        assert!(
            !state
                .push_delta_data(&dc_tx, 10, &entry, Bytes::from_static(b"invalid"))
                .await
        );
        assert!(dc_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn transfer_credit_delays_then_grants_exact_accumulated_bytes() {
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(20).unwrap();

        state.record_data_consumed(&receiver_transport, 6).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sender_transport.recv())
                .await
                .is_err()
        );

        state.record_data_consumed(&receiver_transport, 7).await;
        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::CreditGrant { bytes: 13, ndx: None })
        ));

        state.record_data_consumed(&receiver_transport, 5).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sender_transport.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_full_chunk_enqueue_returns_no_credit() {
        let entry = nas_entry("file.txt", PathBuf::from("file.txt"), false);
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let (dc_tx, mut dc_rx) = mpsc::channel(1);
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(2).unwrap();
        assert!(state.begin_full(&dc_tx, 1, entry.clone()).await);
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileBegin { .. })));
        drop(dc_rx);

        assert!(
            !state
                .accept_full_chunk(
                    &receiver_transport,
                    &dc_tx,
                    entry,
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"ab"),
                    },
                )
                .await
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sender_transport.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn file_outcomes_apply_one_retry_then_one_terminal_result() {
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let progress = ReceiverProgress::default();
        let mut state = RemoteSessionState::default();

        assert!(
            !state
                .handle_file_outcome(&receiver_transport, &progress, 7, FileOutcome::HashMismatch)
                .await
        );
        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::Redo { ndx: 7 })
        ));
        assert_eq!(state.counts(), (0, 0));

        assert!(
            !state
                .handle_file_outcome(&receiver_transport, &progress, 7, FileOutcome::SizeMismatch)
                .await
        );
        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::Error { ndx: 7, .. })
        ));
        assert_eq!(state.counts(), (1, 0));
        assert_eq!(progress.error_count.load(Ordering::Relaxed), 1);

        assert!(
            !state
                .handle_file_outcome(
                    &receiver_transport,
                    &progress,
                    8,
                    FileOutcome::HardError("storage".into()),
                )
                .await
        );
        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::Error { ndx: 8, .. })
        ));
        assert!(!state.attempts.contains_key(&8));
    }

    #[tokio::test]
    async fn finish_emits_terminal_snapshot_before_all_done() {
        let dest_dir = tempdir().unwrap();
        let dest = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let progress = Arc::new(ReceiverProgress::default());
        progress.files_transferred.store(3, Ordering::Relaxed);
        progress.error_count.store(1, Ordering::Relaxed);
        let session = RemoteReceiverSession::new(dest, session_config(), FeatureFlags::current(), u64::MAX, progress);
        let (sender_transport, receiver_transport) = create_in_process_pair();

        session.finish(&receiver_transport, std::time::Instant::now()).await;

        assert!(matches!(
            sender_transport.recv().await,
            Some(ReceiverMsg::Progress(ProgressSnapshot {
                files_transferred: 3,
                error_count: 1,
                ..
            }))
        ));
        assert!(matches!(sender_transport.recv().await, Some(ReceiverMsg::AllDone)));
    }
}
