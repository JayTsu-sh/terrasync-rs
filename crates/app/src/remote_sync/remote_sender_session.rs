//! 协商完成后的 Remote Sender 会话 seam。
//!
//! 握手、鉴权、`SessionConfig`、依赖构造和资源关闭由外层 orchestration 负责；本模块
//! 从文件列表发布开始运行到 Receiver 返回 `AllDone`。后续迁移会逐步把会话状态与转换
//! 收入此模块，调用者与测试始终只依赖这里的 interface。

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use data_mover::dir_tree::NdxEvent;
use data_mover::qos::QosManager;
use data_mover::{ChangeKind, ErrorEvent, StorageEntryMessage};
use data_mover::{ConsistencyCheck, EntryEnum, StorageEnum, WalkDirAsyncIterator2};
use sync_delta::DeltaToken;
use sync_delta::matcher::DeltaMatcher;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};
use transport::message::{BlockSignature, NdxTable, ProgressSnapshot, ReceiverMsg, SenderMsg, TransferDecision};
use transport::traits::SenderTransport;

use super::{AppError, Result, StatisticConsumer};
use crate::consumer::stats::format_bytes;

/// 构造 negotiated session 所需的既有 adapter 与配置。
pub(super) struct RemoteSenderSessionDeps<'a> {
    pub(super) transport: &'a dyn SenderTransport,
    pub(super) src_storage: &'a Arc<StorageEnum>,
    pub(super) walkdir_iter: &'a WalkDirAsyncIterator2,
    pub(super) qos: Option<&'a QosManager>,
    pub(super) enable_acl: bool,
    pub(super) checkpoint_path: &'a Path,
    pub(super) stats_consumer: &'a Arc<AsyncMutex<StatisticConsumer>>,
}

/// 会话完成后交还给外层 orchestration 的终态事实。
#[derive(Debug)]
pub(super) struct RemoteSenderSessionSummary {
    pub(super) page_count: u64,
    pub(super) advertised_entries: usize,
    pub(super) transfer_count: u64,
    pub(super) success_count: u64,
    pub(super) error_count: u64,
}

/// 协商完成后的单个 Remote Sender 会话。
pub(super) struct RemoteSenderSession<'a> {
    deps: RemoteSenderSessionDeps<'a>,
    lifecycle: RemoteSenderSessionLifecycle,
}

/// source-transfer operation 的 typed terminal fact。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceTransferOutcome {
    Sent,
    SourceFailed,
}

/// negotiated session 的 authoritative request/terminal ledger。
pub(super) struct SenderSessionLedger {
    transfer_count: u64,
    success_count: u64,
    error_count: u64,
    errored_ndx: HashSet<i32>,
    transfer_done_sent: bool,
}

pub(super) struct RequestAckSummary {
    pub(super) transfer_count: u64,
    pub(super) success_count: u64,
    pub(super) error_count: u64,
    pub(super) transfer_done_sent: bool,
}

impl SenderSessionLedger {
    pub(super) fn new() -> Self {
        Self {
            transfer_count: 0,
            success_count: 0,
            error_count: 0,
            errored_ndx: HashSet::new(),
            transfer_done_sent: false,
        }
    }

    pub(super) fn record_transfer(&mut self) {
        self.transfer_count += 1;
    }

    pub(super) fn transfer_count(&self) -> u64 {
        self.transfer_count
    }

    pub(super) fn record_success(&mut self) -> u64 {
        self.success_count += 1;
        self.success_count
    }

    pub(super) fn record_entry_error(&mut self) {
        self.error_count += 1;
    }

    pub(super) fn record_indexed_error(&mut self, ndx: i32) -> bool {
        if !self.errored_ndx.insert(ndx) {
            return false;
        }
        self.error_count += 1;
        true
    }

    pub(super) fn mark_transfer_done_sent(&mut self) {
        self.transfer_done_sent = true;
    }

    pub(super) fn finish(self) -> RequestAckSummary {
        RequestAckSummary {
            transfer_count: self.transfer_count,
            success_count: self.success_count,
            error_count: self.error_count,
            transfer_done_sent: self.transfer_done_sent,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteSenderSessionLifecycle {
    Advertising,
    RequestsOpen,
    TransferDoneSent,
    Completed,
    Failed,
}

impl<'a> RemoteSenderSession<'a> {
    pub(super) fn new(deps: RemoteSenderSessionDeps<'a>) -> Self {
        Self {
            deps,
            lifecycle: RemoteSenderSessionLifecycle::Advertising,
        }
    }

    pub(super) async fn run(mut self) -> Result<RemoteSenderSessionSummary> {
        let ndx_table = Mutex::new(NdxTable::new());
        let mut ledger = SenderSessionLedger::new();
        let mut completed_paths = load_checkpoint(self.deps.checkpoint_path).await;
        if !completed_paths.is_empty() {
            info!(
                "[Sender Remote] Loaded checkpoint: {} entries already completed",
                completed_paths.len()
            );
        }
        self.transition(RemoteSenderSessionLifecycle::RequestsOpen);
        let advertise = async {
            advertise_file_list(
                self.deps.transport,
                self.deps.walkdir_iter,
                &ndx_table,
                self.deps.stats_consumer,
            )
            .await
            .map_err(|source| stage_error("advertising", source))
        };
        let requests = async {
            process_requests_and_acks(
                self.deps.transport,
                self.deps.src_storage,
                &ndx_table,
                self.deps.qos,
                self.deps.enable_acl,
                &mut completed_paths,
                self.deps.checkpoint_path,
                self.deps.stats_consumer,
                &mut ledger,
            )
            .await
            .map_err(|source| stage_error("requests/acks", source))
        };
        let joined = tokio::try_join!(advertise, requests);
        let (page_count, ()) = match joined {
            Ok(result) => result,
            Err(error) => {
                self.transition(RemoteSenderSessionLifecycle::Failed);
                return Err(error);
            }
        };
        let request_summary = ledger.finish();
        save_or_clear_checkpoint(self.deps.checkpoint_path, &completed_paths, request_summary.error_count).await;
        if request_summary.transfer_done_sent {
            self.transition(RemoteSenderSessionLifecycle::TransferDoneSent);
        }
        self.transition(RemoteSenderSessionLifecycle::Completed);

        Ok(RemoteSenderSessionSummary {
            page_count,
            advertised_entries: ndx_table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            transfer_count: request_summary.transfer_count,
            success_count: request_summary.success_count,
            error_count: request_summary.error_count,
        })
    }

    fn transition(&mut self, next: RemoteSenderSessionLifecycle) {
        debug_assert!(matches!(
            (self.lifecycle, next),
            (
                RemoteSenderSessionLifecycle::Advertising,
                RemoteSenderSessionLifecycle::RequestsOpen | RemoteSenderSessionLifecycle::Failed
            ) | (
                RemoteSenderSessionLifecycle::RequestsOpen,
                RemoteSenderSessionLifecycle::TransferDoneSent
                    | RemoteSenderSessionLifecycle::Completed
                    | RemoteSenderSessionLifecycle::Failed
            ) | (
                RemoteSenderSessionLifecycle::TransferDoneSent,
                RemoteSenderSessionLifecycle::Completed | RemoteSenderSessionLifecycle::Failed
            ) | (
                RemoteSenderSessionLifecycle::Completed,
                RemoteSenderSessionLifecycle::Failed
            )
        ));
        self.lifecycle = next;
    }
}

/// 发布文件列表并建立本会话唯一的 index correlation ledger。
///
/// 这是 session implementation 的内部操作；生产调用者只使用
/// [`RemoteSenderSession::run`]。
pub(super) async fn advertise_file_list(
    transport: &dyn SenderTransport, walkdir_iter: &WalkDirAsyncIterator2, ndx_table: &Mutex<NdxTable>,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>,
) -> Result<u64> {
    info!("[Sender Remote] Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.next().await {
        match event {
            NdxEvent::Page(page) => {
                {
                    let mut consumer = stats_consumer.lock().await;
                    for file in &page.files {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(file.entry.clone()));
                    }
                    for directory in &page.subdirs {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(directory.entry.clone()));
                    }
                }
                ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .ingest_page(&page);
                page_count += 1;
                transport.send(SenderMsg::FilePage(page)).await?;
            }
            NdxEvent::Error { path, reason } => {
                transport.send(SenderMsg::FileListError { path, reason }).await?;
            }
            NdxEvent::Done => break,
        }
    }
    transport.send(SenderMsg::FileListDone).await?;
    Ok(page_count)
}

/// 发送一个 full entry；目录、符号链接和 bounded file stream 共用一个 typed outcome。
pub(super) async fn send_full_transfer(
    transport: &dyn SenderTransport, src_storage: &Arc<StorageEnum>, entry: &Arc<EntryEnum>, ndx: i32,
    qos: Option<&QosManager>, enable_acl: bool,
) -> Result<SourceTransferOutcome> {
    let outcome = if entry.get_is_dir() {
        transport.send(SenderMsg::CreateDir { entry: entry.clone() }).await?;
        SourceTransferOutcome::Sent
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target) => {
                transport
                    .send(SenderMsg::CreateSymlink {
                        entry: entry.clone(),
                        target,
                    })
                    .await?;
                SourceTransferOutcome::Sent
            }
            Err(error) => {
                error!("[Sender Remote] read_symlink {:?}: {error}", entry.get_relative_path());
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason: error.to_string(),
                        ndx: Some(ndx),
                    })
                    .await?;
                SourceTransferOutcome::SourceFailed
            }
        }
    } else {
        let (mut chunks, read_join) = StorageEnum::read_chunk_stream(src_storage, entry, None, qos.cloned(), true, 8);
        transport
            .send(SenderMsg::FileBegin {
                ndx,
                entry: entry.clone(),
            })
            .await?;
        while let Some(chunk) = chunks.recv().await {
            transport
                .send(SenderMsg::FileData {
                    entry: entry.clone(),
                    chunk,
                })
                .await?;
        }
        let read_result = match read_join.await {
            Ok(inner) => inner.map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match read_result {
            Ok(hasher) => {
                transport
                    .send(SenderMsg::EndOfFile {
                        ndx,
                        entry: entry.clone(),
                        source_hash: hasher.map(ConsistencyCheck::finalize),
                    })
                    .await?;
                SourceTransferOutcome::Sent
            }
            Err(reason) => {
                error!("[Sender Remote] read file {:?}: {reason}", entry.get_relative_path());
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason,
                        ndx: Some(ndx),
                    })
                    .await?;
                SourceTransferOutcome::SourceFailed
            }
        }
    };
    if outcome == SourceTransferOutcome::Sent {
        send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    }
    Ok(outcome)
}

/// ACL 跨进程传输：仅在启用且 entry 不是符号链接时发送。
pub(super) async fn send_acl_if_enabled(
    transport: &dyn SenderTransport, src_storage: &Arc<StorageEnum>, entry: &Arc<EntryEnum>, enable_acl: bool,
) {
    if enable_acl
        && !entry.get_is_symlink()
        && let Ok(Some(acl_data)) = src_storage.get_acl_bytes(entry.get_relative_path()).await
    {
        let _ = transport
            .send(SenderMsg::SetAcl {
                entry: entry.clone(),
                acl_data: bytes::Bytes::from(acl_data),
            })
            .await;
    }
}

/// Delta source stream：matcher 与 token emission 都由 session implementation 持有。
#[allow(
    clippy::too_many_arguments,
    reason = "internal transition receives one decoded delta request"
)]
pub(super) async fn send_delta_transfer(
    transport: &dyn SenderTransport, src_storage: &Arc<StorageEnum>, entry: &Arc<EntryEnum>, ndx: i32, block_size: u32,
    signatures: Vec<BlockSignature>, qos: Option<&QosManager>, enable_acl: bool,
) -> Result<SourceTransferOutcome> {
    let signatures = signatures
        .into_iter()
        .map(|signature| sync_delta::BlockSignature {
            rolling: signature.rolling,
            strong: signature.strong,
        })
        .collect::<Vec<_>>();
    let mut matcher = DeltaMatcher::new(&signatures, block_size);
    let mut token_count = 0usize;
    let (mut chunks, read_join) = StorageEnum::read_chunk_stream(src_storage, entry, None, qos.cloned(), true, 8);
    while let Some(chunk) = chunks.recv().await {
        let tokens = matcher.push(&chunk.data);
        token_count += tokens.len();
        send_delta_tokens(transport, ndx, tokens, qos).await?;
    }
    let tokens = matcher.finish();
    token_count += tokens.len();
    send_delta_tokens(transport, ndx, tokens, qos).await?;

    let read_result = match read_join.await {
        Ok(inner) => inner.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    match read_result {
        Ok(hasher) => {
            transport
                .send(SenderMsg::EndOfFile {
                    ndx,
                    entry: entry.clone(),
                    source_hash: hasher.map(ConsistencyCheck::finalize),
                })
                .await?;
            info!(
                "[Sender Remote] Delta transfer {:?}: {} tokens",
                entry.get_relative_path(),
                token_count
            );
            send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
            Ok(SourceTransferOutcome::Sent)
        }
        Err(reason) => {
            error!("[Sender Remote] read file {:?}: {reason}", entry.get_relative_path());
            transport
                .send(SenderMsg::EntryError {
                    path: entry.get_relative_path().to_path_buf(),
                    reason,
                    ndx: Some(ndx),
                })
                .await?;
            Ok(SourceTransferOutcome::SourceFailed)
        }
    }
}

async fn send_delta_tokens(
    transport: &dyn SenderTransport, ndx: i32, tokens: Vec<DeltaToken>, qos: Option<&QosManager>,
) -> Result<()> {
    for token in tokens {
        match token {
            DeltaToken::Match { block_index } => {
                transport.send(SenderMsg::DeltaMatch { ndx, block_index }).await?;
            }
            DeltaToken::Data(data) => {
                if let Some(qos) = qos {
                    qos.acquire(data.len() as u64).await;
                }
                transport.send(SenderMsg::DeltaData { ndx, data }).await?;
            }
        }
    }
    Ok(())
}

/// 唯一的 Receiver-message consumer；完成协议与所有 terminal transitions 在此收口。
#[allow(
    clippy::too_many_arguments,
    reason = "internal loop owns the complete negotiated-session context"
)]
pub(super) async fn process_requests_and_acks(
    transport: &dyn SenderTransport, src_storage: &Arc<StorageEnum>, ndx_table: &Mutex<NdxTable>,
    qos: Option<&QosManager>, enable_acl: bool, completed_paths: &mut HashSet<String>, checkpoint_path: &Path,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>, ledger: &mut SenderSessionLedger,
) -> Result<()> {
    info!("[Sender Remote] Processing transfer requests + collecting acks");
    loop {
        match transport.recv().await {
            Some(ReceiverMsg::TransferRequest { ndx, decision }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    record_classification(stats_consumer, decision, entry.clone()).await;
                    if matches!(
                        send_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await?,
                        SourceTransferOutcome::SourceFailed
                    ) && ledger.record_indexed_error(ndx)
                    {
                        record_copy_error(
                            stats_consumer,
                            entry.get_relative_path().to_path_buf(),
                            format!("source read failed for ndx {ndx}"),
                        )
                        .await;
                    }
                    ledger.record_transfer();
                } else {
                    error!("[Sender Remote] Unknown NDX {ndx}");
                }
            }
            Some(ReceiverMsg::DeltaTransferRequest {
                ndx,
                block_size,
                signatures,
            }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    record_classification(stats_consumer, TransferDecision::DeltaTransfer, entry.clone()).await;
                    if matches!(
                        send_delta_transfer(
                            transport,
                            src_storage,
                            &entry,
                            ndx,
                            block_size,
                            signatures,
                            qos,
                            enable_acl,
                        )
                        .await?,
                        SourceTransferOutcome::Sent
                    ) {
                        ledger.record_transfer();
                    } else if ledger.record_indexed_error(ndx) {
                        record_copy_error(
                            stats_consumer,
                            entry.get_relative_path().to_path_buf(),
                            format!("delta source read failed for ndx {ndx}"),
                        )
                        .await;
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {ndx} for delta");
                }
            }
            Some(ReceiverMsg::Classified { entry, decision }) => {
                record_classification(stats_consumer, decision, entry).await;
            }
            Some(ReceiverMsg::Redo { ndx }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    if matches!(
                        send_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await?,
                        SourceTransferOutcome::SourceFailed
                    ) {
                        ledger.record_indexed_error(ndx);
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {ndx} for redo");
                }
            }
            Some(ReceiverMsg::RequestsDone) => {
                if !ledger.transfer_done_sent {
                    info!(
                        "[Sender Remote] All requests received, {} files to transfer",
                        ledger.transfer_count()
                    );
                    transport.send(SenderMsg::TransferDone).await?;
                    ledger.mark_transfer_done_sent();
                }
            }
            Some(ReceiverMsg::Success { ndx }) => {
                let relative_path = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .map(|entry| entry.get_relative_path().to_string_lossy().to_string());
                record_success_and_checkpoint(relative_path, ledger, completed_paths, checkpoint_path).await;
            }
            Some(ReceiverMsg::EntrySuccess { ref entry }) => {
                record_success_and_checkpoint(
                    Some(entry.get_relative_path().to_string_lossy().to_string()),
                    ledger,
                    completed_paths,
                    checkpoint_path,
                )
                .await;
            }
            Some(ReceiverMsg::Progress(snapshot)) => apply_progress(stats_consumer, snapshot).await,
            Some(ReceiverMsg::EntryError { entry, reason }) => {
                let path = entry.get_relative_path().to_path_buf();
                error!("[Sender Remote] Entry failed {path:?}: {reason}");
                ledger.record_entry_error();
                record_copy_error(stats_consumer, path, reason).await;
            }
            Some(ReceiverMsg::Error { ndx, reason }) => {
                error!("[Sender Remote] NDX {ndx} failed: {reason}");
                if ledger.record_indexed_error(ndx) {
                    let path = ndx_table
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get(ndx)
                        .map_or_else(
                            || PathBuf::from(format!("<ndx-{ndx}>")),
                            |entry| entry.get_relative_path().to_path_buf(),
                        );
                    record_copy_error(stats_consumer, path, reason).await;
                }
            }
            Some(ReceiverMsg::AllDone) => break,
            Some(other) => {
                debug!("[Sender Remote] Ignoring message: {:?}", std::mem::discriminant(&other));
            }
            None => return Err(AppError::CopyError("Transport closed during request/ack phase".into())),
        }
    }
    Ok(())
}

pub(super) async fn record_classification(
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>, decision: TransferDecision, entry: Arc<EntryEnum>,
) {
    if let Some(message) = classification_to_stats_message(decision, entry) {
        stats_consumer.lock().await.update_statistics(&message);
    }
}

pub(super) async fn record_copy_error(
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>, path: PathBuf, reason: String,
) {
    let message = entry_error_stats_message(path, reason);
    stats_consumer.lock().await.update_statistics(&message);
}

pub(super) async fn apply_progress(stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>, snapshot: ProgressSnapshot) {
    stats_consumer
        .lock()
        .await
        .get_bytes_tracker()
        .store(snapshot.bytes_transferred, Ordering::Relaxed);
    info!(
        "[Sender Remote] [{}] Progress: {} files ({}) transferred, {} dirs, {} skipped, {} errors, {:.1}s, {}/s",
        snapshot.receiver_id,
        snapshot.files_transferred,
        format_bytes(snapshot.bytes_transferred as f64, true),
        snapshot.dirs_created,
        snapshot.files_skipped,
        snapshot.error_count,
        snapshot.elapsed_secs,
        format_bytes(snapshot.speed_bytes_per_sec, true),
    );
}

pub(super) fn entry_error_stats_message(path: PathBuf, reason: String) -> StorageEntryMessage {
    StorageEntryMessage::Error {
        event: ErrorEvent::Copy,
        path,
        entry: None,
        reason,
    }
}

pub(super) fn classification_to_stats_message(
    decision: TransferDecision, entry: Arc<EntryEnum>,
) -> Option<StorageEntryMessage> {
    match decision {
        TransferDecision::FullTransfer => Some(StorageEntryMessage::New(entry)),
        TransferDecision::DeltaTransfer => Some(StorageEntryMessage::Changed {
            entry,
            kind: ChangeKind::DataOnly,
        }),
        TransferDecision::MetadataOnly => Some(StorageEntryMessage::Changed {
            entry,
            kind: ChangeKind::MetadataOnly,
        }),
        TransferDecision::Skip => None,
        TransferDecision::Deleted => Some(StorageEntryMessage::Deleted(entry)),
    }
}

pub(super) async fn record_success_and_checkpoint(
    relative_path: Option<String>, ledger: &mut SenderSessionLedger, completed_paths: &mut HashSet<String>,
    checkpoint_path: &Path,
) {
    let success_count = ledger.record_success();
    if let Some(path) = relative_path {
        completed_paths.insert(path);
    }
    if success_count.is_multiple_of(100)
        && let Ok(data) = serde_json::to_string(&completed_paths)
    {
        let _ = tokio::fs::write(checkpoint_path, data).await;
    }
}

async fn load_checkpoint(path: &Path) -> HashSet<String> {
    match tokio::fs::read_to_string(path).await {
        Ok(data) if !data.is_empty() => serde_json::from_str(&data).unwrap_or_else(|error| {
            warn!("[Sender Remote] Checkpoint 解析失败: {error}, 将从头开始");
            HashSet::new()
        }),
        Ok(_) => HashSet::new(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashSet::new(),
        Err(error) => {
            warn!("[Sender Remote] 读取 checkpoint 失败: {error}, 将从头开始");
            HashSet::new()
        }
    }
}

async fn save_or_clear_checkpoint(path: &Path, completed_paths: &HashSet<String>, error_count: u64) {
    if error_count == 0 {
        let _ = tokio::fs::remove_file(path).await;
        info!("[Sender Remote] Checkpoint cleared (all entries succeeded)");
    } else if let Ok(data) = serde_json::to_string(completed_paths) {
        let _ = tokio::fs::write(path, data).await;
        info!(
            "[Sender Remote] Checkpoint saved: {} entries completed",
            completed_paths.len()
        );
    }
}

fn stage_error(stage: &'static str, source: AppError) -> AppError {
    AppError::SenderSessionStage {
        stage,
        source: Box::new(source),
    }
}
