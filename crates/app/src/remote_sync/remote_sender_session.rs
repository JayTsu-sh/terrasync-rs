//! 协商完成后的 Remote Sender 会话 seam。
//!
//! 握手、鉴权、`SessionConfig`、依赖构造和资源关闭由外层 orchestration 负责；本模块
//! 从文件列表发布开始运行到 Receiver 返回 `AllDone`。后续迁移会逐步把会话状态与转换
//! 收入此模块，调用者与测试始终只依赖这里的 interface。

use blake3::Hash;
use data_mover::transfer::ExpertSourcePayload;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedItem;
use transport::chunk::DataChunk;

use crate::remote_inventory::{DirectoryPages, NdxEvent};
use data_mover::model::ObservedEntry;
use data_mover::qos::QosManager;
use storage_adapter::{StorageContext, StorageError};
use sync_delta::DeltaToken;
use sync_delta::matcher::DeltaMatcher;
use task_model::{ChangeKind, ErrorEvent, StorageEntryMessage};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};
use transport::message::{BlockSignature, NdxTable, ProgressSnapshot, ReceiverMsg, SenderMsg, TransferDecision};
use transport::traits::SenderTransport;

use super::{AppError, Result, StatisticConsumer};
use crate::consumer::stats::format_bytes;

/// 构造 negotiated session 所需的既有 adapter 与配置。
pub(super) struct RemoteSenderSessionDeps<'a> {
    pub(super) transport: &'a dyn SenderTransport,
    pub(super) src_storage: &'a Arc<StorageContext>,
    pub(super) walkdir_iter: &'a DirectoryPages,
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
    lifecycle: RemoteSenderSessionLifecycle,
}

pub(super) struct RequestAckSummary {
    pub(super) transfer_count: u64,
    pub(super) success_count: u64,
    pub(super) error_count: u64,
}

struct SourceTransferContext<'a> {
    transport: &'a dyn SenderTransport,
    src_storage: &'a Arc<StorageContext>,
    qos: Option<&'a QosManager>,
    enable_acl: bool,
}

struct RequestLoopContext<'a> {
    source: SourceTransferContext<'a>,
    ndx_table: &'a Mutex<NdxTable>,
    completed_paths: &'a mut HashSet<String>,
    checkpoint_path: &'a Path,
    stats_consumer: &'a Arc<AsyncMutex<StatisticConsumer>>,
    ledger: &'a mut SenderSessionLedger,
}

impl SenderSessionLedger {
    pub(super) fn new() -> Self {
        Self {
            transfer_count: 0,
            success_count: 0,
            error_count: 0,
            errored_ndx: HashSet::new(),
            lifecycle: RemoteSenderSessionLifecycle::Advertising,
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

    fn transition(&mut self, next: RemoteSenderSessionLifecycle) {
        debug_assert!(matches!(
            (self.lifecycle, next),
            (
                RemoteSenderSessionLifecycle::Advertising,
                RemoteSenderSessionLifecycle::RequestsOpen
            ) | (
                RemoteSenderSessionLifecycle::RequestsOpen,
                RemoteSenderSessionLifecycle::TransferDoneSent
            ) | (
                RemoteSenderSessionLifecycle::TransferDoneSent,
                RemoteSenderSessionLifecycle::Completed
            ) | (_, RemoteSenderSessionLifecycle::Failed)
        ));
        self.lifecycle = next;
    }

    fn transfer_done_sent(&self) -> bool {
        self.lifecycle == RemoteSenderSessionLifecycle::TransferDoneSent
    }

    pub(super) fn finish(self) -> RequestAckSummary {
        RequestAckSummary {
            transfer_count: self.transfer_count,
            success_count: self.success_count,
            error_count: self.error_count,
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
        Self { deps }
    }

    pub(super) async fn run(self) -> Result<RemoteSenderSessionSummary> {
        let ndx_table = Mutex::new(NdxTable::new());
        let mut ledger = SenderSessionLedger::new();
        let mut completed_paths = load_checkpoint(self.deps.checkpoint_path).await;
        if !completed_paths.is_empty() {
            info!(
                "[Sender Remote] Loaded checkpoint: {} entries already completed",
                completed_paths.len()
            );
        }
        ledger.transition(RemoteSenderSessionLifecycle::RequestsOpen);
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
            process_requests_and_acks(RequestLoopContext {
                source: SourceTransferContext {
                    transport: self.deps.transport,
                    src_storage: self.deps.src_storage,
                    qos: self.deps.qos,
                    enable_acl: self.deps.enable_acl,
                },
                ndx_table: &ndx_table,
                completed_paths: &mut completed_paths,
                checkpoint_path: self.deps.checkpoint_path,
                stats_consumer: self.deps.stats_consumer,
                ledger: &mut ledger,
            })
            .await
            .map_err(|source| stage_error("requests/acks", source))
        };
        let joined = tokio::try_join!(advertise, requests);
        let (page_count, ()) = match joined {
            Ok(result) => result,
            Err(error) => {
                ledger.transition(RemoteSenderSessionLifecycle::Failed);
                return Err(error);
            }
        };
        let request_summary = ledger.finish();
        save_or_clear_checkpoint(self.deps.checkpoint_path, &completed_paths, request_summary.error_count).await;

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
}

/// 发布文件列表并建立本会话唯一的 index correlation ledger。
///
/// 这是 session implementation 的内部操作；生产调用者只使用
/// [`RemoteSenderSession::run`]。
pub(super) async fn advertise_file_list(
    transport: &dyn SenderTransport, walkdir_iter: &DirectoryPages, ndx_table: &Mutex<NdxTable>,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>,
) -> Result<u64> {
    info!("[Sender Remote] Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.recv().await.ok() {
        match event {
            NdxEvent::Page(page) => {
                {
                    let mut consumer = stats_consumer.lock().await;
                    for file in &page.files {
                        consumer.update_scanned_view(file.entry.view());
                    }
                    for directory in &page.subdirs {
                        consumer.update_scanned_view(directory.entry.view());
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
    transport: &dyn SenderTransport, src_storage: &Arc<StorageContext>, entry: &ObservedItem, ndx: i32,
    qos: Option<&QosManager>, enable_acl: bool,
) -> Result<SourceTransferOutcome> {
    let outcome = if entry.view().version().is_some_and(|version| version.is_delete_marker()) {
        transport.send(SenderMsg::WriteDeleteMarker { ndx }).await?;
        SourceTransferOutcome::Sent
    } else if entry.entry.history().is_none() && entry.get_is_dir() && entry.entry.directory_marker_object().is_none() {
        transport
            .send(SenderMsg::CreateDir {
                entry: Arc::clone(&entry.entry),
            })
            .await?;
        SourceTransferOutcome::Sent
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(&entry.entry).await {
            Ok(target) => {
                transport
                    .send(SenderMsg::CreateSymlink {
                        entry: Arc::clone(&entry.entry),
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
        let Some(mut payload) = open_source_payload(transport, src_storage, entry, ndx, qos).await? else {
            return Ok(SourceTransferOutcome::SourceFailed);
        };
        transport.send(SenderMsg::FileBegin { ndx }).await?;
        let mut offset = 0;
        loop {
            match payload.next_chunk().await {
                Ok(Some(data)) => {
                    let length = data.len() as u64;
                    transport
                        .send(SenderMsg::FileData {
                            ndx,
                            chunk: DataChunk { offset, data },
                        })
                        .await?;
                    offset += length;
                }
                Ok(None) => break,
                Err(error) => return report_source_failure(transport, entry, ndx, error.to_string()).await,
            }
        }
        match payload.finish().await {
            Ok(evidence) => {
                transport
                    .send(SenderMsg::EndOfFile {
                        ndx,
                        source_hash: Some(Hash::from_bytes(evidence.blake3).to_hex().to_string()),
                    })
                    .await?;
                SourceTransferOutcome::Sent
            }
            Err(error) => return report_source_failure(transport, entry, ndx, error.to_string()).await,
        }
    };
    if outcome == SourceTransferOutcome::Sent && entry.entry.history().is_none() {
        send_acl_if_enabled(transport, src_storage, &entry.entry, enable_acl).await;
    }
    Ok(outcome)
}

/// ACL 跨进程传输：仅在启用且 entry 不是符号链接时发送。
pub(super) async fn send_acl_if_enabled(
    transport: &dyn SenderTransport, src_storage: &Arc<StorageContext>, entry: &Arc<ObservedEntry>, enable_acl: bool,
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
async fn send_delta_transfer(
    context: &SourceTransferContext<'_>, entry: &ObservedItem, ndx: i32, block_size: u32,
    signatures: Vec<BlockSignature>,
) -> Result<SourceTransferOutcome> {
    let SourceTransferContext {
        transport,
        src_storage,
        qos,
        enable_acl,
    } = context;
    let signatures = signatures
        .into_iter()
        .map(|signature| sync_delta::BlockSignature {
            rolling: signature.rolling,
            strong: signature.strong,
        })
        .collect::<Vec<_>>();
    let mut matcher = DeltaMatcher::new(&signatures, block_size);
    let mut token_count = 0usize;
    let Some(mut payload) = open_source_payload(*transport, src_storage, entry, ndx, *qos).await? else {
        return Ok(SourceTransferOutcome::SourceFailed);
    };
    loop {
        match payload.next_chunk().await {
            Ok(Some(data)) => {
                let tokens = matcher.push(&data);
                token_count += tokens.len();
                send_delta_tokens(*transport, ndx, tokens, *qos).await?;
            }
            Ok(None) => break,
            Err(error) => return report_source_failure(*transport, entry, ndx, error.to_string()).await,
        }
    }
    let evidence = match payload.finish().await {
        Ok(evidence) => evidence,
        Err(error) => return report_source_failure(*transport, entry, ndx, error.to_string()).await,
    };
    let tokens = matcher.finish();
    token_count += tokens.len();
    send_delta_tokens(*transport, ndx, tokens, *qos).await?;
    transport
        .send(SenderMsg::EndOfFile {
            ndx,
            source_hash: Some(Hash::from_bytes(evidence.blake3).to_hex().to_string()),
        })
        .await?;
    info!(
        "[Sender Remote] Delta transfer {:?}: {} tokens",
        entry.get_relative_path(),
        token_count
    );
    send_acl_if_enabled(*transport, src_storage, &entry.entry, *enable_acl).await;
    Ok(SourceTransferOutcome::Sent)
}

/// 准入成功后才允许目标准备或消费源数据；完整性证据由原生 producer 给出。
async fn open_source_payload(
    transport: &dyn SenderTransport, storage: &StorageContext, entry: &ObservedItem, ndx: i32, qos: Option<&QosManager>,
) -> Result<Option<ExpertSourcePayload>> {
    match StorageContext::open_observed_source(storage, entry, qos).await {
        Ok(session) => Ok(Some(session.stream_from(0).map_err(StorageError::from)?)),
        Err(error) => {
            report_source_failure(transport, entry, ndx, error.to_string()).await?;
            Ok(None)
        }
    }
}

async fn report_source_failure(
    transport: &dyn SenderTransport, entry: &ObservedItem, ndx: i32, reason: String,
) -> Result<SourceTransferOutcome> {
    error!("[Sender Remote] source {:?}: {reason}", entry.get_relative_path());
    transport
        .send(SenderMsg::EntryError {
            path: entry.get_relative_path().to_path_buf(),
            reason,
            ndx: Some(ndx),
        })
        .await?;
    Ok(SourceTransferOutcome::SourceFailed)
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
async fn process_requests_and_acks(context: RequestLoopContext<'_>) -> Result<()> {
    let RequestLoopContext {
        source,
        ndx_table,
        completed_paths,
        checkpoint_path,
        stats_consumer,
        ledger,
    } = context;
    let SourceTransferContext {
        transport,
        src_storage,
        qos,
        enable_acl,
    } = &source;
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
                        send_full_transfer(*transport, src_storage, &entry, ndx, *qos, *enable_acl).await?,
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
                        send_delta_transfer(&source, &entry, ndx, block_size, signatures).await?,
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
            Some(ReceiverMsg::ClassifiedObservation { ndx, decision }) => {
                let item = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(item) = item {
                    record_classification(stats_consumer, decision, item).await;
                }
            }
            Some(ReceiverMsg::Classified { entry, decision }) => {
                record_classification(stats_consumer, decision, entry.into()).await;
            }
            Some(ReceiverMsg::Redo { ndx }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    if matches!(
                        send_full_transfer(*transport, src_storage, &entry, ndx, *qos, *enable_acl).await?,
                        SourceTransferOutcome::SourceFailed
                    ) {
                        ledger.record_indexed_error(ndx);
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {ndx} for redo");
                }
            }
            Some(ReceiverMsg::RequestsDone) => {
                if !ledger.transfer_done_sent() {
                    info!(
                        "[Sender Remote] All requests received, {} files to transfer",
                        ledger.transfer_count()
                    );
                    transport.send(SenderMsg::TransferDone).await?;
                    ledger.transition(RemoteSenderSessionLifecycle::TransferDoneSent);
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
            Some(ReceiverMsg::Error { ndx, reason }) if ndx < 0 => {
                return Err(AppError::CopyError(reason));
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
            Some(ReceiverMsg::AllDone) => {
                if !ledger.transfer_done_sent() {
                    return Err(AppError::CopyError(
                        "Received AllDone before RequestsDone/TransferDone".into(),
                    ));
                }
                ledger.transition(RemoteSenderSessionLifecycle::Completed);
                break;
            }
            Some(other) => {
                debug!("[Sender Remote] Ignoring message: {:?}", std::mem::discriminant(&other));
            }
            None => return Err(AppError::CopyError("Transport closed during request/ack phase".into())),
        }
    }
    Ok(())
}

pub(super) async fn record_classification(
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>, decision: TransferDecision, entry: ObservedItem,
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
    decision: TransferDecision, entry: ObservedItem,
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
        TransferDecision::Deleted => Some(StorageEntryMessage::Deleted(entry.entry)),
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
