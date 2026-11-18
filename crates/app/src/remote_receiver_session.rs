//! 远端 Receiver 按会话比较目标、接收数据并收束落盘结果。
use crate::file_copy::copied_tags_required;
use crate::file_copy::metadata as copy_metadata;
use crate::file_copy::namespace as copy_namespace;
use crate::file_copy::selection::unrepresentable_object;
use crate::rm::progress as deletion;
use data_mover::storage::read_current_range;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(feature = "remote", test))]
use std::time::Duration;
use std::time::Instant;
use task_model::TransferMode;
use task_model::observation::ObservedItem;
use task_model::{ObservedEntryExt, StorageEntryMessage};
use tokio_util::sync::CancellationToken;

use crate::error::storage::Result as StorageResult;
use crate::storage_factory::StorageContext;
use data_mover::model::{FailureClass, ObservedEntry};
#[cfg(feature = "remote")]
use data_mover::redact_storage_url;
use tokio::sync::mpsc::{self, Receiver, Sender};
#[cfg(feature = "remote")]
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use transport::acl::decode_acl;
use transport::chunk::DataChunk;
use transport::directory::DirPageResult;
#[cfg(feature = "remote")]
use transport::error::TransportError;
use transport::flow_control::{ReceiverCreditState, payload_credit_cost};
use transport::message::{
    BlockSignature, FeatureFlags, ProgressSnapshot, ReceiverMsg, SenderMsg, SessionConfig, TransferDecision,
};
#[cfg(feature = "remote")]
use transport::message::{HandshakeResult, ProtocolHandshake};
use transport::traits::ReceiverTransport;

use crate::byte_resume::is_part_file;
use crate::disk_commit::{DcAck, DiskCommitMsg, FileOutcome, disk_commit_task};
use crate::error::{AppError, Result, StorageError};
use crate::remote_inventory::NdxTable;
use crate::sync::parse_size;

mod dest_index;
pub(crate) use dest_index::DestIndex;

// ============================================================
// Receiver 进度跟踪（原子计数器，支持多 Receiver 聚合）
// ============================================================

/// Receiver 侧进度计数器
pub(crate) struct ReceiverProgress {
    pub(crate) files_transferred: AtomicU64,
    pub(crate) dirs_created: AtomicU64,
    pub(crate) bytes_transferred: AtomicU64,
    pub(crate) files_skipped: AtomicU64,
    pub(crate) metadata_only: AtomicU64,
    pub(crate) error_count: AtomicU64,
}

impl Default for ReceiverProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl ReceiverProgress {
    pub(crate) fn new() -> Self {
        Self {
            files_transferred: AtomicU64::new(0),
            dirs_created: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            files_skipped: AtomicU64::new(0),
            metadata_only: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        }
    }

    /// 生成进度快照
    fn snapshot(&self, start_time: Instant) -> ProgressSnapshot {
        let elapsed = start_time.elapsed().as_secs_f64();
        let bytes = self.bytes_transferred.load(Ordering::Relaxed);
        ProgressSnapshot {
            receiver_id: "default".to_string(),
            files_transferred: self.files_transferred.load(Ordering::Relaxed),
            dirs_created: self.dirs_created.load(Ordering::Relaxed),
            bytes_transferred: bytes,
            files_skipped: self.files_skipped.load(Ordering::Relaxed),
            metadata_only: self.metadata_only.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            elapsed_secs: elapsed,
            speed_bytes_per_sec: if elapsed > 0.0 { bytes as f64 / elapsed } else { 0.0 },
        }
    }
}

/// 双进程模式的 Receiver task（入口）
///
/// 协议阶段：
/// 0. 接收 `Handshake` → 校验协议版本/能力 → 回 ack/reject（不兼容则中止）
/// 0.5 接收 `Auth` → 校验 token → 回 `AuthResult`（鉴权失败则中止，不进入 `SessionConfig`）
/// 1. 接收 `SessionConfig`
/// 2. 接收文件列表（`FilePage` → `DestIndex` 逐页比较 → 发 `TransferRequest`）与接收数据流
///    （写入目标端 → 发 `EntrySuccess`/`EntryError`）**合并为一个消费者循环并发处理**
///    （`RemoteReceiverSession::run`），两个流可以同时交付数据。
///
/// `expected_token`：`serve --token` 配置的期望值；为 `None` 时跳过鉴权（向后兼容，
/// 未配置 token 时不校验，仍需完成 `Auth` 消息交换以保持协议阶段一致）。
#[cfg(feature = "remote")]
pub(crate) async fn receive(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: Arc<StorageContext>, expected_token: Option<&str>,
) -> Result<()> {
    info!("[Receiver Remote] Started, waiting for Handshake");

    // ── 阶段 -1: 协议握手（版本 + 能力协商），不兼容则回 reject 后立即中止 ──
    let negotiated_features = recv_and_negotiate_handshake(transport, &dest_storage).await?;

    // ── 阶段 -0.5: Token 鉴权，失败则回 AuthResult{ok:false} 并断连、中止 ──
    recv_and_check_auth(transport, expected_token).await?;

    // ── 阶段 0: 接收 SessionConfig ──
    let session_config = recv_session_config(transport).await?;
    info!(
        "[Receiver Remote] SessionConfig: src_path={}, integrity={}, acl={}",
        redact_storage_url(&session_config.src_path),
        session_config.enable_integrity_check,
        session_config.enable_acl
    );
    let delta_size_threshold =
        if session_config.transfer_mode == TransferMode::Direct && session_config.delta_size_threshold.is_none() {
            u64::MAX
        } else {
            resolve_delta_size_threshold(&session_config.delta_size_threshold)?
        };

    // ── 进度跟踪 ──
    let progress = Arc::new(ReceiverProgress::new());
    let start_time = Instant::now();
    let (progress_tx, progress_rx) = mpsc::channel::<ProgressSnapshot>(4);
    let progress_reporter = {
        let progress = progress.clone();
        let tx = progress_tx;
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let snapshot = progress.snapshot(start_time);
                if tx.send(snapshot).await.is_err() {
                    break;
                }
            }
        })
    };

    // ── 阶段 1+2: 文件列表与数据流合并处理，无阶段 barrier（见函数入口文档） ──
    let session = RemoteReceiverSession::new(
        dest_storage,
        session_config,
        negotiated_features,
        delta_size_threshold,
        progress.clone(),
    );
    let run_result = session.run(transport, progress_rx).await;

    // 停止进度 reporter，发最终快照 + AllDone
    progress_reporter.abort();
    run_result?;
    session.finish(transport, start_time).await;
    info!("[Receiver Remote] Completed");
    Ok(())
}

/// 阶段 -1：等待 Sender 的 `Handshake`，校验协议版本与能力，回发 ack/reject
///
/// 版本不兼容时发送 `Rejected` 后立即返回错误，不进入 `SessionConfig` / 文件列表阶段，
/// 确保拒绝发生在任何 `FilePage` / `TransferRequest` / `CopyEntry` 之前。
#[cfg(feature = "remote")]
async fn recv_and_negotiate_handshake(
    transport: &(dyn ReceiverTransport + 'static), destination: &StorageContext,
) -> Result<FeatureFlags> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::Handshake(remote)) => {
                let result = ProtocolHandshake::current().negotiate(&remote);
                let mut ack = result.clone();
                if let HandshakeResult::Accepted { copy_tags, .. } = &mut ack {
                    *copy_tags = copied_tags_required(destination)?;
                }
                match result {
                    HandshakeResult::Accepted { features, .. } => {
                        info!(
                            "[Receiver Remote] Handshake accepted, negotiated features: {:?}",
                            features
                        );
                        transport.send(ReceiverMsg::HandshakeAck(ack)).await?;
                        return Ok(features);
                    }
                    HandshakeResult::Rejected { reason } => {
                        warn!("[Receiver Remote] Handshake rejected: {}", reason);
                        transport.send(ReceiverMsg::HandshakeAck(ack)).await?;
                        return Err(TransportError::IncompatibleProtocol { reason }.into());
                    }
                }
            }
            Some(_) => warn!("[Receiver Remote] Expected Handshake, skipping"),
            None => return Err(AppError::CopyError("Transport closed before Handshake".into())),
        }
    }
}

/// 阶段 -0.5：等待 Sender 的 `Auth`，校验 token，回发 `AuthResult`
///
/// `expected_token` 为 `None` 时（Receiver 未配置 `--token`）接受任意 token，仅记录日志；
/// 配置了 token 时必须与 Sender 提供的值完全一致，否则回复失败并 `close()` 连接，
/// 确保鉴权失败发生在 `SessionConfig` / 文件列表 / 数据阶段之前。
#[cfg(feature = "remote")]
async fn recv_and_check_auth(
    transport: &(dyn ReceiverTransport + 'static), expected_token: Option<&str>,
) -> Result<()> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::Auth { token }) => {
                let ok = match expected_token {
                    Some(expected) => token == expected,
                    None => true,
                };
                if ok {
                    info!("[Receiver Remote] Auth accepted");
                    transport
                        .send(ReceiverMsg::AuthResult { ok: true, reason: None })
                        .await?;
                    return Ok(());
                }
                let reason = "invalid token".to_string();
                warn!("[Receiver Remote] Auth rejected: {}", reason);
                transport
                    .send(ReceiverMsg::AuthResult {
                        ok: false,
                        reason: Some(reason.clone()),
                    })
                    .await?;
                transport.close().await?;
                return Err(TransportError::AuthFailed { reason }.into());
            }
            Some(_) => warn!("[Receiver Remote] Expected Auth, skipping"),
            None => return Err(AppError::CopyError("Transport closed before Auth".into())),
        }
    }
}

/// 阶段 0：等待并返回 SessionConfig，忽略非预期消息
#[cfg(feature = "remote")]
async fn recv_session_config(transport: &(dyn ReceiverTransport + 'static)) -> Result<SessionConfig> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::SessionConfig(config)) => return Ok(config),
            Some(_) => warn!("[Receiver Remote] Expected SessionConfig, skipping"),
            None => return Err(AppError::CopyError("Transport closed before SessionConfig".into())),
        }
    }
}

/// delta 传输 size 门槛默认值：512MiB（当前 delta 峰值内存 ≈3× 文件大小，最坏锁在 ~1.5GB，
/// 见 issue #54 阶段 0 spec）
const DEFAULT_DELTA_SIZE_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

/// 解析 `SessionConfig.delta_size_threshold`：`None` 时使用默认值 512MiB，`Some` 时复用
/// `parse_size`（与 `block_size` 同款人类可读格式，如 "512MiB"）。超过该 size 的文件即使
/// 数据不匹配也降级为全量传输（见远端会话的差分分类）。
fn resolve_delta_size_threshold(threshold: &Option<String>) -> Result<u64> {
    match threshold {
        Some(s) => Ok(parse_size(s)? as u64),
        None => Ok(DEFAULT_DELTA_SIZE_THRESHOLD_BYTES),
    }
}

/// 校验 Sender 提供的相对路径是否安全：拒绝绝对路径与含 `..` 组件的路径
///
/// 双进程远端模式下 Sender 不可信，若 `entry.get_relative_path()` 被恶意构造为绝对路径
/// 或含 `..` 的路径，会导致目标端在 `dest_path` 之外写入/覆盖文件（路径穿越）。
/// 此校验必须在所有以该路径驱动 `dest_storage` 写操作之前调用。
pub(crate) fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(AppError::UnsafeRelativePath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

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
    Full {
        ndx: i32,
    },
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
    created_dirs: Vec<Arc<ObservedEntry>>,
    active_transfer: ActiveTransfer,
    credit: ReceiverCreditState,
    attempts: HashMap<i32, u8>,
}

impl RemoteSessionState {
    pub(crate) async fn skip_unrepresentable(
        &self, transport: &(dyn ReceiverTransport + 'static), destination: &StorageContext, ndx: i32,
        entry: &ObservedItem,
    ) -> Result<bool> {
        let Some(reason) = unrepresentable_object(&entry.entry, destination) else {
            return Ok(false);
        };
        // 历史段内所有版本共用一个 entry；只在首个版本报告一次跳过。
        if entry.version_index.is_none_or(|index| index == 0) {
            transport
                .send(ReceiverMsg::CopySkipped {
                    ndx,
                    reason: reason.into(),
                })
                .await?;
        }
        Ok(true)
    }
    pub(crate) fn ingest_page(&mut self, page: &DirPageResult) {
        self.ndx_table.ingest_page(page);
    }

    pub(crate) fn indexed_entry(&self, ndx: i32) -> Option<&ObservedItem> {
        self.ndx_table.get(ndx)
    }

    pub(crate) fn indexed_len(&self) -> usize {
        self.ndx_table.len()
    }

    /// 处理不需要 delta implementation 的基础分类。返回该分类是否已完成处理。
    pub(crate) async fn handle_base_classification(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &StorageContext, progress: &ReceiverProgress,
        ndx: i32, entry: &ObservedItem, decision: TransferDecision,
    ) -> bool {
        match decision {
            TransferDecision::FullTransfer => {
                let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
                self.ledger.record_request();
                true
            }
            TransferDecision::MetadataOnly => {
                if let Err(error) = copy_metadata::set_entry_metadata(dest.storage(), &entry.entry, None).await {
                    let _ = transport
                        .send(ReceiverMsg::EntryError {
                            entry: Arc::clone(&entry.entry),
                            reason: error.to_string(),
                        })
                        .await;
                    return true;
                }
                progress
                    .metadata_only
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::ClassifiedObservation { ndx, decision })
                    .await;
                true
            }
            TransferDecision::Skip => {
                progress
                    .files_skipped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::ClassifiedObservation { ndx, decision })
                    .await;
                true
            }
            TransferDecision::DeltaTransfer | TransferDecision::Deleted => false,
        }
    }

    /// 处理 changed entry 的 delta eligibility、basis signature 与 full fallback。
    pub(crate) async fn handle_delta_classification(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &Arc<StorageContext>, features: &FeatureFlags,
        delta_size_threshold: u64, ndx: i32, entry: &ObservedItem, decision: TransferDecision,
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
        let mut sig_calc = sync_delta::signature::SignatureCalculator::new(block_size);
        let basis_read_result: StorageResult<()> = async {
            let source = dest.storage().read_source()?;
            let maximum = NonZeroUsize::new(dest.block_size() as usize).unwrap_or(NonZeroUsize::MIN);
            let mut input = read_current_range(
                source.as_ref(),
                entry.entry.path().clone(),
                None,
                maximum,
                CancellationToken::new(),
            )
            .await?;
            while let Some(chunk) = input.next().await {
                sig_calc.push(&chunk?.data);
            }
            Ok(())
        }
        .await;

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
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &StorageContext, page: &DirPageResult,
        dest_index: &mut DestIndex, delete_target: bool,
    ) -> Result<()> {
        for subdir in &page.subdirs {
            dest_index.mark_matched(&subdir.entry.entry);
            if self
                .skip_unrepresentable(transport, dest, subdir.ndx, &subdir.entry)
                .await?
            {
                continue;
            }
            if let Err(error) = validate_relative_path(subdir.entry.get_relative_path()) {
                warn!("[Receiver Remote] Rejecting unsafe subdir path: {}", error);
                let _ = transport
                    .send(ReceiverMsg::EntryError {
                        entry: Arc::clone(&subdir.entry.entry),
                        reason: format!("{error}"),
                    })
                    .await;
                continue;
            }
            if dest.storage().holds_directory_objects() {
                if subdir.entry.entry.directory_marker_object().is_some() {
                    let copy_tags = match copied_tags_required(dest) {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = transport
                                .send(ReceiverMsg::EntryError {
                                    entry: Arc::clone(&subdir.entry.entry),
                                    reason: format!("{error}"),
                                })
                                .await;
                            continue;
                        }
                    };
                    let decision = dest_index.check_directory_marker(&subdir.entry.entry, copy_tags);
                    if decision == TransferDecision::Skip {
                        let _ = transport
                            .send(ReceiverMsg::ClassifiedObservation {
                                ndx: subdir.ndx,
                                decision,
                            })
                            .await;
                    } else {
                        let _ = transport
                            .send(ReceiverMsg::TransferRequest {
                                ndx: subdir.ndx,
                                decision,
                            })
                            .await;
                        self.ledger.record_request();
                    }
                } else if delete_target && dest_index.has_directory_marker(&subdir.entry.entry) {
                    if let Err(error) =
                        copy_namespace::remove_directory_marker(dest.storage(), &subdir.entry.entry).await
                    {
                        let _ = transport
                            .send(ReceiverMsg::EntryError {
                                entry: Arc::clone(&subdir.entry.entry),
                                reason: format!("{error}"),
                            })
                            .await;
                    } else {
                        dest_index.mark_directory_object_matched(&subdir.entry.entry);
                    }
                }
                continue;
            }
            if let Err(error) = copy_namespace::create_dir_all(dest.storage(), &subdir.entry.entry).await {
                warn!(
                    "[Receiver Remote] create_dir {:?}: {}",
                    subdir.entry.get_relative_path(),
                    error
                );
                let _ = transport
                    .send(ReceiverMsg::EntryError {
                        entry: Arc::clone(&subdir.entry.entry),
                        reason: format!("{error}"),
                    })
                    .await;
                continue;
            }
            self.created_dirs.push(Arc::clone(&subdir.entry.entry));
        }

        if !delete_target {
            return Ok(());
        }
        for orphan in dest_index.orphaned_entries() {
            let path = orphan.get_relative_path();
            if is_part_file(path) {
                debug!("[Receiver Remote] Skipping in-progress part file: {:?}", path);
                continue;
            }
            let result = if orphan.get_is_dir() {
                info!("[Receiver Remote] Deleting orphaned dir: {:?}", path);
                deletion::delete_dir_all(dest.storage(), orphan).await
            } else {
                info!("[Receiver Remote] Deleting orphaned file: {:?}", path);
                copy_namespace::delete_file(dest.storage(), orphan).await
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
        Ok(())
    }

    /// 枚举失败时放弃待回写的目录元数据，避免将不完整任务视为完成。
    pub(crate) fn discard_directory_metadata(&mut self) {
        self.created_dirs.clear();
    }

    /// 在所有文件 durable outcome 完成后恢复目录 metadata。
    pub(crate) async fn finalize_directories(&self, dest: &StorageContext) {
        for entry in &self.created_dirs {
            if let Err(error) = copy_metadata::set_entry_metadata(dest.storage(), entry, None).await {
                warn!(
                    "[Receiver Remote] 回写目录元数据 {:?} 失败: {}",
                    entry.get_relative_path(),
                    error
                );
            }
        }
    }

    pub(crate) async fn begin_full(&mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: ObservedItem) -> bool {
        if self.ledger.terminated_ndx.contains(&ndx) {
            debug!("[Receiver Remote] dropping late FileBegin for terminated ndx {}", ndx);
            return false;
        }
        if self.active_transfer != ActiveTransfer::Idle {
            warn!("[Receiver Remote] rejecting FileBegin while another transfer is active");
            return false;
        }
        self.active_transfer = ActiveTransfer::Full { ndx };
        let _ = dc_tx
            .send(DiskCommitMsg::FileBegin {
                ndx,
                entry: entry.into(),
            })
            .await;
        true
    }

    async fn push_full_chunk(&self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, chunk: DataChunk) -> bool {
        if !matches!(self.active_transfer, ActiveTransfer::Full { ndx: active } if active == ndx) {
            warn!("[Receiver Remote] rejecting FileData without an active full transfer");
            return false;
        }
        dc_tx.send(DiskCommitMsg::FileChunk { ndx, chunk }).await.is_ok()
    }

    /// Accepts one full-data chunk at the bounded disk-commit seam and returns
    /// credit only after that enqueue succeeds.
    pub(crate) async fn accept_full_chunk(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dc_tx: &Sender<DiskCommitMsg>, ndx: i32,
        chunk: DataChunk,
    ) -> Result<bool> {
        let bytes = payload_credit_cost(&chunk.data);
        if !self.push_full_chunk(dc_tx, ndx, chunk).await {
            return Ok(false);
        }
        self.credit.accepted(transport, bytes).await?;
        Ok(true)
    }

    /// 提交 active full transfer；返回 false 表示该 `EndOfFile` 应由 delta 路径处理。
    pub(crate) async fn commit_full(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<ObservedEntry>, source_hash: &Option<String>,
    ) -> bool {
        if self.ledger.terminated_ndx.contains(&ndx)
            || !matches!(self.active_transfer, ActiveTransfer::Full { ndx: active_ndx } if active_ndx == ndx)
        {
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
    async fn ensure_delta_active(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<ObservedEntry>,
    ) -> bool {
        if self.ledger.terminated_ndx.contains(&ndx) {
            debug!("[Receiver Remote] dropping late delta event for terminated ndx {}", ndx);
            return false;
        }
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
            ActiveTransfer::Full { .. } => {
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
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<ObservedEntry>, block_index: u32,
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

    async fn push_delta_data(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<ObservedEntry>, data: bytes::Bytes,
    ) -> bool {
        if !self.ensure_delta_active(dc_tx, ndx, entry).await {
            return false;
        }
        dc_tx
            .send(DiskCommitMsg::DeltaData {
                entry: entry.clone(),
                data,
            })
            .await
            .is_ok()
    }

    /// Accepts delta literal data through the same bounded-sink credit transition
    /// as full-file chunks. Match tokens intentionally carry no byte credit.
    pub(crate) async fn accept_delta_data(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dc_tx: &Sender<DiskCommitMsg>, ndx: i32,
        entry: &Arc<ObservedEntry>, data: bytes::Bytes,
    ) -> Result<bool> {
        let bytes = payload_credit_cost(&data);
        if !self.push_delta_data(dc_tx, ndx, entry, data).await {
            return Ok(false);
        }
        self.credit.accepted(transport, bytes).await?;
        Ok(true)
    }

    /// Commits a delta stream, lazily beginning it when it contains zero tokens.
    pub(crate) async fn commit_delta(
        &mut self, dc_tx: &Sender<DiskCommitMsg>, ndx: i32, entry: &Arc<ObservedEntry>, source_hash: &Option<String>,
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
        match ndx {
            Some(ndx) => {
                let first_terminal = self.record_terminal(ndx);
                if matches!(
                    self.active_transfer,
                    ActiveTransfer::Full { ndx: active_ndx } | ActiveTransfer::Delta { ndx: active_ndx }
                        if active_ndx == ndx
                ) {
                    self.abort_active_file(dc_tx).await;
                }
                first_terminal && self.is_complete()
            }
            None => {
                self.abort_active_file(dc_tx).await;
                self.complete_unindexed()
            }
        }
    }

    /// Stops disk commit, drains every durable outcome, then restores directory metadata.
    pub(crate) async fn shutdown_disk_commit(
        &mut self, dc_tx: Sender<DiskCommitMsg>, dc_join: tokio::task::JoinHandle<Result<()>>,
        ack_rx: &mut tokio::sync::mpsc::UnboundedReceiver<DcAck>, transport: &(dyn ReceiverTransport + 'static),
        progress: &ReceiverProgress, dest: &StorageContext,
    ) -> Result<()> {
        let _ = dc_tx.send(DiskCommitMsg::Shutdown).await;
        drop(dc_tx);
        dc_join
            .await
            .map_err(|error| crate::error::AppError::CopyError(format!("disk-commit task join: {error}")))??;
        while let Ok(DcAck { ndx, outcome }) = ack_rx.try_recv() {
            let _ = self.handle_file_outcome(transport, progress, ndx, outcome).await;
        }
        self.finalize_directories(dest).await;
        Ok(())
    }

    /// Residual delayed credit is connection-local and no longer useful after
    /// the receive loop terminates; explicitly discard it on every terminal path.
    pub(crate) fn discard_residual_credit(&mut self) {
        self.credit.reset();
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
struct RemoteReceiverSession {
    dest_storage: Arc<StorageContext>,
    session_config: SessionConfig,
    negotiated_features: FeatureFlags,
    delta_size_threshold: u64,
    progress: Arc<ReceiverProgress>,
}

impl RemoteReceiverSession {
    fn new(
        dest_storage: Arc<StorageContext>, session_config: SessionConfig, negotiated_features: FeatureFlags,
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

    /// 控制流的 `TransferDone` 可能先于数据抵达。请求和落盘结果都已结清时才能结束。
    async fn run(
        &self, transport: &(dyn ReceiverTransport + 'static), progress_rx: Receiver<ProgressSnapshot>,
    ) -> Result<()> {
        self.run_with_state(transport, progress_rx, &mut RemoteSessionState::default())
            .await
    }

    async fn run_with_state(
        &self, transport: &(dyn ReceiverTransport + 'static), mut progress_rx: Receiver<ProgressSnapshot>,
        state: &mut RemoteSessionState,
    ) -> Result<()> {
        let dest_storage = &self.dest_storage;
        let session_config = &self.session_config;
        let negotiated_features = &self.negotiated_features;
        let delta_size_threshold = self.delta_size_threshold;
        let progress = &self.progress;
        info!("[Receiver Remote] Receiving file list and file data (pipelined, streaming mode)");
        // ndx 终态去重与完成判断由 session ledger 统一持有。
        // credit 策略由 transport::flow_control 的 ReceiverCreditState 持有。此循环只通过
        // session 的 accept_full_chunk/accept_delta_data 报告 bounded dc_tx 已接受 payload；
        // 阈值、累计量和 CreditGrant 构造不再属于应用层。
        // disk-commit task：全量/delta 文件均三段流式落盘（去整文件 BytesMut/token Vec）；ack
        // 经 unbounded channel 回流，避免路由 select 阻在 dc_tx.send 时不 drain ack → dc 阻在
        // ack_tx.send 的双向死锁。
        let keeps_history = dest_storage.storage().staged_destination()?.keeps_history().await?;
        let (dc_tx, dc_rx) = mpsc::channel::<DiskCommitMsg>(16);
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<DcAck>();
        let disk_cancel = CancellationToken::new();
        let _cancel_on_drop = disk_cancel.clone().drop_guard();
        let dc_join = tokio::spawn(disk_commit_task(
            dest_storage.clone(),
            session_config.clone(),
            dc_rx,
            ack_tx,
            progress.clone(),
            disk_cancel.clone(),
        ));
        let mut transport_closed = false;
        let mut receive_failure = None;

        loop {
            // 同时处理 transport 消息和 progress 上报
            tokio::select! {
                Some(snapshot) = progress_rx.recv() => {
                    let _ = transport.send(ReceiverMsg::Progress(snapshot)).await;
                }
                // 落盘结果由会话统一决定确认、重试或失败。
                Some(DcAck { ndx, outcome }) = ack_rx.recv() => {
                    if state.handle_file_outcome(transport, progress, ndx, outcome).await {
                        break;
                    }
                }
                msg = transport.recv() => {
                match msg {
                // ── 文件列表：FilePage → DestIndex 比较 → 发 TransferRequest/DeltaTransferRequest ──
                Some(SenderMsg::FilePage(page)) => {
                    state.ingest_page(&page);

                    let mut dest_index = match read_destination_index(dest_storage, &page.dir_path).await {
                        Ok(index) => index,
                        Err(error) => {
                            receive_failure = Some(error);
                            break;
                        }
                    };

                    // 逐文件比较 → 发 TransferRequest/DeltaTransferRequest（New/Changed，前者
                    // 附带 decision 供 Sender 侧统计正确分类）或 Classified（MetadataOnly/Skip，
                    // Receiver 本地执行/判定，此前零 wire 流量，见 issue #23）
                    for nf in &page.files {
                        if state.skip_unrepresentable(transport, dest_storage, nf.ndx, &nf.entry).await? {
                            dest_index.mark_matched(&nf.entry.entry);
                            continue;
                        }
                        if let Some(version) = nf.entry.view().version() {
                            dest_index.mark_matched(&nf.entry.entry);
                            if !keeps_history && (!version.is_latest() || version.is_delete_marker()) { continue; }
                            if !keeps_history && !dest_storage.storage().holds_directory_objects() && nf.entry.get_is_dir() {
                                copy_namespace::create_dir_all(dest_storage.storage(), &nf.entry.entry).await?;
                                progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                                transport.send(ReceiverMsg::ClassifiedObservation { ndx: nf.ndx, decision: TransferDecision::FullTransfer }).await?;
                                transport.send(ReceiverMsg::Success { ndx: nf.ndx }).await?;
                                continue;
                            }
                        }
                        let decision = if nf.entry.view().version().is_some() { TransferDecision::FullTransfer } else { dest_index.check(&nf.entry.entry) };
                        if state
                            .handle_base_classification(transport, dest_storage, progress, nf.ndx, &nf.entry, decision)
                            .await
                        {
                            continue;
                        }
                        if state
                            .handle_delta_classification(
                                transport,
                                dest_storage,
                                negotiated_features,
                                delta_size_threshold,
                                nf.ndx,
                                &nf.entry,
                                decision,
                            )
                            .await
                        {
                            continue;
                        }
                        debug_assert_eq!(decision, TransferDecision::Deleted, "all other classifications are handled");
                    }

                    state
                        .handle_directory_lifecycle(
                            transport,
                            dest_storage,
                            &page,
                            &mut dest_index,
                            session_config.delete_target,
                        )
                        .await?;
                }
                Some(SenderMsg::FileListError { path, reason }) => {
                    receive_failure = Some(AppError::CopyError(format!("Source listing failed at {path}: {reason}")));
                    break;
                }
                Some(SenderMsg::FileListDone) => {
                    info!(
                        "[Receiver Remote] File list complete, {} entries indexed",
                        state.indexed_len()
                    );
                    let _ = transport.send(ReceiverMsg::RequestsDone).await;
                }

                // ── 文件开始：标记走全量流式路径，交 disk-commit task 起 resume_prepare + write_chunk_stream ──
                Some(SenderMsg::WriteDeleteMarker { ndx }) => {
                    if let Some(entry) = state.indexed_entry(ndx).cloned() {
                        dc_tx.send(DiskCommitMsg::WriteDeleteMarker { ndx, entry }).await
                            .map_err(|_| AppError::CopyError("Disk commit task closed before delete marker".into()))?;
                    }
                }
                Some(SenderMsg::FileBegin { ndx }) => {
                    if let Some(entry) = state.indexed_entry(ndx).cloned() { state.begin_full(&dc_tx, ndx, entry).await; }
                }

                // ── 数据流模式：目录 ──
                Some(SenderMsg::CreateDir { entry }) => {
                    if let Err(e) = validate_relative_path(entry.get_relative_path()) {
                        warn!("[Receiver Remote] Rejecting unsafe relative path: {}", e);
                        let _ = transport
                            .send(ReceiverMsg::EntryError { entry, reason: format!("{e}") })
                            .await;
                        continue;
                    }
                    if let Err(e) = copy_namespace::create_dir_all(dest_storage.storage(), &entry).await {
                        warn!("[Receiver Remote] create_dir {:?}: {}", entry.get_relative_path(), e);
                    }
                    let _ = copy_metadata::set_entry_metadata(dest_storage.storage(), &entry, None).await;
                    progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                    let _ = transport.send(ReceiverMsg::EntrySuccess { entry }).await;
                }

                // ── 数据流模式：符号链接 ──
                Some(SenderMsg::CreateSymlink { entry, target }) => {
                    if let Err(e) = validate_relative_path(entry.get_relative_path()) {
                        warn!("[Receiver Remote] Rejecting unsafe relative path: {}", e);
                        let _ = transport
                            .send(ReceiverMsg::EntryError { entry, reason: format!("{e}") })
                            .await;
                        if state.complete_unindexed() {
                            info!("[Receiver Remote] All transfers complete");
                            break;
                        }
                        continue;
                    }
                    match copy_namespace::create_symlink(dest_storage.storage(), &entry, &target).await {
                        Ok(()) => {
                            progress.files_transferred.fetch_add(1, Ordering::Relaxed);
                            let _ = transport.send(ReceiverMsg::EntrySuccess { entry }).await;
                        }
                        Err(e) => {
                            error!("[Receiver Remote] create_symlink {:?}: {}", entry.get_relative_path(), e);
                            let _ = transport
                                .send(ReceiverMsg::EntryError { entry, reason: format!("{e}") })
                                .await;
                        }
                    }
                    if state.complete_unindexed() {
                        info!("[Receiver Remote] All transfers complete");
                        break;
                    }
                }

                // ── 数据流模式：文件数据块 → 转发给 disk-commit task 的 write_chunk_stream ──
                Some(SenderMsg::FileData { ndx, chunk }) => {
                    if let Err(error) = state.accept_full_chunk(transport, &dc_tx, ndx, chunk).await {
                        receive_failure = Some(error);
                        break;
                    }
                }

                // ── Delta token 接收：逐 token 转发给 disk-commit task（不再攒整文件 Vec，见
                //    issue #54 阶段 3），首个属于该 ndx 的 token 先触发一次 DeltaBegin ──
                Some(SenderMsg::DeltaMatch { ndx, block_index }) => {
                    if let Some(entry) = state.indexed_entry(ndx).cloned() {
                        state.push_delta_match(&dc_tx, ndx, &entry.entry, block_index).await;
                    } else {
                        warn!("[Receiver Remote] DeltaMatch for unknown ndx {}", ndx);
                    }
                }
                Some(SenderMsg::DeltaData { ndx, data }) => {
                    if let Some(entry) = state.indexed_entry(ndx).cloned() {
                        if let Err(error) = state.accept_delta_data(transport, &dc_tx, ndx, &entry.entry, data).await {
                            receive_failure = Some(error);
                            break;
                        }
                    } else {
                        warn!("[Receiver Remote] DeltaData for unknown ndx {}", ndx);
                    }
                }

                // ── 文件结束：见过 FileBegin → 全量交 disk-commit task 收尾（读回 .part hash 校验 → 原子
                //    rename）；否则是 delta 路径（含零 token 的空文件 delta，session state
                //    幂等补发 DeltaBegin），同样交 disk-commit task 三段式收尾。完成计数统一
                //    在 dc ack 回流时才 +1（session outcome policy 做 redo 决策，全量/delta 共用一份状态机）──
                Some(SenderMsg::EndOfFile { ndx, source_hash }) => {
                    if let Some(entry) = state.indexed_entry(ndx).cloned() {
                        if !state.commit_full(&dc_tx, ndx, &entry.entry, &source_hash).await {
                            state.commit_delta(&dc_tx, ndx, &entry.entry, &source_hash).await;
                        }
                    }
                }

                // ── ACL 跨进程 ──
                Some(SenderMsg::SetAcl { entry, acl_data }) => {
                    if session_config.enable_acl {
                        let result = async {
                            let acl = decode_acl(&acl_data)?;
                            copy_metadata::apply_acl(dest_storage.storage(), entry.get_relative_path(), acl).await?;
                            Ok::<_, AppError>(())
                        }.await;
                        if let Err(e) = result {
                            warn!("[Receiver Remote] set ACL {:?}: {}", entry.get_relative_path(), e);
                        }
                    }
                }

                Some(SenderMsg::TransferDone) => {
                    let complete = state.observe_transfer_done();
                    let (completed, requested) = state.counts();
                    debug!(
                        "[Receiver Remote] TransferDone received ({}/{} data transfers completed so far)",
                        completed, requested
                    );
                    if complete {
                        info!("[Receiver Remote] All transfers complete");
                        break;
                    }
                }
                // ── Sender 自检失败（源读/符号链接读失败）：中止正在写入的文件（丢弃 .part，
                //    dc 不发 ack）。Sender 已在本地自增 error_count，这里只完成该 ndx 的
                //    计数、不回发 ReceiverMsg::Error（避免与 Sender 自增重复）；按 ndx 去重
                //    防止与 dc task 独立上报的终态重复计数（[1]）──
                Some(SenderMsg::EntryError { path, reason, ndx }) => {
                    warn!("[Receiver Remote] Sender EntryError {:?}: {} — 中止该文件", path, reason);
                    let should_break = state.handle_source_failure(&dc_tx, ndx).await;
                    if should_break {
                        info!("[Receiver Remote] All transfers complete");
                        break;
                    }
                }
                Some(_) => {}
                None => {
                    transport_closed = true;
                    break;
                }
            }} // close match + select! msg arm
            } // close select!
        } // close loop

        if receive_failure.is_some() || transport_closed {
            disk_cancel.cancel();
            if receive_failure.is_some() {
                progress.error_count.fetch_add(1, Ordering::Relaxed);
            }
            // 先通知对端停止，再收尾已开始的磁盘任务；失败不能触发目录完成元数据。
            let _ = transport.close().await;
            state.abort_active_file(&dc_tx).await;
            state.discard_directory_metadata();
        }
        let shutdown_result = state
            .shutdown_disk_commit(dc_tx, dc_join, &mut ack_rx, transport, progress, dest_storage)
            .await;
        // Connection-local residual credit is meaningless after every terminal path,
        // including a disk-commit shutdown failure.
        state.discard_residual_credit();
        if let Some(error) = receive_failure {
            if let Err(cleanup_error) = shutdown_result {
                warn!(%cleanup_error, "disk cleanup failed after enumeration failure");
            }
            return Err(error);
        }
        shutdown_result?;

        if transport_closed {
            return Err(AppError::CopyError(
                "Transport closed during file list/data phase".into(),
            ));
        }

        Ok(())
    }

    /// 落盘收束成功后发送最终快照和会话终态。
    async fn finish(&self, transport: &(dyn ReceiverTransport + 'static), start_time: Instant) {
        let _ = transport
            .send(ReceiverMsg::Progress(self.progress.snapshot(start_time)))
            .await;
        let _ = transport.send(ReceiverMsg::AllDone).await;
    }
}

/// 目标端枚举失败时，不能据此判定文件缺失或需要删除。
async fn read_destination_index(storage: &StorageContext, directory: &str) -> Result<DestIndex> {
    let mut index = DestIndex::new();
    let iterator = match storage
        .read_directory(Path::new(directory), copied_tags_required(storage)?)
        .await
    {
        Ok(iterator) => iterator,
        Err(error) if error.is_not_found() => return Ok(index),
        Err(error) => return Err(error.into()),
    };
    let mut observed = false;
    while let Some(message) = iterator.next().await {
        if let StorageEntryMessage::Scanned(entry) = message {
            observed = true;
            index.insert(entry);
        }
    }
    if let Some(error) = iterator.take_error() {
        let missing_root = matches!(&error,
            StorageError::TraversalEntry { path, source, .. }
                if path.as_str() == directory && source.class() == FailureClass::NotFound);
        if !observed && (missing_root || error.is_not_found()) {
            return Ok(index);
        }
        return Err(error.into());
    }
    Ok(index)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::field_reassign_with_default, clippy::unwrap_used)]
mod tests {
    use crate::scan::observe_path;
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::remote_inventory::NdxEvent;
    use async_trait::async_trait;
    use bytes::Bytes;
    use transport::directory::{DirPageResult, NdxEntry};

    use crate::remote_inventory::directory_pages;
    use crate::scan::source::WalkOptions;
    use crate::storage_factory::{StorageRole, create_storage_for_role};
    use tempfile::tempdir;
    use tokio::sync::{Mutex as TokioMutex, mpsc};
    use transport::error::Result as TransportResult;
    use transport::in_process::create_in_process_pair;
    use transport::traits::{ReceiverTransport, SenderTransport};

    use super::*;

    async fn create_storage(path: &str, block_size: Option<u64>, ensure_dir: bool) -> Result<StorageContext> {
        let role = if ensure_dir {
            StorageRole::Destination
        } else {
            StorageRole::Source
        };
        create_storage_for_role(path, block_size, ensure_dir, role).await
    }

    struct RecordingReceiverTransport {
        incoming: TokioMutex<mpsc::Receiver<SenderMsg>>,
        sent: TokioMutex<Vec<ReceiverMsg>>,
        fail_credit: bool,
        closed: AtomicBool,
    }

    #[async_trait]
    impl ReceiverTransport for RecordingReceiverTransport {
        async fn recv(&self) -> Option<SenderMsg> {
            self.incoming.lock().await.recv().await
        }

        async fn send(&self, message: ReceiverMsg) -> TransportResult<()> {
            if self.fail_credit && matches!(message, ReceiverMsg::CreditGrant { .. }) {
                return Err(transport::error::TransportError::ChannelClosed);
            }
            self.sent.lock().await.push(message);
            Ok(())
        }

        async fn close(&self) -> TransportResult<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn session_config() -> SessionConfig {
        SessionConfig {
            transfer_mode: TransferMode::Direct,
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

    async fn assert_terminal_path_discards_residual(normal_completion: bool) {
        let dest_dir = tempdir().unwrap();
        let dest = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        if normal_completion {
            incoming_tx.send(SenderMsg::TransferDone).await.unwrap();
        }
        drop(incoming_tx);
        let transport = RecordingReceiverTransport {
            incoming: TokioMutex::new(incoming_rx),
            sent: TokioMutex::new(Vec::new()),
            fail_credit: false,
            closed: AtomicBool::new(false),
        };
        let progress = Arc::new(ReceiverProgress::new());
        let (_progress_tx, progress_rx) = mpsc::channel(1);
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(8).unwrap();
        state.credit.accepted(&transport, 1).await.unwrap();

        let session = RemoteReceiverSession::new(dest, session_config(), FeatureFlags::current(), u64::MAX, progress);
        let result = session.run_with_state(&transport, progress_rx, &mut state).await;
        if normal_completion {
            result.unwrap();
        } else {
            assert!(result.is_err());
        }

        // 1 residual byte must have been discarded. Otherwise 1 + 3 reaches
        // the half-window threshold and emits a CreditGrant here.
        state.credit.accepted(&transport, 3).await.unwrap();
        assert!(
            !transport
                .sent
                .lock()
                .await
                .iter()
                .any(|message| matches!(message, ReceiverMsg::CreditGrant { .. }))
        );
    }

    #[tokio::test]
    async fn failed_credit_delivery_cancels_and_joins_direct_disk_work() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("file"), b"AAAABBBB").unwrap();
        let dest = Arc::new(
            create_storage(root.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let source_root = tempdir().unwrap();
        fs::write(source_root.path().join("file"), b"AAAAXXXX").unwrap();
        let source = create_storage(source_root.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let entry = Arc::new(
            observe_path(source.storage(), std::path::Path::new("file"))
                .await
                .unwrap(),
        );
        let (incoming_tx, incoming_rx) = mpsc::channel(3);
        incoming_tx
            .send(SenderMsg::FilePage(DirPageResult {
                dir_path: String::new(),
                ndx_start: 0,
                files: vec![NdxEntry {
                    ndx: 0,
                    entry: entry.clone().into(),
                }],
                subdirs: Vec::new(),
                gap_ndx: -1,
            }))
            .await
            .unwrap();
        incoming_tx.send(SenderMsg::FileBegin { ndx: 0 }).await.unwrap();
        incoming_tx
            .send(SenderMsg::FileData {
                ndx: 0,
                chunk: DataChunk {
                    offset: 0,
                    data: Bytes::from_static(b"AAAAXXXX"),
                },
            })
            .await
            .unwrap();
        let transport = RecordingReceiverTransport {
            incoming: TokioMutex::new(incoming_rx),
            sent: TokioMutex::new(Vec::new()),
            fail_credit: true,
            closed: AtomicBool::new(false),
        };
        let mut config = session_config();
        config.transfer_mode = TransferMode::Direct;
        let progress = Arc::new(ReceiverProgress::new());
        let (_progress_tx, progress_rx) = mpsc::channel(1);
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(2).unwrap();
        let session = RemoteReceiverSession::new(
            dest.clone(),
            config,
            FeatureFlags::current(),
            u64::MAX,
            progress.clone(),
        );
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            session.run_with_state(&transport, progress_rx, &mut state),
        )
        .await
        .unwrap();
        drop(session);
        assert!(result.is_err());
        assert!(transport.closed.load(Ordering::SeqCst));
        assert_eq!(Arc::strong_count(&dest), 1, "返回前必须等待落盘任务释放目标");
        assert_eq!(progress.files_transferred.load(Ordering::Relaxed), 0);
        assert_eq!(fs::read(root.path().join("file")).unwrap(), b"AAAABBBB");
    }

    #[tokio::test]
    async fn normal_completion_discards_residual_credit() {
        assert_terminal_path_discards_residual(true).await;
    }

    #[tokio::test]
    async fn abrupt_close_discards_residual_credit() {
        assert_terminal_path_discards_residual(false).await;
    }

    fn nas_entry(_name: &str, relative_path: PathBuf, is_dir: bool) -> Arc<ObservedEntry> {
        Arc::new({
            let fixture_path: std::path::PathBuf = relative_path;
            crate::observed_fixtures::observation(
                data_mover::model::BackendKind::Nfs,
                fixture_path,
                if is_dir {
                    data_mover::model::EntryKind::Directory
                } else if false {
                    data_mover::model::EntryKind::Symlink
                } else {
                    data_mover::model::EntryKind::File
                },
                0,
                [0, 0, 0],
                Some(data_mover::model::OwnershipMode {
                    mode: if is_dir { 0o755 } else { 0o644 },
                    uid: 0u32,
                    gid: 0u32,
                }),
                None,
                None,
                None,
            )
        })
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
        let walk = directory_pages(
            &src_storage,
            WalkOptions {
                concurrency: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let page = match walk.recv().await.ok() {
            Some(NdxEvent::Page(page)) => page,
            Some(NdxEvent::Error { path, reason }) => panic!("listing failed for {path}: {reason}"),
            other => panic!("expected one file page, got {other:?}"),
        };
        let ndx = page.files[0].ndx;

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

        let session = RemoteReceiverSession::new(
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
        sender_transport.send(SenderMsg::FileBegin { ndx }).await.unwrap();

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
                ndx,
                chunk: DataChunk {
                    offset: 0,
                    data: content.clone(),
                },
            })
            .await
            .unwrap();
        sender_transport
            .send(SenderMsg::EndOfFile { ndx, source_hash: None })
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
        let session = RemoteReceiverSession::new(
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
    async fn session_creates_directory_and_symlink_and_acknowledges_both() {
        let src = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let directory_path = PathBuf::from("sub");
        let link_path = PathBuf::from("link.txt");
        let target = PathBuf::from("target.txt");
        fs::create_dir(src.path().join(&directory_path)).unwrap();
        fs::set_permissions(src.path().join(&directory_path), fs::Permissions::from_mode(0o755)).unwrap();
        symlink(&target, src.path().join(&link_path)).unwrap();

        let source = create_storage(src.path().to_str().unwrap(), None, false).await.unwrap();
        let directory = Arc::new(observe_path(source.storage(), &directory_path).await.unwrap());
        let link = Arc::new(observe_path(source.storage(), &link_path).await.unwrap());
        let destination = Arc::new(create_storage(dest.path().to_str().unwrap(), None, true).await.unwrap());
        let (sender, receiver) = create_in_process_pair();
        let (_progress_tx, progress_rx) = mpsc::channel(1);
        let session = RemoteReceiverSession::new(
            destination,
            session_config(),
            FeatureFlags::current(),
            u64::MAX,
            Arc::new(ReceiverProgress::new()),
        );

        let ndx = 7;
        for message in [
            SenderMsg::CreateDir { entry: directory },
            SenderMsg::FilePage(DirPageResult {
                dir_path: String::new(),
                ndx_start: ndx,
                files: vec![NdxEntry {
                    ndx,
                    entry: link.clone().into(),
                }],
                subdirs: Vec::new(),
                gap_ndx: ndx + 1,
            }),
            SenderMsg::FileListDone,
            SenderMsg::CreateSymlink {
                entry: link,
                target: target.clone(),
            },
            SenderMsg::TransferDone,
        ] {
            sender.send(message).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), session.run(&receiver, progress_rx))
            .await
            .unwrap()
            .unwrap();
        receiver.close().await.unwrap();

        assert!(matches!(sender.recv().await, Some(ReceiverMsg::EntrySuccess { entry })
            if entry.get_relative_path() == directory_path));
        assert!(
            matches!(sender.recv().await, Some(ReceiverMsg::TransferRequest { ndx: requested, decision: TransferDecision::FullTransfer })
            if requested == ndx)
        );
        assert!(matches!(sender.recv().await, Some(ReceiverMsg::RequestsDone)));
        assert!(matches!(sender.recv().await, Some(ReceiverMsg::EntrySuccess { entry })
            if entry.get_relative_path() == link_path));

        let directory_metadata = fs::metadata(dest.path().join(directory_path)).unwrap();
        assert!(directory_metadata.is_dir());
        assert_eq!(directory_metadata.permissions().mode() & 0o777, 0o755);
        let link = dest.path().join(link_path);
        let metadata = fs::symlink_metadata(&link).unwrap();
        assert!(metadata.file_type().is_symlink());
        let owner = fs::metadata(dest.path()).unwrap();
        assert_eq!((metadata.uid(), metadata.gid()), (owner.uid(), owner.gid()));
        assert_eq!(fs::read_link(link).unwrap(), target);
    }

    #[tokio::test]
    async fn metadata_only_failure_reports_error_without_success_confirmation() {
        let source_root = tempdir().unwrap();
        let target_root = tempdir().unwrap();
        fs::write(source_root.path().join("missing"), b"source").unwrap();
        let source = create_storage(source_root.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let target = create_storage(target_root.path().to_str().unwrap(), None, true)
            .await
            .unwrap();
        let entry = Arc::new(
            observe_path(source.storage(), std::path::Path::new("missing"))
                .await
                .unwrap(),
        )
        .into();
        let (sender, receiver) = create_in_process_pair();
        let progress = ReceiverProgress::new();
        let mut state = RemoteSessionState::default();
        assert!(
            state
                .handle_base_classification(&receiver, &target, &progress, 0, &entry, TransferDecision::MetadataOnly)
                .await
        );
        assert!(matches!(sender.recv().await, Some(ReceiverMsg::EntryError { .. })));
        assert_eq!(progress.metadata_only.load(Ordering::Relaxed), 0);
        assert_eq!(state.counts(), (0, 0));
        assert!(!target_root.path().join("missing").exists());
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
        let walk = directory_pages(
            &src_storage,
            WalkOptions {
                concurrency: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let page = match walk.recv().await.ok() {
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
                Some(ReceiverMsg::ClassifiedObservation {
                    decision: TransferDecision::MetadataOnly,
                    ..
                }) => saw_metadata = true,
                Some(ReceiverMsg::ClassifiedObservation {
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
        let walk = directory_pages(
            &src_storage,
            WalkOptions {
                concurrency: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let page = match walk.recv().await.ok() {
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
                entry: nas_entry(&escape_name, escape_path, true).into(),
            }],
            gap_ndx: -1,
        };
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();
        let mut dest_index = DestIndex::new();

        state
            .handle_directory_lifecycle(&receiver_transport, &dest_storage, &page, &mut dest_index, false)
            .await
            .unwrap();

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
            .await
            .unwrap();

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
                    1,
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"invalid"),
                    },
                )
                .await
        );
        assert!(dc_rx.try_recv().is_err());

        assert!(state.begin_full(&dc_tx, 7, entry.clone().into()).await);
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::FileBegin { ndx: 7, .. })
        ));
        assert!(!state.begin_full(&dc_tx, 8, entry.clone().into()).await);
        assert!(dc_rx.try_recv().is_err());

        assert!(
            state
                .push_full_chunk(
                    &dc_tx,
                    7,
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

        assert!(state.begin_full(&dc_tx, 10, entry.clone().into()).await);
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileBegin { .. })));
        assert!(
            !state
                .push_delta_data(&dc_tx, 10, &entry, Bytes::from_static(b"invalid"))
                .await
        );
        assert!(dc_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn accepted_delta_literal_uses_the_same_credit_transition_as_full_data() {
        let entry = nas_entry("delta.txt", PathBuf::from("delta.txt"), false);
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let (dc_tx, mut dc_rx) = mpsc::channel(2);
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(4).unwrap();

        assert!(
            state
                .accept_delta_data(&receiver_transport, &dc_tx, 7, &entry, Bytes::from_static(b"ab"),)
                .await
                .unwrap()
        );
        assert!(matches!(
            dc_rx.recv().await,
            Some(DiskCommitMsg::DeltaBegin { ndx: 7, .. })
        ));
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::DeltaData { .. })));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sender_transport.recv())
                .await
                .is_err(),
            "flow-control grants must not escape the in-process adapter"
        );
    }

    #[tokio::test]
    async fn failed_full_chunk_enqueue_returns_no_credit() {
        let entry = nas_entry("file.txt", PathBuf::from("file.txt"), false);
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let (dc_tx, mut dc_rx) = mpsc::channel(1);
        let mut state = RemoteSessionState::default();
        state.credit = ReceiverCreditState::new(2).unwrap();
        assert!(state.begin_full(&dc_tx, 1, entry.clone().into()).await);
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileBegin { .. })));
        drop(dc_rx);

        assert!(
            !state
                .accept_full_chunk(
                    &receiver_transport,
                    &dc_tx,
                    1,
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"ab"),
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sender_transport.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn slow_bounded_sink_stops_accepting_additional_payloads_until_capacity_returns() {
        let entry = nas_entry("slow.bin", PathBuf::from("slow.bin"), false);
        let (_sender_transport, receiver_transport) = create_in_process_pair();
        let (dc_tx, mut dc_rx) = mpsc::channel(1);
        let mut state = RemoteSessionState::default();
        assert!(state.begin_full(&dc_tx, 1, entry.clone().into()).await);
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileBegin { .. })));

        assert!(
            state
                .accept_full_chunk(
                    &receiver_transport,
                    &dc_tx,
                    1,
                    DataChunk {
                        offset: 0,
                        data: Bytes::from_static(b"a"),
                    },
                )
                .await
                .unwrap()
        );

        let blocked = tokio::spawn(async move {
            state
                .accept_full_chunk(
                    &receiver_transport,
                    &dc_tx,
                    1,
                    DataChunk {
                        offset: 1,
                        data: Bytes::from_static(b"b"),
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !blocked.is_finished(),
            "a full bounded sink must stop accepting additional payloads"
        );

        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileChunk { .. })));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), blocked)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        assert!(matches!(dc_rx.recv().await, Some(DiskCommitMsg::FileChunk { .. })));
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
    async fn source_failure_before_file_begin_drops_late_data_stream() {
        let (dc_tx, mut dc_rx) = mpsc::channel(4);
        let mut state = RemoteSessionState::default();
        let ndx = 7;
        state.ledger.record_request();

        // QUIC control stream may deliver EntryError before the data stream's FileBegin.
        assert!(!state.handle_source_failure(&dc_tx, Some(ndx)).await);
        assert_eq!(state.counts(), (1, 1));
        assert!(
            dc_rx.try_recv().is_err(),
            "idle source failure must not enqueue a stray abort"
        );

        let entry = nas_entry("late.bin", PathBuf::from("late.bin"), false);
        assert!(!state.begin_full(&dc_tx, ndx, entry.clone().into()).await);
        assert!(!state.commit_delta(&dc_tx, ndx, &entry, &None).await);
        assert!(
            dc_rx.try_recv().is_err(),
            "late data messages for a terminated ndx must be dropped"
        );

        assert!(state.observe_transfer_done());
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

    #[tokio::test]
    async fn destination_index_distinguishes_a_missing_directory_from_a_failed_listing() -> Result<()> {
        let root = tempfile::tempdir()?;
        tokio::fs::write(root.path().join("not-a-directory"), b"preserve").await?;
        let storage =
            create_storage_for_role(root.path().to_str().unwrap(), None, false, StorageRole::Destination).await?;
        assert!(read_destination_index(&storage, "not-a-directory").await.is_err());
        assert!(
            read_destination_index(&storage, "missing")
                .await?
                .orphaned_entries()
                .is_empty()
        );
        assert_eq!(tokio::fs::read(root.path().join("not-a-directory")).await?, b"preserve");
        Ok(())
    }

    #[tokio::test]
    async fn failed_source_or_destination_enumeration_closes_the_session_without_success() -> Result<()> {
        for source_failure in [true, false] {
            let root = tempfile::tempdir()?;
            tokio::fs::write(root.path().join("not-a-directory"), b"preserve").await?;
            let storage = Arc::new(
                create_storage_for_role(root.path().to_str().unwrap(), None, false, StorageRole::Destination).await?,
            );
            let (sender, receiver) = create_in_process_pair();
            let message = if source_failure {
                SenderMsg::FileListError {
                    path: "source".into(),
                    reason: "forced source listing error".into(),
                }
            } else {
                SenderMsg::FilePage(DirPageResult {
                    dir_path: "not-a-directory".into(),
                    ndx_start: 0,
                    files: Vec::new(),
                    subdirs: Vec::new(),
                    gap_ndx: -1,
                })
            };
            sender.send(message).await?;
            let config = SessionConfig {
                transfer_mode: TransferMode::Direct,
                src_path: "source".into(),
                qos: None,
                peak_qos_rate: 1.0,
                iops: None,
                enable_integrity_check: false,
                enable_acl: false,
                is_source_reserved: true,
                block_size: None,
                delete_target: true,
                delta_size_threshold: None,
            };
            let progress = Arc::new(ReceiverProgress::new());
            let (_progress_tx, progress_rx) = mpsc::channel(1);
            let session = RemoteReceiverSession::new(
                storage,
                config,
                FeatureFlags::current(),
                DEFAULT_DELTA_SIZE_THRESHOLD_BYTES,
                progress.clone(),
            );
            let result = tokio::time::timeout(Duration::from_secs(1), session.run(&receiver, progress_rx))
                .await
                .unwrap();
            assert!(result.is_err());
            assert_eq!(progress.error_count.load(Ordering::Relaxed), 1);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), sender.recv())
                    .await
                    .unwrap()
                    .is_none(),
                "failed enumeration must close the peer without success messages"
            );
            assert_eq!(tokio::fs::read(root.path().join("not-a-directory")).await?, b"preserve");
        }
        Ok(())
    }

    #[test]
    fn validate_relative_path_accepts_normal_paths() {
        assert!(validate_relative_path(Path::new("a.txt")).is_ok());
        assert!(validate_relative_path(Path::new("sub/deeper/b.bin")).is_ok());
        assert!(validate_relative_path(Path::new("")).is_ok());
    }

    #[test]
    fn validate_relative_path_rejects_parent_dir_escape() {
        assert!(validate_relative_path(Path::new("../etc/passwd")).is_err());
        assert!(validate_relative_path(Path::new("sub/../../escape")).is_err());
        assert!(validate_relative_path(Path::new("..")).is_err());
    }

    #[test]
    fn validate_relative_path_rejects_absolute_path() {
        assert!(validate_relative_path(Path::new("/etc/passwd")).is_err());
    }

    // ── resolve_delta_size_threshold：delta size 门槛解析（issue #54 阶段 0） ──

    #[test]
    fn resolve_delta_size_threshold_none_uses_default_512mib() {
        let threshold = resolve_delta_size_threshold(&None).unwrap();
        assert_eq!(threshold, 512 * 1024 * 1024);
    }

    #[test]
    fn resolve_delta_size_threshold_parses_valid_override() {
        let threshold = resolve_delta_size_threshold(&Some("1MiB".to_string())).unwrap();
        assert_eq!(threshold, 1024 * 1024);
    }

    #[test]
    fn resolve_delta_size_threshold_rejects_invalid_format() {
        assert!(resolve_delta_size_threshold(&Some("not-a-size".to_string())).is_err());
    }
}
