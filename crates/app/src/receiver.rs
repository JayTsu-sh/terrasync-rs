//! Receiver 侧逻辑
//!
//! 从 transport 接收 Sender 发来的消息，在目标存储上执行写入操作。
//! 单进程模式下 Receiver 拥有 src+dest 两个 storage（命令模式）。
//! 双进程模式下 Receiver 仅拥有 dest storage（数据流模式，Phase 3）。

// 标准库
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// 外部 crate
use bytes::{BufMut, BytesMut};
use storage_v2::qos::QosManager;
use storage_v2::{EntryEnum, StorageEnum};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::message::{DestIndex, NdxTable, ProgressSnapshot, ReceiverMsg, SenderMsg, SessionConfig};
use transport::traits::ReceiverTransport;

// 内部模块
use crate::error::{AppError, Result};

// ============================================================
// Receiver 进度跟踪（原子计数器，支持多 Receiver 聚合）
// ============================================================

/// Receiver 侧进度计数器
pub struct ReceiverProgress {
    pub files_transferred: AtomicU64,
    pub dirs_created: AtomicU64,
    pub bytes_transferred: AtomicU64,
    pub files_skipped: AtomicU64,
    pub metadata_only: AtomicU64,
    pub error_count: AtomicU64,
}

impl Default for ReceiverProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl ReceiverProgress {
    pub fn new() -> Self {
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
    pub fn snapshot(&self, start_time: std::time::Instant) -> ProgressSnapshot {
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

/// Receiver 运行时配置
pub struct ReceiverConfig {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
}

/// 单进程模式的 Receiver 主 task（R1 + R2 合一）
///
/// 从 transport 接收 `SenderMsg`，调用 `StorageEnum` 方法执行写入。
/// 每完成一个 entry 后通过 transport 发送 `ReceiverMsg::EntrySuccess` 或 `EntryError`。
///
/// 单进程模式下收到 `CopyEntry` 消息时，Receiver 有 src+dest storage，
/// 直接调用 `process_entry_on_receiver()` 完成完整复制。
pub async fn receiver_task(
    transport: &(dyn ReceiverTransport + 'static), src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>,
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
                    Ok(()) => {
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

            SenderMsg::EntryError { path, reason } => {
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
    entry: &EntryEnum, src_storage: &Arc<StorageEnum>, dest_storage: &Arc<StorageEnum>, config: &ReceiverConfig,
    qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
) -> Result<()> {
    let relative_path = entry.get_relative_path();
    let span = info_span!("recv_entry", path = %relative_path.display());

    async {
        if entry.get_is_dir() {
            if let Err(e) = dest_storage.create_dir_all(entry).await {
                error!("[Receiver] Error creating directory {:?}: {}", relative_path, e);
                // 目录创建失败不返回错误，继续尝试设置元数据和 ACL（与原逻辑一致）
            }
        } else if entry.get_is_symlink() {
            let target_path = src_storage
                .read_symlink(entry)
                .await
                .map_err(|e| AppError::CopyError(format!("Failed to read symlink {}: {e}", relative_path.display())))?;
            dest_storage.create_symlink(entry, &target_path).await.map_err(|e| {
                AppError::CopyError(format!("Failed to create symlink {}: {e}", relative_path.display()))
            })?;

            // 源端符号链接删除（is_source_reserved=false 时）
            if !config.is_source_reserved
                && let Err(e) = src_storage.delete_file(entry).await
            {
                error!("[Receiver] Failed to remove source symlink {:?}: {}", relative_path, e);
            }
        } else {
            StorageEnum::copy_file(
                src_storage,
                dest_storage,
                entry,
                qos_manager,
                config.enable_integrity_check,
                config.is_source_reserved,
                bytes_counter,
            )
            .await
            .map_err(|e| AppError::CopyError(format!("Failed to copy {}: {e}", relative_path.display())))?;

            // 设置目标文件元数据（mtime/uid/gid/mode 或 S3 mtime/tags）
            dest_storage.set_entry_metadata(entry).await.map_err(|e| {
                AppError::CopyError(format!("Failed to set metadata for {}: {e}", relative_path.display()))
            })?;
        }

        // ACL 复制（目录和文件均需要，symlink 除外）
        if config.enable_acl
            && !entry.get_is_symlink()
            && let Err(e) = StorageEnum::copy_acl(src_storage, dest_storage, relative_path).await
        {
            error!("[Receiver] Failed to copy ACL for {:?}: {}", relative_path, e);
        }

        Ok(())
    }
    .instrument(span)
    .await
}

// ============================================================
// 双进程模式 Receiver（数据流模式）
// ============================================================

/// 双进程模式的 Receiver task（入口）
///
/// 分阶段顺序协议：
/// 1. 接收 `SessionConfig`
/// 2. 接收 `FilePage` → `DestIndex` 逐页比较 → 发 `TransferRequest`
/// 3. 接收数据流 → 写入目标端 → 发 EntrySuccess/Error
pub async fn receiver_task_remote(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: Arc<StorageEnum>,
) -> Result<()> {
    info!("[Receiver Remote] Started, waiting for SessionConfig");

    // ── 阶段 0: 接收 SessionConfig ──
    let session_config = recv_session_config(transport).await?;
    info!(
        "[Receiver Remote] SessionConfig: src_path={}, integrity={}, acl={}",
        session_config.src_path, session_config.enable_integrity_check, session_config.enable_acl
    );

    // ── 进度跟踪 ──
    let progress = Arc::new(ReceiverProgress::new());
    let start_time = std::time::Instant::now();
    let (progress_tx, progress_rx) = tokio::sync::mpsc::channel::<ProgressSnapshot>(4);
    let progress_reporter = {
        let progress = progress.clone();
        let tx = progress_tx;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let snapshot = progress.snapshot(start_time);
                if tx.send(snapshot).await.is_err() {
                    break;
                }
            }
        })
    };

    // ── 阶段 1: 接收文件列表 → 发 TransferRequest ──
    recv_file_list_phase(transport, &dest_storage, &session_config, &progress).await?;

    // ── 阶段 2: 接收数据流 → 写入目标端 ──
    recv_file_data_phase(transport, &dest_storage, &session_config, &progress, progress_rx).await?;

    // 停止进度 reporter，发最终快照 + AllDone
    progress_reporter.abort();
    let final_snapshot = progress.snapshot(start_time);
    let _ = transport.send(ReceiverMsg::Progress(final_snapshot)).await;
    let _ = transport.send(ReceiverMsg::AllDone).await;
    info!("[Receiver Remote] Completed");
    Ok(())
}

/// 阶段 0：等待并返回 SessionConfig，忽略非预期消息
async fn recv_session_config(transport: &(dyn ReceiverTransport + 'static)) -> Result<SessionConfig> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::SessionConfig(config)) => return Ok(config),
            Some(_) => warn!("[Receiver Remote] Expected SessionConfig, skipping"),
            None => return Err(AppError::CopyError("Transport closed before SessionConfig".into())),
        }
    }
}

/// 阶段 1：接收 `FilePage` → 构建 `DestIndex` → 发 `TransferRequest` / `DeltaTransferRequest`
async fn recv_file_list_phase(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, session_config: &SessionConfig,
    progress: &Arc<ReceiverProgress>,
) -> Result<()> {
    info!("[Receiver Remote] Phase 1: Receiving file list");
    let mut ndx_table = NdxTable::new();

    loop {
        match transport.recv().await {
            Some(SenderMsg::FilePage(page)) => {
                ndx_table.ingest_page(&page);

                // 构建该目录的 DestIndex（walkdir 目标端 depth=1）
                let mut dest_index = DestIndex::new();
                if let Ok(iter) = dest_storage
                    .walkdir(
                        Some(std::path::Path::new(&page.dir_path)),
                        Some(1),
                        None,
                        None,
                        1,
                        false,
                        false,
                        0,
                    )
                    .await
                {
                    while let Some(msg) = iter.next().await {
                        if let storage_v2::StorageEntryMessage::Scanned(entry) = msg {
                            dest_index.insert(entry);
                        }
                    }
                }

                // 逐文件比较 → 发 TransferRequest 或 DeltaTransferRequest
                for nf in &page.files {
                    match dest_index.check(&nf.entry) {
                        transport::message::TransferDecision::FullTransfer => {
                            let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx }).await;
                        }
                        transport::message::TransferDecision::DeltaTransfer => {
                            let size = nf.entry.get_size();
                            match StorageEnum::read_file_from(dest_storage, &nf.entry, size).await {
                                Ok(basis_data) => {
                                    let block_size = sync_delta::calculate_block_size(size);
                                    let signatures =
                                        sync_delta::signature::compute_block_signatures(&basis_data, block_size);
                                    let transport_sigs: Vec<transport::message::BlockSignature> = signatures
                                        .into_iter()
                                        .map(|s| transport::message::BlockSignature {
                                            rolling: s.rolling,
                                            strong: s.strong,
                                        })
                                        .collect();
                                    let _ = transport
                                        .send(ReceiverMsg::DeltaTransferRequest {
                                            ndx: nf.ndx,
                                            block_size,
                                            signatures: transport_sigs,
                                        })
                                        .await;
                                }
                                Err(e) => {
                                    warn!(
                                        "[Receiver Remote] 读取 basis file {:?} 失败: {}, 降级全量传输",
                                        nf.entry.get_relative_path(),
                                        e
                                    );
                                    let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx }).await;
                                }
                            }
                        }
                        transport::message::TransferDecision::MetadataOnly => {
                            let _ = dest_storage.set_entry_metadata(&nf.entry).await;
                            progress.metadata_only.fetch_add(1, Ordering::Relaxed);
                        }
                        transport::message::TransferDecision::Skip => {
                            progress.files_skipped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // 子目录在目标端创建
                for ns in &page.subdirs {
                    if let Err(e) = dest_storage.create_dir_all(&ns.entry).await {
                        warn!("[Receiver Remote] create_dir {:?}: {}", ns.entry.get_relative_path(), e);
                    }
                }

                // --delete-target: 删除目标端多余文件
                if session_config.delete_target {
                    for orphan in dest_index.orphaned_entries() {
                        let path = orphan.get_relative_path();
                        if orphan.get_is_dir() {
                            info!("[Receiver Remote] Deleting orphaned dir: {:?}", path);
                            if let Err(e) = dest_storage.delete_dir_all(orphan).await {
                                warn!("[Receiver Remote] delete dir {:?}: {}", path, e);
                            }
                        } else {
                            info!("[Receiver Remote] Deleting orphaned file: {:?}", path);
                            if let Err(e) = dest_storage.delete_file(orphan).await {
                                warn!("[Receiver Remote] delete file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
            Some(SenderMsg::FileListError { path, reason }) => {
                error!("[Receiver Remote] walkdir error {}: {}", path, reason);
            }
            Some(SenderMsg::FileListDone) => {
                info!(
                    "[Receiver Remote] File list complete, {} entries indexed",
                    ndx_table.len()
                );
                break;
            }
            Some(_) => {}
            None => return Err(AppError::CopyError("Transport closed during file list".into())),
        }
    }

    let _ = transport.send(ReceiverMsg::RequestsDone).await;
    Ok(())
}

/// 阶段 2：接收文件数据流，写入目标端
///
/// 使用 `BytesMut` 缓冲多 chunk 数据，`FileBegin` 消息重置缓冲区避免跨文件状态污染。
async fn recv_file_data_phase(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, session_config: &SessionConfig,
    progress: &Arc<ReceiverProgress>, mut progress_rx: MpscReceiver<ProgressSnapshot>,
) -> Result<()> {
    info!("[Receiver Remote] Phase 2: Receiving file data (streaming mode)");

    // delta 重建 token 缓冲
    let mut delta_tokens: Vec<sync_delta::DeltaToken> = Vec::new();
    // 文件数据流缓冲（BytesMut，支持多 chunk 追加，FileBegin 时清空）
    let mut file_data_buf = BytesMut::new();

    loop {
        // 同时处理 transport 消息和 progress 上报
        tokio::select! {
            Some(snapshot) = progress_rx.recv() => {
                let _ = transport.send(ReceiverMsg::Progress(snapshot)).await;
            }
            msg = transport.recv() => { match msg {
            // ── 文件开始：重置缓冲区，防止跨文件状态污染 ──
            Some(SenderMsg::FileBegin { .. }) => {
                file_data_buf.clear();
                delta_tokens.clear();
            }

            // ── 数据流模式：目录 ──
            Some(SenderMsg::CreateDir { entry }) => {
                if let Err(e) = dest_storage.create_dir_all(&entry).await {
                    warn!("[Receiver Remote] create_dir {:?}: {}", entry.get_relative_path(), e);
                }
                let _ = dest_storage.set_entry_metadata(&entry).await;
                progress.dirs_created.fetch_add(1, Ordering::Relaxed);
                let _ = transport.send(ReceiverMsg::EntrySuccess { entry }).await;
            }

            // ── 数据流模式：符号链接 ──
            Some(SenderMsg::CreateSymlink { entry, target }) => {
                match dest_storage.create_symlink(&entry, &target).await {
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
            }

            // ── 数据流模式：文件数据块（追加到 BytesMut） ──
            Some(SenderMsg::FileData { chunk, .. }) => {
                file_data_buf.put_slice(&chunk.data);
            }

            // ── Delta token 接收 ──
            Some(SenderMsg::DeltaMatch { ndx: _, block_index }) => {
                delta_tokens.push(sync_delta::DeltaToken::Match { block_index });
            }
            Some(SenderMsg::DeltaData { ndx: _, data }) => {
                delta_tokens.push(sync_delta::DeltaToken::Data(data));
            }

            // ── 文件结束：校验 + 写入 ──
            Some(SenderMsg::EndOfFile { entry, source_hash }) => {
                let tokens = std::mem::take(&mut delta_tokens);
                let file_data = file_data_buf.split().freeze();
                handle_end_of_file(transport, dest_storage, entry, source_hash, tokens, file_data, progress).await;
            }

            // ── ACL 跨进程 ──
            Some(SenderMsg::SetAcl { entry, acl_data }) => {
                if session_config.enable_acl
                    && let Err(e) = dest_storage.set_acl_bytes(entry.get_relative_path(), &acl_data).await
                {
                    warn!("[Receiver Remote] set ACL {:?}: {}", entry.get_relative_path(), e);
                }
            }

            Some(SenderMsg::TransferDone) => {
                info!("[Receiver Remote] All transfers complete");
                break;
            }
            Some(_) => {}
            None => return Err(AppError::CopyError("Transport closed during data phase".into())),
        }} // close match + select! msg arm
        } // close select!
    } // close loop

    Ok(())
}

/// `EndOfFile` 处理：重建文件字节（delta 或全量） → hash 校验 → 写入目标端 → 发送 Ack
///
/// `tokens` 非空时执行 delta 重建，为空时直接使用 `file_data`。
/// 所有非致命错误（basis 读失败、hash 不符、写入失败）均自行发送 `EntryError` 后返回，不向上传播。
async fn handle_end_of_file(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, entry: Arc<storage_v2::EntryEnum>,
    source_hash: Option<String>, tokens: Vec<sync_delta::DeltaToken>, file_data: bytes::Bytes,
    progress: &Arc<ReceiverProgress>,
) {
    let relative_path = entry.get_relative_path();

    // 重建文件字节：delta 或全量
    let file_bytes: bytes::Bytes = if tokens.is_empty() {
        file_data
    } else {
        info!(
            "[Receiver Remote] Delta reconstruct {:?}: {} tokens",
            relative_path,
            tokens.len()
        );
        let size = entry.get_size();
        match StorageEnum::read_file_from(dest_storage, &entry, size).await {
            Ok(basis_data) => {
                let block_size = sync_delta::calculate_block_size(size);
                bytes::Bytes::from(sync_delta::reconstruct::reconstruct(&basis_data, &tokens, block_size))
            }
            Err(e) => {
                error!("[Receiver Remote] read basis failed {:?}: {}", relative_path, e);
                let _ = transport
                    .send(ReceiverMsg::EntryError {
                        entry,
                        reason: format!("{e}"),
                    })
                    .await;
                return;
            }
        }
    };

    // 验证 hash
    if let Some(ref expected_hash) = source_hash {
        let actual_hash = blake3::hash(&file_bytes).to_hex().to_string();
        if &actual_hash != expected_hash {
            error!(
                "[Receiver Remote] Hash mismatch {:?}: expected {}, got {}",
                relative_path, expected_hash, actual_hash
            );
            let _ = transport
                .send(ReceiverMsg::EntryError {
                    entry,
                    reason: "hash mismatch".into(),
                })
                .await;
            return;
        }
    }

    // 写入文件
    let data_len = file_bytes.len() as u64;
    match StorageEnum::write_file_from_bytes(dest_storage, &entry, file_bytes).await {
        Ok(()) => {
            let _ = dest_storage.set_entry_metadata(&entry).await;
            progress.files_transferred.fetch_add(1, Ordering::Relaxed);
            progress.bytes_transferred.fetch_add(data_len, Ordering::Relaxed);
            let _ = transport.send(ReceiverMsg::EntrySuccess { entry }).await;
        }
        Err(e) => {
            error!("[Receiver Remote] write failed {:?}: {}", relative_path, e);
            progress.error_count.fetch_add(1, Ordering::Relaxed);
            let _ = transport
                .send(ReceiverMsg::EntryError {
                    entry,
                    reason: format!("{e}"),
                })
                .await;
        }
    }
}
