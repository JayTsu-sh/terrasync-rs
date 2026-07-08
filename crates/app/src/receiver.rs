//! Receiver 侧逻辑
//!
//! 从 transport 接收 Sender 发来的消息，在目标存储上执行写入操作。
//! 单进程模式下 Receiver 拥有 src+dest 两个 storage（命令模式）。
//! 双进程模式下 Receiver 仅拥有 dest storage（数据流模式，Phase 3）。

// 标准库
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// 外部 crate
use data_mover::qos::QosManager;
use data_mover::{EntryEnum, StorageEnum};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::message::{DestIndex, DiskCommitMsg, NdxTable, ProgressSnapshot, ReceiverMsg, SenderMsg, SessionConfig};
use transport::traits::ReceiverTransport;

// 内部模块
use crate::byte_resume::is_part_file;
use crate::disk_commit::disk_commit_task;
use crate::error::{AppError, Result};
use crate::sync::{ResumeOpts, copy_file_with_resume, should_resume};

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
#[allow(clippy::struct_excessive_bools)] // 按用途分组的开关集合，重构为枚举会失去可读性
pub struct ReceiverConfig {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    /// 任务目录（字节级断点续传状态存于 `<job_dir>/byte_resume/`）；为空则禁用续传
    pub job_dir: String,
    /// 显式关闭字节级断点续传（强制整体复制）
    pub no_resume: bool,
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
            // 全量复制：多块大文件写 .part（建立续传基础，正常结束即 rename；
            // 中断则留 .part + 进度状态供后续增量续传）；小文件/S3 走整文件拷贝。
            let resume = ResumeOpts {
                job_dir: config.job_dir.clone(),
                no_resume: config.no_resume,
            };
            if should_resume(&resume, entry, src_storage, dest_storage) {
                copy_file_with_resume(
                    entry,
                    src_storage,
                    dest_storage,
                    config.enable_integrity_check,
                    config.is_source_reserved,
                    qos_manager,
                    bytes_counter,
                    &config.job_dir,
                )
                .await?;
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
            }

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
                        if let data_mover::StorageEntryMessage::Scanned(entry) = msg {
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
                        // 跳过续传临时文件：源端不存在同名 .terrasync-part 条目，
                        // 会被误判为孤儿；删掉会破坏进行中的续传进度
                        if is_part_file(path) {
                            debug!("[Receiver Remote] Skipping in-progress part file: {:?}", path);
                            continue;
                        }
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

/// 阶段 2：接收文件数据流，路由到 disk-commit task 落盘
///
/// 全量文件（`FileBegin`/`FileData`/`EndOfFile`-无 token）转成 `DiskCommitMsg` 交给
/// disk-commit task 做 3 段流式写入（不再整文件缓冲进 `BytesMut`）；delta 路径
/// （`DeltaMatch`/`DeltaData`/`EndOfFile`-带 token）保持 inline 重建不变。disk-commit
/// task 的 ack 经本 loop 转发回 transport。`TransferDone` 后通知 task 退出并 drain 剩余 ack。
pub async fn recv_file_data_phase(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, session_config: &SessionConfig,
    progress: &Arc<ReceiverProgress>, mut progress_rx: MpscReceiver<ProgressSnapshot>,
) -> Result<()> {
    info!("[Receiver Remote] Phase 2: Receiving file data (streaming)");

    // disk-commit task：全量文件落盘（3 段流式），ack 经 ack_rx 回流。
    // ack 通道用 unbounded：dc task 发 ack 永不 back-pressure，避免路由阻在 dc_tx.send
    // 时不 drain ack → dc 阻在 ack_tx.send → 停止消费 dc_rx 的双向死锁。
    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel::<DiskCommitMsg>(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<ReceiverMsg>();
    let dc_join = tokio::spawn(disk_commit_task(
        dest_storage.clone(),
        session_config.clone(),
        dc_rx,
        ack_tx,
        progress.clone(),
    ));

    // delta 重建 token 缓冲（仅 delta 路径使用，保持原样）
    let mut delta_tokens: Vec<sync_delta::DeltaToken> = Vec::new();
    // 当前文件是否走全量路径：以是否见过 FileBegin 判定，而非 token 是否为空。
    // 源端缩到 0 字节的 delta 传输 token 也为空且无 FileBegin，若按空 token 判定会误路由到
    // FileCommit（dc task 无 active → no-op），导致目标端保留旧内容且不报错。
    let mut full_active = false;

    loop {
        // 同时处理 transport 消息、progress 上报、disk-commit task 的 ack 回流
        tokio::select! {
            Some(snapshot) = progress_rx.recv() => {
                let _ = transport.send(ReceiverMsg::Progress(snapshot)).await;
            }
            Some(ack) = ack_rx.recv() => {
                let _ = transport.send(ack).await;
            }
            msg = transport.recv() => { match msg {
            // ── 目录 / 符号链接：路由给 disk-commit task ──
            Some(SenderMsg::CreateDir { entry }) => {
                let _ = dc_tx.send(DiskCommitMsg::CreateDir { entry }).await;
            }
            Some(SenderMsg::CreateSymlink { entry, target }) => {
                let _ = dc_tx.send(DiskCommitMsg::CreateSymlink { entry, target }).await;
            }

            // ── 全量文件：FileBegin / FileData → disk-commit task ──
            Some(SenderMsg::FileBegin { entry }) => {
                delta_tokens.clear();
                full_active = true;
                let _ = dc_tx.send(DiskCommitMsg::FileBegin { entry }).await;
            }
            Some(SenderMsg::FileData { entry, chunk }) => {
                let _ = dc_tx.send(DiskCommitMsg::FileChunk { entry, chunk }).await;
            }

            // ── delta 路径：token 接收保持原有 inline 逻辑不变 ──
            Some(SenderMsg::DeltaMatch { block_index, .. }) => {
                delta_tokens.push(sync_delta::DeltaToken::Match { block_index });
            }
            Some(SenderMsg::DeltaData { data, .. }) => {
                delta_tokens.push(sync_delta::DeltaToken::Data(data));
            }

            // ── 文件结束：见过 FileBegin → 全量 FileCommit；否则走 delta inline 重建 ──
            Some(SenderMsg::EndOfFile { entry, source_hash }) => {
                if full_active {
                    let _ = dc_tx.send(DiskCommitMsg::FileCommit { entry, source_hash }).await;
                } else {
                    let tokens = std::mem::take(&mut delta_tokens);
                    handle_end_of_file(transport, dest_storage, entry, source_hash, tokens, bytes::Bytes::new(), progress)
                        .await;
                }
                full_active = false;
            }

            // ── Sender 读源失败：中止 disk-commit task 当前正在写入的文件（丢弃 .part） ──
            Some(SenderMsg::EntryError { path, reason }) => {
                warn!("[Receiver Remote] Sender aborted {:?}: {}", path, reason);
                let _ = dc_tx.send(DiskCommitMsg::AbortFile).await;
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

    // 收尾：通知 disk-commit task 退出 → 等其处理完积压 → drain 剩余 ack
    let _ = dc_tx.send(DiskCommitMsg::Shutdown).await;
    drop(dc_tx);
    dc_join
        .await
        .map_err(|e| AppError::CopyError(format!("disk-commit task join: {e}")))??;
    while let Ok(ack) = ack_rx.try_recv() {
        let _ = transport.send(ack).await;
    }
    Ok(())
}

/// `EndOfFile` 处理：重建文件字节（delta 或全量） → hash 校验 → 写入目标端 → 发送 Ack
///
/// `tokens` 非空时执行 delta 重建，为空时直接使用 `file_data`。
/// 所有非致命错误（basis 读失败、hash 不符、写入失败）均自行发送 `EntryError` 后返回，不向上传播。
async fn handle_end_of_file(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, entry: Arc<data_mover::EntryEnum>,
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
