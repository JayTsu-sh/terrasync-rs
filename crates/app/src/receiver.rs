//! Receiver 侧逻辑
//!
//! 从 transport 接收 Sender 发来的消息，在目标存储上执行写入操作。
//! 单进程模式下 Receiver 拥有 src+dest 两个 storage（命令模式）。
//! 双进程模式下 Receiver 仅拥有 dest storage（数据流模式，Phase 3）。

// 标准库
use std::collections::HashMap;
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// 外部 crate
use bytes::Bytes;
use data_mover::qos::QosManager;
use data_mover::{EntryEnum, StorageEnum};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::error::TransportError;
use transport::message::{
    DcAck, DestIndex, DiskCommitMsg, FeatureFlags, FileOutcome, HandshakeResult, NdxTable, ProgressSnapshot,
    ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig,
};
use transport::traits::ReceiverTransport;

// 内部模块
use crate::byte_resume::is_part_file;
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
/// 协议阶段：
/// 0. 接收 `Handshake` → 校验协议版本/能力 → 回 ack/reject（不兼容则中止）
/// 0.5 接收 `Auth` → 校验 token → 回 `AuthResult`（鉴权失败则中止，不进入 `SessionConfig`）
/// 1. 接收 `SessionConfig`
/// 2. 接收文件列表（`FilePage` → `DestIndex` 逐页比较 → 发 `TransferRequest`）与接收数据流
///    （写入目标端 → 发 `EntrySuccess`/`EntryError`）**合并为一个消费者循环并发处理**
///    （`recv_file_list_and_data_phase`），不再是"文件列表收完才能开始收数据"的顺序
///    barrier——多路复用后 Sender 可能在文件列表还没发完时就已经开始回传已确定文件的
///    数据（两者走不同物理 stream），这里按 variant dispatch，不区分先后。
///
/// `expected_token`：`serve --token` 配置的期望值；为 `None` 时跳过鉴权（向后兼容，
/// 未配置 token 时不校验，仍需完成 `Auth` 消息交换以保持协议阶段一致）。
pub async fn receiver_task_remote(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: Arc<StorageEnum>, expected_token: Option<&str>,
) -> Result<()> {
    info!("[Receiver Remote] Started, waiting for Handshake");

    // ── 阶段 -1: 协议握手（版本 + 能力协商），不兼容则回 reject 后立即中止 ──
    let negotiated_features = recv_and_negotiate_handshake(transport).await?;

    // ── 阶段 -0.5: Token 鉴权，失败则回 AuthResult{ok:false} 并断连、中止 ──
    recv_and_check_auth(transport, expected_token).await?;

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

    // ── 阶段 1+2: 文件列表与数据流合并处理，无阶段 barrier（见函数入口文档） ──
    recv_file_list_and_data_phase(
        transport,
        &dest_storage,
        &session_config,
        &negotiated_features,
        &progress,
        progress_rx,
    )
    .await?;

    // 停止进度 reporter，发最终快照 + AllDone
    progress_reporter.abort();
    let final_snapshot = progress.snapshot(start_time);
    let _ = transport.send(ReceiverMsg::Progress(final_snapshot)).await;
    let _ = transport.send(ReceiverMsg::AllDone).await;
    info!("[Receiver Remote] Completed");
    Ok(())
}

/// 阶段 -1：等待 Sender 的 `Handshake`，校验协议版本与能力，回发 ack/reject
///
/// 版本不兼容时发送 `Rejected` 后立即返回错误，不进入 `SessionConfig` / 文件列表阶段，
/// 确保拒绝发生在任何 `FilePage` / `TransferRequest` / `CopyEntry` 之前。
async fn recv_and_negotiate_handshake(transport: &(dyn ReceiverTransport + 'static)) -> Result<FeatureFlags> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::Handshake(remote)) => {
                let result = ProtocolHandshake::current().negotiate(&remote);
                let ack = result.clone();
                match result {
                    HandshakeResult::Accepted { features } => {
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
async fn recv_session_config(transport: &(dyn ReceiverTransport + 'static)) -> Result<SessionConfig> {
    loop {
        match transport.recv().await {
            Some(SenderMsg::SessionConfig(config)) => return Ok(config),
            Some(_) => warn!("[Receiver Remote] Expected SessionConfig, skipping"),
            None => return Err(AppError::CopyError("Transport closed before SessionConfig".into())),
        }
    }
}

/// 校验 Sender 提供的相对路径是否安全：拒绝绝对路径与含 `..` 组件的路径
///
/// 双进程远端模式下 Sender 不可信，若 `entry.get_relative_path()` 被恶意构造为绝对路径
/// 或含 `..` 的路径，会导致目标端在 `dest_path` 之外写入/覆盖文件（路径穿越）。
/// 此校验必须在所有以该路径驱动 `dest_storage` 写操作之前调用。
fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(AppError::UnsafeRelativePath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// 合并处理文件列表（`FilePage` → 构建 `DestIndex` → 发 `TransferRequest`/
/// `DeltaTransferRequest`）与文件数据流（写入目标端 → 发 `EntrySuccess`/`EntryError`），
/// 单一消费者循环内按 variant dispatch，去掉两者之间的顺序 barrier。
///
/// 拆分多路复用 stream 后，Sender 可能在文件列表还没发完（`FileListDone` 之前）就已经
/// 开始为已确定的文件发送数据（file list 与 data 走不同物理 stream，可以流水线并发），
/// 若仍分成"先收完文件列表、再收数据"两个各自独立调用 `recv()` 的阶段，数据消息会被
/// "文件列表"阶段的 catch-all 分支直接丢弃。合并为一个循环后不再有这个问题
/// （transport 层只暴露一个 `recv()`，见 `crates/transport/src/quic/mux.rs`）。
///
/// `FileListDone` 到达时立即发送 `RequestsDone`（与改造前时序一致），但循环不 break，
/// 继续处理后续到达的数据消息。
///
/// `negotiated_features` 为握手阶段协商后的能力交集；`delta` 能力未协商成功时
/// 即使数据不匹配也降级为全量传输（`TransferRequest`），不发送 `DeltaTransferRequest`。
/// 使用 `BytesMut` 缓冲多 chunk 数据，`FileBegin` 消息重置缓冲区避免跨文件状态污染。
///
/// **`TransferDone` 到达≠数据已全部收完**：`TransferDone` 走 `Control` stream，实际文件
/// 数据走独立的 `Data` stream——多路复用后二者是完全独立的物理 stream，QUIC/我们自己的
/// 后台 reader task 都不保证"写入更早的 stream 一定先于写入更晚的 stream 被对端处理完"，
/// 小体积的 `TransferDone` 完全可能抢在大文件的 `FileData`/`EndOfFile` 之前就被收到并处理
/// （两个真实子进程联调时用大文件实际触发过：收到 `TransferDone` 就直接跳出循环，丢了还在
/// `Data` stream 上飞的文件）。因此改为显式计数：`requested_count`（发出过多少个
/// `TransferRequest`/`DeltaTransferRequest`）与 `completed_count`（收到过多少个对应的
/// `EndOfFile`/`CreateSymlink` 完成处理），只有二者相等**且**已经见过 `TransferDone`，
/// 才真正结束循环。
async fn recv_file_list_and_data_phase(
    transport: &(dyn ReceiverTransport + 'static), dest_storage: &Arc<StorageEnum>, session_config: &SessionConfig,
    negotiated_features: &FeatureFlags, progress: &Arc<ReceiverProgress>,
    mut progress_rx: MpscReceiver<ProgressSnapshot>,
) -> Result<()> {
    info!("[Receiver Remote] Receiving file list and file data (pipelined, streaming mode)");
    let mut ndx_table = NdxTable::new();
    // ndx 级失败尝试计数（redo 决策用）：ndx → 已失败次数，见 decide_file_ack
    let mut attempts: HashMap<i32, u8> = HashMap::new();
    // delta 重建 token 缓冲
    let mut delta_tokens: Vec<sync_delta::DeltaToken> = Vec::new();
    // TransferDone 与实际数据完成的解耦计数（见函数文档）
    let mut requested_count: u64 = 0;
    let mut completed_count: u64 = 0;
    let mut transfer_done_seen = false;
    // 全量文件是否走 disk-commit 流式路径：以是否见过 FileBegin 判定（而非 token 是否为空），
    // 避免源端缩到 0 字节的 delta 传输（token 空且无 FileBegin）被误判为全量。
    let mut full_active = false;
    // 已创建的目录：所有文件传输完成后统一回写 mtime/mode(写子文件会把目录 mtime 顶到当前时间;
    // 且 0500 等受限权限须在写完子项后才能设),对齐单进程 orchestrator 的目录 mtime 收尾。
    let mut created_dirs: Vec<Arc<EntryEnum>> = Vec::new();
    // disk-commit task：全量文件三段流式落盘（去整文件 BytesMut）；ack 经 unbounded channel 回流，
    // 避免路由 select 阻在 dc_tx.send 时不 drain ack → dc 阻在 ack_tx.send 的双向死锁。
    let (dc_tx, dc_rx) = tokio::sync::mpsc::channel::<DiskCommitMsg>(16);
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<DcAck>();
    let dc_join = tokio::spawn(crate::disk_commit::disk_commit_task(
        dest_storage.clone(),
        session_config.clone(),
        dc_rx,
        ack_tx,
        progress.clone(),
    ));

    loop {
        // 同时处理 transport 消息和 progress 上报
        tokio::select! {
            Some(snapshot) = progress_rx.recv() => {
                let _ = transport.send(ReceiverMsg::Progress(snapshot)).await;
            }
            // ── disk-commit task 回流的 ack（目录/符号链接直接透传；全量文件 outcome
            //    交 decide_file_ack 做统一 redo 决策）──
            Some(dc_ack) = ack_rx.recv() => {
                match dc_ack {
                    DcAck::Entry(ack) => {
                        let _ = transport.send(ack).await;
                        completed_count += 1;
                        if transfer_done_seen && completed_count >= requested_count {
                            info!("[Receiver Remote] All transfers complete");
                            break;
                        }
                    }
                    DcAck::FileOutcome { ndx, outcome } => {
                        if dispatch_file_outcome(transport, &mut attempts, progress, ndx, outcome).await
                            && complete_ndx(&mut completed_count, requested_count, transfer_done_seen)
                        {
                            break;
                        }
                    }
                }
            }
            msg = transport.recv() => { match msg {
            // ── 文件列表：FilePage → DestIndex 比较 → 发 TransferRequest/DeltaTransferRequest ──
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
                            requested_count += 1;
                        }
                        transport::message::TransferDecision::DeltaTransfer if !negotiated_features.delta => {
                            // delta 能力未协商成功（对端不支持）→ 降级为全量传输
                            let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx }).await;
                            requested_count += 1;
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
                                    requested_count += 1;
                                }
                                Err(e) => {
                                    warn!(
                                        "[Receiver Remote] 读取 basis file {:?} 失败: {}, 降级全量传输",
                                        nf.entry.get_relative_path(),
                                        e
                                    );
                                    let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx }).await;
                                    requested_count += 1;
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
                    if let Err(e) = validate_relative_path(ns.entry.get_relative_path()) {
                        warn!("[Receiver Remote] Rejecting unsafe subdir path: {}", e);
                        let _ = transport
                            .send(ReceiverMsg::EntryError {
                                entry: ns.entry.clone(),
                                reason: format!("{e}"),
                            })
                            .await;
                        continue;
                    }
                    if let Err(e) = dest_storage.create_dir_all(&ns.entry).await {
                        warn!("[Receiver Remote] create_dir {:?}: {}", ns.entry.get_relative_path(), e);
                    }
                    // 目录 mtime/mode 待所有传输完成后统一回写（见 created_dirs 声明处）
                    created_dirs.push(ns.entry.clone());
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
                let _ = transport.send(ReceiverMsg::RequestsDone).await;
            }

            // ── 文件开始：标记走全量流式路径，交 disk-commit task 起 resume_prepare + write_chunk_stream ──
            Some(SenderMsg::FileBegin { ndx, entry }) => {
                full_active = true;
                delta_tokens.clear();
                let _ = dc_tx.send(DiskCommitMsg::FileBegin { ndx, entry }).await;
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
                if let Err(e) = dest_storage.create_dir_all(&entry).await {
                    warn!("[Receiver Remote] create_dir {:?}: {}", entry.get_relative_path(), e);
                }
                let _ = dest_storage.set_entry_metadata(&entry).await;
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
                    completed_count += 1;
                    if transfer_done_seen && completed_count >= requested_count {
                        info!("[Receiver Remote] All transfers complete");
                        break;
                    }
                    continue;
                }
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
                completed_count += 1;
                if transfer_done_seen && completed_count >= requested_count {
                    info!("[Receiver Remote] All transfers complete");
                    break;
                }
            }

            // ── 数据流模式：文件数据块 → 转发给 disk-commit task 的 write_chunk_stream ──
            Some(SenderMsg::FileData { ndx, entry, chunk }) => {
                let _ = dc_tx.send(DiskCommitMsg::FileChunk { ndx, entry, chunk }).await;
            }

            // ── Delta token 接收 ──
            Some(SenderMsg::DeltaMatch { ndx: _, block_index }) => {
                delta_tokens.push(sync_delta::DeltaToken::Match { block_index });
            }
            Some(SenderMsg::DeltaData { ndx: _, data }) => {
                delta_tokens.push(sync_delta::DeltaToken::Data(data));
            }

            // ── 文件结束：见过 FileBegin → 全量交 disk-commit task 收尾（读回 .part hash 校验 → 原子
            //    rename），completed_count 在 dc ack 回流时才 +1；否则走 delta inline 重建，outcome
            //    同样交 decide_file_ack 做统一 redo 决策（与全量路径共用一份状态机）──
            Some(SenderMsg::EndOfFile { ndx, entry, source_hash }) => {
                if full_active {
                    full_active = false;
                    let _ = dc_tx.send(DiskCommitMsg::FileCommit { ndx, entry, source_hash }).await;
                } else {
                    let tokens = std::mem::take(&mut delta_tokens);
                    let outcome =
                        handle_end_of_file(dest_storage, &entry, source_hash, tokens, Bytes::new(), progress).await;
                    if dispatch_file_outcome(transport, &mut attempts, progress, ndx, outcome).await
                        && complete_ndx(&mut completed_count, requested_count, transfer_done_seen)
                    {
                        break;
                    }
                }
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
                transfer_done_seen = true;
                debug!(
                    "[Receiver Remote] TransferDone received ({}/{} data transfers completed so far)",
                    completed_count, requested_count
                );
                if completed_count >= requested_count {
                    info!("[Receiver Remote] All transfers complete");
                    break;
                }
            }
            // ── Sender 源读失败：中止正在写入的文件（丢弃 .part，dc 不发 ack），并计一次完成 ──
            Some(SenderMsg::EntryError { path, reason }) => {
                warn!("[Receiver Remote] Sender EntryError {:?}: {} — 中止该文件", path, reason);
                full_active = false;
                let _ = dc_tx.send(DiskCommitMsg::AbortFile).await;
                completed_count += 1;
                if transfer_done_seen && completed_count >= requested_count {
                    info!("[Receiver Remote] All transfers complete");
                    break;
                }
            }
            Some(_) => {}
            None => return Err(AppError::CopyError("Transport closed during file list/data phase".into())),
        }} // close match + select! msg arm
        } // close select!
    } // close loop

    // 收尾：关闭 dc_tx → disk-commit task 处理完积压后退出；等待 join；drain 剩余 ack。
    let _ = dc_tx.send(DiskCommitMsg::Shutdown).await;
    drop(dc_tx);
    dc_join
        .await
        .map_err(|e| AppError::CopyError(format!("disk-commit task join: {e}")))??;
    while let Ok(dc_ack) = ack_rx.try_recv() {
        match dc_ack {
            DcAck::Entry(ack) => {
                let _ = transport.send(ack).await;
            }
            DcAck::FileOutcome { ndx, outcome } => {
                let _ = dispatch_file_outcome(transport, &mut attempts, progress, ndx, outcome).await;
            }
        }
    }

    // 回写目录 mtime/mode：所有文件已落盘,此时设目录元数据不会再被子文件写入顶掉。
    for dir_entry in &created_dirs {
        if let Err(e) = dest_storage.set_entry_metadata(dir_entry).await {
            warn!(
                "[Receiver Remote] 回写目录元数据 {:?} 失败: {}",
                dir_entry.get_relative_path(),
                e
            );
        }
    }

    Ok(())
}

/// ndx 级文件传输 redo 决策（方案 A 的统一决策点）
///
/// dc task（全量路径）与 `handle_end_of_file`（delta inline 路径）上报的 outcome
/// 在此统一转换为终态/重试 ack：
/// - 校验失败（`HashMismatch`）·首次 → `Redo{ndx}`，不计入 `completed_count`（文件仍在途）。
/// - 校验失败·二次+ → `Error{ndx,"hash mismatch"}`，计入 `completed_count`。
/// - 校验通过（`Success`）→ `Success{ndx}`，计入 `completed_count`。
/// - 硬错误（`HardError`，非 hash 类）→ 直接 `Error{ndx,reason}` 终态，不 redo。
///
/// 返回 `(ack, is_terminal)`：`is_terminal=true` 时调用方需计入 `completed_count`。
fn decide_file_ack(attempts: &mut HashMap<i32, u8>, ndx: i32, outcome: FileOutcome) -> (ReceiverMsg, bool) {
    match outcome {
        FileOutcome::Success => (ReceiverMsg::Success { ndx }, true),
        FileOutcome::HashMismatch => {
            let count = attempts.entry(ndx).or_insert(0);
            *count += 1;
            if *count >= 2 {
                (
                    ReceiverMsg::Error {
                        ndx,
                        reason: "hash mismatch".into(),
                    },
                    true,
                )
            } else {
                (ReceiverMsg::Redo { ndx }, false)
            }
        }
        FileOutcome::HardError(reason) => (ReceiverMsg::Error { ndx, reason }, true),
    }
}

/// `FileOutcome` 上报的统一处理入口（dc task 全量路径 / delta inline 路径共用）：
/// 调用 `decide_file_ack` 做 redo 决策，终态为 `Error` 时累加 `progress.error_count`，
/// 把决策结果发回 Sender。返回 `is_terminal`，调用方据此决定是否 `complete_ndx`。
async fn dispatch_file_outcome(
    transport: &(dyn ReceiverTransport + 'static), attempts: &mut HashMap<i32, u8>, progress: &Arc<ReceiverProgress>,
    ndx: i32, outcome: FileOutcome,
) -> bool {
    let (ack, is_terminal) = decide_file_ack(attempts, ndx, outcome);
    if matches!(ack, ReceiverMsg::Error { .. }) {
        progress.error_count.fetch_add(1, Ordering::Relaxed);
    }
    let _ = transport.send(ack).await;
    is_terminal
}

/// 完成一次 ndx 级传输：`completed_count += 1`；若已见过 `TransferDone` 且计数打平，
/// 返回 `true`（调用方应 break 主循环收尾）。
fn complete_ndx(completed_count: &mut u64, requested_count: u64, transfer_done_seen: bool) -> bool {
    *completed_count += 1;
    if transfer_done_seen && *completed_count >= requested_count {
        info!("[Receiver Remote] All transfers complete");
        return true;
    }
    false
}

/// `EndOfFile` 处理（delta inline 路径）：重建文件字节（delta 或全量） → hash 校验 → 写入目标端
///
/// `tokens` 非空时执行 delta 重建，为空时直接使用 `file_data`。返回 `FileOutcome`，不再自行
/// 发送终态 ack —— 调用方通过 `decide_file_ack` 统一做 redo 决策（与全量路径共用一份状态机）。
async fn handle_end_of_file(
    dest_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>, source_hash: Option<String>,
    tokens: Vec<sync_delta::DeltaToken>, file_data: bytes::Bytes, progress: &Arc<ReceiverProgress>,
) -> FileOutcome {
    let relative_path = entry.get_relative_path();

    if let Err(e) = validate_relative_path(relative_path) {
        warn!(
            "[Receiver Remote] Rejecting unsafe relative path {:?}: {}",
            relative_path, e
        );
        return FileOutcome::HardError(format!("{e}"));
    }

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
        match StorageEnum::read_file_from(dest_storage, entry, size).await {
            Ok(basis_data) => {
                let block_size = sync_delta::calculate_block_size(size);
                bytes::Bytes::from(sync_delta::reconstruct::reconstruct(&basis_data, &tokens, block_size))
            }
            Err(e) => {
                error!("[Receiver Remote] read basis failed {:?}: {}", relative_path, e);
                return FileOutcome::HardError(format!("{e}"));
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
            return FileOutcome::HashMismatch;
        }
    }

    // 写入文件
    let data_len = file_bytes.len() as u64;
    match StorageEnum::write_file_from_bytes(dest_storage, entry, file_bytes).await {
        Ok(()) => {
            let _ = dest_storage.set_entry_metadata(entry).await;
            progress.files_transferred.fetch_add(1, Ordering::Relaxed);
            progress.bytes_transferred.fetch_add(data_len, Ordering::Relaxed);
            FileOutcome::Success
        }
        Err(e) => {
            error!("[Receiver Remote] write failed {:?}: {}", relative_path, e);
            FileOutcome::HardError(format!("{e}"))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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

    // ── decide_file_ack：ndx 级 redo 决策纯函数单测（方案 A 的核心状态机） ──

    #[test]
    fn decide_file_ack_success_is_terminal() {
        let mut attempts = HashMap::new();
        let (ack, is_terminal) = decide_file_ack(&mut attempts, 7, FileOutcome::Success);
        assert!(matches!(ack, ReceiverMsg::Success { ndx: 7 }));
        assert!(is_terminal);
    }

    #[test]
    fn decide_file_ack_first_hash_mismatch_redos_and_is_not_terminal() {
        let mut attempts = HashMap::new();
        let (ack, is_terminal) = decide_file_ack(&mut attempts, 3, FileOutcome::HashMismatch);
        assert!(matches!(ack, ReceiverMsg::Redo { ndx: 3 }));
        assert!(!is_terminal);
        assert_eq!(attempts.get(&3), Some(&1));
    }

    #[test]
    fn decide_file_ack_second_hash_mismatch_errors_and_is_terminal() {
        let mut attempts = HashMap::new();
        let (first_ack, first_terminal) = decide_file_ack(&mut attempts, 3, FileOutcome::HashMismatch);
        assert!(matches!(first_ack, ReceiverMsg::Redo { ndx: 3 }));
        assert!(!first_terminal);

        let (second_ack, second_terminal) = decide_file_ack(&mut attempts, 3, FileOutcome::HashMismatch);
        match second_ack {
            ReceiverMsg::Error { ndx: 3, reason } => assert_eq!(reason, "hash mismatch"),
            other => panic!("expected Error, got {other:?}"),
        }
        assert!(second_terminal);
    }

    #[test]
    fn decide_file_ack_hard_error_is_terminal_without_redo() {
        let mut attempts = HashMap::new();
        let (ack, is_terminal) =
            decide_file_ack(&mut attempts, 9, FileOutcome::HardError("resume_prepare failed".into()));
        match ack {
            ReceiverMsg::Error { ndx: 9, reason } => assert_eq!(reason, "resume_prepare failed"),
            other => panic!("expected Error, got {other:?}"),
        }
        assert!(is_terminal);
        // 硬错误不计入 attempts（不参与 redo 计数）
        assert!(attempts.get(&9).is_none());
    }

    #[test]
    fn decide_file_ack_different_ndx_track_attempts_independently() {
        let mut attempts = HashMap::new();
        let _ = decide_file_ack(&mut attempts, 1, FileOutcome::HashMismatch);
        let (ack, is_terminal) = decide_file_ack(&mut attempts, 2, FileOutcome::HashMismatch);
        assert!(
            matches!(ack, ReceiverMsg::Redo { ndx: 2 }),
            "ndx 2 应是首次失败，与 ndx 1 互不影响"
        );
        assert!(!is_terminal);
    }
}
