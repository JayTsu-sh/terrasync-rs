//! Receiver 侧逻辑
//!
//! 从 transport 接收 Sender 发来的消息，在目标存储上执行写入操作。
//! 单进程模式下 Receiver 拥有 src+dest 两个 storage（命令模式）。
//! 双进程模式下 Receiver 仅拥有 dest storage（数据流模式，Phase 3）。

// 标准库
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// 外部 crate
use data_mover::qos::QosManager;
use data_mover::{EntryEnum, StorageEnum};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::error::TransportError;
use transport::message::{
    DcAck, DestIndex, DiskCommitMsg, FeatureFlags, FileOutcome, HandshakeResult, NdxTable, ProgressSnapshot,
    ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig, TransferDecision, credit_cost,
};
use transport::quic::credit::DEFAULT_CREDIT_WINDOW_BYTES;
use transport::traits::ReceiverTransport;

// 内部模块
use crate::byte_resume::is_part_file;
use crate::error::{AppError, Result};
use crate::sync::{ResumeOpts, copy_file_with_resume, parse_size, should_resume};

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

            SenderMsg::EntryError { path, reason, .. } => {
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
    let delta_size_threshold = resolve_delta_size_threshold(&session_config.delta_size_threshold)?;

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
        delta_size_threshold,
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

/// delta 传输 size 门槛默认值：512MiB（当前 delta 峰值内存 ≈3× 文件大小，最坏锁在 ~1.5GB，
/// 见 issue #54 阶段 0 spec）
const DEFAULT_DELTA_SIZE_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

/// credit 批量授信阈值：半个 credit 窗口（issue #59 方案 b，仿 TCP 延迟 ack，避免逐帧
/// 授信的开销与慢链路授信限速）。与 Sender 侧 `quic::credit::DEFAULT_CREDIT_WINDOW_BYTES`
/// 共享同一个常量来源，避免两端窗口大小假设漂移。
const CREDIT_GRANT_THRESHOLD_BYTES: u64 = DEFAULT_CREDIT_WINDOW_BYTES / 2;

/// credit 累计消费达半窗口阈值时批量发放一次 grant（仿 TCP 延迟 ack）：累计 `consumed`
/// 达到 `threshold` 才返回 `Some`，否则原地累加、返回 `None`。
///
/// 返回值是**实际累计的消费字节数**（而非固定的 `threshold` 常量）：消息大小通常不整除
/// `threshold`，触发时的累计值可能略高于阈值，直接把这个精确值作为 grant 金额发出——
/// 保证长期账目精确相等（授信总量 == 消费总量），不会因为改发恒定阈值而产生系统性
/// drift（授信不足 = credit 泄漏，Sender 早晚耗尽窗口；授信过量 = 双计，突破记账不变量
/// `outstanding ∈ [0, window]` 的上界）。
fn accumulate_credit(consumed: &mut u64, delta: u64, threshold: u64) -> Option<u64> {
    *consumed += delta;
    if *consumed >= threshold {
        let grant = *consumed;
        *consumed = 0;
        Some(grant)
    } else {
        None
    }
}

/// 解析 `SessionConfig.delta_size_threshold`：`None` 时使用默认值 512MiB，`Some` 时复用
/// `parse_size`（与 `block_size` 同款人类可读格式，如 "512MiB"）。超过该 size 的文件即使
/// 数据不匹配也降级为全量传输（见 `recv_file_list_and_data_phase` 的 `DeltaTransfer` 分支）。
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
/// `delta_size_threshold`（`resolve_delta_size_threshold` 解析，默认 512MiB）同理：文件
/// size 超过该门槛时同样降级为全量传输，避免 delta 重建的整文件内存缓冲（issue #54 阶段 0）。
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
    negotiated_features: &FeatureFlags, delta_size_threshold: u64, progress: &Arc<ReceiverProgress>,
    mut progress_rx: MpscReceiver<ProgressSnapshot>,
) -> Result<()> {
    info!("[Receiver Remote] Receiving file list and file data (pipelined, streaming mode)");
    let mut ndx_table = NdxTable::new();
    // ndx 级失败尝试计数（redo 决策用）：ndx → 已失败次数，见 decide_file_ack
    let mut attempts: HashMap<i32, u8> = HashMap::new();
    // 已终结的 ndx：redo 期间 dc task（全量路径 HardError）与 Sender 自检失败
    // （SenderMsg::EntryError）可能各自独立上报同一 ndx 的终态，按 ndx 去重只在首次
    // 生效，防止 completed_count 被多次计入导致主循环提前 break、丢在途文件（[1]）。
    let mut terminated: HashSet<i32> = HashSet::new();
    // TransferDone 与实际数据完成的解耦计数（见函数文档）
    let mut requested_count: u64 = 0;
    let mut completed_count: u64 = 0;
    let mut transfer_done_seen = false;
    // 全量文件是否走 disk-commit 流式路径：以是否见过 FileBegin 判定（而非 token 是否为空），
    // 避免源端缩到 0 字节的 delta 传输（token 空且无 FileBegin）被误判为全量。
    let mut full_active = false;
    // delta 文件是否已向 disk-commit task 发过 DeltaBegin：镜像 full_active，首个属于该
    // ndx 的 delta 数据事件（token 或 EndOfFile）才触发一次 DeltaBegin，见
    // `ensure_delta_active` 文档（避免 FilePage 阶段流水线化发出多个
    // DeltaTransferRequest、早于任何响应到达导致 dc_tx 收到乱序 DeltaBegin）。
    let mut delta_active = false;
    // 已创建的目录：所有文件传输完成后统一回写 mtime/mode(写子文件会把目录 mtime 顶到当前时间;
    // 且 0500 等受限权限须在写完子项后才能设),对齐单进程 orchestrator 的目录 mtime 收尾。
    let mut created_dirs: Vec<Arc<EntryEnum>> = Vec::new();
    // credit 累计消费（issue #59 方案 b）：FileData/DeltaData 处理完成（送 dc_tx）后累加，
    // 达半窗口阈值批量发一次 CreditGrant 并清零，见 `accumulate_credit`。选"送达即补"而非
    // "落盘才补"：时序上与落盘 ack 近似等价、落盘 ack 通路改动更大且窗口利用率更差、dc
    // 缓冲本身有界，三者共同构成确定的应用层积压上界（window + dc 固定缓冲常量）。
    let mut credit_consumed: u64 = 0;
    // disk-commit task：全量/delta 文件均三段流式落盘（去整文件 BytesMut/token Vec）；ack
    // 经 unbounded channel 回流，避免路由 select 阻在 dc_tx.send 时不 drain ack → dc 阻在
    // ack_tx.send 的双向死锁。
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
                        if dispatch_file_outcome(
                            transport,
                            &mut attempts,
                            &mut terminated,
                            progress,
                            ndx,
                            outcome,
                            &mut completed_count,
                            requested_count,
                            transfer_done_seen,
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
            msg = transport.recv() => {
            // credit 花费口径与 Sender 侧扣减共用同一份 credit_cost（issue #59），
            // 在 match 移动 msg 之前算好，FileData/DeltaData 分支据此累计消费。
            let credit_delta = msg.as_ref().and_then(credit_cost);
            match msg {
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

                // 逐文件比较 → 发 TransferRequest/DeltaTransferRequest（New/Changed，前者
                // 附带 decision 供 Sender 侧统计正确分类）或 Classified（MetadataOnly/Skip，
                // Receiver 本地执行/判定，此前零 wire 流量，见 issue #23）
                for nf in &page.files {
                    let decision = dest_index.check(&nf.entry);
                    match decision {
                        TransferDecision::FullTransfer => {
                            let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx, decision }).await;
                            requested_count += 1;
                        }
                        TransferDecision::DeltaTransfer if !negotiated_features.delta => {
                            // delta 能力未协商成功（对端不支持）→ 降级为全量传输（wire 动作变了，
                            // 分类判定不变：decision 仍是 DeltaTransfer，供 Sender 侧统计为 Changed）
                            let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx, decision }).await;
                            requested_count += 1;
                        }
                        TransferDecision::DeltaTransfer if nf.entry.get_size() > delta_size_threshold => {
                            // 文件 size 超过 delta_size_threshold 门槛 → 降级为全量传输（同上，
                            // wire 动作降级、分类判定不变），避免大文件 basis 全量读入内存 +
                            // delta 重建峰值内存 ≈3× 文件大小的风险（issue #54 阶段 0）
                            info!(
                                "[Receiver Remote] {:?} size {} exceeds delta_size_threshold {} bytes, downgrading to full transfer",
                                nf.entry.get_relative_path(),
                                nf.entry.get_size(),
                                delta_size_threshold
                            );
                            let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx, decision }).await;
                            requested_count += 1;
                        }
                        TransferDecision::DeltaTransfer => {
                            let size = nf.entry.get_size();
                            let block_size = sync_delta::calculate_block_size(size);
                            // 流式读 basis file：read_chunk_stream 按块读驱动签名状态机
                            // 逐块喂入（不再整文件驻留 RAM，见 issue #54 阶段 1）
                            let (mut basis_rx, basis_handle) =
                                StorageEnum::read_chunk_stream(dest_storage, &nf.entry, None, None, false, 8);
                            let mut sig_calc = sync_delta::signature::SignatureCalculator::new(block_size);
                            while let Some(chunk) = basis_rx.recv().await {
                                sig_calc.push(&chunk.data);
                            }
                            // 读任务收尾：JoinError 或内层读错误统一归一为原因字符串（与
                            // Sender 侧源文件流式读点一致的归一模式）
                            let basis_read_result = match basis_handle.await {
                                Ok(inner) => inner.map_err(|e| e.to_string()),
                                Err(e) => Err(e.to_string()),
                            };
                            match basis_read_result {
                                Ok(_) => {
                                    let signatures = sig_calc.finish();
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
                                    let _ = transport.send(ReceiverMsg::TransferRequest { ndx: nf.ndx, decision }).await;
                                    requested_count += 1;
                                }
                            }
                        }
                        TransferDecision::MetadataOnly => {
                            let _ = dest_storage.set_entry_metadata(&nf.entry).await;
                            progress.metadata_only.fetch_add(1, Ordering::Relaxed);
                            let _ = transport
                                .send(ReceiverMsg::Classified {
                                    entry: nf.entry.clone(),
                                    decision,
                                })
                                .await;
                        }
                        TransferDecision::Skip => {
                            progress.files_skipped.fetch_add(1, Ordering::Relaxed);
                            let _ = transport
                                .send(ReceiverMsg::Classified {
                                    entry: nf.entry.clone(),
                                    decision,
                                })
                                .await;
                        }
                        TransferDecision::Deleted => {
                            // DestIndex::check() 从不产生 Deleted（该分类只在下方
                            // orphaned_entries() 路径产生）；此分支纯粹为满足 match 穷尽性检查。
                            debug_assert!(false, "DestIndex::check() 不应返回 Deleted");
                        }
                    }
                }

                // 子目录在目标端创建
                for ns in &page.subdirs {
                    // 源端存在该子目录 → 目标端同名条目不是孤儿（须在 orphan 扫描前登记，
                    // 与创建成功与否无关；不登记则预存子目录被误判孤儿 → 整树误删后重传）
                    dest_index.mark_matched(&ns.entry);
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

                // --delete-target: 删除目标端多余文件；成功发 Classified{Deleted}、
                // 失败发 EntryError（此前失败只 warn! 静默吞掉，不计入任何统计/错误数）
                if session_config.delete_target {
                    for orphan in dest_index.orphaned_entries() {
                        let path = orphan.get_relative_path();
                        // 跳过续传临时文件：源端不存在同名 .terrasync-part 条目，
                        // 会被误判为孤儿；删掉会破坏进行中的续传进度
                        if is_part_file(path) {
                            debug!("[Receiver Remote] Skipping in-progress part file: {:?}", path);
                            continue;
                        }
                        let result = if orphan.get_is_dir() {
                            info!("[Receiver Remote] Deleting orphaned dir: {:?}", path);
                            dest_storage.delete_dir_all(orphan).await
                        } else {
                            info!("[Receiver Remote] Deleting orphaned file: {:?}", path);
                            dest_storage.delete_file(orphan).await
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
                            Err(e) => {
                                warn!("[Receiver Remote] delete {:?}: {}", path, e);
                                let _ = transport
                                    .send(ReceiverMsg::EntryError {
                                        entry: orphan.clone(),
                                        reason: format!("{e}"),
                                    })
                                    .await;
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
            Some(SenderMsg::FileData { entry, chunk }) => {
                let _ = dc_tx.send(DiskCommitMsg::FileChunk { entry, chunk }).await;
                if let Some(bytes) = credit_delta
                    && let Some(grant) = accumulate_credit(&mut credit_consumed, bytes, CREDIT_GRANT_THRESHOLD_BYTES)
                {
                    let _ = transport.send(ReceiverMsg::CreditGrant { bytes: grant, ndx: None }).await;
                }
            }

            // ── Delta token 接收：逐 token 转发给 disk-commit task（不再攒整文件 Vec，见
            //    issue #54 阶段 3），首个属于该 ndx 的 token 先触发一次 DeltaBegin ──
            Some(SenderMsg::DeltaMatch { ndx, block_index }) => {
                if let Some(entry) = ndx_table.get(ndx).cloned() {
                    ensure_delta_active(&dc_tx, &mut delta_active, ndx, &entry).await;
                    let _ = dc_tx.send(DiskCommitMsg::DeltaMatch { entry, block_index }).await;
                } else {
                    warn!("[Receiver Remote] DeltaMatch for unknown ndx {}", ndx);
                }
            }
            Some(SenderMsg::DeltaData { ndx, data }) => {
                if let Some(entry) = ndx_table.get(ndx).cloned() {
                    ensure_delta_active(&dc_tx, &mut delta_active, ndx, &entry).await;
                    let _ = dc_tx.send(DiskCommitMsg::DeltaData { entry, data }).await;
                } else {
                    warn!("[Receiver Remote] DeltaData for unknown ndx {}", ndx);
                }
                if let Some(bytes) = credit_delta
                    && let Some(grant) = accumulate_credit(&mut credit_consumed, bytes, CREDIT_GRANT_THRESHOLD_BYTES)
                {
                    let _ = transport.send(ReceiverMsg::CreditGrant { bytes: grant, ndx: None }).await;
                }
            }

            // ── 文件结束：见过 FileBegin → 全量交 disk-commit task 收尾（读回 .part hash 校验 → 原子
            //    rename）；否则是 delta 路径（含零 token 的空文件 delta，ensure_delta_active
            //    幂等补发 DeltaBegin），同样交 disk-commit task 三段式收尾。completed_count 统一
            //    在 dc ack 回流时才 +1（decide_file_ack 做 redo 决策，全量/delta 共用一份状态机）──
            Some(SenderMsg::EndOfFile { ndx, entry, source_hash }) => {
                if full_active {
                    full_active = false;
                    let _ = dc_tx.send(DiskCommitMsg::FileCommit { ndx, entry, source_hash }).await;
                } else {
                    ensure_delta_active(&dc_tx, &mut delta_active, ndx, &entry).await;
                    delta_active = false;
                    let _ = dc_tx.send(DiskCommitMsg::DeltaCommit { ndx, entry, source_hash }).await;
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
            // ── Sender 自检失败（源读/符号链接读失败）：中止正在写入的文件（丢弃 .part，
            //    dc 不发 ack）。Sender 已在本地自增 error_count，这里只完成该 ndx 的
            //    计数、不回发 ReceiverMsg::Error（避免与 Sender 自增重复）；按 ndx 去重
            //    防止与 dc task 独立上报的终态重复计数（[1]）──
            Some(SenderMsg::EntryError { path, reason, ndx }) => {
                warn!("[Receiver Remote] Sender EntryError {:?}: {} — 中止该文件", path, reason);
                full_active = false;
                delta_active = false;
                let _ = dc_tx.send(DiskCommitMsg::AbortFile).await;
                let should_break = if let Some(ndx) = ndx {
                    record_ndx_completion(&mut terminated, ndx, &mut completed_count, requested_count, transfer_done_seen)
                } else {
                    // 双进程主循环下理应总带 ndx（单进程 tar 打包失败等无 ndx 场景走
                    // receiver_task，不会到这里）；防御性兜底：仍完成一次计数，避免缺失
                    // 关联导致 completed_count 永远打不平。
                    completed_count += 1;
                    transfer_done_seen && completed_count >= requested_count
                };
                if should_break {
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
                let _ = dispatch_file_outcome(
                    transport,
                    &mut attempts,
                    &mut terminated,
                    progress,
                    ndx,
                    outcome,
                    &mut completed_count,
                    requested_count,
                    transfer_done_seen,
                )
                .await;
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
/// dc task（全量/delta 路径统一经 `disk_commit_task` 上报，见 issue #54 阶段 3）上报的
/// outcome 在此统一转换为终态/重试 ack：
/// - 校验失败（`HashMismatch`/`SizeMismatch`）·首次 → `Redo{ndx}`，不计入
///   `completed_count`（文件仍在途）。
/// - 校验失败·二次+ → `Error{ndx,reason}`，计入 `completed_count`。
/// - 校验通过（`Success`）→ `Success{ndx}`，计入 `completed_count`。
/// - 硬错误（`HardError`，非校验类）→ 直接 `Error{ndx,reason}` 终态，不 redo。
///
/// 返回 `(ack, is_terminal)`：`is_terminal=true` 时调用方需计入 `completed_count`。
fn decide_file_ack(attempts: &mut HashMap<i32, u8>, ndx: i32, outcome: FileOutcome) -> (ReceiverMsg, bool) {
    match outcome {
        FileOutcome::Success => (ReceiverMsg::Success { ndx }, true),
        FileOutcome::HashMismatch => redo_or_error(attempts, ndx, "hash mismatch"),
        FileOutcome::SizeMismatch => redo_or_error(attempts, ndx, "size mismatch"),
        FileOutcome::HardError(reason) => (ReceiverMsg::Error { ndx, reason }, true),
    }
}

/// 校验失败类 outcome（`HashMismatch`/`SizeMismatch`）共用的重试计数：同一 ndx 首次 →
/// `Redo{ndx}`，二次+ → `Error{ndx,reason}` 终态。
fn redo_or_error(attempts: &mut HashMap<i32, u8>, ndx: i32, reason: &str) -> (ReceiverMsg, bool) {
    let count = attempts.entry(ndx).or_insert(0);
    *count += 1;
    if *count >= 2 {
        (
            ReceiverMsg::Error {
                ndx,
                reason: reason.into(),
            },
            true,
        )
    } else {
        (ReceiverMsg::Redo { ndx }, false)
    }
}

/// `FileOutcome` 上报的统一处理入口（dc task 全量/delta 路径共用）：
/// 调用 `decide_file_ack` 做 redo 决策；非终态（`Redo`）直接发 ack、不动
/// `completed_count`。终态先按 `ndx` 去重（redo 期间 dc task 与 Sender 自检失败
/// 可能各自独立上报同一 ndx 的终态，去重只在首次生效，见 [`record_ndx_completion`]）：
/// 去重命中则连 ack 都不再发（避免 Sender 收到同一 ndx 的重复终态 ack），否则累加
/// `progress.error_count`（若为 `Error`）、发 ack、计入 `completed_count`。
///
/// 返回是否应 break 主循环（`completed_count` 打平且已见过 `TransferDone`）。
#[allow(clippy::too_many_arguments)]
async fn dispatch_file_outcome(
    transport: &(dyn ReceiverTransport + 'static), attempts: &mut HashMap<i32, u8>, terminated: &mut HashSet<i32>,
    progress: &Arc<ReceiverProgress>, ndx: i32, outcome: FileOutcome, completed_count: &mut u64, requested_count: u64,
    transfer_done_seen: bool,
) -> bool {
    let (ack, is_terminal) = decide_file_ack(attempts, ndx, outcome);
    if !is_terminal {
        let _ = transport.send(ack).await;
        return false;
    }
    if terminated.contains(&ndx) {
        debug!("[Receiver Remote] ndx {} 已终结，丢弃重复终态: {:?}", ndx, ack);
        return false;
    }
    if matches!(ack, ReceiverMsg::Error { .. }) {
        progress.error_count.fetch_add(1, Ordering::Relaxed);
    }
    let _ = transport.send(ack).await;
    record_ndx_completion(terminated, ndx, completed_count, requested_count, transfer_done_seen)
}

/// ndx 首次终结时计入 `completed_count` 并判断是否应收尾 break；同一 ndx 的重复终结
/// （已在 `terminated` 中）直接丢弃、返回 `false`，防止 redo 期间多个来源独立上报
/// 同一 ndx 导致 `completed_count` 被多次计入、主循环提前 break 丢在途文件（[1]）。
fn record_ndx_completion(
    terminated: &mut HashSet<i32>, ndx: i32, completed_count: &mut u64, requested_count: u64, transfer_done_seen: bool,
) -> bool {
    if !terminated.insert(ndx) {
        return false;
    }
    *completed_count += 1;
    if transfer_done_seen && *completed_count >= requested_count {
        info!("[Receiver Remote] All transfers complete");
        return true;
    }
    false
}

/// 首个属于该 ndx 的 delta 数据事件（token 或 EndOfFile）才触发一次 `DeltaBegin`，幂等
/// （`delta_active` 已为 `true` 时直接返回）。
///
/// 不能在 Receiver 发出 `DeltaTransferRequest` 时就触发：`FilePage` 阶段可能流水线化
/// 连续发出多个 `DeltaTransferRequest`（不等待任何一个的响应），若在发送请求的同时就
/// `DeltaBegin`，dc_tx 会收到与 Sender 实际数据流顺序不一致的 `DeltaBegin` 序列（dc
/// task 严格串行、同一时刻只有一个 `ActiveFile`）。依据：Sender 侧
/// `process_requests_and_acks` 单一消费者循环严格串行处理每个 ndx 的数据阶段（无并发
/// spawn），故 Receiver 单一收消息循环里首个属于某 ndx 的 delta 数据事件必然对应"当前
/// 正在流的文件"。
async fn ensure_delta_active(
    dc_tx: &mpsc::Sender<DiskCommitMsg>, delta_active: &mut bool, ndx: i32, entry: &Arc<EntryEnum>,
) {
    if *delta_active {
        return;
    }
    let block_size = sync_delta::calculate_block_size(entry.get_size());
    let _ = dc_tx
        .send(DiskCommitMsg::DeltaBegin {
            ndx,
            entry: entry.clone(),
            block_size,
        })
        .await;
    *delta_active = true;
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
    fn decide_file_ack_first_size_mismatch_redos_and_is_not_terminal() {
        let mut attempts = HashMap::new();
        let (ack, is_terminal) = decide_file_ack(&mut attempts, 5, FileOutcome::SizeMismatch);
        assert!(matches!(ack, ReceiverMsg::Redo { ndx: 5 }));
        assert!(!is_terminal);
        assert_eq!(attempts.get(&5), Some(&1));
    }

    #[test]
    fn decide_file_ack_second_size_mismatch_errors_and_is_terminal() {
        let mut attempts = HashMap::new();
        let (first_ack, first_terminal) = decide_file_ack(&mut attempts, 5, FileOutcome::SizeMismatch);
        assert!(matches!(first_ack, ReceiverMsg::Redo { ndx: 5 }));
        assert!(!first_terminal);

        let (second_ack, second_terminal) = decide_file_ack(&mut attempts, 5, FileOutcome::SizeMismatch);
        match second_ack {
            ReceiverMsg::Error { ndx: 5, reason } => assert_eq!(reason, "size mismatch"),
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

    // ── accumulate_credit：半窗批量授信金额精确（issue #59，防双计/泄漏） ──

    #[test]
    fn accumulate_credit_below_threshold_returns_none_and_keeps_accumulating() {
        let mut consumed = 0u64;
        assert_eq!(accumulate_credit(&mut consumed, 3, 10), None);
        assert_eq!(consumed, 3);
        assert_eq!(accumulate_credit(&mut consumed, 4, 10), None);
        assert_eq!(consumed, 7);
    }

    #[test]
    fn accumulate_credit_exact_threshold_grants_exact_amount_and_resets() {
        let mut consumed = 0u64;
        assert_eq!(accumulate_credit(&mut consumed, 6, 10), None);
        assert_eq!(accumulate_credit(&mut consumed, 4, 10), Some(10));
        // 触发后立即清零，不残留上一轮的累计值
        assert_eq!(consumed, 0);
    }

    #[test]
    fn accumulate_credit_overshoot_grants_exact_accumulated_value_not_fixed_threshold() {
        // 单条消息一步越过阈值：grant 金额应是实际累计值（13），而非固定阈值（10），
        // 否则长期账目会产生系统性 drift（少发 3 字节 = credit 泄漏）。
        let mut consumed = 0u64;
        assert_eq!(accumulate_credit(&mut consumed, 13, 10), Some(13));
        assert_eq!(consumed, 0);
    }

    #[test]
    fn accumulate_credit_next_round_does_not_leak_previous_overshoot() {
        // 连续两轮：第一轮越过阈值触发 grant 并清零后，第二轮的累计应从 0 重新开始，
        // 不会把上一轮的余量（越过阈值的部分）带入下一轮（防双计）。
        let mut consumed = 0u64;
        assert_eq!(accumulate_credit(&mut consumed, 13, 10), Some(13));
        assert_eq!(accumulate_credit(&mut consumed, 5, 10), None);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn credit_grant_threshold_is_half_of_default_credit_window() {
        assert_eq!(CREDIT_GRANT_THRESHOLD_BYTES, DEFAULT_CREDIT_WINDOW_BYTES / 2);
    }
}
