//! 双进程模式远端同步（Sender 侧）
//!
//! 将 QUIC 连接、文件列表发送、传输请求处理、Ack 收集等阶段
//! 提取为独立函数，降低单函数复杂度并提升可读性。
//!
//! 握手/鉴权/`SessionConfig` 之后，文件列表发送（`send_file_list_phase`，只
//! `send()`、从不 `recv()`）与请求处理 + Ack 收集（`process_requests_and_acks`，
//! 唯一的 `recv()` 消费者）通过 `tokio::try_join!` 并发运行，不再是「文件列表发完
//! 才能开始处理请求」的顺序 barrier；`NdxTable` 因此改为 `Mutex` 包裹以支持并发
//! 读写（写者只有文件列表任务，读者只有请求处理任务，不存在二义性）。

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use data_mover::dir_tree::NdxEvent;
use data_mover::filter::parse_filter_expression;
use data_mover::qos::QosManager;
use data_mover::{
    ChangeKind, ConsistencyCheck, EntryEnum, StorageEntryMessage, StorageEnum, WalkDirAsyncIterator2, create_storage,
};
use rustls::pki_types::CertificateDer;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};
use transport::error::TransportError;
use transport::message::{
    BlockSignature, HandshakeResult, NdxTable, ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig,
    TransferDecision,
};
use transport::traits::SenderTransport;
use utils::app_config::AppConfig;
use utils::logger;

use crate::config::{JobType, SyncJobConfig};
use crate::consumer::stats::{IncrementalStats, ProgressBar, StatisticConsumer, StatsKind, format_bytes};
use crate::error::{AppError, Result};
use crate::orchestrator::create_qos_manager;
use crate::sync::parse_size;

/// 双进程全量同步 — Sender 侧入口
///
/// 依次执行：QUIC 连接 → 握手协商 → Token 鉴权 → `SessionConfig`；随后文件列表发送与
/// 请求处理+Ack 收集并发运行（见模块文档）。
pub(crate) async fn run(
    config: &SyncJobConfig, remote_addr: &str, tls_cert_bytes: Option<&[u8]>, auth_token: Option<&str>,
) -> Result<()> {
    info!("[Sender Remote] Connecting to Receiver at {}", remote_addr);

    // ── 1. 连接 QUIC ──
    let addr: SocketAddr = remote_addr
        .parse()
        .map_err(|e| AppError::CopyError(format!("Invalid remote address '{remote_addr}': {e}")))?;
    let server_cert = tls_cert_bytes.map(|b| CertificateDer::from(b.to_vec()));
    let transport = transport::quic::connect(addr, "localhost", server_cert).await?;
    info!("[Sender Remote] Connected");

    // ── 2. 握手：协商协议版本与能力，不兼容则在发送任何 FilePage/CopyEntry 前中止 ──
    negotiate_handshake(&transport).await?;

    // ── 2.5 Token 鉴权：握手通过后、SessionConfig 之前，鉴权失败则中止连接 ──
    send_and_check_auth(&transport, auth_token).await?;

    // ── 3. 发送 SessionConfig ──
    transport
        .send(SenderMsg::SessionConfig(SessionConfig {
            src_path: config.src_path.clone(),
            qos: config.qos.clone(),
            peak_qos_rate: config.peak_qos_rate,
            iops: config.iops,
            enable_integrity_check: config.enable_integrity_check,
            enable_acl: config.enable_acl,
            is_source_reserved: true,
            block_size: config.block_size.clone(),
            delete_target: config.delete_target,
            delta_size_threshold: config.delta_size_threshold.clone(),
        }))
        .await?;

    // ── 3.5 结构化统计报表（Sender 侧，复用本地 StatisticConsumer/IncrementalStats——
    //    双进程远端不分 full/incremental，报表形状本身就是 delta 语义，与 orchestrator
    //    的 ScanType 路由无关，统一用 JobType::IncrementalCopy）──
    let stats_consumer = Arc::new(AsyncMutex::new(StatisticConsumer {
        stats: StatsKind::Incremental(IncrementalStats::new(
            JobType::IncrementalCopy,
            config.job_id.clone(),
            config.raw_command_line.clone(),
            logger::get_current_app_log_path(),
        )),
        progress_bar: ProgressBar::new(JobType::IncrementalCopy),
        job_dir: config.job_dir.clone(),
        callback_url: config.progress_callback_url.clone(),
        pb_handle: None,
    }));
    let callback_guard = StatisticConsumer::begin(stats_consumer.clone()).await;

    // ── 4. 创建源端 storage + walkdir_2 ──
    let block_size = match &config.block_size {
        Some(s) => Some(parse_size(s)?),
        None => None,
    };
    let src_storage = Arc::new(create_storage(&config.src_path, block_size.map(|s| s as u64), false).await?);
    let match_expr = config.r#match.as_deref().and_then(|e| parse_filter_expression(e).ok());
    let exclude_expr = config.exclude.as_deref().and_then(|e| parse_filter_expression(e).ok());
    let app_config = AppConfig::fetch()?;
    let walkdir_iter = src_storage
        .walkdir_2(
            None,
            None,
            match_expr,
            exclude_expr,
            app_config.scan.concurrency,
            app_config.scan.include_tags,
        )
        .await?;

    // ── 5. QoS 管理器 + checkpoint 加载 ──
    let qos_manager = create_qos_manager(config.qos.as_ref(), config.peak_qos_rate, config.iops);
    let checkpoint_path = std::path::PathBuf::from(&config.job_dir).join("remote_checkpoint.json");
    let mut completed_paths = load_checkpoint(&checkpoint_path).await;
    if !completed_paths.is_empty() {
        info!(
            "[Sender Remote] Loaded checkpoint: {} entries already completed",
            completed_paths.len()
        );
    }

    // ── 6+7+8. 文件列表发送 与 请求处理+Ack收集 并发运行（流水线，无阶段 barrier） ──
    let ndx_table = Mutex::new(NdxTable::new());
    let file_list_fut = send_file_list_phase(&transport, &walkdir_iter, &ndx_table, &stats_consumer);
    let requests_acks_fut = process_requests_and_acks(
        &transport,
        &src_storage,
        &ndx_table,
        qos_manager.as_ref(),
        config.enable_acl,
        &mut completed_paths,
        &checkpoint_path,
        &stats_consumer,
    );
    let (page_count, (transfer_count, success_count, error_count)) =
        tokio::try_join!(file_list_fut, requests_acks_fut)?;
    info!(
        "[Sender Remote] File list sent: {} pages, {} entries",
        page_count,
        ndx_table.lock().unwrap_or_else(PoisonError::into_inner).len()
    );
    info!("[Sender Remote] {} transfer requests processed", transfer_count);
    info!(
        "[Sender Remote] Complete: {} success, {} errors",
        success_count, error_count
    );

    // ── 9. Checkpoint 处理 + 清理 ──
    save_or_clear_checkpoint(&checkpoint_path, &completed_paths, error_count).await;
    if let Some(ref qos) = qos_manager {
        qos.shutdown();
    }
    transport.close().await?;
    // 结束统计报表生命周期：abort 回调循环 → 发最终回调(is_final=true) → 打印终态报表
    StatisticConsumer::end(stats_consumer, callback_guard).await;
    finalize_run_result(error_count)
}

/// `error_count>0` 时返回具名错误，使 `main.rs` 的 `exit(1)` 生效；否则 `Ok(())`。
///
/// ndx 级 redo 二次失败（`ReceiverMsg::Error`）与 Entry 级失败（`EntryError`）都计入
/// `error_count`，任一存在都视为本次同步未完全成功。
fn finalize_run_result(error_count: u64) -> Result<()> {
    if error_count > 0 {
        return Err(AppError::RemoteSyncFailed { errors: error_count });
    }
    Ok(())
}

// ============================================================
// 握手：协议版本与能力协商
// ============================================================

/// 发送 `Handshake` 并等待 Receiver 的 `HandshakeAck`。
///
/// 不兼容时 Receiver 会回 `Rejected`，此处直接返回错误，
/// 调用方（`run()`）不会再发送 `SessionConfig` 及后续任何 Phase 1 数据。
async fn negotiate_handshake(transport: &(dyn SenderTransport + 'static)) -> Result<()> {
    transport
        .send(SenderMsg::Handshake(ProtocolHandshake::current()))
        .await?;
    match transport.recv().await {
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { features })) => {
            info!(
                "[Sender Remote] Handshake accepted, negotiated features: {:?}",
                features
            );
            Ok(())
        }
        Some(ReceiverMsg::HandshakeAck(HandshakeResult::Rejected { reason })) => {
            Err(TransportError::IncompatibleProtocol { reason }.into())
        }
        Some(_) => Err(AppError::CopyError("Unexpected message during handshake".into())),
        None => Err(AppError::CopyError("Transport closed during handshake".into())),
    }
}

// ============================================================
// Token 鉴权
// ============================================================

/// 发送 `Auth` 并等待 Receiver 的 `AuthResult`
///
/// `auth_token` 为 `None` 时发送空字符串 token（兼容 Receiver 未配置 `--token` 的场景）；
/// 鉴权失败时直接返回错误，调用方不会再发送 `SessionConfig` 及后续任何数据。
async fn send_and_check_auth(transport: &(dyn SenderTransport + 'static), auth_token: Option<&str>) -> Result<()> {
    let token = auth_token.unwrap_or_default().to_string();
    transport.send(SenderMsg::Auth { token }).await?;
    match transport.recv().await {
        Some(ReceiverMsg::AuthResult { ok: true, .. }) => {
            info!("[Sender Remote] Auth accepted");
            Ok(())
        }
        Some(ReceiverMsg::AuthResult { ok: false, reason }) => Err(TransportError::AuthFailed {
            reason: reason.unwrap_or_else(|| "unknown reason".into()),
        }
        .into()),
        Some(_) => Err(AppError::CopyError("Unexpected message during auth".into())),
        None => Err(AppError::CopyError("Transport closed during auth".into())),
    }
}

// ============================================================
// 文件列表发送（与 `process_requests_and_acks` 并发运行，见模块文档）
// ============================================================

/// 遍历 `walkdir_2` 并按页发送给 Receiver，填充 `ndx_table`，返回发送的页数。
///
/// 只调用 `transport.send()`、从不 `recv()`，可安全地与 `process_requests_and_acks`
/// 并发运行（`ndx_table` 用 `Mutex` 支持并发读写：本函数是唯一的写者）。
///
/// 每页遍历到的 entry 额外喂一次 `StorageEntryMessage::Scanned`，填充
/// `IncrementalStats.scanned`（扩展名/时间/大小分布）——Sender 本来就要完整遍历一次
/// 源端才能生成 file list，这个遍历本身就是「Scanned」阶段的等价物，不需要额外协议
/// 往返（远端没有独立 scan 阶段，见 issue #23）。
async fn send_file_list_phase(
    transport: &(dyn SenderTransport + 'static), walkdir_iter: &WalkDirAsyncIterator2, ndx_table: &Mutex<NdxTable>,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>,
) -> Result<u64> {
    info!("[Sender Remote] Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.next().await {
        match event {
            NdxEvent::Page(page) => {
                {
                    let mut consumer = stats_consumer.lock().await;
                    for nf in &page.files {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(nf.entry.clone()));
                    }
                    for ns in &page.subdirs {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(ns.entry.clone()));
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

// ============================================================
// 传输请求处理 + Ack 收集（唯一的 recv() 消费者，与 `send_file_list_phase` 并发运行）
// ============================================================

/// 接收 Receiver 的 `TransferRequest`/`DeltaTransferRequest`（发送对应数据）与
/// `EntrySuccess`/`EntryError`/`Progress`（记录 ack/进度），直到 `AllDone`。
///
/// 合并原先顺序执行的「请求处理」与「Ack 收集」两个阶段：拆分多路复用 stream 后，
/// Receiver 可能在所有请求处理完之前就已经开始发送 ack/progress（两者走不同的物理
/// stream），若仍分成两个各自独立调用 `recv()` 的阶段，晚到的 ack 会被"请求处理"
/// 阶段的 catch-all 分支直接丢弃。合并为单一消费者循环、按 variant dispatch 后，
/// 不再有这个丢消息风险（transport 层只暴露一个 `recv()`，见 `crates/transport/src/quic/mux.rs`）。
///
/// `RequestsDone` 到达时立即发送 `TransferDone`（与改造前时序一致），但循环不break，
/// 继续处理后续到达的 ack/progress，直到收到 `AllDone`。
///
/// `TransferRequest{decision}`/`DeltaTransferRequest`/`Classified{entry, decision}`
/// 各自额外翻译为 `StorageEntryMessage` 喂 `stats_consumer.update_statistics()`，驱动
/// Sender 侧结构化报表（New/Changed/MetadataOnly/Deleted，`Skip` 不产生统计，见
/// `classification_to_stats_message`，issue #23）；`Redo{ndx}` 是重发、不是新的分类
/// 事件，不重复计数。
/// 返回 `(transfer_count, success_count, error_count)`。
async fn process_requests_and_acks(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, ndx_table: &Mutex<NdxTable>,
    qos: Option<&QosManager>, enable_acl: bool, completed_paths: &mut HashSet<String>, checkpoint_path: &Path,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>,
) -> Result<(u64, u64, u64)> {
    info!("[Sender Remote] Processing transfer requests + collecting acks");
    let mut transfer_count = 0u64;
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    // 已计过 error_count 的 ndx：复合失败（同 ndx 源读失败 + dest 侧 resume_prepare
    // 失败）时，Sender 自检失败与 Receiver 回传的 Error{ndx} 可能各自独立触发一次
    // error_count += 1，按 ndx 去重只计一次（见 count_ndx_error）。
    let mut errored_ndx: HashSet<i32> = HashSet::new();
    loop {
        match transport.recv().await {
            Some(ReceiverMsg::TransferRequest { ndx, decision }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    if let Some(msg) = classification_to_stats_message(decision, entry.clone()) {
                        stats_consumer.lock().await.update_statistics(&msg);
                    }
                    // 源读/符号链接读失败：Sender 已发 EntryError 通知 Receiver 完成该
                    // ndx，这里自增 error_count（Sender 自检失败由 Sender 自己计数，见 [0][3]）
                    if !handle_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await? {
                        count_ndx_error(&mut errored_ndx, ndx, &mut error_count);
                    }
                    transfer_count += 1;
                } else {
                    error!("[Sender Remote] Unknown NDX {}", ndx);
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
                    // 收到 DeltaTransferRequest 本身即无歧义地代表 Changed，不需要额外的
                    // decision 字段（该消息只在 DestIndex::check() 判定 DeltaTransfer 且
                    // delta 能力协商成功时才会发出）
                    stats_consumer
                        .lock()
                        .await
                        .update_statistics(&StorageEntryMessage::Changed {
                            entry: entry.clone(),
                            kind: ChangeKind::DataOnly,
                        });
                    // delta: 仅 src 读取成功时计入传输数（与原逻辑保持一致）；读取失败已发
                    // EntryError 通知 Receiver 完成该 ndx，这里自增 error_count（见 [2]）
                    if handle_delta_transfer(
                        transport,
                        src_storage,
                        &entry,
                        ndx,
                        block_size,
                        signatures,
                        qos,
                        enable_acl,
                    )
                    .await?
                    {
                        transfer_count += 1;
                    } else {
                        count_ndx_error(&mut errored_ndx, ndx, &mut error_count);
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {} for delta", ndx);
                }
            }
            // ── 分类信号（MetadataOnly/Skip/Deleted）：Receiver 本地执行/判定后上行，
            //    只驱动统计，不触发任何 Sender 侧动作 ──
            Some(ReceiverMsg::Classified { entry, decision }) => {
                if let Some(msg) = classification_to_stats_message(decision, entry) {
                    stats_consumer.lock().await.update_statistics(&msg);
                }
            }
            // ── ndx 级重传请求：hash 校验失败首次上报后，Receiver 要求重发。delta redo 一律
            //    降级为全量重发——Sender 对 redo 无状态，不保留 signatures/mode ──
            Some(ReceiverMsg::Redo { ndx }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    if !handle_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await? {
                        count_ndx_error(&mut errored_ndx, ndx, &mut error_count);
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {} for redo", ndx);
                }
            }
            Some(ReceiverMsg::RequestsDone) => {
                info!(
                    "[Sender Remote] All requests received, {} files to transfer",
                    transfer_count
                );
                transport.send(SenderMsg::TransferDone).await?;
            }
            // ── ndx 级文件传输终态：与 EntrySuccess（目录/符号链接）共用同一个 success_count ──
            Some(ReceiverMsg::Success { ndx }) => {
                let relative_path = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .map(|entry| entry.get_relative_path().to_string_lossy().to_string());
                record_success_and_checkpoint(relative_path, &mut success_count, completed_paths, checkpoint_path)
                    .await;
            }
            Some(ReceiverMsg::EntrySuccess { ref entry }) => {
                let relative_path = entry.get_relative_path().to_string_lossy().to_string();
                record_success_and_checkpoint(
                    Some(relative_path),
                    &mut success_count,
                    completed_paths,
                    checkpoint_path,
                )
                .await;
            }
            Some(ReceiverMsg::Progress(snapshot)) => {
                // 远端写盘发生在 Receiver 侧，Sender 自己不产生 chunk 级字节计数，
                // 复用 StatisticConsumer 的实时字节计数器承载 Receiver 汇报的进度
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
            Some(ReceiverMsg::EntryError { entry, reason }) => {
                error!(
                    "[Sender Remote] Entry failed {:?}: {}",
                    entry.get_relative_path(),
                    reason
                );
                error_count += 1;
            }
            // ── ndx 级文件传输终态失败（redo 二次失败）：与 EntryError 共用同一个 error_count ──
            Some(ReceiverMsg::Error { ndx, reason }) => {
                error!("[Sender Remote] NDX {} failed: {}", ndx, reason);
                count_ndx_error(&mut errored_ndx, ndx, &mut error_count);
            }
            Some(ReceiverMsg::AllDone) => break,
            Some(other) => {
                debug!("[Sender Remote] Ignoring message: {:?}", std::mem::discriminant(&other));
            }
            None => return Err(AppError::CopyError("Transport closed during request/ack phase".into())),
        }
    }
    Ok((transfer_count, success_count, error_count))
}

/// 同一 ndx 的失败只计一次 `error_count`：复合失败（同 ndx 源读失败 **且** dest 侧
/// `resume_prepare` 失败）时，Sender 自检失败（`handle_full_transfer`/
/// `handle_delta_transfer` 返回 `false`）与 Receiver 回传的 `ReceiverMsg::Error{ndx}`
/// 可能各自独立触发一次增量；ndx 唯一对应一个文件，一个文件至多算一次错误。
fn count_ndx_error(errored: &mut HashSet<i32>, ndx: i32, error_count: &mut u64) {
    if errored.insert(ndx) {
        *error_count += 1;
    }
}

/// 把 Receiver 的分类判定翻译为 `StorageEntryMessage`，供 Sender 侧 `StatisticConsumer`
/// 累计统计（与本地单进程管线复用同一套类型/口径，不发明新格式，见 issue #23）。
///
/// - `FullTransfer` → `New`；`MetadataOnly` → `Changed{kind: MetadataOnly}`（与本地口径
///   对齐：本地模式里 MetadataOnly 本来就是 `Changed` 的子类型，不是独立顶层分类）；
///   `Deleted` → `Deleted`。
/// - `DeltaTransfer` → `Changed{kind: DataOnly}`：`DestIndex::check()` 只要
///   `data_check` 不一致就判定 `DeltaTransfer`（不区分是否同时 `metadata_check` 也不
///   一致），Sender 拿不到目标端 entry、无法像本地模式 `ChangeKind::from_entry_diff`
///   那样精确区分 `DataOnly`/`Both`，取"表示内容变更"的那个 variant 作为口径。
/// - `Skip` → `None`：与本地模式完全匹配的条目从不广播一致，不产生任何统计。
fn classification_to_stats_message(decision: TransferDecision, entry: Arc<EntryEnum>) -> Option<StorageEntryMessage> {
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

/// 记录一次成功（ndx 级 `Success` 与 Entry 级 `EntrySuccess` 共用同一份逻辑）：
/// `success_count` 无条件 `+= 1`；`relative_path` 有值时写入 `completed_paths`
/// （`Success{ndx}` 在 `ndx_table` 查不到 entry 时为 `None`，仍计入 `success_count`
/// 但不记录路径，与原逻辑一致）；每满 100 个周期性落盘 checkpoint。
async fn record_success_and_checkpoint(
    relative_path: Option<String>, success_count: &mut u64, completed_paths: &mut HashSet<String>,
    checkpoint_path: &Path,
) {
    *success_count += 1;
    if let Some(path) = relative_path {
        completed_paths.insert(path);
    }
    if success_count.is_multiple_of(100)
        && let Ok(data) = serde_json::to_string(&completed_paths)
    {
        let _ = tokio::fs::write(checkpoint_path, data).await;
    }
}

/// 全量传输一个 entry（目录 / 符号链接 / 文件分块）。
///
/// `ndx` 串入 `FileBegin`/`FileData`/`EndOfFile`，使 Receiver 能把校验结果关联回该 ndx
/// （redo 决策所需，见 `receiver::decide_file_ack`）；也是 `Redo{ndx}` 重发的入口——
/// delta redo 与首次全量传输走同一份实现。
///
/// 返回 `Ok(true)` = 已成功发出该 entry 的数据（目录/符号链接/文件三选一）；
/// `Ok(false)` = 源读失败（符号链接读失败 / 源文件读失败），已发送带 `ndx` 的
/// `SenderMsg::EntryError` 通知 Receiver 完成该 ndx（Receiver 不再回发
/// `ReceiverMsg::Error`），调用方需据此自增 `error_count`（Sender 自检失败由 Sender
/// 自己计数，不依赖 Receiver 回传）。
async fn handle_full_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    ndx: i32, qos: Option<&QosManager>, enable_acl: bool,
) -> Result<bool> {
    let ok = if entry.get_is_dir() {
        transport.send(SenderMsg::CreateDir { entry: entry.clone() }).await?;
        true
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target) => {
                transport
                    .send(SenderMsg::CreateSymlink {
                        entry: entry.clone(),
                        target,
                    })
                    .await?;
                true
            }
            Err(e) => {
                error!("[Sender Remote] read_symlink {:?}: {}", entry.get_relative_path(), e);
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason: format!("{e}"),
                        ndx: Some(ndx),
                    })
                    .await?;
                false
            }
        }
    } else {
        // 流式读源文件：read_chunk_stream 内部按块读 + per-chunk QoS + hash（不再整文件驻留 RAM）
        let (mut rx, hash_handle) = StorageEnum::read_chunk_stream(src_storage, entry, None, qos.cloned(), true, 8);
        transport
            .send(SenderMsg::FileBegin {
                ndx,
                entry: entry.clone(),
            })
            .await?;
        while let Some(chunk) = rx.recv().await {
            transport
                .send(SenderMsg::FileData {
                    entry: entry.clone(),
                    chunk,
                })
                .await?;
        }
        // 读任务收尾：JoinError 或内层读错误统一归一为原因字符串
        let read_result = match hash_handle.await {
            Ok(inner) => inner.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match read_result {
            // 成功：拿到源 hash（整文件 BLAKE3 十六进制）并收尾
            Ok(hasher) => {
                let source_hash = hasher.map(ConsistencyCheck::finalize);
                transport
                    .send(SenderMsg::EndOfFile {
                        ndx,
                        entry: entry.clone(),
                        source_hash,
                    })
                    .await?;
                true
            }
            // 读失败：记录日志 + 通知 Receiver 丢弃该文件已收分片、完成该 ndx（不发 ACL、不中断会话）
            Err(reason) => {
                error!("[Sender Remote] read file {:?}: {}", entry.get_relative_path(), reason);
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason,
                        ndx: Some(ndx),
                    })
                    .await?;
                false
            }
        }
    };
    if ok {
        send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    }
    Ok(ok)
}

/// Delta 传输一个 entry：读取源文件 → 计算 delta tokens → 逐 token 发送。
///
/// 返回 `Ok(true)` = 源文件读取成功并已发送；`Ok(false)` = 源读失败，已发送带 `ndx`
/// 的 `SenderMsg::EntryError` 通知 Receiver 完成该 ndx（不留悬空请求），调用方需据此
/// 自增 `error_count`（与 `handle_full_transfer` 语义一致）。
#[allow(clippy::too_many_arguments)]
async fn handle_delta_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    ndx: i32, block_size: u32, signatures: Vec<BlockSignature>, qos: Option<&QosManager>, enable_acl: bool,
) -> Result<bool> {
    let size = entry.get_size();
    let src_data = match StorageEnum::read_file_from(src_storage, entry, size).await {
        Ok(d) => d,
        Err(e) => {
            error!(
                "[Sender Remote] Failed to read source file for delta NDX {}: {}",
                ndx, e
            );
            transport
                .send(SenderMsg::EntryError {
                    path: entry.get_relative_path().to_path_buf(),
                    reason: format!("{e}"),
                    ndx: Some(ndx),
                })
                .await?;
            return Ok(false);
        }
    };

    let delta_sigs: Vec<sync_delta::BlockSignature> = signatures
        .into_iter()
        .map(|s| sync_delta::BlockSignature {
            rolling: s.rolling,
            strong: s.strong,
        })
        .collect();
    let tokens = sync_delta::matcher::delta_match(&src_data, &delta_sigs, block_size);

    for token in &tokens {
        match token {
            sync_delta::DeltaToken::Match { block_index } => {
                transport
                    .send(SenderMsg::DeltaMatch {
                        ndx,
                        block_index: *block_index,
                    })
                    .await?;
            }
            sync_delta::DeltaToken::Data(data) => {
                if let Some(q) = qos {
                    q.acquire(data.len() as u64).await;
                }
                transport
                    .send(SenderMsg::DeltaData {
                        ndx,
                        data: data.clone(),
                    })
                    .await?;
            }
        }
    }

    let hash = blake3::hash(&src_data).to_hex().to_string();
    transport
        .send(SenderMsg::EndOfFile {
            ndx,
            entry: entry.clone(),
            source_hash: Some(hash),
        })
        .await?;
    info!(
        "[Sender Remote] Delta transfer {:?}: {} tokens",
        entry.get_relative_path(),
        tokens.len()
    );
    send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    Ok(true)
}

/// ACL 跨进程传输：仅在 `enable_acl=true` 且非符号链接时发送。
async fn send_acl_if_enabled(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    enable_acl: bool,
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

// ============================================================
// Checkpoint 辅助
// ============================================================

/// 加载断点续传 checkpoint；文件不存在或解析失败时返回空集合。
async fn load_checkpoint(path: &Path) -> HashSet<String> {
    match tokio::fs::read_to_string(path).await {
        Ok(data) if !data.is_empty() => serde_json::from_str(&data).unwrap_or_else(|e| {
            warn!("[Sender Remote] Checkpoint 解析失败: {}, 将从头开始", e);
            HashSet::new()
        }),
        Ok(_) => HashSet::new(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashSet::new(),
        Err(e) => {
            warn!("[Sender Remote] 读取 checkpoint 失败: {}, 将从头开始", e);
            HashSet::new()
        }
    }
}

/// 全部成功时删除 checkpoint；有错误时保存已完成条目供下次断点续传。
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use data_mover::DataChunk;
    use data_mover::create_storage;
    use data_mover::dir_tree::DirPageResult;
    use tempfile::tempdir;
    use tokio::task::JoinHandle;
    use transport::error::Result as TransportResult;
    use transport::in_process::{InProcessReceiverTransport, InProcessSenderTransport, create_in_process_pair};
    use transport::message::FeatureFlags;
    use transport::traits::ReceiverTransport;

    use super::*;
    use crate::consumer::stats::{JobSummary, ProgressDetail, ProgressReport};
    use crate::receiver::receiver_task_remote;

    /// 构造测试用 `Arc<AsyncMutex<StatisticConsumer>>`：不设 `callback_url`（不起 HTTP
    /// 回调循环），不调用 `begin()`/`end()`（这些测试只关心 `send_file_list_phase`/
    /// `process_requests_and_acks` 本身的传输/ack 行为，不测试统计生命周期）。
    fn test_stats_consumer() -> Arc<AsyncMutex<StatisticConsumer>> {
        Arc::new(AsyncMutex::new(StatisticConsumer {
            stats: StatsKind::Incremental(IncrementalStats::new(
                JobType::IncrementalCopy,
                "test-job".to_string(),
                "test".to_string(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::IncrementalCopy),
            job_dir: String::new(),
            callback_url: None,
            pb_handle: None,
        }))
    }

    // ── finalize_run_result：error_count → 退出码决策纯函数单测 ──

    #[test]
    fn finalize_run_result_ok_when_no_errors() {
        assert!(finalize_run_result(0).is_ok());
    }

    #[test]
    fn finalize_run_result_err_when_errors_present() {
        match finalize_run_result(3) {
            Err(AppError::RemoteSyncFailed { errors: 3 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 3}}, got {other:?}"),
        }
    }

    /// 双端联调（in-process transport，不依赖真实 QUIC）：验证多路复用改造后的
    /// Sender 侧「文件列表发送」与「请求处理+Ack 收集」并发路径、Receiver 侧合并
    /// 后的单一消费者循环，端到端 dest == src，且无消息丢失（success 数与文件+目录
    /// 总数一致、error 数为 0）。
    #[tokio::test]
    async fn sender_receiver_pipeline_roundtrip_in_process() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        // 构造多级目录 + 若干文件，覆盖 CreateDir / 文件全量传输两条路径
        fs::write(src_dir.path().join("a.txt"), b"hello").unwrap();
        fs::create_dir_all(src_dir.path().join("sub")).unwrap();
        fs::write(src_dir.path().join("sub/b.txt"), b"world, nested content").unwrap();
        fs::create_dir_all(src_dir.path().join("sub/deeper")).unwrap();
        fs::write(src_dir.path().join("sub/deeper/c.bin"), vec![7u8; 4096]).unwrap();

        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );

        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();

        let (sender_transport, receiver_transport) = create_in_process_pair();

        // Receiver 侧：调用公开入口，与生产环境走同一条代码路径
        let receiver_handle =
            tokio::spawn(async move { receiver_task_remote(&receiver_transport, dest_storage, None).await });

        // Sender 侧：握手 + 鉴权 + SessionConfig（顺序不变），随后文件列表与请求/ack 并发
        negotiate_handshake(&sender_transport).await.unwrap();
        send_and_check_auth(&sender_transport, None).await.unwrap();
        sender_transport
            .send(SenderMsg::SessionConfig(SessionConfig {
                src_path: src_dir.path().to_string_lossy().to_string(),
                qos: None,
                peak_qos_rate: 1.0,
                iops: None,
                enable_integrity_check: true,
                enable_acl: false,
                is_source_reserved: true,
                block_size: None,
                delete_target: false,
                delta_size_threshold: None,
            }))
            .await
            .unwrap();

        let ndx_table = Mutex::new(NdxTable::new());
        let mut completed_paths = HashSet::new();
        let checkpoint_path = src_dir.path().join("unused_checkpoint.json");
        let stats_consumer = test_stats_consumer();
        let (page_count, (transfer_count, success_count, error_count)) = tokio::try_join!(
            send_file_list_phase(&sender_transport, &walkdir_iter, &ndx_table, &stats_consumer),
            process_requests_and_acks(
                &sender_transport,
                &src_storage,
                &ndx_table,
                None,
                false,
                &mut completed_paths,
                &checkpoint_path,
                &stats_consumer,
            )
        )
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();

        assert!(page_count >= 1, "应至少发送一页文件列表");
        // 子目录（sub, sub/deeper）在文件列表阶段由 Receiver 直接创建，不走 TransferRequest/
        // EntrySuccess（与改造前一致，非本 issue 改动范围）；3 个文件全部走全量传输请求。
        assert_eq!(transfer_count, 3, "应有 3 个文件走全量传输请求");
        assert_eq!(error_count, 0, "不应有任何 EntryError");
        assert_eq!(success_count, 3, "3 个文件应全部收到 EntrySuccess，证明无消息丢失");

        // dest == src：逐文件比对内容
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"hello");
        assert_eq!(
            fs::read(dest_dir.path().join("sub/b.txt")).unwrap(),
            b"world, nested content"
        );
        assert_eq!(
            fs::read(dest_dir.path().join("sub/deeper/c.bin")).unwrap(),
            vec![7u8; 4096]
        );
    }

    // ============================================================
    // ndx 级 redo/ack 状态机集成测试（spec 测试计划 b–f）
    //
    // 用 HashMismatchInjector 包装 Sender 侧 transport，篡改特定 ndx 的
    // `EndOfFile.source_hash`，人为制造 hash mismatch（不改动实际传输数据），
    // 驱动真实的 Receiver 主 task 决策点（decide_file_ack）与 Sender 侧
    // Redo/Success/Error 处理——与生产环境走同一套代码路径，只是用 in-process
    // channel 代替真实 QUIC。
    // ============================================================

    /// 测试专用 Sender transport 包装：按 ndx 篡改 `EndOfFile.source_hash`，
    /// 人为制造 hash mismatch，用于验证 redo 状态机。`corrupt_remaining`：
    /// ndx → 还需损坏几次（每经过一次该 ndx 的 `EndOfFile` 递减，0 = 不再损坏）。
    struct HashMismatchInjector {
        inner: InProcessSenderTransport,
        corrupt_remaining: Mutex<HashMap<i32, u32>>,
    }

    impl HashMismatchInjector {
        fn new(inner: InProcessSenderTransport, corrupt_remaining: HashMap<i32, u32>) -> Self {
            Self {
                inner,
                corrupt_remaining: Mutex::new(corrupt_remaining),
            }
        }
    }

    #[async_trait]
    impl SenderTransport for HashMismatchInjector {
        async fn send(&self, msg: SenderMsg) -> TransportResult<()> {
            let msg = match msg {
                SenderMsg::EndOfFile {
                    ndx,
                    entry,
                    source_hash,
                } => {
                    let mut remaining = self.corrupt_remaining.lock().unwrap_or_else(PoisonError::into_inner);
                    let budget = remaining.entry(ndx).or_insert(0);
                    let source_hash = if *budget > 0 {
                        *budget -= 1;
                        Some("0".repeat(64))
                    } else {
                        source_hash
                    };
                    SenderMsg::EndOfFile {
                        ndx,
                        entry,
                        source_hash,
                    }
                }
                other => other,
            };
            self.inner.send(msg).await
        }

        async fn recv(&self) -> Option<ReceiverMsg> {
            self.inner.recv().await
        }

        async fn close(&self) -> TransportResult<()> {
            self.inner.close().await
        }
    }

    /// 起 Receiver（`receiver_task_remote`）+ 跑 Sender 侧握手/鉴权/`SessionConfig` +
    /// 文件列表与请求/ack 并发处理，返回 `(success_count, error_count)`。
    /// `sender_transport` 可传入包装过的 transport（如 `HashMismatchInjector`），
    /// 复用生产代码路径验证 redo 状态机。
    async fn run_pipeline(
        sender_transport: &(dyn SenderTransport + 'static), receiver_transport: InProcessReceiverTransport,
        src_dir: &std::path::Path, dest_storage: Arc<StorageEnum>, enable_integrity_check: bool,
    ) -> (u64, u64) {
        let src_storage = Arc::new(create_storage(src_dir.to_str().unwrap(), None, false).await.unwrap());
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();

        let receiver_handle =
            tokio::spawn(async move { receiver_task_remote(&receiver_transport, dest_storage, None).await });

        negotiate_handshake(sender_transport).await.unwrap();
        send_and_check_auth(sender_transport, None).await.unwrap();
        sender_transport
            .send(SenderMsg::SessionConfig(SessionConfig {
                src_path: src_dir.to_string_lossy().to_string(),
                qos: None,
                peak_qos_rate: 1.0,
                iops: None,
                enable_integrity_check,
                enable_acl: false,
                is_source_reserved: true,
                block_size: None,
                delete_target: false,
                delta_size_threshold: None,
            }))
            .await
            .unwrap();

        let ndx_table = Mutex::new(NdxTable::new());
        let mut completed_paths = HashSet::new();
        let checkpoint_path = src_dir.join("unused_checkpoint.json");
        let stats_consumer = test_stats_consumer();
        let (_page_count, (_transfer_count, success_count, error_count)) = tokio::try_join!(
            send_file_list_phase(sender_transport, &walkdir_iter, &ndx_table, &stats_consumer),
            process_requests_and_acks(
                sender_transport,
                &src_storage,
                &ndx_table,
                None,
                false,
                &mut completed_paths,
                &checkpoint_path,
                &stats_consumer,
            )
        )
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();
        (success_count, error_count)
    }

    /// 与 `run_pipeline` 类似，但不用 `tokio::try_join!` 并发跑文件列表与请求处理，
    /// 而是先完整跑完 `send_file_list_phase`（此时 `ndx_table` 已确定，size 等元数据
    /// 已从源端读到），再执行 `disrupt`（测试在“文件列表已确定、请求处理还没开始”这个
    /// 确定的时间点做破坏性操作，如删除源文件模拟源读失败），最后再跑
    /// `process_requests_and_acks`。用于确定性地制造 Sender 侧源读失败场景，避免真实
    /// 并发下的时序不确定性（见 issue #22 review [0]/[1]/[2]/[3]）。
    async fn run_pipeline_with_disruption(
        sender_transport: &(dyn SenderTransport + 'static), receiver_transport: InProcessReceiverTransport,
        src_dir: &std::path::Path, dest_storage: Arc<StorageEnum>, disrupt: impl FnOnce(),
    ) -> (u64, u64) {
        let src_storage = Arc::new(create_storage(src_dir.to_str().unwrap(), None, false).await.unwrap());
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();

        let receiver_handle =
            tokio::spawn(async move { receiver_task_remote(&receiver_transport, dest_storage, None).await });

        negotiate_handshake(sender_transport).await.unwrap();
        send_and_check_auth(sender_transport, None).await.unwrap();
        sender_transport
            .send(SenderMsg::SessionConfig(SessionConfig {
                src_path: src_dir.to_string_lossy().to_string(),
                qos: None,
                peak_qos_rate: 1.0,
                iops: None,
                enable_integrity_check: true,
                enable_acl: false,
                is_source_reserved: true,
                block_size: None,
                delete_target: false,
                delta_size_threshold: None,
            }))
            .await
            .unwrap();

        let ndx_table = Mutex::new(NdxTable::new());
        let stats_consumer = test_stats_consumer();
        send_file_list_phase(sender_transport, &walkdir_iter, &ndx_table, &stats_consumer)
            .await
            .unwrap();

        disrupt();

        let mut completed_paths = HashSet::new();
        let checkpoint_path = src_dir.join("unused_checkpoint.json");
        let (_transfer_count, success_count, error_count) = process_requests_and_acks(
            sender_transport,
            &src_storage,
            &ndx_table,
            None,
            false,
            &mut completed_paths,
            &checkpoint_path,
            &stats_consumer,
        )
        .await
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();
        (success_count, error_count)
    }

    /// (b) 全量·一次 mismatch → Sender 收 Redo 全量重发 → Success，`finalize_run_result` Ok。
    #[tokio::test]
    async fn full_transfer_redo_recovers_on_first_hash_mismatch() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"full transfer redo content").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        // 唯一文件的 ndx（root 页首个 entry，可预期为 0）首次 EndOfFile 被篡改一次 hash
        let injector = HashMismatchInjector::new(sender_transport, HashMap::from([(0, 1)]));

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, true).await;

        assert_eq!(error_count, 0, "首次 mismatch 应通过 redo 恢复，不应计入 error");
        assert_eq!(success_count, 1, "唯一文件应最终收到 Success{{ndx}}");
        assert!(finalize_run_result(error_count).is_ok());
        assert_eq!(
            fs::read(dest_dir.path().join("a.txt")).unwrap(),
            b"full transfer redo content"
        );
    }

    /// (c) 全量·连续两次 mismatch → Error，`finalize_run_result` Err（退出路径）。
    #[tokio::test]
    async fn full_transfer_redo_second_mismatch_errors() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"always corrupted content").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        // ndx 0 两次 EndOfFile 都被篡改 → 首次 Redo，二次 Error
        let injector = HashMismatchInjector::new(sender_transport, HashMap::from([(0, 2)]));

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, true).await;

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1, "连续两次 mismatch 应进入 Error 终态");
        match finalize_run_result(error_count) {
            Err(AppError::RemoteSyncFailed { errors: 1 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 1}}, got {other:?}"),
        }
        // .part 已清理，无残留最终文件（全量重发失败，目标端应保持无该文件）
        assert!(!dest_dir.path().join("a.txt").exists());
    }

    // ============================================================
    // size 断言（issue #53）：截断注入测试
    //
    // 与 HashMismatchInjector 篡改 hash 不同，这里截断实际转发的字节，验证 disk_commit.rs
    // ::finalize_file / receiver.rs::handle_end_of_file 新增的 size 断言能独立于 hash
    // 校验拦截截断的提交并触发 Redo。
    // ============================================================

    /// 测试专用 Sender transport 包装：按目标文件相对路径截断该文件传输中命中的首个
    /// `FileData` chunk 为原长度一半，并丢弃本次 attempt 剩余 chunk，同时把
    /// `EndOfFile.source_hash` 改写为对截断后实际转发字节重新计算的自洽 hash——复现
    /// 生产环境"同源失明"场景（截断发生在 hash 计算前，hash 与截断内容自洽，hash 校验
    /// 拦不住），用于验证 size 断言不依赖 hash 独立拦截。`FileData` 不携带 ndx，测试场景
    /// 单文件足够，故按相对路径定位。`truncate_remaining`：还需截断几次（每次目标文件
    /// attempt 递减，0 = 之后透传，让 redo 重发成功），语义同
    /// `HashMismatchInjector::corrupt_remaining`。
    struct SizeTruncationInjector {
        inner: InProcessSenderTransport,
        target_path: PathBuf,
        truncate_remaining: Mutex<u32>,
        /// 本次 attempt 是否已决定截断（`None` = 未决定；首个匹配 chunk 时决定并固定）
        truncating_this_attempt: Mutex<Option<bool>>,
        /// 本次 attempt 是否已发送过截断后的 chunk（发送后剩余 chunk 全部丢弃）
        truncated_once: Mutex<bool>,
        /// 已转发（截断后）的字节，供 `EndOfFile` 重算自洽 hash
        forwarded: Mutex<Vec<u8>>,
    }

    impl SizeTruncationInjector {
        fn new(inner: InProcessSenderTransport, target_path: PathBuf, truncate_remaining: u32) -> Self {
            Self {
                inner,
                target_path,
                truncate_remaining: Mutex::new(truncate_remaining),
                truncating_this_attempt: Mutex::new(None),
                truncated_once: Mutex::new(false),
                forwarded: Mutex::new(Vec::new()),
            }
        }

        /// 本次 attempt 首个匹配 chunk 时决定是否截断（消耗一次预算），之后同一
        /// attempt 内固定；`EndOfFile` 处重置供下次 attempt（redo 重发）重新决定。
        fn is_truncating_this_attempt(&self) -> bool {
            let mut decided = self
                .truncating_this_attempt
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *decided.get_or_insert_with(|| {
                let mut remaining = self.truncate_remaining.lock().unwrap_or_else(PoisonError::into_inner);
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            })
        }
    }

    #[async_trait]
    impl SenderTransport for SizeTruncationInjector {
        async fn send(&self, msg: SenderMsg) -> TransportResult<()> {
            match msg {
                SenderMsg::FileData { entry, chunk } if entry.get_relative_path() == self.target_path.as_path() => {
                    if !self.is_truncating_this_attempt() {
                        return self.inner.send(SenderMsg::FileData { entry, chunk }).await;
                    }
                    let already_truncated = {
                        let mut truncated_once = self.truncated_once.lock().unwrap_or_else(PoisonError::into_inner);
                        std::mem::replace(&mut *truncated_once, true)
                    };
                    if already_truncated {
                        // 已发过截断 chunk：本 attempt 剩余 chunk 全部丢弃
                        return Ok(());
                    }
                    let half = chunk.data.len() / 2;
                    let truncated_data = chunk.data.slice(0..half);
                    self.forwarded
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .extend_from_slice(&truncated_data);
                    self.inner
                        .send(SenderMsg::FileData {
                            entry,
                            chunk: DataChunk {
                                offset: chunk.offset,
                                data: truncated_data,
                            },
                        })
                        .await
                }
                SenderMsg::EndOfFile {
                    ndx,
                    entry,
                    source_hash,
                } if entry.get_relative_path() == self.target_path.as_path() => {
                    let was_truncating = self
                        .truncating_this_attempt
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        == Some(true);
                    *self.truncated_once.lock().unwrap_or_else(PoisonError::into_inner) = false;
                    let source_hash = if was_truncating {
                        let forwarded =
                            std::mem::take(&mut *self.forwarded.lock().unwrap_or_else(PoisonError::into_inner));
                        Some(blake3::hash(&forwarded).to_hex().to_string())
                    } else {
                        source_hash
                    };
                    self.inner
                        .send(SenderMsg::EndOfFile {
                            ndx,
                            entry,
                            source_hash,
                        })
                        .await
                }
                other => self.inner.send(other).await,
            }
        }

        async fn recv(&self) -> Option<ReceiverMsg> {
            self.inner.recv().await
        }

        async fn close(&self) -> TransportResult<()> {
            self.inner.close().await
        }
    }

    /// (g) 全量·size 断言：截断 + 自洽 hash（同源失明复现）→ hash 校验通过但 size 不符
    /// → `SizeMismatch` 触发 Redo → 全量重发恢复成功。验证 size 断言不依赖 hash 独立
    /// 拦截截断的提交。
    #[tokio::test]
    async fn full_transfer_size_mismatch_redo_recovers_with_integrity_check() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"full transfer size truncation content").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let injector = SizeTruncationInjector::new(sender_transport, PathBuf::from("a.txt"), 1);

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, true).await;

        assert_eq!(error_count, 0, "首次 size mismatch 应通过 redo 恢复，不应计入 error");
        assert_eq!(success_count, 1, "唯一文件应最终收到 Success{{ndx}}");
        assert_eq!(
            fs::read(dest_dir.path().join("a.txt")).unwrap(),
            b"full transfer size truncation content"
        );
    }

    /// (h) 全量·size 断言：`enable_integrity_check=false` 时同样生效——hash 校验完全
    /// 跳过，size 断言仍能独立拦截截断并触发 Redo，全量重发恢复成功。
    #[tokio::test]
    async fn full_transfer_size_mismatch_redo_recovers_without_integrity_check() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"full transfer size truncation content").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let injector = SizeTruncationInjector::new(sender_transport, PathBuf::from("a.txt"), 1);

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, false).await;

        assert_eq!(error_count, 0);
        assert_eq!(success_count, 1);
        assert_eq!(
            fs::read(dest_dir.path().join("a.txt")).unwrap(),
            b"full transfer size truncation content"
        );
    }

    /// dest 目录下预置一个与 `name` 同名、同大小但内容不同的文件，并把 mtime 拨到明确
    /// 不同的过去时间点——保证 `DestIndex::check` 命中 `TransferDecision::DeltaTransfer`
    /// （data_check 仅比较 mtime+size，任一不符即判定不匹配），触发 delta 传输路径。
    fn seed_delta_basis(dest_dir: &std::path::Path, name: &str, size: usize) {
        fs::write(dest_dir.join(name), vec![b'B'; size]).unwrap();
        let f = File::open(dest_dir.join(name)).unwrap();
        f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000))
            .unwrap();
    }

    /// (d) delta·一次 mismatch → Redo → Sender 降级全量重发 → Success。
    #[tokio::test]
    async fn delta_transfer_redo_downgrades_to_full_and_recovers() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let injector = HashMismatchInjector::new(sender_transport, HashMap::from([(0, 1)]));

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, true).await;

        assert_eq!(error_count, 0);
        assert_eq!(success_count, 1, "delta redo 降级全量重发后应最终成功");
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), content);
    }

    /// (e) delta·连续两次 mismatch（首次 delta、redo 后降级全量再次 mismatch）→ Error。
    #[tokio::test]
    async fn delta_transfer_redo_second_mismatch_errors() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let injector = HashMismatchInjector::new(sender_transport, HashMap::from([(0, 2)]));

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, true).await;

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1, "delta 降级全量重发后再次 mismatch 应进入 Error 终态");
    }

    /// 测试专用 Sender transport 包装：按目标 ndx 丢弃该次传输的首个 delta token
    /// （`DeltaMatch`/`DeltaData` 之一），模拟 delta 重建路径的截断，覆盖
    /// `receiver.rs::handle_end_of_file` 新增的 size 断言。不改写 hash——配合
    /// `enable_integrity_check=false` 使用，验证 hash 校验关闭时 size 断言仍能独立
    /// 拦截。`drop_remaining`：还需丢弃几次（每次目标 ndx attempt 递减，0 = 之后
    /// 透传），语义同 `HashMismatchInjector::corrupt_remaining`。
    struct DeltaTruncationInjector {
        inner: InProcessSenderTransport,
        target_ndx: i32,
        drop_remaining: Mutex<u32>,
        /// 本次 attempt 是否已丢弃过一个 token（丢一个即可制造截断，其余 token 照常转发）
        dropped_this_attempt: Mutex<bool>,
    }

    impl DeltaTruncationInjector {
        fn new(inner: InProcessSenderTransport, target_ndx: i32, drop_remaining: u32) -> Self {
            Self {
                inner,
                target_ndx,
                drop_remaining: Mutex::new(drop_remaining),
                dropped_this_attempt: Mutex::new(false),
            }
        }

        /// 本次 attempt 遇到的第一个 delta token 是否应丢弃（消耗一次预算），之后同一
        /// attempt 内固定为不再丢。同步函数（无 `.await`），锁可安全跨整个函数体持有。
        fn should_drop(&self) -> bool {
            let mut dropped = self.dropped_this_attempt.lock().unwrap_or_else(PoisonError::into_inner);
            if *dropped {
                return false;
            }
            let mut remaining = self.drop_remaining.lock().unwrap_or_else(PoisonError::into_inner);
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
            *dropped = true;
            true
        }
    }

    #[async_trait]
    impl SenderTransport for DeltaTruncationInjector {
        async fn send(&self, msg: SenderMsg) -> TransportResult<()> {
            match &msg {
                SenderMsg::DeltaMatch { ndx, .. } | SenderMsg::DeltaData { ndx, .. } if *ndx == self.target_ndx => {
                    if self.should_drop() {
                        return Ok(());
                    }
                }
                SenderMsg::EndOfFile { ndx, .. } if *ndx == self.target_ndx => {
                    *self.dropped_this_attempt.lock().unwrap_or_else(PoisonError::into_inner) = false;
                }
                _ => {}
            }
            self.inner.send(msg).await
        }

        async fn recv(&self) -> Option<ReceiverMsg> {
            self.inner.recv().await
        }

        async fn close(&self) -> TransportResult<()> {
            self.inner.close().await
        }
    }

    /// (i) delta·size 断言：`enable_integrity_check=false` 时丢弃一个 delta token 制造
    /// 重建截断 → size 断言拦截（`handle_end_of_file`）→ Redo → 降级全量重发恢复成功。
    #[tokio::test]
    async fn delta_transfer_size_mismatch_redo_recovers_without_integrity_check() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let injector = DeltaTruncationInjector::new(sender_transport, 0, 1);

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, false).await;

        assert_eq!(error_count, 0);
        assert_eq!(success_count, 1, "delta size mismatch redo 降级全量重发后应最终成功");
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), content);
    }

    /// (f) 计数不重复：文件走 `Success{ndx}`、符号链接走 `EntrySuccess`，
    /// 共用同一个 `success_count` 但互不重复计数（无 mismatch 注入的基线场景）。
    #[tokio::test]
    async fn counts_do_not_double_count_across_ndx_and_entry_acks() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"file one").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"file two, slightly longer").unwrap();
        std::os::unix::fs::symlink("a.txt", src_dir.path().join("link")).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();

        let (success_count, error_count) = run_pipeline(
            &sender_transport,
            receiver_transport,
            src_dir.path(),
            dest_storage,
            true,
        )
        .await;

        assert_eq!(error_count, 0);
        assert_eq!(
            success_count, 3,
            "2 个文件 Success{{ndx}} + 1 个符号链接 EntrySuccess，共 3，互不重复计数"
        );
    }

    // ============================================================
    // Sender 自检失败 accounting 回归测试（review 轮次 #48 finding [0]/[1]/[2]/[3]）
    //
    // 用 `run_pipeline_with_disruption` 在“文件列表已确定、请求处理还没开始”这个
    // 确定的时间点删除源文件（或预置目标端阻塞路径），确定性地制造 Sender 侧源读
    // 失败 / 符号链接读失败 / Receiver 侧 resume_prepare 失败，避免真实并发下的时序
    // 不确定性。所有断言「不 hang」的测试都用 tokio::time::timeout 包住，回归时超时
    // 失败而不是把整个测试套 hang 死。
    // ============================================================

    /// [0] 全量源读失败：Sender 自增 error_count → `finalize_run_result` 返回 Err
    /// （使 `main.rs` 的 exit(1) 生效），不再是原先的「仅 completed_count++、exit 0
    /// 且目标端静默缺文件」。
    #[tokio::test]
    async fn full_transfer_source_read_failure_bumps_error_count() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let src_file = src_dir.path().join("a.txt");
        fs::write(&src_file, b"will be deleted before Sender reads it").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();

        let (success_count, error_count) = tokio::time::timeout(
            Duration::from_secs(10),
            run_pipeline_with_disruption(
                &sender_transport,
                receiver_transport,
                src_dir.path(),
                dest_storage,
                || {
                    fs::remove_file(&src_file).unwrap();
                },
            ),
        )
        .await
        .expect("全量源读失败不应导致流水线 hang");

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1, "源读失败应使 Sender 自增 error_count");
        match finalize_run_result(error_count) {
            Err(AppError::RemoteSyncFailed { errors: 1 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 1}}, got {other:?}"),
        }
        assert!(!dest_dir.path().join("a.txt").exists(), "目标端不应静默出现该文件");
    }

    /// [2] delta 源读失败：该 ndx 正常完成（不 hang），Sender 自增 error_count →
    /// `finalize_run_result` 返回 Err。修复前 `handle_delta_transfer` 读失败只
    /// `return Ok(false)`、不发任何消息，Receiver 该 ndx 永不完成，两端 hang。
    #[tokio::test]
    async fn delta_transfer_source_read_failure_completes_ndx_without_hang() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let src_file = src_dir.path().join("a.txt");
        fs::write(&src_file, vec![b'A'; 4096]).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();

        let (success_count, error_count) = tokio::time::timeout(
            Duration::from_secs(10),
            run_pipeline_with_disruption(
                &sender_transport,
                receiver_transport,
                src_dir.path(),
                dest_storage,
                || {
                    fs::remove_file(&src_file).unwrap();
                },
            ),
        )
        .await
        .expect("delta 源读失败不应导致该 ndx 永不完成（hang）");

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1);
        match finalize_run_result(error_count) {
            Err(AppError::RemoteSyncFailed { errors: 1 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 1}}, got {other:?}"),
        }
    }

    /// [3] 符号链接读失败：该 ndx 正常完成（不 hang），Sender 自增 error_count →
    /// `finalize_run_result` 返回 Err。修复前 `read_symlink` 失败只记日志、不发任何
    /// 消息，Receiver 该 ndx 永不完成，两端 hang。
    #[tokio::test]
    async fn symlink_read_failure_completes_ndx_without_hang() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let link_path = src_dir.path().join("link");
        std::os::unix::fs::symlink("target-does-not-matter", &link_path).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();

        let (success_count, error_count) = tokio::time::timeout(
            Duration::from_secs(10),
            run_pipeline_with_disruption(
                &sender_transport,
                receiver_transport,
                src_dir.path(),
                dest_storage,
                || {
                    fs::remove_file(&link_path).unwrap();
                },
            ),
        )
        .await
        .expect("符号链接读失败不应导致该 ndx 永不完成（hang）");

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1);
        match finalize_run_result(error_count) {
            Err(AppError::RemoteSyncFailed { errors: 1 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 1}}, got {other:?}"),
        }
    }

    /// [1] 同一 ndx 上「Sender 源读失败」与「Receiver resume_prepare 失败」并发触发
    /// （预置目标端 `.terrasync-part` 路径为目录，令 dc task 的 `FileBegin` 独立上报
    /// `HardError`；同时删除源文件令 Sender 自己的 chunk 读取也失败）：这是 redo 期间
    /// 两个独立来源可能各自上报同一 ndx 终态/失败的场景。修复前 Receiver 侧
    /// completed_count 会被双计，导致主循环在其余在途文件（healthy.txt）到达前就
    /// 提前 break、丢消息或 hang；Sender 侧 error_count 也会被复合失败双计（一次
    /// 自检 + 一次 Receiver 回传的 Error{ndx}）。修复后两侧都按 ndx 去重：
    /// healthy.txt 正常送达、流水线不 hang，error_count 恰好等于故意失败的文件数。
    #[tokio::test]
    async fn same_ndx_double_failure_does_not_drop_inflight_files() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let broken_file = src_dir.path().join("broken.txt");
        fs::write(&broken_file, b"will be deleted before Sender reads it").unwrap();
        fs::write(src_dir.path().join("healthy.txt"), b"should still arrive").unwrap();
        // 让 broken.txt 的 resume_prepare 必然失败（EISDIR）：预先在目标端占用它的
        // .part 路径为目录。
        fs::create_dir_all(dest_dir.path().join("broken.txt.terrasync-part")).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();

        let (success_count, error_count) = tokio::time::timeout(
            Duration::from_secs(10),
            run_pipeline_with_disruption(
                &sender_transport,
                receiver_transport,
                src_dir.path(),
                dest_storage,
                || {
                    fs::remove_file(&broken_file).unwrap();
                },
            ),
        )
        .await
        .expect("同一 ndx 的双重终态若未去重会导致 completed_count 打不平、主循环 hang");

        assert_eq!(
            success_count, 1,
            "healthy.txt 不应因 broken.txt 的双重终态被提前 break 而丢失"
        );
        // broken.txt 的失败同时被 Sender 自检（源读失败）与 Receiver 回传
        // （resume_prepare 失败的 Error{ndx}）各自独立上报一次；按 ndx 去重后
        // error_count 应恰好等于故意失败的文件数（1），而不是被复合失败计成 2。
        assert_eq!(
            error_count, 1,
            "同一 ndx 的复合失败（源读失败 + resume_prepare 失败）应只计一次 error_count"
        );
        assert_eq!(
            fs::read(dest_dir.path().join("healthy.txt")).unwrap(),
            b"should still arrive"
        );
    }

    /// [5] `enable_integrity_check=false` 时 delta 重建 hash 不符不应触发 redo、
    /// 不应导致非零退出——按原样写入（与全量路径 `disk_commit.rs::finalize_file`
    /// 的门控行为一致）。用 `HashMismatchInjector` 篡改 hash（若未正确门控，这会被
    /// 误判为 mismatch 触发 redo）。
    #[tokio::test]
    async fn delta_hash_mismatch_ignored_when_integrity_check_disabled() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        // 每次 EndOfFile 都篡改 hash（若门控生效，篡改的 hash 应被直接忽略，不触发 redo）
        let injector = HashMismatchInjector::new(sender_transport, HashMap::from([(0, u32::MAX)]));

        let (success_count, error_count) =
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage, false).await;

        assert_eq!(error_count, 0, "关闭完整性校验时不应因篡改的 hash 而失败");
        assert_eq!(success_count, 1, "应直接按原样写入成功，不触发 redo");
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), content);
    }

    // ============================================================
    // Receiver 分类信号正确性（issue #23 测试计划 2）：双端真实 StorageEnum +
    // receiver_task_remote，验证 New/Changed(含 MetadataOnly)/Skip/Deleted 五类判定
    // 各自产生正确的 wire 信号，且执行动作（是否传输/是否本地改元数据/是否删除）与
    // 判定一致。
    // ============================================================

    /// 起 `receiver_task_remote` 并完成握手（`delta_enabled=false` 时构造 delta:false
    /// 的本端能力集触发降级协商，走 `TransferRequest{decision:DeltaTransfer}` 而非
    /// `DeltaTransferRequest`）+ 鉴权 + `SessionConfig`，返回
    /// `(sender_transport, receiver_handle)`。
    async fn spawn_receiver_and_handshake(
        dest_storage: Arc<StorageEnum>, delete_target: bool, delta_enabled: bool,
    ) -> (InProcessSenderTransport, JoinHandle<Result<()>>) {
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let receiver_handle =
            tokio::spawn(async move { receiver_task_remote(&receiver_transport, dest_storage, None).await });

        if delta_enabled {
            negotiate_handshake(&sender_transport).await.unwrap();
        } else {
            let handshake = ProtocolHandshake {
                features: FeatureFlags {
                    delta: false,
                    ..FeatureFlags::current()
                },
                ..ProtocolHandshake::current()
            };
            sender_transport.send(SenderMsg::Handshake(handshake)).await.unwrap();
            match sender_transport.recv().await {
                Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { features })) => {
                    assert!(!features.delta, "delta 应协商为 false");
                }
                other => panic!("expected HandshakeAck(Accepted), got {other:?}"),
            }
        }

        send_and_check_auth(&sender_transport, None).await.unwrap();
        sender_transport
            .send(SenderMsg::SessionConfig(SessionConfig {
                src_path: String::new(),
                qos: None,
                peak_qos_rate: 1.0,
                iops: None,
                enable_integrity_check: true,
                enable_acl: false,
                is_source_reserved: true,
                block_size: None,
                delete_target,
                delta_size_threshold: None,
            }))
            .await
            .unwrap();

        (sender_transport, receiver_handle)
    }

    /// 用真实 `walkdir_2` 遍历 `dir`（测试前提：目录下恰好一个顶层文件）返回第一页 +
    /// 该文件的 ndx。
    async fn single_file_page(dir: &std::path::Path) -> (DirPageResult, i32) {
        let storage = Arc::new(create_storage(dir.to_str().unwrap(), None, false).await.unwrap());
        let walkdir_iter = storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walkdir_iter.next().await {
            Some(NdxEvent::Page(p)) => p,
            other => panic!("expected Page, got {other:?}"),
        };
        assert_eq!(page.files.len(), 1, "测试前提：目录下应恰好一个顶层文件");
        let ndx = page.files[0].ndx;
        (page, ndx)
    }

    /// 构造一个空的根目录页（`dir_path=""`，无文件/子目录），仅用于驱动 orphan-delete
    /// 分支（`DestIndex::orphaned_entries()`），不触发任何 `TransferRequest`。
    fn empty_root_page() -> DirPageResult {
        DirPageResult {
            dir_path: String::new(),
            ndx_start: 0,
            files: vec![],
            subdirs: vec![],
            gap_ndx: -1,
        }
    }

    /// 将 `path` 的 mtime 精确设为 `secs`（Unix 纪元秒）：`data_check` 按 mtime+size
    /// 精确比较，两次独立 `fs::write` 之间哪怕微秒级的写入时间差都会被判定为不匹配，
    /// 必须显式对齐 src/dest 双方的 mtime 才能可靠触发 `MetadataOnly`/`Skip`。
    fn set_mtime(path: &std::path::Path, secs: u64) {
        let f = File::open(path).unwrap();
        f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
            .unwrap();
    }

    /// 跳过间歇性到达的 `Progress` 快照：`receiver_task_remote` 内 `progress_reporter`
    /// 每 5s 上报一次，`tokio::time::interval` 首个 tick 立即触发（tokio 默认行为），
    /// 可能与本测试关心的分类信号交错到达，与断言无关，跳过后返回下一条非 `Progress`
    /// 消息（真正非预期的消息仍会被调用方的 `other => panic!` 分支捕获，不会被此函数
    /// 悄悄吞掉）。
    async fn recv_skip_progress(sender_transport: &InProcessSenderTransport) -> Option<ReceiverMsg> {
        loop {
            match sender_transport.recv().await {
                Some(ReceiverMsg::Progress(_)) => continue,
                other => return other,
            }
        }
    }

    /// 发 `FileListDone`（消费 `RequestsDone`，若此前多出一条非预期消息会在此 panic，
    /// 天然验证"没有额外分类信号"）→ 对 `pending_ndx` 逐个发合成 `EntryError` 完成该
    /// ndx（测试不需要真实文件传输，只关心分类信号本身）→ `TransferDone` → drain 到
    /// `AllDone` → `close` + `join`。
    async fn finish_phase(
        sender_transport: &InProcessSenderTransport, receiver_handle: JoinHandle<Result<()>>, pending_ndx: &[i32],
    ) {
        sender_transport.send(SenderMsg::FileListDone).await.unwrap();
        match recv_skip_progress(sender_transport).await {
            Some(ReceiverMsg::RequestsDone) => {}
            other => panic!("expected RequestsDone, got {other:?}"),
        }
        for &ndx in pending_ndx {
            sender_transport
                .send(SenderMsg::EntryError {
                    path: PathBuf::from("synthetic"),
                    reason: "test synthetic completion".into(),
                    ndx: Some(ndx),
                })
                .await
                .unwrap();
        }
        sender_transport.send(SenderMsg::TransferDone).await.unwrap();
        loop {
            match sender_transport.recv().await {
                Some(ReceiverMsg::AllDone) => break,
                Some(_) => continue,
                None => panic!("transport closed before AllDone"),
            }
        }
        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();
    }

    /// dest 缺失该条目 → 发 `TransferRequest{decision: FullTransfer}`，无 `Classified`
    #[tokio::test]
    async fn recv_file_list_full_transfer_signal() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"new file").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, true).await;

        let (page, ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::TransferRequest {
                ndx: got,
                decision: TransferDecision::FullTransfer,
            }) => assert_eq!(got, ndx),
            other => panic!("expected TransferRequest{{decision:FullTransfer}}, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[ndx]).await;
    }

    /// dest 存在但内容不同 + delta 能力未协商成功 → 降级为整份传输，但 `decision` 仍
    /// 携带 `DeltaTransfer`（wire 动作降级，分类判定不变，供 Sender 侧统计为 Changed）
    #[tokio::test]
    async fn recv_file_list_delta_transfer_downgrade_signal() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, false).await;

        let (page, ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::TransferRequest {
                ndx: got,
                decision: TransferDecision::DeltaTransfer,
            }) => assert_eq!(got, ndx),
            other => panic!("expected TransferRequest{{decision:DeltaTransfer}}（delta 降级）, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[ndx]).await;
    }

    /// dest 存在但内容不同 + delta 协商成功 → 发 `DeltaTransferRequest`（回归：本消息
    /// 本身无歧义地代表 Changed，不需要携带 `decision` 字段）
    #[tokio::test]
    async fn recv_file_list_delta_transfer_negotiated_signal() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = vec![b'A'; 4096];
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();
        seed_delta_basis(dest_dir.path(), "a.txt", 4096);

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, true).await;

        let (page, ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::DeltaTransferRequest { ndx: got, .. }) => assert_eq!(got, ndx),
            other => panic!("expected DeltaTransferRequest, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[ndx]).await;
    }

    /// dest 存在，仅 mode 不同 → 本地 `set_entry_metadata` + 发
    /// `Classified{decision:MetadataOnly}`，不产生任何传输请求
    #[tokio::test]
    async fn recv_file_list_metadata_only_signal() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"same content").unwrap();
        fs::write(dest_dir.path().join("a.txt"), b"same content").unwrap();
        set_mtime(&src_dir.path().join("a.txt"), 1_700_000_000);
        set_mtime(&dest_dir.path().join("a.txt"), 1_700_000_000);
        // 源端 mode 与目标端不同 → metadata_check 不一致，data_check（mtime+size）一致
        fs::set_permissions(src_dir.path().join("a.txt"), fs::Permissions::from_mode(0o640)).unwrap();
        fs::set_permissions(dest_dir.path().join("a.txt"), fs::Permissions::from_mode(0o644)).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, true).await;

        let (page, _ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::Classified {
                entry,
                decision: TransferDecision::MetadataOnly,
            }) => assert_eq!(entry.get_relative_path(), std::path::Path::new("a.txt")),
            other => panic!("expected Classified{{decision:MetadataOnly}}, got {other:?}"),
        }

        // 本地已执行 set_entry_metadata：目标端 mode 应变为源端 mode
        let dest_mode = fs::metadata(dest_dir.path().join("a.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dest_mode, 0o640, "MetadataOnly 应本地更新目标端 mode");

        finish_phase(&sender_transport, receiver_handle, &[]).await;
    }

    /// dest 完全一致（内容+mtime+size+mode）→ 发 `Classified{decision:Skip}`，不产生
    /// 任何传输请求，不产生任何写操作
    #[tokio::test]
    async fn recv_file_list_skip_signal() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"identical").unwrap();
        fs::write(dest_dir.path().join("a.txt"), b"identical").unwrap();
        set_mtime(&src_dir.path().join("a.txt"), 1_700_000_000);
        set_mtime(&dest_dir.path().join("a.txt"), 1_700_000_000);
        fs::set_permissions(src_dir.path().join("a.txt"), fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(dest_dir.path().join("a.txt"), fs::Permissions::from_mode(0o644)).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, true).await;

        let (page, _ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::Classified {
                decision: TransferDecision::Skip,
                ..
            }) => {}
            other => panic!("expected Classified{{decision:Skip}}, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[]).await;
    }

    /// `delete_target=true` + dest 孤儿 → 本地删除 + 发 `Classified{decision:Deleted}`
    #[tokio::test]
    async fn recv_file_list_delete_target_true_emits_classified_deleted() {
        let dest_dir = tempdir().unwrap();
        fs::write(dest_dir.path().join("orphan.txt"), b"stale").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, true, true).await;

        sender_transport
            .send(SenderMsg::FilePage(empty_root_page()))
            .await
            .unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::Classified {
                entry,
                decision: TransferDecision::Deleted,
            }) => assert_eq!(entry.get_relative_path(), std::path::Path::new("orphan.txt")),
            other => panic!("expected Classified{{decision:Deleted}}, got {other:?}"),
        }
        assert!(!dest_dir.path().join("orphan.txt").exists(), "orphan 应已被删除");

        finish_phase(&sender_transport, receiver_handle, &[]).await;
    }

    /// `delete_target=false` → 孤儿不删除，无 `Classified`（`finish_phase` 内部期望
    /// `FileListDone` 之后紧接着就是 `RequestsDone`，若中间插了 `Classified` 会在此
    /// panic，天然验证没有多余的分类信号）
    #[tokio::test]
    async fn recv_file_list_delete_target_false_keeps_orphan_no_classified() {
        let dest_dir = tempdir().unwrap();
        fs::write(dest_dir.path().join("orphan.txt"), b"stale").unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, false, true).await;

        sender_transport
            .send(SenderMsg::FilePage(empty_root_page()))
            .await
            .unwrap();
        finish_phase(&sender_transport, receiver_handle, &[]).await;

        assert!(
            dest_dir.path().join("orphan.txt").exists(),
            "delete_target=false 时孤儿应保留"
        );
    }

    /// dest 预先存在与源端一致的子目录 + `delete_target=true` → 子目录**不是**孤儿：
    /// 不发 `Classified{Deleted}`、不整树删除。回归：`page.subdirs` 走 `create_dir_all`
    /// 路径、不经 `DestIndex::check()`，此前从不登记 `matched`，预存子目录被
    /// `orphaned_entries()` 误判为孤儿 → `delete_dir_all` 整树误删后按 New 重传。
    #[tokio::test]
    async fn recv_file_list_delete_target_preserves_existing_subdir() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        for dir in [src_dir.path(), dest_dir.path()] {
            fs::create_dir_all(dir.join("sub")).unwrap();
            fs::write(dir.join("sub/nested.txt"), b"same content").unwrap();
            set_mtime(&dir.join("sub/nested.txt"), 1_700_000_000);
            fs::set_permissions(dir.join("sub/nested.txt"), fs::Permissions::from_mode(0o644)).unwrap();
        }

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, true, true).await;

        // 真实 walkdir 逐页下发：根页（subdirs=[sub]，files 空）+ 子页（files=[nested.txt]）
        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        while let Some(event) = walkdir_iter.next().await {
            if let NdxEvent::Page(page) = event {
                sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();
            }
        }

        // 唯一预期信号：nested.txt 完全一致 → Classified{Skip}。若子目录被误判孤儿，
        // 这里会先收到 Classified{Deleted}（或整树误删后 nested.txt 的 TransferRequest）
        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::Classified {
                entry,
                decision: TransferDecision::Skip,
            }) => assert_eq!(entry.get_relative_path(), std::path::Path::new("sub/nested.txt")),
            other => panic!("expected Classified{{decision:Skip}} for sub/nested.txt, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[]).await;
        assert!(
            dest_dir.path().join("sub/nested.txt").exists(),
            "预存子目录内容不得被当作孤儿删除"
        );
    }

    /// 删除失败（父目录无写权限）→ 发 `EntryError`，不发 `Classified`
    #[tokio::test]
    async fn recv_file_list_delete_failure_emits_entry_error() {
        let dest_dir = tempdir().unwrap();
        fs::write(dest_dir.path().join("orphan.txt"), b"stale").unwrap();
        // unlink 需要父目录写权限；只留 r-x 制造确定性删除失败（walkdir 列目录仍可用）
        fs::set_permissions(dest_dir.path(), fs::Permissions::from_mode(0o555)).unwrap();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_handle) = spawn_receiver_and_handshake(dest_storage, true, true).await;

        sender_transport
            .send(SenderMsg::FilePage(empty_root_page()))
            .await
            .unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::EntryError { entry, reason }) => {
                assert_eq!(entry.get_relative_path(), std::path::Path::new("orphan.txt"));
                assert!(!reason.is_empty());
            }
            other => panic!("expected EntryError（删除失败）, got {other:?}"),
        }

        // 恢复权限：finish_phase 内部无需再写 dest_dir，但保险起见先复原，避免 tempdir 清理受阻
        fs::set_permissions(dest_dir.path(), fs::Permissions::from_mode(0o755)).unwrap();

        finish_phase(&sender_transport, receiver_handle, &[]).await;
    }

    // ============================================================
    // Sender 侧统计桥接正确性（issue #23 测试计划 3）：分类消息 → `update_statistics`
    // 的翻译函数单元测试 + 报表口径一致性，不需要真实 QUIC。
    // ============================================================

    /// 构造一个仅用于测试的最小 `NASEntry`（字段值无实际语义，只满足统计消息类型要求）
    fn dummy_entry(name: &str, is_dir: bool, size: u64) -> Arc<EntryEnum> {
        Arc::new(EntryEnum::NAS(data_mover::NASEntry {
            name: name.to_string(),
            relative_path: PathBuf::from(name),
            extension: None,
            is_dir,
            size,
            atime: 0,
            ctime: 0,
            mtime: 0,
            mode: 0o644,
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

    #[test]
    fn classification_to_stats_message_maps_full_transfer_to_new() {
        let entry = dummy_entry("a.txt", false, 10);
        match classification_to_stats_message(TransferDecision::FullTransfer, entry) {
            Some(StorageEntryMessage::New(_)) => {}
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn classification_to_stats_message_maps_delta_transfer_to_changed_data_only() {
        let entry = dummy_entry("a.txt", false, 10);
        match classification_to_stats_message(TransferDecision::DeltaTransfer, entry) {
            Some(StorageEntryMessage::Changed {
                kind: ChangeKind::DataOnly,
                ..
            }) => {}
            other => panic!("expected Changed{{kind:DataOnly}}, got {other:?}"),
        }
    }

    #[test]
    fn classification_to_stats_message_maps_metadata_only_to_changed_metadata_only() {
        let entry = dummy_entry("a.txt", false, 10);
        match classification_to_stats_message(TransferDecision::MetadataOnly, entry) {
            Some(StorageEntryMessage::Changed {
                kind: ChangeKind::MetadataOnly,
                ..
            }) => {}
            other => panic!("expected Changed{{kind:MetadataOnly}}, got {other:?}"),
        }
    }

    #[test]
    fn classification_to_stats_message_skip_produces_no_message() {
        let entry = dummy_entry("a.txt", false, 10);
        assert!(classification_to_stats_message(TransferDecision::Skip, entry).is_none());
    }

    #[test]
    fn classification_to_stats_message_maps_deleted_to_deleted() {
        let entry = dummy_entry("a.txt", false, 10);
        match classification_to_stats_message(TransferDecision::Deleted, entry) {
            Some(StorageEntryMessage::Deleted(_)) => {}
            other => panic!("expected Deleted, got {other:?}"),
        }
    }

    /// 五类分类输入分别落到 `IncrementalStats.new`/`changed`（含 MetadataOnly 子类）/
    /// `deleted`，`Skip` 不产生任何计数变化；`renamed` 恒为 0（远端从不产生 Renamed）。
    #[tokio::test]
    async fn stats_bridging_updates_incremental_stats_new_changed_deleted() {
        let stats_consumer = test_stats_consumer();
        let events = [
            (TransferDecision::FullTransfer, dummy_entry("new.txt", false, 100)),
            (TransferDecision::DeltaTransfer, dummy_entry("changed.txt", false, 200)),
            (TransferDecision::MetadataOnly, dummy_entry("meta.txt", false, 50)),
            (TransferDecision::Skip, dummy_entry("skip.txt", false, 999)),
            (TransferDecision::Deleted, dummy_entry("gone.txt", false, 30)),
        ];
        for (decision, entry) in events {
            if let Some(msg) = classification_to_stats_message(decision, entry) {
                stats_consumer.lock().await.update_statistics(&msg);
            }
        }

        let consumer = stats_consumer.lock().await;
        match &consumer.stats {
            StatsKind::Incremental(s) => {
                assert_eq!(s.new.regular_file_count, 1, "FullTransfer 应计入 new");
                assert_eq!(s.new.regular_file_size, 100);
                // DeltaTransfer + MetadataOnly 都落在 changed（与本地口径对齐，
                // MetadataOnly 是 Changed 的子类型，不是独立顶层分类）
                assert_eq!(
                    s.changed.regular_file_count, 2,
                    "DeltaTransfer+MetadataOnly 应合计计入 changed"
                );
                assert_eq!(s.changed.regular_file_size, 250);
                assert_eq!(s.deleted.regular_file_count, 1, "Deleted 应计入 deleted");
                assert_eq!(s.deleted.regular_file_size, 30);
                assert_eq!(s.renamed.regular_file_count, 0, "远端从不产生 Renamed，应恒为 0");
                // Skip 不产生任何 StorageEntryMessage，因此不应体现在 new/changed/deleted 任何一项，
                // 上面三个断言的计数总和（1+2+1=4）已隐含验证：5 个事件中只有 Skip 未被计入。
            }
            other => panic!("expected StatsKind::Incremental, got {other:?}"),
        }
    }

    /// `send_file_list_phase` 遍历 `walkdir_iter` 时随条目数正确累加 `scanned`
    /// （不需要真实 QUIC：只驱动统计喂入，用 in-process transport 承接 `FilePage` 发送）。
    #[tokio::test]
    async fn send_file_list_phase_accumulates_scanned_stats() {
        let src_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"aaa").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"bbbbb").unwrap();
        fs::create_dir_all(src_dir.path().join("sub")).unwrap();
        fs::write(src_dir.path().join("sub/c.txt"), b"c").unwrap();

        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        // 只消费 FilePage/FileListDone，不驱动任何写入，测试只关心 Sender 侧统计累加
        let drain_handle = tokio::spawn(async move { while receiver_transport.recv().await.is_some() {} });

        let ndx_table = Mutex::new(NdxTable::new());
        let stats_consumer = test_stats_consumer();
        let page_count = send_file_list_phase(&sender_transport, &walkdir_iter, &ndx_table, &stats_consumer)
            .await
            .unwrap();
        assert!(page_count >= 1);

        sender_transport.close().await.unwrap();
        drop(drain_handle);

        let consumer = stats_consumer.lock().await;
        match &consumer.stats {
            StatsKind::Incremental(s) => {
                // 2 个顶层文件 + 1 个子目录 + 1 个子目录下文件 = 4 个 Scanned 条目
                assert_eq!(s.scanned.regular_file_count, 3, "3 个文件应全部计入 scanned");
                assert_eq!(s.scanned.dir_count, 1, "1 个子目录应计入 scanned");
            }
            other => panic!("expected StatsKind::Incremental, got {other:?}"),
        }
    }

    /// 结构化报表与本地口径一致性：混合 New/Changed(content)/Changed(MetadataOnly)/
    /// Skip/Deleted 场景后，`to_final_stats()`/`to_job_result()` 产出的字段值与预期
    /// 计数逐一匹配（New/Changed 合计含 MetadataOnly、Deleted、Renamed 恒 0）。
    #[tokio::test]
    async fn structured_report_matches_expected_classification_counts() {
        let stats_consumer = test_stats_consumer();
        let events = [
            (TransferDecision::FullTransfer, dummy_entry("n1.txt", false, 10)),
            (TransferDecision::FullTransfer, dummy_entry("n2.txt", false, 20)),
            (TransferDecision::DeltaTransfer, dummy_entry("c1.txt", false, 30)),
            (TransferDecision::MetadataOnly, dummy_entry("c2.txt", false, 40)),
            (TransferDecision::Skip, dummy_entry("s1.txt", false, 50)),
            (TransferDecision::Deleted, dummy_entry("d1.txt", false, 60)),
        ];
        for (decision, entry) in events {
            if let Some(msg) = classification_to_stats_message(decision, entry) {
                stats_consumer.lock().await.update_statistics(&msg);
            }
        }

        let consumer = stats_consumer.lock().await;
        let final_stats = consumer.stats.to_final_stats();
        let incremental = final_stats.incremental.as_ref().expect("增量报表应带 incremental 字段");
        assert_eq!(incremental.new.regular_file_count, 2, "2 个 FullTransfer 应计入 new");
        assert_eq!(
            incremental.changed.regular_file_count, 2,
            "DeltaTransfer+MetadataOnly 应合计计入 changed"
        );
        assert_eq!(incremental.deleted.regular_file_count, 1);
        assert_eq!(incremental.renamed.regular_file_count, 0, "远端从不产生 Renamed");

        let job_result = consumer.stats.to_job_result();
        match job_result.summary {
            JobSummary::Incremental {
                new,
                changed,
                deleted,
                renamed,
                ..
            } => {
                assert_eq!(new.regular_file_count, 2);
                assert_eq!(changed.regular_file_count, 2);
                assert_eq!(deleted.regular_file_count, 1);
                assert_eq!(renamed.regular_file_count, 0);
            }
            other => panic!("expected JobSummary::Incremental, got {other:?}"),
        }
    }

    // ============================================================
    // callback payload/频率（issue #23 测试计划 6）
    // ============================================================

    /// 起一个最小的本地 HTTP mock server（raw TCP + 手写 HTTP/1.1 帧解析，避免引入新
    /// 依赖），返回 `(base_url,收到的请求体集合)`。
    async fn spawn_mock_callback_server() -> (String, Arc<AsyncMutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bodies: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::new()));
        let bodies_for_server = bodies.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let bodies = bodies_for_server.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
                    let mut reader = BufReader::new(&mut stream);
                    let mut content_length = 0usize;
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        if line == "\r\n" || line == "\n" {
                            break;
                        }
                        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                            content_length = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; content_length];
                    if content_length > 0 && reader.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    bodies.lock().await.push(String::from_utf8_lossy(&body).to_string());
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                });
            }
        });
        (format!("http://{addr}"), bodies)
    }

    /// callback payload 结构 + 频率：`StatisticConsumer` 按 `remote_sync::run()` 的同一套
    /// 构造方式（`JobType::IncrementalCopy`）跑一轮 begin → 喂消息 → 等待跨过一个周期
    /// 回调间隔 → end，断言周期性(非 final) + 恰好一次 final 回调都送达，payload 结构为
    /// `ProgressReport`/`ProgressDetail::Incremental`/`FinalStats`（与本地 `IncrementalCopy`
    /// 任务完全同构——同一 Rust 类型，天然保证）。
    ///
    /// 不经真实 QUIC/`remote_sync::run()`：callback 机制完全活在 `StatisticConsumer` 内部，
    /// 与 transport 无关；`run()` 到这里的唯一接线是一行
    /// `callback_url: config.progress_callback_url.clone()`（类型检查即可保证正确），
    /// 双进程整条链路已由 `tests/remote_process_e2e.rs` + 本文件的分类信号测试覆盖，
    /// 这里只聚焦 callback 本身此前完全没有测试覆盖的行为。
    #[tokio::test]
    async fn statistic_consumer_progress_callback_matches_local_incremental_copy_payload() {
        let (callback_url, bodies) = spawn_mock_callback_server().await;

        let stats_consumer = Arc::new(AsyncMutex::new(StatisticConsumer {
            stats: StatsKind::Incremental(IncrementalStats::new(
                JobType::IncrementalCopy,
                "callback-test".to_string(),
                "test callback".to_string(),
                String::new(),
            )),
            progress_bar: ProgressBar::new(JobType::IncrementalCopy),
            job_dir: String::new(),
            callback_url: Some(callback_url),
            pb_handle: None,
        }));
        let callback_guard = StatisticConsumer::begin(stats_consumer.clone()).await;

        {
            let mut c = stats_consumer.lock().await;
            c.update_statistics(&StorageEntryMessage::New(dummy_entry("a.txt", false, 10)));
            c.update_statistics(&StorageEntryMessage::Changed {
                entry: dummy_entry("b.txt", false, 20),
                kind: ChangeKind::DataOnly,
            });
        }

        // 跨过至少一个周期回调间隔（consumer.rs::CALLBACK_INTERVAL_SECS=2），确保拿到
        // 至少一次非 final 回调（用真实 sleep 而非 QoS 限速，确定性更高、不引入 flaky 风险）
        tokio::time::sleep(Duration::from_millis(2_200)).await;

        StatisticConsumer::end(stats_consumer, callback_guard).await;
        // 给 mock server 处理最后一次请求的时间（写响应发生在 POST 之后短暂延迟内）
        tokio::time::sleep(Duration::from_millis(200)).await;

        let collected = bodies.lock().await;
        let reports: Vec<ProgressReport> = collected
            .iter()
            .filter_map(|b| serde_json::from_str::<ProgressReport>(b).ok())
            .collect();
        assert_eq!(
            reports.len(),
            collected.len(),
            "所有收到的回调 body 都应能反序列化为 ProgressReport"
        );

        let non_final: Vec<_> = reports.iter().filter(|r| !r.is_final).collect();
        let final_reports: Vec<_> = reports.iter().filter(|r| r.is_final).collect();
        assert!(!non_final.is_empty(), "应至少收到一次周期性(非 final)回调");
        assert_eq!(final_reports.len(), 1, "应恰好收到一次 final 回调");
        assert!(final_reports[0].final_stats.is_some(), "final 回调应带 final_stats");

        for report in &reports {
            assert_eq!(report.job_type, "incremental_copy");
            match &report.detail {
                ProgressDetail::Incremental { .. } => {}
                other => panic!("expected ProgressDetail::Incremental, got {other:?}"),
            }
        }
    }
}
