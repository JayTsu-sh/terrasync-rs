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
use std::sync::{Arc, Mutex, PoisonError};

use data_mover::dir_tree::NdxEvent;
use data_mover::filter::parse_filter_expression;
use data_mover::qos::QosManager;
use data_mover::{ConsistencyCheck, StorageEnum, WalkDirAsyncIterator2, create_storage};
use rustls::pki_types::CertificateDer;
use tracing::{debug, error, info, warn};
use transport::error::TransportError;
use transport::message::{
    BlockSignature, HandshakeResult, NdxTable, ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig,
};
use transport::traits::SenderTransport;
use utils::app_config::AppConfig;

use crate::config::SyncJobConfig;
use crate::consumer::stats::format_bytes;
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
            delete_target: false,
        }))
        .await?;

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
    let file_list_fut = send_file_list_phase(&transport, &walkdir_iter, &ndx_table);
    let requests_acks_fut = process_requests_and_acks(
        &transport,
        &src_storage,
        &ndx_table,
        qos_manager.as_ref(),
        config.enable_acl,
        &mut completed_paths,
        &checkpoint_path,
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
async fn send_file_list_phase(
    transport: &(dyn SenderTransport + 'static), walkdir_iter: &WalkDirAsyncIterator2, ndx_table: &Mutex<NdxTable>,
) -> Result<u64> {
    info!("[Sender Remote] Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.next().await {
        match event {
            NdxEvent::Page(page) => {
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
/// 返回 `(transfer_count, success_count, error_count)`。
async fn process_requests_and_acks(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, ndx_table: &Mutex<NdxTable>,
    qos: Option<&QosManager>, enable_acl: bool, completed_paths: &mut HashSet<String>, checkpoint_path: &Path,
) -> Result<(u64, u64, u64)> {
    info!("[Sender Remote] Processing transfer requests + collecting acks");
    let mut transfer_count = 0u64;
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    loop {
        match transport.recv().await {
            Some(ReceiverMsg::TransferRequest { ndx }) => {
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    handle_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await?;
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
                    // delta: 仅 src 读取成功时计入传输数（与原逻辑保持一致）
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
                    }
                } else {
                    error!("[Sender Remote] Unknown NDX {} for delta", ndx);
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
                    handle_full_transfer(transport, src_storage, &entry, ndx, qos, enable_acl).await?;
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
                success_count += 1;
                let entry = ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(ndx)
                    .cloned();
                if let Some(entry) = entry {
                    completed_paths.insert(entry.get_relative_path().to_string_lossy().to_string());
                }
                // 周期性保存 checkpoint
                if success_count.is_multiple_of(100)
                    && let Ok(data) = serde_json::to_string(&completed_paths)
                {
                    let _ = tokio::fs::write(checkpoint_path, data).await;
                }
            }
            Some(ReceiverMsg::EntrySuccess { ref entry }) => {
                success_count += 1;
                completed_paths.insert(entry.get_relative_path().to_string_lossy().to_string());
                // 周期性保存 checkpoint
                if success_count.is_multiple_of(100)
                    && let Ok(data) = serde_json::to_string(&completed_paths)
                {
                    let _ = tokio::fs::write(checkpoint_path, data).await;
                }
            }
            Some(ReceiverMsg::Progress(snapshot)) => {
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
                error_count += 1;
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

/// 全量传输一个 entry（目录 / 符号链接 / 文件分块）。
///
/// `ndx` 串入 `FileBegin`/`FileData`/`EndOfFile`，使 Receiver 能把校验结果关联回该 ndx
/// （redo 决策所需，见 `receiver::decide_file_ack`）；也是 `Redo{ndx}` 重发的入口——
/// delta redo 与首次全量传输走同一份实现。
///
/// 源文件读取失败时仅记录日志，不向 Receiver 发送任何数据（Receiver 不会收到该文件的 Ack）。
async fn handle_full_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    ndx: i32, qos: Option<&QosManager>, enable_acl: bool,
) -> Result<()> {
    if entry.get_is_dir() {
        transport.send(SenderMsg::CreateDir { entry: entry.clone() }).await?;
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target) => {
                transport
                    .send(SenderMsg::CreateSymlink {
                        entry: entry.clone(),
                        target,
                    })
                    .await?;
            }
            Err(e) => {
                error!("[Sender Remote] read_symlink {:?}: {}", entry.get_relative_path(), e);
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
                    ndx,
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
            }
            // 读失败：记录日志 + 通知 Receiver 丢弃该文件已收分片，跳过（不发 ACL、不中断会话）
            Err(reason) => {
                error!("[Sender Remote] read file {:?}: {}", entry.get_relative_path(), reason);
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason,
                    })
                    .await?;
                return Ok(());
            }
        }
    }
    send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    Ok(())
}

/// Delta 传输一个 entry：读取源文件 → 计算 delta tokens → 逐 token 发送。
///
/// 返回 `Ok(true)` = 源文件读取成功并已发送，`Ok(false)` = 读取失败（已记录日志）。
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
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use data_mover::create_storage;
    use tempfile::tempdir;
    use transport::error::Result as TransportResult;
    use transport::in_process::{InProcessReceiverTransport, InProcessSenderTransport, create_in_process_pair};

    use super::*;
    use crate::receiver::receiver_task_remote;

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
            }))
            .await
            .unwrap();

        let ndx_table = Mutex::new(NdxTable::new());
        let mut completed_paths = HashSet::new();
        let checkpoint_path = src_dir.path().join("unused_checkpoint.json");
        let (page_count, (transfer_count, success_count, error_count)) = tokio::try_join!(
            send_file_list_phase(&sender_transport, &walkdir_iter, &ndx_table),
            process_requests_and_acks(
                &sender_transport,
                &src_storage,
                &ndx_table,
                None,
                false,
                &mut completed_paths,
                &checkpoint_path,
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
        src_dir: &std::path::Path, dest_storage: Arc<StorageEnum>,
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
            }))
            .await
            .unwrap();

        let ndx_table = Mutex::new(NdxTable::new());
        let mut completed_paths = HashSet::new();
        let checkpoint_path = src_dir.join("unused_checkpoint.json");
        let (_page_count, (_transfer_count, success_count, error_count)) = tokio::try_join!(
            send_file_list_phase(sender_transport, &walkdir_iter, &ndx_table),
            process_requests_and_acks(
                sender_transport,
                &src_storage,
                &ndx_table,
                None,
                false,
                &mut completed_paths,
                &checkpoint_path,
            )
        )
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
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage).await;

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
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage).await;

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1, "连续两次 mismatch 应进入 Error 终态");
        match finalize_run_result(error_count) {
            Err(AppError::RemoteSyncFailed { errors: 1 }) => {}
            other => panic!("expected RemoteSyncFailed{{errors: 1}}, got {other:?}"),
        }
        // .part 已清理，无残留最终文件（全量重发失败，目标端应保持无该文件）
        assert!(!dest_dir.path().join("a.txt").exists());
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
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage).await;

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
            run_pipeline(&injector, receiver_transport, src_dir.path(), dest_storage).await;

        assert_eq!(success_count, 0);
        assert_eq!(error_count, 1, "delta 降级全量重发后再次 mismatch 应进入 Error 终态");
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

        let (success_count, error_count) =
            run_pipeline(&sender_transport, receiver_transport, src_dir.path(), dest_storage).await;

        assert_eq!(error_count, 0);
        assert_eq!(
            success_count, 3,
            "2 个文件 Success{{ndx}} + 1 个符号链接 EntrySuccess，共 3，互不重复计数"
        );
    }
}
