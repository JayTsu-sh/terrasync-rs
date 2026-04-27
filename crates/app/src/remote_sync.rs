//! 双进程模式远端同步（Sender 侧）
//!
//! 将 QUIC 连接、文件列表发送、传输请求处理、Ack 收集等阶段
//! 提取为独立函数，降低单函数复杂度并提升可读性。

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use data_mover::dir_tree::NdxEvent;
use data_mover::filter::parse_filter_expression;
use data_mover::qos::QosManager;
use data_mover::{DataChunk, StorageEnum, WalkDirAsyncIterator2, create_storage};
use rustls::pki_types::CertificateDer;
use tracing::{debug, error, info, warn};
use transport::message::{BlockSignature, NdxTable, ReceiverMsg, SenderMsg, SessionConfig};
use transport::traits::SenderTransport;
use utils::app_config::AppConfig;

use crate::config::SyncJobConfig;
use crate::consumer::stats::format_bytes;
use crate::error::{AppError, Result};
use crate::orchestrator::create_qos_manager;
use crate::sync::parse_size;

/// 文件分块传输大小（4 MiB）
const FILE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// 双进程全量同步 — Sender 侧入口
///
/// 依次执行：QUIC 连接 → `SessionConfig` → Phase 1 文件列表 → Phase 2 请求处理 → Phase 3 Ack。
pub(crate) async fn run(config: &SyncJobConfig, remote_addr: &str, tls_cert_bytes: Option<&[u8]>) -> Result<()> {
    info!("[Sender Remote] Connecting to Receiver at {}", remote_addr);

    // ── 1. 连接 QUIC ──
    let addr: SocketAddr = remote_addr
        .parse()
        .map_err(|e| AppError::CopyError(format!("Invalid remote address '{remote_addr}': {e}")))?;
    let server_cert = tls_cert_bytes.map(|b| CertificateDer::from(b.to_vec()));
    let transport = transport::quic::connect(addr, "localhost", server_cert).await?;
    info!("[Sender Remote] Connected");

    // ── 2. 发送 SessionConfig ──
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

    // ── 3. 创建源端 storage + walkdir_2 ──
    let block_size = match &config.block_size {
        Some(s) => Some(parse_size(s)?),
        None => None,
    };
    let src_storage = Arc::new(create_storage(&config.src_path, block_size.map(|s| s as u64)).await?);
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

    // ── 4. QoS 管理器 + checkpoint 加载 ──
    let qos_manager = create_qos_manager(config.qos.as_ref(), config.peak_qos_rate, config.iops);
    let checkpoint_path = std::path::PathBuf::from(&config.job_dir).join("remote_checkpoint.json");
    let mut completed_paths = load_checkpoint(&checkpoint_path).await;
    if !completed_paths.is_empty() {
        info!(
            "[Sender Remote] Loaded checkpoint: {} entries already completed",
            completed_paths.len()
        );
    }

    // ── 5. Phase 1: 发送文件列表 ──
    let mut ndx_table = NdxTable::new();
    let page_count = send_file_list_phase(&transport, &walkdir_iter, &mut ndx_table).await?;
    info!(
        "[Sender Remote] File list sent: {} pages, {} entries",
        page_count,
        ndx_table.len()
    );

    // ── 6. Phase 2: 处理传输请求 ──
    let transfer_count = process_requests(
        &transport,
        &src_storage,
        &ndx_table,
        qos_manager.as_ref(),
        config.enable_acl,
    )
    .await?;
    transport.send(SenderMsg::TransferDone).await?;
    info!("[Sender Remote] Phase 2 done, {} transfer requests", transfer_count);

    // ── 7. Phase 3: 等待 Ack ──
    let (success_count, error_count) = process_acks(&transport, &mut completed_paths, &checkpoint_path).await?;
    info!(
        "[Sender Remote] Complete: {} success, {} errors",
        success_count, error_count
    );

    // ── 8. Checkpoint 处理 + 清理 ──
    save_or_clear_checkpoint(&checkpoint_path, &completed_paths, error_count).await;
    if let Some(ref qos) = qos_manager {
        qos.shutdown();
    }
    transport.close().await?;
    Ok(())
}

// ============================================================
// Phase 1: 文件列表发送
// ============================================================

/// 遍历 `walkdir_2` 并按页发送给 Receiver，填充 `ndx_table`，返回发送的页数。
async fn send_file_list_phase(
    transport: &(dyn SenderTransport + 'static), walkdir_iter: &WalkDirAsyncIterator2, ndx_table: &mut NdxTable,
) -> Result<u64> {
    info!("[Sender Remote] Phase 1: Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.next().await {
        match event {
            NdxEvent::Page(page) => {
                ndx_table.ingest_page(&page);
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
// Phase 2: 传输请求处理
// ============================================================

/// 接收 Receiver 的 `TransferRequest` / `DeltaTransferRequest`，发送对应数据流。
/// 返回实际处理的传输请求数。
async fn process_requests(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, ndx_table: &NdxTable,
    qos: Option<&QosManager>, enable_acl: bool,
) -> Result<u64> {
    info!("[Sender Remote] Phase 2: Processing transfer requests");
    let mut transfer_count = 0u64;
    loop {
        match transport.recv().await {
            Some(ReceiverMsg::TransferRequest { ndx }) => {
                if let Some(entry) = ndx_table.get(ndx) {
                    handle_full_transfer(transport, src_storage, entry, qos, enable_acl).await?;
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
                if let Some(entry) = ndx_table.get(ndx) {
                    // delta: 仅 src 读取成功时计入传输数（与原逻辑保持一致）
                    if handle_delta_transfer(
                        transport,
                        src_storage,
                        entry,
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
            Some(ReceiverMsg::RequestsDone) => {
                info!(
                    "[Sender Remote] All requests received, {} files to transfer",
                    transfer_count
                );
                break;
            }
            Some(other) => {
                debug!("[Sender Remote] Ignoring message: {:?}", std::mem::discriminant(&other));
            }
            None => return Err(AppError::CopyError("Transport closed during request phase".into())),
        }
    }
    Ok(transfer_count)
}

/// 全量传输一个 entry（目录 / 符号链接 / 文件分块）。
///
/// 源文件读取失败时仅记录日志，不向 Receiver 发送任何数据（Receiver 不会收到该文件的 Ack）。
async fn handle_full_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<data_mover::EntryEnum>,
    qos: Option<&QosManager>, enable_acl: bool,
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
        let size = entry.get_size();
        match StorageEnum::read_file_from(src_storage, entry, size).await {
            Ok(data) => {
                let hash = blake3::hash(&data).to_hex().to_string();
                transport.send(SenderMsg::FileBegin { entry: entry.clone() }).await?;
                let mut offset = 0usize;
                while offset < data.len() {
                    let end = (offset + FILE_CHUNK_SIZE).min(data.len());
                    let chunk = data.slice(offset..end);
                    if let Some(q) = qos {
                        q.acquire(chunk.len() as u64).await;
                    }
                    transport
                        .send(SenderMsg::FileData {
                            entry: entry.clone(),
                            chunk: DataChunk {
                                offset: offset as u64,
                                data: chunk,
                            },
                        })
                        .await?;
                    offset = end;
                }
                transport
                    .send(SenderMsg::EndOfFile {
                        entry: entry.clone(),
                        source_hash: Some(hash),
                    })
                    .await?;
            }
            Err(e) => {
                error!("[Sender Remote] read file {:?}: {}", entry.get_relative_path(), e);
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
// Phase 3: Ack 收集
// ============================================================

/// 接收 Receiver 侧的 `EntrySuccess` / `EntryError` / `Progress`，直到 `AllDone`。
/// 返回 `(success_count, error_count)`。
async fn process_acks(
    transport: &(dyn SenderTransport + 'static), completed_paths: &mut HashSet<String>, checkpoint_path: &Path,
) -> Result<(u64, u64)> {
    info!("[Sender Remote] Phase 3: Waiting for acks");
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    loop {
        match transport.recv().await {
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
            Some(ReceiverMsg::AllDone) => break,
            Some(_) => {}
            None => return Err(AppError::CopyError("Transport closed during ack phase".into())),
        }
    }
    Ok((success_count, error_count))
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
