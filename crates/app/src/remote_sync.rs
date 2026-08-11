//! 双进程模式远端同步（Sender 侧）
//!
//! 将 QUIC 连接、文件列表发送、传输请求处理、Ack 收集等阶段
//! 提取为独立函数，降低单函数复杂度并提升可读性。
//!
//! 握手/鉴权/`SessionConfig` 之后，完整生命周期由 `RemoteSenderSession` 持有：
//! advertising 与 request/ack consumer 并发运行，避免文件列表 barrier；唯一的
//! `recv()` consumer、index correlation、source transfer、reporting、checkpoint 和
//! terminal protocol 都隐藏在 session interface 后面。

mod remote_sender_session;

use remote_sender_session::{RemoteSenderSession, RemoteSenderSessionDeps};
#[cfg(test)]
use remote_sender_session::{classification_to_stats_message, entry_error_stats_message};

use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, PoisonError};

#[cfg(test)]
use data_mover::StorageEnum;
use data_mover::create_storage;
#[cfg(test)]
use data_mover::dir_tree::NdxEvent;
use data_mover::filter::parse_filter_expression;
#[cfg(test)]
use data_mover::{ChangeKind, EntryEnum, ErrorEvent, StorageEntryMessage};
use rustls::pki_types::CertificateDer;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{info, warn};
use transport::error::TransportError;
#[cfg(test)]
use transport::message::DestIndex;
#[cfg(test)]
use transport::message::TransferDecision;
use transport::message::{HandshakeResult, ProtocolHandshake, ReceiverMsg, SenderMsg, SessionConfig};
use transport::traits::SenderTransport;
use utils::app_config::AppConfig;
use utils::logger;

use crate::config::{JobType, SyncJobConfig};
use crate::consumer::stats::{IncrementalStats, ProgressBar, StatisticConsumer, StatsKind};
use crate::error::{AppError, Result};
use crate::orchestrator::create_qos_manager;
#[cfg(test)]
use crate::remote_receiver_session::RemoteSessionState;
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

    // ── 5. QoS 管理器；checkpoint lifecycle 由 negotiated session 持有 ──
    let qos_manager = create_qos_manager(config.qos.as_ref(), config.peak_qos_rate, config.iops);
    let checkpoint_path = std::path::PathBuf::from(&config.job_dir).join("remote_checkpoint.json");

    // ── 6+7+8. 文件列表发送 与 请求处理+Ack收集 并发运行（流水线，无阶段 barrier） ──
    let summary = RemoteSenderSession::new(RemoteSenderSessionDeps {
        transport: &transport,
        src_storage: &src_storage,
        walkdir_iter: &walkdir_iter,
        qos: qos_manager.as_ref(),
        enable_acl: config.enable_acl,
        checkpoint_path: &checkpoint_path,
        stats_consumer: &stats_consumer,
    })
    .run()
    .await?;
    info!(
        "[Sender Remote] File list sent: {} pages, {} entries",
        summary.page_count, summary.advertised_entries
    );
    info!("[Sender Remote] {} transfer requests processed", summary.transfer_count);
    info!(
        "[Sender Remote] Complete: {} success, {} errors",
        summary.success_count, summary.error_count
    );

    // ── 9. 资源清理 ──
    if let Some(ref qos) = qos_manager {
        qos.shutdown();
    }
    transport.close().await?;
    // 结束统计报表生命周期：abort 回调循环 → 发最终回调(is_final=true) → 打印终态报表
    StatisticConsumer::end(stats_consumer, callback_guard).await;
    finalize_run_result(summary.error_count)
}

/// 双进程模式下 entry 级失败（部分或全部）不再影响进程退出码——与单进程语义对齐，
/// 改由终态报表 ERROR STATISTICS 与 HTTP 回调 `error_count` 承载（报表驱动，见 issue
/// #57 spec v3）。保留该函数与 `error_count>0` 时的日志，调用方（`run()`）结构不变。
///
/// ndx 级 redo 二次失败（`ReceiverMsg::Error`）与 Entry 级失败（`EntryError`）都计入
/// `error_count`，任一存在都会记录日志，但不再使 `main.rs` 的 `exit(1)` 生效。
fn finalize_run_result(error_count: u64) -> Result<()> {
    if error_count > 0 {
        warn!(
            "[Sender Remote] {} entry-level error(s) occurred; see ERROR STATISTICS report (exit code unaffected)",
            error_count
        );
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
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

    // ── finalize_run_result：报表驱动，恒 Ok（issue #57 spec v3：双进程 entry 级失败不再
    //    影响退出码，仅记录日志）──

    #[test]
    fn finalize_run_result_ok_when_no_errors() {
        assert!(finalize_run_result(0).is_ok());
    }

    #[test]
    fn finalize_run_result_ok_even_when_errors_present() {
        assert!(
            finalize_run_result(3).is_ok(),
            "entry 级失败不应再使 finalize_run_result 返回 Err（报表驱动，见 issue #57）"
        );
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

        let checkpoint_path = dest_dir.path().join("unused_checkpoint.json");
        fs::write(&checkpoint_path, r#"["previously-completed.txt"]"#).unwrap();
        let stats_consumer = test_stats_consumer();
        let summary = RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: &sender_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();

        assert!(summary.page_count >= 1, "应至少发送一页文件列表");
        // 子目录（sub, sub/deeper）在文件列表阶段由 Receiver 直接创建，不走 TransferRequest/
        // EntrySuccess（与改造前一致，非本 issue 改动范围）；3 个文件全部走全量传输请求。
        assert_eq!(summary.transfer_count, 3, "应有 3 个文件走全量传输请求");
        assert_eq!(summary.error_count, 0, "不应有任何 EntryError");
        assert!(!checkpoint_path.exists(), "成功会话应清理已有 checkpoint");
        assert_eq!(
            summary.success_count, 3,
            "3 个文件应全部收到 EntrySuccess，证明无消息丢失"
        );

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

    #[tokio::test]
    async fn negotiated_sender_session_reports_typed_failure_when_peer_closes_before_all_done() {
        let src_dir = tempdir().unwrap();
        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 1, false).await.unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let peer = tokio::spawn(async move {
            while let Some(message) = receiver_transport.recv().await {
                if matches!(message, SenderMsg::FileListDone) {
                    break;
                }
            }
        });
        let stats_consumer = test_stats_consumer();
        let checkpoint_path = src_dir.path().join("unused_checkpoint.json");
        let error = RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: &sender_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap_err();
        peer.await.unwrap();

        assert!(matches!(
            error,
            AppError::SenderSessionStage {
                stage: "requests/acks",
                source,
            } if matches!(*source, AppError::CopyError(_))
        ));
    }

    #[tokio::test]
    async fn negotiated_sender_session_sends_transfer_done_once_for_duplicate_requests_done() {
        let src_dir = tempdir().unwrap();
        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 1, false).await.unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let peer = tokio::spawn(async move {
            while let Some(message) = receiver_transport.recv().await {
                if matches!(message, SenderMsg::FileListDone) {
                    break;
                }
            }
            receiver_transport.send(ReceiverMsg::RequestsDone).await.unwrap();
            receiver_transport.send(ReceiverMsg::RequestsDone).await.unwrap();
            assert!(matches!(receiver_transport.recv().await, Some(SenderMsg::TransferDone)));
            assert!(
                tokio::time::timeout(Duration::from_millis(50), receiver_transport.recv())
                    .await
                    .is_err(),
                "duplicate RequestsDone must not emit duplicate TransferDone"
            );
            receiver_transport.send(ReceiverMsg::AllDone).await.unwrap();
        });
        let checkpoint_path = src_dir.path().join("unused_checkpoint.json");
        let stats_consumer = test_stats_consumer();

        RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: &sender_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap();
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn negotiated_sender_session_rejects_all_done_before_transfer_done() {
        let src_dir = tempdir().unwrap();
        let src_storage = Arc::new(
            create_storage(src_dir.path().to_str().unwrap(), None, false)
                .await
                .unwrap(),
        );
        let walkdir_iter = src_storage.walkdir_2(None, None, None, None, 1, false).await.unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let peer = tokio::spawn(async move {
            while let Some(message) = receiver_transport.recv().await {
                if matches!(message, SenderMsg::FileListDone) {
                    break;
                }
            }
            receiver_transport.send(ReceiverMsg::AllDone).await.unwrap();
        });
        let checkpoint_path = src_dir.path().join("unused_checkpoint.json");
        let stats_consumer = test_stats_consumer();

        let error = RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: &sender_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap_err();
        peer.await.unwrap();

        assert!(matches!(
            error,
            AppError::SenderSessionStage {
                stage: "requests/acks",
                source,
            } if matches!(*source, AppError::CopyError(ref reason) if reason.contains("AllDone"))
        ));
    }

    // ============================================================
    // ndx 级 redo/ack 状态机集成测试（spec 测试计划 b–f）
    //
    // 用 HashMismatchInjector 包装 Sender 侧 transport，篡改特定 ndx 的
    // `EndOfFile.source_hash`，人为制造 hash mismatch（不改动实际传输数据），
    // 驱动真实的 Receiver session outcome policy 与 Sender 侧
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

    /// 在首个实际 transfer request 交给 Sender session 前执行一次故障注入。
    /// 这是测试 transport adapter，不暴露 session 私有阶段，也不恢复旧 phase seam。
    struct DisruptOnFirstRequest<'a> {
        inner: &'a dyn SenderTransport,
        disrupt: Mutex<Option<Box<dyn FnOnce() + Send + 'a>>>,
    }

    #[async_trait]
    impl SenderTransport for DisruptOnFirstRequest<'_> {
        async fn send(&self, msg: SenderMsg) -> TransportResult<()> {
            self.inner.send(msg).await
        }

        async fn recv(&self) -> Option<ReceiverMsg> {
            let message = self.inner.recv().await;
            if matches!(
                message,
                Some(ReceiverMsg::TransferRequest { .. } | ReceiverMsg::DeltaTransferRequest { .. })
            ) && let Some(disrupt) = self.disrupt.lock().unwrap_or_else(PoisonError::into_inner).take()
            {
                disrupt();
            }
            message
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

        let checkpoint_path = src_dir.join("unused_checkpoint.json");
        let stats_consumer = test_stats_consumer();
        let summary = RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: sender_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();
        (summary.success_count, summary.error_count)
    }

    /// 与 `run_pipeline` 类似，但不用 `tokio::try_join!` 并发跑文件列表与请求处理，
    /// 而是先完整跑完 `send_file_list_phase`（此时 `ndx_table` 已确定，size 等元数据
    /// 已从源端读到），再执行 `disrupt`（测试在“文件列表已确定、请求处理还没开始”这个
    /// 确定的时间点做破坏性操作，如删除源文件模拟源读失败），最后再跑
    /// `process_requests_and_acks`。用于确定性地制造 Sender 侧源读失败场景，避免真实
    /// 并发下的时序不确定性（见 issue #22 review [0]/[1]/[2]/[3]）。
    ///
    /// 返回值带上 `stats_consumer`（issue #57）：供调用方断言 Sender 自检失败是否已
    /// 正确喂入 `ErrorStats`（报表口径），而不只是本地 `error_count` u64。
    async fn run_pipeline_with_disruption<'a>(
        sender_transport: &'a (dyn SenderTransport + 'static), receiver_transport: InProcessReceiverTransport,
        src_dir: &std::path::Path, dest_storage: Arc<StorageEnum>, disrupt: impl FnOnce() + Send + 'a,
    ) -> (Arc<AsyncMutex<StatisticConsumer>>, u64, u64) {
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

        let stats_consumer = test_stats_consumer();
        let checkpoint_path = src_dir.join("unused_checkpoint.json");
        let disrupting_transport = DisruptOnFirstRequest {
            inner: sender_transport,
            disrupt: Mutex::new(Some(Box::new(disrupt))),
        };
        let summary = RemoteSenderSession::new(RemoteSenderSessionDeps {
            transport: &disrupting_transport,
            src_storage: &src_storage,
            walkdir_iter: &walkdir_iter,
            qos: None,
            enable_acl: false,
            checkpoint_path: &checkpoint_path,
            stats_consumer: &stats_consumer,
        })
        .run()
        .await
        .unwrap();

        sender_transport.close().await.unwrap();
        receiver_handle.await.unwrap().unwrap();
        (stats_consumer, summary.success_count, summary.error_count)
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

    /// (c) 全量·连续两次 mismatch → Error 终态（`error_count` 计入，但 `finalize_run_result`
    /// 报表驱动恒 Ok，不再是退出路径，见 issue #57）。
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
        assert!(
            src_dir.path().join("unused_checkpoint.json").exists(),
            "部分失败应保留 checkpoint"
        );
        assert!(
            finalize_run_result(error_count).is_ok(),
            "entry 级失败不应再使 finalize_run_result 返回 Err"
        );
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

    /// [0] 全量源读失败：Sender 自增 error_count，且喂入 `ErrorStats`（issue #57：报表
    /// 驱动，`finalize_run_result` 恒 Ok，失败改由终态报表承载），不再是原先的
    /// 「仅 completed_count++、exit 0 且目标端静默缺文件」。
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

        let (stats_consumer, success_count, error_count) = tokio::time::timeout(
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
        assert!(
            finalize_run_result(error_count).is_ok(),
            "entry 级失败不应再使 finalize_run_result 返回 Err"
        );
        assert!(!dest_dir.path().join("a.txt").exists(), "目标端不应静默出现该文件");

        let consumer = stats_consumer.lock().await;
        match &consumer.stats {
            StatsKind::Incremental(s) => {
                assert_eq!(
                    s.error_stats.copy, 1,
                    "Sender 自检读失败应喂入 ErrorStats.copy（issue #57），报表才能如实反映失败"
                );
            }
            other => panic!("expected StatsKind::Incremental, got {other:?}"),
        }
    }

    /// [2] delta 源读失败：该 ndx 正常完成（不 hang），Sender 自增 error_count 并喂入
    /// `ErrorStats`（issue #57）。修复前 `handle_delta_transfer` 读失败只
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

        let (stats_consumer, success_count, error_count) = tokio::time::timeout(
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
        assert!(
            finalize_run_result(error_count).is_ok(),
            "entry 级失败不应再使 finalize_run_result 返回 Err"
        );

        let consumer = stats_consumer.lock().await;
        match &consumer.stats {
            StatsKind::Incremental(s) => {
                assert_eq!(
                    s.error_stats.copy, 1,
                    "delta 自检读失败应喂入 ErrorStats.copy（issue #57）"
                );
            }
            other => panic!("expected StatsKind::Incremental, got {other:?}"),
        }
    }

    /// [3] 符号链接读失败：该 ndx 正常完成（不 hang），Sender 自增 error_count。修复前
    /// `read_symlink` 失败只记日志、不发任何消息，Receiver 该 ndx 永不完成，两端 hang。
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

        let (_stats_consumer, success_count, error_count) = tokio::time::timeout(
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
        assert!(
            finalize_run_result(error_count).is_ok(),
            "entry 级失败不应再使 finalize_run_result 返回 Err"
        );
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

        let (stats_consumer, success_count, error_count) = tokio::time::timeout(
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

        // 报表口径（issue #57）应与本地 error_count 去重结果一致：同一 ndx 的复合失败
        // 只应喂入一次 ErrorStats，不应因两个独立触发源（Sender 自检 + Receiver 回传）
        // 各喂一次而被重复计入 2。
        let consumer = stats_consumer.lock().await;
        match &consumer.stats {
            StatsKind::Incremental(s) => {
                assert_eq!(
                    s.error_stats.total(),
                    1,
                    "同一 ndx 的复合失败应只喂入一次 ErrorStats（与 error_count 去重口径一致）"
                );
            }
            other => panic!("expected StatsKind::Incremental, got {other:?}"),
        }
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
    /// `(sender_transport, receiver_handle)`。`delta_size_threshold` 恒为 `None`
    /// （receiver 侧使用默认 512MiB），需要 override 见
    /// `spawn_receiver_and_handshake_with_threshold`。
    async fn spawn_receiver_and_handshake(
        dest_storage: Arc<StorageEnum>, delete_target: bool, delta_enabled: bool,
    ) -> (InProcessSenderTransport, JoinHandle<Result<()>>) {
        spawn_receiver_and_handshake_with_threshold(dest_storage, delete_target, delta_enabled, None).await
    }

    /// 同 `spawn_receiver_and_handshake`，另可指定 `delta_size_threshold`（透传给
    /// `SessionConfig`，供 issue #54 阶段 0 的 size 门槛降级测试使用）。
    async fn spawn_receiver_and_handshake_with_threshold(
        dest_storage: Arc<StorageEnum>, delete_target: bool, delta_enabled: bool, delta_size_threshold: Option<String>,
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
                delta_size_threshold,
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

    /// dest 存在但内容不同 + delta 协商成功，但文件 size 超过 `delta_size_threshold` 门槛
    /// → 降级为整份传输，`decision` 仍携带 `DeltaTransfer`（同
    /// `recv_file_list_delta_transfer_downgrade_signal`，触发条件不同：这里是 size 门槛而非
    /// 能力协商失败，见 issue #54 阶段 0）
    #[tokio::test]
    async fn recv_file_list_delta_transfer_size_threshold_downgrade_signal() {
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
        // 阈值 1KiB < 文件 4096 字节 → 应触发 size 门槛降级
        let (sender_transport, receiver_handle) =
            spawn_receiver_and_handshake_with_threshold(dest_storage, false, true, Some("1KiB".to_string())).await;

        let (page, ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::TransferRequest {
                ndx: got,
                decision: TransferDecision::DeltaTransfer,
            }) => assert_eq!(got, ndx),
            other => panic!("expected TransferRequest{{decision:DeltaTransfer}}（size 门槛降级）, got {other:?}"),
        }

        finish_phase(&sender_transport, receiver_handle, &[ndx]).await;
    }

    /// dest 存在但内容不同 + delta 协商成功 + 文件 size 未超过 `delta_size_threshold` 门槛
    /// → 正常走 `DeltaTransferRequest`，不受 override 阈值影响（回归：低于阈值的文件不应被
    /// 误伤，见 issue #54 阶段 0）
    #[tokio::test]
    async fn recv_file_list_delta_transfer_within_size_threshold_signal() {
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
        // 阈值 1MiB > 文件 4096 字节 → 不应触发降级
        let (sender_transport, receiver_handle) =
            spawn_receiver_and_handshake_with_threshold(dest_storage, false, true, Some("1MiB".to_string())).await;

        let (page, ndx) = single_file_page(src_dir.path()).await;
        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::DeltaTransferRequest { ndx: got, .. }) => assert_eq!(got, ndx),
            other => panic!("expected DeltaTransferRequest（阈值内不受影响）, got {other:?}"),
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

    /// 删除失败（storage root 在索引后变成普通文件）→ 发 `EntryError`，不发 `Classified`。
    /// 不使用 Unix 权限位造错，因为 root/CAP_DAC_OVERRIDE 可以绕过目录写权限。
    #[tokio::test]
    async fn recv_file_list_delete_failure_emits_entry_error() {
        let dest_dir = tempdir().unwrap();
        let root = dest_dir.path().to_path_buf();
        let dest_storage = create_storage(root.to_str().unwrap(), None, true).await.unwrap();
        let orphan = dummy_entry("orphan.txt", false, 5);
        let mut dest_index = DestIndex::new();
        dest_index.insert(orphan);

        // DestIndex 已持有 orphan；把 root 从目录替换成普通文件后，真实 local storage
        // 删除 root/orphan.txt 会稳定返回 NotADirectory，与执行测试的 uid/capability 无关。
        fs::remove_dir(&root).unwrap();
        fs::write(&root, b"not a directory").unwrap();

        let (sender_transport, receiver_transport) = create_in_process_pair();
        let mut state = RemoteSessionState::default();
        state
            .handle_directory_lifecycle(
                &receiver_transport,
                &dest_storage,
                &empty_root_page(),
                &mut dest_index,
                true,
            )
            .await;

        match recv_skip_progress(&sender_transport).await {
            Some(ReceiverMsg::EntryError { entry, reason }) => {
                assert_eq!(entry.get_relative_path(), std::path::Path::new("orphan.txt"));
                assert!(!reason.is_empty());
            }
            other => panic!("expected EntryError（删除失败）, got {other:?}"),
        }

        // 恢复 TempDir 的目录形态，确保清理不依赖平台对“根路径变成文件”的处理。
        fs::remove_file(&root).unwrap();
        fs::create_dir(&root).unwrap();
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

    /// `entry_error_stats_message`：双进程 entry 级失败统一映射为 `ErrorEvent::Copy`，
    /// path/reason 原样保留（issue #57：报表补齐所依赖的翻译逻辑）。
    #[test]
    fn entry_error_stats_message_maps_to_copy_error_event() {
        let path = PathBuf::from("sub/broken.txt");
        match entry_error_stats_message(path.clone(), "boom".to_string()) {
            StorageEntryMessage::Error {
                event, path: p, reason, ..
            } => {
                assert_eq!(event, ErrorEvent::Copy);
                assert_eq!(p, path);
                assert_eq!(reason, "boom");
            }
            other => panic!("expected StorageEntryMessage::Error, got {other:?}"),
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

        // 等待 mock server 确认收到周期回调。不能只 sleep 到 interval 边界后立即
        // abort：调度繁忙时 HTTP 请求可能已经发出、但 server task 尚未来得及记录。
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !bodies.lock().await.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("应在超时前收到周期性回调");

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
