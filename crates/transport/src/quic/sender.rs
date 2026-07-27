//! QUIC Sender 侧传输实现

// 标准库
use std::net::SocketAddr;
use std::sync::Arc;

// 外部 crate
use async_trait::async_trait;
use rustls::pki_types::CertificateDer;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::task::JoinHandle;
use tracing::info;

// 内部模块
use super::cert;
use super::credit::{CreditWindow, DEFAULT_CREDIT_WINDOW_BYTES};
use super::mux;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg, credit_cost};
use crate::traits::SenderTransport;

/// QUIC Sender 侧传输
///
/// 与 Receiver 之间建立 4 条 bidirectional stream 做多路复用（控制 / 文件列表 /
/// 数据 / ack+进度，见 `quic::mux`），大文件数据流不再阻塞 progress/ack 等控制消息：
/// - `send()` 按 `SenderMsg` variant 路由到对应物理 stream；Data 类消息（`credit_cost`
///   命中）发送前先扣减应用层 credit（见 `quic::credit`），不足则挂起等待 Receiver 授信
/// - `recv()` 从**已过滤掉 `CreditGrant`** 的 channel 读取，不会看到该 variant
///
/// `CreditGrant` 的过滤/补授不放在 `recv()` 里（见 `connect_with_credit_window` 文档
/// 「credit-grant 过滤 task」小节）：真实调用方（`process_requests_and_acks`）是唯一的
/// `recv()` 消费者，且在同一个循环里内联调用 `send()` 处理请求——若 `send()` 卡在
/// `credit.acquire()` 里，这个任务就没有机会转回去 `recv()` 读到解锁自己的 `CreditGrant`，
/// 会自己把自己锁死（两个真实子进程联调 `tests/remote_process_e2e.rs` 大文件用例时
/// 实际触发过：Sender 单任务的 send/recv 是严格顺序的，不是两个并发协程）。
pub struct QuicSenderTransport {
    conn: quinn::Connection,
    send_streams: Vec<Mutex<mux::StreamSlot>>,
    incoming_rx: Mutex<UnboundedReceiver<ReceiverMsg>>,
    reader_tasks: Vec<JoinHandle<()>>,
    credit: Arc<CreditWindow>,
}

impl Drop for QuicSenderTransport {
    fn drop(&mut self) {
        for handle in &self.reader_tasks {
            handle.abort();
        }
    }
}

/// 连接到远端 Receiver
///
/// - `server_cert`: 服务端 DER 证书（来自 `serve --tls-cert-out`），用于验证服务端身份。
///   `None` 时跳过验证（仅限内部可信网络，会打印 WARNING）。
///
/// credit 窗口固定使用 `DEFAULT_CREDIT_WINDOW_BYTES`（v1 不提供配置项，见 `quic::credit`
/// 模块文档）；测试注入自定义窗口见 crate 内的 `connect_with_credit_window`。
pub async fn connect(
    addr: SocketAddr, server_name: &str, server_cert: Option<CertificateDer<'static>>,
) -> Result<QuicSenderTransport> {
    connect_with_credit_window(addr, server_name, server_cert, DEFAULT_CREDIT_WINDOW_BYTES).await
}

/// 测试注入口：用自定义 credit 窗口大小建立连接（生产路径固定走 [`connect`]）。
/// 仅供 crate 内测试触发 credit 窗口耗尽/授信解阻塞路径，不对外暴露——v1 不提供配置项
/// （见 `quic::credit` 模块文档）。
///
/// ## credit-grant 过滤 task
///
/// `mux::sender_setup` 返回的 `incoming_rx` 混杂了全部 4 类 `ReceiverMsg`（含
/// `CreditGrant`），这里立即 spawn 一个独立后台 task 把它接管：`CreditGrant` 就地
/// `credit.grant()` 消费掉，其余消息转发进一个新的 channel 交给 `QuicSenderTransport::
/// recv()`（`send()` 看不到、也不需要看到 `CreditGrant`）。这个 task 完全独立于任何
/// 调用方是否在调 `recv()`——`send()` 挂在 `credit.acquire()` 里等待补授时，正是靠这个
/// 后台 task 持续读 AckProgress 物理 stream 才能收到并应用 grant，不依赖调用方主动
/// 转回来 `recv()`（该假设在真实 Sender 主循环里不成立，见结构体文档）。
pub(crate) async fn connect_with_credit_window(
    addr: SocketAddr, server_name: &str, server_cert: Option<CertificateDer<'static>>, window_bytes: u64,
) -> Result<QuicSenderTransport> {
    let client_config = if let Some(cert) = server_cert {
        cert::build_client_config_with_ca(cert)
            .map_err(|e| TransportError::SendFailed(format!("client config (ca): {e}")))?
    } else {
        tracing::warn!(
            "[QUIC Sender] WARNING: TLS 服务端证书验证已禁用！\
             存在 MITM 攻击风险。请使用 --tls-server-cert 提供服务端证书。"
        );
        cert::build_client_config_insecure()
            .map_err(|e| TransportError::SendFailed(format!("client config (insecure): {e}")))?
    };

    let mut endpoint = quinn::Endpoint::client(
        "[::]:0"
            .parse()
            .map_err(|e: std::net::AddrParseError| TransportError::SendFailed(format!("bind: {e}")))?,
    )
    .map_err(|e| TransportError::SendFailed(format!("endpoint: {e}")))?;

    endpoint.set_default_client_config(client_config);

    info!("[QUIC Sender] Connecting to {}...", addr);
    let conn = endpoint
        .connect(addr, server_name)
        .map_err(|e| TransportError::SendFailed(format!("connect: {e}")))?
        .await
        .map_err(|e| TransportError::SendFailed(format!("handshake: {e}")))?;

    info!("[QUIC Sender] Connected to {}", addr);

    let (send_streams, mut raw_incoming_rx, mut reader_tasks) = mux::sender_setup(&conn).await?;
    let credit = Arc::new(CreditWindow::new(window_bytes));

    // credit-grant 过滤 task：见本函数文档。独立于调用方是否在调 recv()，持续把
    // CreditGrant 应用到 credit 上，其余消息转发给真正暴露给调用方的 channel。
    let (app_tx, app_rx) = mpsc::unbounded_channel::<ReceiverMsg>();
    let filter_credit = credit.clone();
    reader_tasks.push(tokio::spawn(async move {
        while let Some(msg) = raw_incoming_rx.recv().await {
            match msg {
                ReceiverMsg::CreditGrant { bytes, .. } => filter_credit.grant(bytes),
                other => {
                    if app_tx.send(other).is_err() {
                        break;
                    }
                }
            }
        }
    }));

    Ok(QuicSenderTransport {
        conn,
        send_streams,
        incoming_rx: Mutex::new(app_rx),
        reader_tasks,
        credit,
    })
}

#[async_trait]
impl SenderTransport for QuicSenderTransport {
    async fn send(&self, msg: SenderMsg) -> Result<()> {
        if let Some(bytes) = credit_cost(&msg) {
            self.credit.acquire(bytes).await?;
        }
        let kind = mux::sender_stream_kind(&msg);
        mux::send_routed(&self.send_streams, kind, &msg).await
    }

    async fn recv(&self) -> Option<ReceiverMsg> {
        let mut rx = self.incoming_rx.lock().await;
        rx.recv().await
    }

    async fn close(&self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        for handle in &self.reader_tasks {
            handle.abort();
        }
        Ok(())
    }
}

/// crate 内测试：用真实 loopback QUIC 连接（非同进程 mock）验证 credit 窗口在实际发送
/// 路径上的阻塞/解阻塞行为，以及 credit 耗尽期间控制类消息不受影响——这两点都依赖
/// `connect_with_credit_window`（`pub(crate)`，注入自定义小窗口触发耗尽路径），只能放在
/// crate 内部，无法用 `tests/quic_roundtrip.rs`（外部 crate，只能看到 `pub` API）覆盖。
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use data_mover::{DataChunk, EntryEnum, NASEntry};

    use super::*;
    use crate::quic::framing;

    fn install_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn create_server_endpoint() -> (quinn::Endpoint, SocketAddr) {
        let (certs, key) = super::cert::generate_self_signed(&[]).unwrap();
        let server_config = super::cert::build_server_config(certs, key).unwrap();
        let endpoint = quinn::Endpoint::server(server_config, "[::1]:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();
        (endpoint, addr)
    }

    /// 构造一个仅用于测试的最小 `NASEntry`（字段值无实际语义，只满足 `FileData` 的类型要求）
    fn dummy_file_entry(name: &str) -> Arc<EntryEnum> {
        Arc::new(EntryEnum::NAS(NASEntry {
            name: name.to_string(),
            relative_path: PathBuf::from(name),
            extension: None,
            is_dir: false,
            size: 0,
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

    /// 真实 QUIC：credit 窗口耗尽后 `send()` 应挂起，直到收到 `ReceiverMsg::CreditGrant`
    /// 补授足够字节才解除阻塞——这是"背压真传导回 Sender 应用层"的直接证据。
    ///
    /// 关键：测试期间**不**并发调用 `sender.recv()`（真实 Sender 主循环 `process_requests_
    /// and_acks` 是唯一的 recv() 消费者，且与 send() 严格顺序执行在同一个任务里，没有
    /// 另一个协程替它并发 recv()）——若 grant 的应用依赖调用方主动调 recv() 才生效，这里
    /// 就会永久挂死，是死锁的直接回归测试（两个真实子进程联调大文件用例时实际触发过）。
    #[tokio::test]
    async fn send_blocks_on_exhausted_credit_until_grant_unblocks() {
        install_crypto_provider();
        let (server_endpoint, server_addr) = create_server_endpoint();
        const TINY_WINDOW: u64 = 8;

        let receiver_handle = tokio::spawn(async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            // Control / FileList / Data 由 Sender 发起，依次 accept；AckProgress 由本端
            // open_bi（见 quic::mux 模块文档：Sender 侧 AckProgress 是 accept_bi，
            // 这里作为手工 Receiver harness 要反过来 open_bi）。
            let (_ctrl_send, _ctrl_recv) = conn.accept_bi().await.unwrap();
            let (_fl_send, _fl_recv) = conn.accept_bi().await.unwrap();
            let (_data_send, mut data_recv) = conn.accept_bi().await.unwrap();
            let (mut ack_send, _ack_recv) = conn.open_bi().await.unwrap();

            // 读走第一条 FileData（8B，耗光 TINY_WINDOW 全部 credit）
            let msg1: Option<SenderMsg> = framing::read_msg(&mut data_recv).await.ok().flatten();
            assert!(
                matches!(msg1, Some(SenderMsg::FileData { .. })),
                "expected FileData, got {msg1:?}"
            );

            // 此刻第二条 FileData 应该还没发出（Sender 卡在 credit.acquire() 里），
            // 给测试主体一点时间断言这一点后再授信解除阻塞
            tokio::time::sleep(Duration::from_millis(300)).await;
            framing::write_msg(
                &mut ack_send,
                &ReceiverMsg::CreditGrant {
                    bytes: TINY_WINDOW,
                    ndx: None,
                },
            )
            .await
            .unwrap();

            // 授信后第二条 FileData 应该很快发过来
            let msg2: Option<SenderMsg> =
                tokio::time::timeout(Duration::from_secs(2), framing::read_msg(&mut data_recv))
                    .await
                    .expect("授信后应很快收到第二条 FileData，不应超时")
                    .ok()
                    .flatten();
            assert!(
                matches!(msg2, Some(SenderMsg::FileData { .. })),
                "expected FileData, got {msg2:?}"
            );
        });

        let sender = Arc::new(
            connect_with_credit_window(server_addr, "localhost", None, TINY_WINDOW)
                .await
                .unwrap(),
        );

        let entry = dummy_file_entry("big.bin");
        let chunk1 = DataChunk {
            offset: 0,
            data: Bytes::from(vec![0u8; TINY_WINDOW as usize]),
        };
        sender
            .send(SenderMsg::FileData {
                entry: entry.clone(),
                chunk: chunk1,
            })
            .await
            .unwrap();

        // 窗口已耗尽：第二条 FileData 需要等待 grant 才能发出
        let chunk2 = DataChunk {
            offset: TINY_WINDOW,
            data: Bytes::from(vec![1u8; TINY_WINDOW as usize]),
        };
        let sender2 = sender.clone();
        let send_task = tokio::spawn(async move { sender2.send(SenderMsg::FileData { entry, chunk: chunk2 }).await });

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !send_task.is_finished(),
            "credit 耗尽时 send() 应挂起（前提不成立，测试无意义）"
        );

        tokio::time::timeout(Duration::from_secs(3), send_task)
            .await
            .expect("授信后 send() 应尽快解除阻塞完成")
            .unwrap()
            .unwrap();

        receiver_handle.await.unwrap();
        sender.close().await.unwrap();
    }

    /// 真实 QUIC：即使 Data credit 已耗尽（另一条 `FileData` 卡在 `send()` 里等待授信），
    /// Control 类消息（`credit_cost` 恒返回 `None`）应仍能畅通发送——credit 只约束 Data
    /// 类消息，三类控制消息隔离语义不应因 credit 机制引入回归。
    #[tokio::test]
    async fn control_messages_not_blocked_by_exhausted_data_credit() {
        install_crypto_provider();
        let (server_endpoint, server_addr) = create_server_endpoint();
        const TINY_WINDOW: u64 = 8;

        let receiver_handle = tokio::spawn(async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let (_ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            let (_fl_send, _fl_recv) = conn.accept_bi().await.unwrap();
            let (_data_send, mut data_recv) = conn.accept_bi().await.unwrap();
            let (_ack_send, _ack_recv) = conn.open_bi().await.unwrap();

            // 先读走第一条 FileData（耗光窗口），不授信——本测试不关心第二条 FileData
            let msg1: Option<SenderMsg> = framing::read_msg(&mut data_recv).await.ok().flatten();
            assert!(
                matches!(msg1, Some(SenderMsg::FileData { .. })),
                "expected FileData, got {msg1:?}"
            );

            // 核心断言：Control stream 上的消息应能很快读到，不受 Data credit 耗尽影响
            let ctrl_msg: Option<SenderMsg> =
                tokio::time::timeout(Duration::from_secs(2), framing::read_msg::<SenderMsg>(&mut ctrl_recv))
                    .await
                    .expect("Control 消息应在 Data credit 耗尽期间被及时读到，不应超时")
                    .ok()
                    .flatten();
            assert!(
                matches!(ctrl_msg, Some(SenderMsg::EntryError { .. })),
                "expected EntryError, got {ctrl_msg:?}"
            );
        });

        let sender = Arc::new(
            connect_with_credit_window(server_addr, "localhost", None, TINY_WINDOW)
                .await
                .unwrap(),
        );

        let entry = dummy_file_entry("big.bin");
        let chunk1 = DataChunk {
            offset: 0,
            data: Bytes::from(vec![0u8; TINY_WINDOW as usize]),
        };
        sender
            .send(SenderMsg::FileData {
                entry: entry.clone(),
                chunk: chunk1,
            })
            .await
            .unwrap();

        // 第二条 FileData 卡住（credit 耗尽，无人授信）——不 await，扔到后台
        let chunk2 = DataChunk {
            offset: TINY_WINDOW,
            data: Bytes::from(vec![1u8; TINY_WINDOW as usize]),
        };
        let sender2 = sender.clone();
        let entry2 = entry.clone();
        let stuck_send = tokio::spawn(async move {
            let _ = sender2
                .send(SenderMsg::FileData {
                    entry: entry2,
                    chunk: chunk2,
                })
                .await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !stuck_send.is_finished(),
            "credit 耗尽时第二条 FileData 应仍处于挂起状态（前提不成立，测试无意义）"
        );

        // 与此同时，Control 类消息应该能畅通发送（credit_cost 对它返回 None，不受影响）
        sender
            .send(SenderMsg::EntryError {
                path: "x".into(),
                reason: "test".into(),
                ndx: None,
            })
            .await
            .unwrap();

        receiver_handle.await.unwrap();
        stuck_send.abort();
        sender.close().await.unwrap();
    }
}
