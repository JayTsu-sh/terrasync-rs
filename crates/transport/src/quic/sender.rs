//! QUIC Sender 侧传输实现

// 标准库
use std::net::SocketAddr;

// 外部 crate
use async_trait::async_trait;
use rustls::pki_types::CertificateDer;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

// 内部模块
use super::cert;
use super::mux;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};
use crate::traits::SenderTransport;

/// QUIC Sender 侧传输
///
/// 与 Receiver 之间建立 4 条 bidirectional stream 做多路复用（控制 / 文件列表 /
/// 数据 / ack+进度，见 `quic::mux`），大文件数据流不再阻塞 progress/ack 等控制消息：
/// - `send()` 按 `SenderMsg` variant 路由到对应物理 stream
/// - `recv()` 从统一的 fan-in channel 读取（各 stream 由独立后台 task 读帧后转发进来）
pub struct QuicSenderTransport {
    conn: quinn::Connection,
    send_streams: Vec<Mutex<quinn::SendStream>>,
    incoming_rx: Mutex<mpsc::Receiver<ReceiverMsg>>,
    reader_tasks: Vec<JoinHandle<()>>,
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
pub async fn connect(
    addr: SocketAddr, server_name: &str, server_cert: Option<CertificateDer<'static>>,
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

    let (send_streams, recv_streams) = mux::open_mux_streams(&conn).await?;
    let (incoming_rx, reader_tasks) = mux::spawn_reader_tasks::<ReceiverMsg>(recv_streams);

    Ok(QuicSenderTransport {
        conn,
        send_streams: send_streams.into_iter().map(Mutex::new).collect(),
        incoming_rx: Mutex::new(incoming_rx),
        reader_tasks,
    })
}

#[async_trait]
impl SenderTransport for QuicSenderTransport {
    async fn send(&self, msg: SenderMsg) -> Result<()> {
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
