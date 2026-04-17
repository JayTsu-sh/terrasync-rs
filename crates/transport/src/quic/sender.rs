//! QUIC Sender 侧传输实现

// 标准库
use std::net::SocketAddr;

// 外部 crate
use async_trait::async_trait;
use rustls::pki_types::CertificateDer;
use tokio::sync::Mutex;
use tracing::info;

// 内部模块
use super::cert;
use super::framing;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};
use crate::traits::SenderTransport;

/// QUIC Sender 侧传输
///
/// 通过一个 bidirectional stream 与 Receiver 通信：
/// - send 端发送 `SenderMsg`
/// - recv 端接收 `ReceiverMsg`
pub struct QuicSenderTransport {
    conn: quinn::Connection,
    send: Mutex<quinn::SendStream>,
    recv: Mutex<quinn::RecvStream>,
}

/// 连接到远端 Receiver
///
/// - `server_cert`: 服务端 DER 证书（来自 `serve --tls-cert-out`），用于验证服务端身份。
///   `None` 时跳过验证（仅限内部可信网络，会打印 WARNING）。
pub async fn connect(
    addr: SocketAddr, server_name: &str, server_cert: Option<CertificateDer<'static>>,
) -> Result<QuicSenderTransport> {
    let _ = rustls::crypto::ring::default_provider().install_default();
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

    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| TransportError::SendFailed(format!("open_bi: {e}")))?;

    Ok(QuicSenderTransport {
        conn,
        send: Mutex::new(send),
        recv: Mutex::new(recv),
    })
}

#[async_trait]
impl SenderTransport for QuicSenderTransport {
    async fn send(&self, msg: SenderMsg) -> Result<()> {
        let mut stream = self.send.lock().await;
        framing::write_msg(&mut stream, &msg)
            .await
            .map_err(|e| TransportError::SendFailed(format!("{e}")))
    }

    async fn recv(&self) -> Option<ReceiverMsg> {
        let mut stream = self.recv.lock().await;
        match framing::read_msg(&mut stream).await {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("[QuicSenderTransport] recv error: {e}");
                None
            }
        }
    }

    async fn close(&self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        Ok(())
    }
}
