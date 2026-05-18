//! QUIC Receiver 侧传输实现

// 标准库
use std::net::SocketAddr;

// 外部 crate
use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::info;

// 内部模块
use super::cert;
use super::framing;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};
use crate::traits::ReceiverTransport;

/// QUIC Receiver 侧传输
///
/// 通过一个 bidirectional stream 与 Sender 通信：
/// - recv 端接收 `SenderMsg`
/// - send 端发送 `ReceiverMsg`
pub struct QuicReceiverTransport {
    conn: quinn::Connection,
    send: Mutex<quinn::SendStream>,
    recv: Mutex<quinn::RecvStream>,
}

/// 监听并接受一个 QUIC 连接
///
/// 返回 `(QuicReceiverTransport, Endpoint, cert_der)`：
/// - `cert_der` 是服务端自签名证书的 DER 字节，调用方可写入文件供 Sender 侧验证（防 MITM）。
pub async fn accept(listen_addr: SocketAddr) -> Result<(QuicReceiverTransport, quinn::Endpoint, Vec<u8>)> {
    let (certs, key) = cert::generate_self_signed(&[]).map_err(|e| TransportError::RecvFailed(format!("cert: {e}")))?;

    // 在 certs 被 build_server_config 消费前，先保存 DER 字节
    let cert_der: Vec<u8> = certs[0].as_ref().to_vec();

    let server_config =
        cert::build_server_config(certs, key).map_err(|e| TransportError::RecvFailed(format!("server config: {e}")))?;

    let endpoint = quinn::Endpoint::server(server_config, listen_addr)
        .map_err(|e| TransportError::RecvFailed(format!("bind: {e}")))?;

    let local_addr = endpoint
        .local_addr()
        .map_err(|e| TransportError::RecvFailed(format!("local_addr: {e}")))?;
    info!("[QUIC Receiver] Listening on {}", local_addr);

    let incoming = endpoint.accept().await.ok_or(TransportError::ChannelClosed)?;

    let conn = incoming
        .await
        .map_err(|e| TransportError::RecvFailed(format!("handshake: {e}")))?;

    info!("[QUIC Receiver] Accepted connection from {}", conn.remote_address());

    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|e| TransportError::RecvFailed(format!("accept_bi: {e}")))?;

    Ok((
        QuicReceiverTransport {
            conn,
            send: Mutex::new(send),
            recv: Mutex::new(recv),
        },
        endpoint,
        cert_der,
    ))
}

/// 获取 Endpoint 监听的实际地址（用于测试时获取随机端口）
pub fn local_addr(endpoint: &quinn::Endpoint) -> Result<SocketAddr> {
    endpoint
        .local_addr()
        .map_err(|e| TransportError::RecvFailed(format!("local_addr: {e}")))
}

#[async_trait]
impl ReceiverTransport for QuicReceiverTransport {
    async fn recv(&self) -> Option<SenderMsg> {
        let mut stream = self.recv.lock().await;
        framing::read_msg(&mut stream).await.ok().flatten()
    }

    async fn send(&self, msg: ReceiverMsg) -> Result<()> {
        let mut stream = self.send.lock().await;
        framing::write_msg(&mut stream, &msg)
            .await
            .map_err(|e| TransportError::SendFailed(format!("{e}")))
    }

    async fn close(&self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        Ok(())
    }
}
