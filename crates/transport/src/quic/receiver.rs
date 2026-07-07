//! QUIC Receiver 侧传输实现

// 标准库
use std::net::SocketAddr;

// 外部 crate
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tracing::info;

// 内部模块
use super::cert;
use super::mux;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};
use crate::traits::ReceiverTransport;

/// QUIC Receiver 侧传输
///
/// 与 Sender 之间建立 4 条 bidirectional stream 做多路复用（控制 / 文件列表 /
/// 数据 / ack+进度，见 `quic::mux`），大文件数据流不再阻塞 progress/ack 等控制消息：
/// - `recv()` 从统一的 fan-in channel 读取（各 stream 由独立后台 task 读帧后转发进来）
/// - `send()` 按 `ReceiverMsg` variant 路由到对应物理 stream
pub struct QuicReceiverTransport {
    conn: quinn::Connection,
    send_streams: Vec<Mutex<quinn::SendStream>>,
    incoming_rx: Mutex<UnboundedReceiver<SenderMsg>>,
    reader_tasks: Vec<JoinHandle<()>>,
}

impl Drop for QuicReceiverTransport {
    fn drop(&mut self) {
        for handle in &self.reader_tasks {
            handle.abort();
        }
    }
}

/// 生成自签名证书并 bind 一个 QUIC endpoint，立即返回（不等待任何连接）
///
/// 返回 `(Endpoint, cert_der)`：`cert_der` 是服务端自签名证书的 DER 字节。
///
/// 调用方应在此之后**立即**把 `cert_der` 写入文件供 Sender 侧 `--tls-server-cert` 校验，
/// 再调用 [`accept_connection`] 等待 Sender 连接进来。若把写证书文件放在
/// `accept_connection` 之后，会形成死锁：Sender 在发起连接前必须先读到证书文件，
/// 而 Receiver 却要等一个连接建立后才写文件。
pub fn bind(listen_addr: SocketAddr) -> Result<(quinn::Endpoint, Vec<u8>)> {
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

    Ok((endpoint, cert_der))
}

/// 在已 bind 的 endpoint 上等待并接受一个 QUIC 连接
pub async fn accept_connection(endpoint: &quinn::Endpoint) -> Result<QuicReceiverTransport> {
    let incoming = endpoint.accept().await.ok_or(TransportError::ChannelClosed)?;

    let conn = incoming
        .await
        .map_err(|e| TransportError::RecvFailed(format!("handshake: {e}")))?;

    info!("[QUIC Receiver] Accepted connection from {}", conn.remote_address());

    let (send_streams, incoming_rx, reader_tasks) = mux::receiver_setup(&conn).await?;

    Ok(QuicReceiverTransport {
        conn,
        send_streams,
        incoming_rx: Mutex::new(incoming_rx),
        reader_tasks,
    })
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
        let mut rx = self.incoming_rx.lock().await;
        rx.recv().await
    }

    async fn send(&self, msg: ReceiverMsg) -> Result<()> {
        let kind = mux::receiver_stream_kind(&msg);
        mux::send_routed(&self.send_streams, kind, &msg).await
    }

    async fn close(&self) -> Result<()> {
        self.conn.close(0u32.into(), b"done");
        for handle in &self.reader_tasks {
            handle.abort();
        }
        Ok(())
    }
}
