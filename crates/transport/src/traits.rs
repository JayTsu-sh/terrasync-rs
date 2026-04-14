// 外部 crate
use async_trait::async_trait;

// 内部模块
use crate::error::Result;
use crate::message::{ReceiverMsg, SenderMsg};

/// Sender 侧的传输接口
///
/// 单进程模式：由 InProcessSenderTransport 实现（in-process channel）
/// 双进程模式：由 QuicSenderTransport 实现（QUIC stream）
#[async_trait]
pub trait SenderTransport: Send + Sync {
    /// 发送消息给 Receiver（file list + file data）
    async fn send(&self, msg: SenderMsg) -> Result<()>;

    /// 接收 Receiver 的消息（请求 + ack）
    async fn recv(&self) -> Option<ReceiverMsg>;

    /// 关闭传输通道
    async fn close(&self) -> Result<()>;
}

/// Receiver 侧的传输接口
///
/// 单进程模式：由 InProcessReceiverTransport 实现（in-process channel）
/// 双进程模式：由 QuicReceiverTransport 实现（QUIC stream）
#[async_trait]
pub trait ReceiverTransport: Send + Sync {
    /// 接收 Sender 的消息（file list + file data）
    async fn recv(&self) -> Option<SenderMsg>;

    /// 发送消息给 Sender（请求 + ack）
    async fn send(&self, msg: ReceiverMsg) -> Result<()>;

    /// 关闭传输通道
    async fn close(&self) -> Result<()>;
}
