// 外部 crate
use async_channel::{Receiver, Sender, bounded};
use async_trait::async_trait;

// 内部模块
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};
use crate::traits::{ReceiverTransport, SenderTransport};

/// 单进程模式下的传输通道容量
const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// 创建一对 `InProcess` 传输通道（Sender 端 + Receiver 端）
///
/// 内部使用两对 `async_channel`：
/// - S→R：Sender 发送 `SenderMsg`，Receiver 接收
/// - R→S：Receiver 发送 `ReceiverMsg`，Sender 接收
pub fn create_in_process_pair() -> (InProcessSenderTransport, InProcessReceiverTransport) {
    create_in_process_pair_with_capacity(DEFAULT_CHANNEL_CAPACITY)
}

/// 指定容量创建传输通道对
pub fn create_in_process_pair_with_capacity(capacity: usize) -> (InProcessSenderTransport, InProcessReceiverTransport) {
    let (s2r_tx, s2r_rx) = bounded::<SenderMsg>(capacity);
    let (r2s_tx, r2s_rx) = bounded::<ReceiverMsg>(capacity);

    let sender = InProcessSenderTransport { s2r_tx, r2s_rx };
    let receiver = InProcessReceiverTransport { s2r_rx, r2s_tx };
    (sender, receiver)
}

/// Sender 侧的 in-process 传输实现
pub struct InProcessSenderTransport {
    /// Sender → Receiver channel
    s2r_tx: Sender<SenderMsg>,
    /// Receiver → Sender channel
    r2s_rx: Receiver<ReceiverMsg>,
}

#[async_trait]
impl SenderTransport for InProcessSenderTransport {
    async fn send(&self, msg: SenderMsg) -> Result<()> {
        self.s2r_tx.send(msg).await.map_err(|_| TransportError::ChannelClosed)
    }

    async fn recv(&self) -> Option<ReceiverMsg> {
        self.r2s_rx.recv().await.ok()
    }

    async fn close(&self) -> Result<()> {
        self.s2r_tx.close();
        self.r2s_rx.close();
        Ok(())
    }
}

/// Receiver 侧的 in-process 传输实现
pub struct InProcessReceiverTransport {
    /// Sender → Receiver channel
    s2r_rx: Receiver<SenderMsg>,
    /// Receiver → Sender channel
    r2s_tx: Sender<ReceiverMsg>,
}

#[async_trait]
impl ReceiverTransport for InProcessReceiverTransport {
    async fn recv(&self) -> Option<SenderMsg> {
        self.s2r_rx.recv().await.ok()
    }

    async fn send(&self, msg: ReceiverMsg) -> Result<()> {
        self.r2s_tx.send(msg).await.map_err(|_| TransportError::ChannelClosed)
    }

    async fn close(&self) -> Result<()> {
        self.s2r_rx.close();
        self.r2s_tx.close();
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Characterizes the current adapter seam before #109 aligns it with QUIC:
    /// flow-control messages are still visible to the application consumer.
    #[tokio::test]
    async fn credit_grant_is_currently_visible_to_in_process_sender() {
        let (sender, receiver) = create_in_process_pair();

        receiver
            .send(ReceiverMsg::CreditGrant { bytes: 32, ndx: None })
            .await
            .unwrap();

        assert!(matches!(
            sender.recv().await,
            Some(ReceiverMsg::CreditGrant { bytes: 32, ndx: None })
        ));
    }
}
