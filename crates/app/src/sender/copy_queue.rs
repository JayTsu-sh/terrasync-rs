//! 普通全量复制 worker 间传递的本地工作。

use std::sync::Arc;

use async_channel::{Receiver, Sender, bounded};
use async_trait::async_trait;
use data_mover::model::ObservedEntry;

use crate::error::{AppError, Result};

pub(crate) enum CopyCommand {
    CopyEntry {
        entry: Arc<ObservedEntry>,
    },
    TarPacked {
        tar_entry: Arc<ObservedEntry>,
        manifest_entries: Arc<[Arc<ObservedEntry>]>,
    },
    // 保留打包失败时的队列顺序，原始失败由生产 worker 返回。
    PackFailed,
}

pub(crate) fn copy_queue() -> (Sender<CopyCommand>, Receiver<CopyCommand>) {
    bounded(128)
}

#[async_trait]
pub(crate) trait CopyCommandSink: Send + Sync {
    async fn send(&self, command: CopyCommand) -> Result<()>;
}

#[async_trait]
impl CopyCommandSink for Sender<CopyCommand> {
    async fn send(&self, command: CopyCommand) -> Result<()> {
        Sender::send(self, command).await.map_err(|_| AppError::CopyQueueClosed)
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;

    #[tokio::test]
    async fn full_queue_backpressures_and_receivers_share_the_drain() -> Result<()> {
        let (sender, receiver) = copy_queue();
        let peer = receiver.clone();
        for _ in 0..128 {
            CopyCommandSink::send(&sender, CopyCommand::PackFailed).await?;
        }
        {
            let extra = CopyCommandSink::send(&sender, CopyCommand::PackFailed);
            tokio::pin!(extra);
            tokio::select! {
                biased;
                result = &mut extra => panic!("full queue accepted another command: {result:?}"),
                () = ready(()) => {}
            }
            assert!(matches!(peer.recv().await, Ok(CopyCommand::PackFailed)));
            extra.await?;
        }
        drop(sender);

        // 两个接收端共用同一队列，合计收到剩余 128 条。
        for _ in 0..64 {
            assert!(matches!(receiver.recv().await, Ok(CopyCommand::PackFailed)));
            assert!(matches!(peer.recv().await, Ok(CopyCommand::PackFailed)));
        }
        assert!(receiver.recv().await.is_err());
        assert!(peer.recv().await.is_err());

        let (sender, receiver) = copy_queue();
        drop(receiver);
        assert!(matches!(
            CopyCommandSink::send(&sender, CopyCommand::PackFailed).await,
            Err(AppError::CopyQueueClosed)
        ));
        Ok(())
    }
}
