//! 已协商的远端 Receiver 会话。
//!
//! 该模块提供生产调用方与 characterization tests 共用的高层 seam。握手、鉴权和
//! `SessionConfig` 接收仍由外层负责；磁盘写入仍位于 disk-commit 内部 seam 后面。
//! 当前实现委托既有事件循环，后续 tickets 将在此 interface 后逐步收拢会话状态。

use std::sync::Arc;

use data_mover::StorageEnum;
use tokio::sync::mpsc::Receiver;
use transport::message::{FeatureFlags, ProgressSnapshot, SessionConfig};
use transport::traits::ReceiverTransport;

use crate::error::Result;
use crate::receiver::{ReceiverProgress, recv_file_list_and_data_phase};

/// 一次已完成协议协商与鉴权的远端 Receiver 会话。
pub struct RemoteReceiverSession {
    dest_storage: Arc<StorageEnum>,
    session_config: SessionConfig,
    negotiated_features: FeatureFlags,
    delta_size_threshold: u64,
    progress: Arc<ReceiverProgress>,
}

impl RemoteReceiverSession {
    pub fn new(
        dest_storage: Arc<StorageEnum>, session_config: SessionConfig, negotiated_features: FeatureFlags,
        delta_size_threshold: u64, progress: Arc<ReceiverProgress>,
    ) -> Self {
        Self {
            dest_storage,
            session_config,
            negotiated_features,
            delta_size_threshold,
            progress,
        }
    }

    /// 运行会话直到 Sender 宣告传输结束且所有已请求 entry 均获得 durable outcome。
    pub async fn run(
        self, transport: &(dyn ReceiverTransport + 'static), progress_rx: Receiver<ProgressSnapshot>,
    ) -> Result<()> {
        recv_file_list_and_data_phase(
            transport,
            &self.dest_storage,
            &self.session_config,
            &self.negotiated_features,
            self.delta_size_threshold,
            &self.progress,
            progress_rx,
        )
        .await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use bytes::Bytes;
    use data_mover::dir_tree::NdxEvent;
    use data_mover::{DataChunk, create_storage};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use transport::in_process::create_in_process_pair;
    use transport::message::{ReceiverMsg, SenderMsg, TransferDecision};
    use transport::traits::SenderTransport;

    use super::*;

    fn session_config() -> SessionConfig {
        SessionConfig {
            src_path: String::new(),
            qos: None,
            peak_qos_rate: 1.0,
            iops: None,
            enable_integrity_check: false,
            enable_acl: false,
            is_source_reserved: true,
            block_size: None,
            delete_target: false,
            delta_size_threshold: None,
        }
    }

    fn progress_snapshot() -> ProgressSnapshot {
        ProgressSnapshot {
            receiver_id: "characterization".into(),
            files_transferred: 0,
            dirs_created: 0,
            bytes_transferred: 0,
            files_skipped: 0,
            metadata_only: 0,
            error_count: 0,
            elapsed_secs: 0.0,
            speed_bytes_per_sec: 0.0,
        }
    }

    #[tokio::test]
    async fn session_interleaves_progress_and_waits_for_durable_outcome_after_transfer_done() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let content = Bytes::from_static(b"session seam characterization");
        fs::write(src_dir.path().join("a.txt"), &content).unwrap();

        let src_storage = create_storage(src_dir.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let walk = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walk.next().await {
            Some(NdxEvent::Page(page)) => page,
            other => panic!("expected one file page, got {other:?}"),
        };
        let ndx = page.files[0].ndx;
        let entry = page.files[0].entry.clone();

        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let progress = Arc::new(ReceiverProgress::new());
        let (progress_tx, progress_rx) = mpsc::channel(1);
        progress_tx.send(progress_snapshot()).await.unwrap();
        drop(progress_tx);

        let session = RemoteReceiverSession::new(
            dest_storage,
            session_config(),
            FeatureFlags::current(),
            u64::MAX,
            progress,
        );
        let session_handle = tokio::spawn(async move { session.run(&receiver_transport, progress_rx).await });

        sender_transport.send(SenderMsg::FilePage(page)).await.unwrap();
        let mut saw_progress = false;
        let mut saw_request = false;
        while !saw_progress || !saw_request {
            match sender_transport.recv().await {
                Some(ReceiverMsg::Progress(snapshot)) => {
                    saw_progress = true;
                    assert_eq!(snapshot.receiver_id, "characterization");
                }
                Some(ReceiverMsg::TransferRequest {
                    ndx: requested,
                    decision,
                }) => {
                    assert_eq!(requested, ndx);
                    assert_eq!(decision, TransferDecision::FullTransfer);
                    saw_request = true;
                }
                other => panic!("expected progress or transfer request, got {other:?}"),
            }
        }

        sender_transport.send(SenderMsg::FileListDone).await.unwrap();
        assert!(matches!(sender_transport.recv().await, Some(ReceiverMsg::RequestsDone)));
        sender_transport
            .send(SenderMsg::FileBegin {
                ndx,
                entry: entry.clone(),
            })
            .await
            .unwrap();

        // Control-stream completion may overtake data. The session must stay alive until disk commit
        // reports the requested file's durable outcome.
        sender_transport.send(SenderMsg::TransferDone).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !session_handle.is_finished(),
            "TransferDone must not end a session with pending data"
        );

        sender_transport
            .send(SenderMsg::FileData {
                entry: entry.clone(),
                chunk: DataChunk {
                    offset: 0,
                    data: content.clone(),
                },
            })
            .await
            .unwrap();
        sender_transport
            .send(SenderMsg::EndOfFile {
                ndx,
                entry,
                source_hash: None,
            })
            .await
            .unwrap();

        assert!(
            matches!(sender_transport.recv().await, Some(ReceiverMsg::Success { ndx: completed }) if completed == ndx)
        );
        tokio::time::timeout(Duration::from_secs(2), session_handle)
            .await
            .expect("session should finish after durable outcome")
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), content);
    }
}
