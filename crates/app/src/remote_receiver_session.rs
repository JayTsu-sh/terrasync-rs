//! 已协商的远端 Receiver 会话。
//!
//! 该模块提供生产调用方与 characterization tests 共用的高层 seam。握手、鉴权和
//! `SessionConfig` 接收仍由外层负责；磁盘写入仍位于 disk-commit 内部 seam 后面。
//! 当前实现委托既有事件循环，后续 tickets 将在此 interface 后逐步收拢会话状态。

use std::collections::HashSet;
use std::sync::Arc;

use data_mover::{EntryEnum, StorageEnum};
use tokio::sync::mpsc::Receiver;
use transport::message::{FeatureFlags, NdxTable, ProgressSnapshot, ReceiverMsg, SessionConfig, TransferDecision};
use transport::traits::ReceiverTransport;

use crate::error::Result;
use crate::receiver::{ReceiverProgress, recv_file_list_and_data_phase};

/// 一次会话的请求与终态账本。
///
/// 所有结束判断都通过该类型完成，避免消息分支各自复制计数与 `TransferDone` 条件。
#[derive(Debug, Default)]
struct TransferLedger {
    requested: u64,
    completed: u64,
    transfer_done_seen: bool,
    terminated_ndx: HashSet<i32>,
}

impl TransferLedger {
    fn record_request(&mut self) {
        self.requested += 1;
    }

    /// 记录带 ndx 的首次终态；重复终态不改变完成计数。
    fn record_terminal(&mut self, ndx: i32) -> bool {
        if !self.terminated_ndx.insert(ndx) {
            return false;
        }
        self.completed += 1;
        true
    }

    /// 记录协议中不携带 ndx 的 entry 终态（例如符号链接）。
    fn record_unindexed_terminal(&mut self) {
        self.completed += 1;
    }

    fn observe_transfer_done(&mut self) {
        self.transfer_done_seen = true;
    }

    fn is_complete(&self) -> bool {
        self.transfer_done_seen && self.completed >= self.requested
    }

    fn counts(&self) -> (u64, u64) {
        (self.completed, self.requested)
    }
}

/// Receiver session 当前已迁移的协调状态。
#[derive(Debug, Default)]
pub(crate) struct RemoteSessionState {
    ledger: TransferLedger,
    ndx_table: NdxTable,
}

impl RemoteSessionState {
    pub(crate) fn ingest_page(&mut self, page: &data_mover::dir_tree::DirPageResult) {
        self.ndx_table.ingest_page(page);
    }

    pub(crate) fn indexed_entry(&self, ndx: i32) -> Option<&Arc<EntryEnum>> {
        self.ndx_table.get(ndx)
    }

    pub(crate) fn indexed_len(&self) -> usize {
        self.ndx_table.len()
    }

    pub(crate) fn record_request(&mut self) {
        self.ledger.record_request();
    }

    /// 处理不需要 delta implementation 的基础分类。返回该分类是否已完成处理。
    pub(crate) async fn handle_base_classification(
        &mut self, transport: &(dyn ReceiverTransport + 'static), dest: &StorageEnum, progress: &ReceiverProgress,
        ndx: i32, entry: &Arc<EntryEnum>, decision: TransferDecision,
    ) -> bool {
        match decision {
            TransferDecision::FullTransfer => {
                let _ = transport.send(ReceiverMsg::TransferRequest { ndx, decision }).await;
                self.ledger.record_request();
                true
            }
            TransferDecision::MetadataOnly => {
                let _ = dest.set_entry_metadata(entry).await;
                progress
                    .metadata_only
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::Classified {
                        entry: entry.clone(),
                        decision,
                    })
                    .await;
                true
            }
            TransferDecision::Skip => {
                progress
                    .files_skipped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = transport
                    .send(ReceiverMsg::Classified {
                        entry: entry.clone(),
                        decision,
                    })
                    .await;
                true
            }
            TransferDecision::DeltaTransfer | TransferDecision::Deleted => false,
        }
    }

    pub(crate) fn record_terminal(&mut self, ndx: i32) -> bool {
        self.ledger.record_terminal(ndx)
    }

    pub(crate) fn record_unindexed_terminal(&mut self) {
        self.ledger.record_unindexed_terminal();
    }

    pub(crate) fn observe_transfer_done(&mut self) {
        self.ledger.observe_transfer_done();
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.ledger.is_complete()
    }

    pub(crate) fn counts(&self) -> (u64, u64) {
        self.ledger.counts()
    }
}

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
        let mut state = RemoteSessionState::default();
        recv_file_list_and_data_phase(
            transport,
            &self.dest_storage,
            &self.session_config,
            &self.negotiated_features,
            self.delta_size_threshold,
            &self.progress,
            progress_rx,
            &mut state,
        )
        .await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::Ordering;
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

    #[tokio::test]
    async fn session_with_no_requested_transfers_finishes_after_transfer_done() {
        let dest_dir = tempdir().unwrap();
        let dest_storage = Arc::new(
            create_storage(dest_dir.path().to_str().unwrap(), None, true)
                .await
                .unwrap(),
        );
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let (_progress_tx, progress_rx) = mpsc::channel(1);
        let session = RemoteReceiverSession::new(
            dest_storage,
            session_config(),
            FeatureFlags::current(),
            u64::MAX,
            Arc::new(ReceiverProgress::new()),
        );
        let session_handle = tokio::spawn(async move { session.run(&receiver_transport, progress_rx).await });

        sender_transport.send(SenderMsg::FileListDone).await.unwrap();
        assert!(matches!(sender_transport.recv().await, Some(ReceiverMsg::RequestsDone)));
        sender_transport.send(SenderMsg::TransferDone).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), session_handle)
            .await
            .expect("empty session should finish after TransferDone")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn mixed_page_base_classifications_share_session_state_and_observable_outputs() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("full.txt"), b"new").unwrap();
        fs::write(src_dir.path().join("meta.txt"), b"same meta content").unwrap();
        fs::write(src_dir.path().join("skip.txt"), b"same skip content").unwrap();
        fs::write(dest_dir.path().join("meta.txt"), b"same meta content").unwrap();
        fs::write(dest_dir.path().join("skip.txt"), b"same skip content").unwrap();
        fs::set_permissions(dest_dir.path().join("meta.txt"), fs::Permissions::from_mode(0o600)).unwrap();

        let src_storage = create_storage(src_dir.path().to_str().unwrap(), None, false)
            .await
            .unwrap();
        let walk = src_storage.walkdir_2(None, None, None, None, 4, false).await.unwrap();
        let page = match walk.next().await {
            Some(NdxEvent::Page(page)) => page,
            other => panic!("expected mixed file page, got {other:?}"),
        };
        assert_eq!(page.files.len(), 3);

        let dest_storage = create_storage(dest_dir.path().to_str().unwrap(), None, true)
            .await
            .unwrap();
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let progress = ReceiverProgress::new();
        let mut state = RemoteSessionState::default();
        state.ingest_page(&page);

        for indexed in &page.files {
            let decision = match indexed.entry.get_name() {
                "full.txt" => TransferDecision::FullTransfer,
                "meta.txt" => TransferDecision::MetadataOnly,
                "skip.txt" => TransferDecision::Skip,
                other => panic!("unexpected entry {other}"),
            };
            assert!(
                state
                    .handle_base_classification(
                        &receiver_transport,
                        &dest_storage,
                        &progress,
                        indexed.ndx,
                        &indexed.entry,
                        decision,
                    )
                    .await
            );
        }

        let mut saw_full = false;
        let mut saw_metadata = false;
        let mut saw_skip = false;
        for _ in 0..3 {
            match sender_transport.recv().await {
                Some(ReceiverMsg::TransferRequest {
                    decision: TransferDecision::FullTransfer,
                    ..
                }) => saw_full = true,
                Some(ReceiverMsg::Classified {
                    decision: TransferDecision::MetadataOnly,
                    ..
                }) => saw_metadata = true,
                Some(ReceiverMsg::Classified {
                    decision: TransferDecision::Skip,
                    ..
                }) => saw_skip = true,
                other => panic!("unexpected base classification output: {other:?}"),
            }
        }

        assert!(saw_full && saw_metadata && saw_skip);
        assert_eq!(state.indexed_len(), 3);
        assert_eq!(state.counts(), (0, 1));
        assert_eq!(progress.metadata_only.load(Ordering::Relaxed), 1);
        assert_eq!(progress.files_skipped.load(Ordering::Relaxed), 1);
        assert_eq!(
            fs::metadata(dest_dir.path().join("meta.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        assert_eq!(
            fs::read(dest_dir.path().join("skip.txt")).unwrap(),
            b"same skip content"
        );
    }
}
