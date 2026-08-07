//! 协商完成后的 Remote Sender 会话 seam。
//!
//! 握手、鉴权、`SessionConfig`、依赖构造和资源关闭由外层 orchestration 负责；本模块
//! 从文件列表发布开始运行到 Receiver 返回 `AllDone`。后续迁移会逐步把会话状态与转换
//! 收入此模块，调用者与测试始终只依赖这里的 interface。

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use data_mover::StorageEntryMessage;
use data_mover::dir_tree::NdxEvent;
use data_mover::qos::QosManager;
use data_mover::{ConsistencyCheck, EntryEnum, StorageEnum, WalkDirAsyncIterator2};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info};
use transport::message::{NdxTable, SenderMsg};
use transport::traits::SenderTransport;

use super::{AppError, Result, StatisticConsumer, process_requests_and_acks};

/// 构造 negotiated session 所需的既有 adapter 与配置。
pub(super) struct RemoteSenderSessionDeps<'a> {
    pub(super) transport: &'a (dyn SenderTransport + 'static),
    pub(super) src_storage: &'a Arc<StorageEnum>,
    pub(super) walkdir_iter: &'a WalkDirAsyncIterator2,
    pub(super) qos: Option<&'a QosManager>,
    pub(super) enable_acl: bool,
    pub(super) checkpoint_path: &'a Path,
    pub(super) stats_consumer: &'a Arc<AsyncMutex<StatisticConsumer>>,
}

/// 会话完成后交还给外层 orchestration 的终态事实。
#[derive(Debug)]
pub(super) struct RemoteSenderSessionSummary {
    pub(super) page_count: u64,
    pub(super) advertised_entries: usize,
    pub(super) transfer_count: u64,
    pub(super) success_count: u64,
    pub(super) error_count: u64,
}

/// 协商完成后的单个 Remote Sender 会话。
pub(super) struct RemoteSenderSession<'a> {
    deps: RemoteSenderSessionDeps<'a>,
    lifecycle: RemoteSenderSessionLifecycle,
}

/// source-transfer operation 的 typed terminal fact。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceTransferOutcome {
    Sent,
    SourceFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteSenderSessionLifecycle {
    Advertising,
    RequestsOpen,
    TransferDoneSent,
    Completed,
    Failed,
}

impl<'a> RemoteSenderSession<'a> {
    pub(super) fn new(deps: RemoteSenderSessionDeps<'a>) -> Self {
        Self {
            deps,
            lifecycle: RemoteSenderSessionLifecycle::Advertising,
        }
    }

    pub(super) async fn run(mut self, completed_paths: &mut HashSet<String>) -> Result<RemoteSenderSessionSummary> {
        let ndx_table = Mutex::new(NdxTable::new());
        self.transition(RemoteSenderSessionLifecycle::RequestsOpen);
        let advertise = async {
            advertise_file_list(
                self.deps.transport,
                self.deps.walkdir_iter,
                &ndx_table,
                self.deps.stats_consumer,
            )
            .await
            .map_err(|source| stage_error("advertising", source))
        };
        let requests = async {
            process_requests_and_acks(
                self.deps.transport,
                self.deps.src_storage,
                &ndx_table,
                self.deps.qos,
                self.deps.enable_acl,
                completed_paths,
                self.deps.checkpoint_path,
                self.deps.stats_consumer,
            )
            .await
            .map_err(|source| stage_error("requests/acks", source))
        };
        let joined = tokio::try_join!(advertise, requests);
        let (page_count, request_summary) = match joined {
            Ok(result) => result,
            Err(error) => {
                self.transition(RemoteSenderSessionLifecycle::Failed);
                return Err(error);
            }
        };
        if request_summary.transfer_done_sent {
            self.transition(RemoteSenderSessionLifecycle::TransferDoneSent);
        }
        self.transition(RemoteSenderSessionLifecycle::Completed);

        Ok(RemoteSenderSessionSummary {
            page_count,
            advertised_entries: ndx_table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            transfer_count: request_summary.transfer_count,
            success_count: request_summary.success_count,
            error_count: request_summary.error_count,
        })
    }

    fn transition(&mut self, next: RemoteSenderSessionLifecycle) {
        debug_assert!(matches!(
            (self.lifecycle, next),
            (
                RemoteSenderSessionLifecycle::Advertising,
                RemoteSenderSessionLifecycle::RequestsOpen
            ) | (
                RemoteSenderSessionLifecycle::RequestsOpen,
                RemoteSenderSessionLifecycle::TransferDoneSent
            ) | (
                RemoteSenderSessionLifecycle::TransferDoneSent,
                RemoteSenderSessionLifecycle::Completed
            ) | (
                RemoteSenderSessionLifecycle::RequestsOpen,
                RemoteSenderSessionLifecycle::Completed
            ) | (_, RemoteSenderSessionLifecycle::Failed)
        ));
        self.lifecycle = next;
    }
}

/// 发布文件列表并建立本会话唯一的 index correlation ledger。
///
/// 这是 session implementation 的内部操作；生产调用者只使用
/// [`RemoteSenderSession::run`]。
pub(super) async fn advertise_file_list(
    transport: &(dyn SenderTransport + 'static), walkdir_iter: &WalkDirAsyncIterator2, ndx_table: &Mutex<NdxTable>,
    stats_consumer: &Arc<AsyncMutex<StatisticConsumer>>,
) -> Result<u64> {
    info!("[Sender Remote] Sending file list");
    let mut page_count = 0u64;
    while let Some(event) = walkdir_iter.next().await {
        match event {
            NdxEvent::Page(page) => {
                {
                    let mut consumer = stats_consumer.lock().await;
                    for file in &page.files {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(file.entry.clone()));
                    }
                    for directory in &page.subdirs {
                        consumer.update_statistics(&StorageEntryMessage::Scanned(directory.entry.clone()));
                    }
                }
                ndx_table
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .ingest_page(&page);
                page_count += 1;
                transport.send(SenderMsg::FilePage(page)).await?;
            }
            NdxEvent::Error { path, reason } => {
                transport.send(SenderMsg::FileListError { path, reason }).await?;
            }
            NdxEvent::Done => break,
        }
    }
    transport.send(SenderMsg::FileListDone).await?;
    Ok(page_count)
}

/// 发送一个 full entry；目录、符号链接和 bounded file stream 共用一个 typed outcome。
pub(super) async fn send_full_transfer(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<EntryEnum>, ndx: i32,
    qos: Option<&QosManager>, enable_acl: bool,
) -> Result<SourceTransferOutcome> {
    let outcome = if entry.get_is_dir() {
        transport.send(SenderMsg::CreateDir { entry: entry.clone() }).await?;
        SourceTransferOutcome::Sent
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target) => {
                transport
                    .send(SenderMsg::CreateSymlink {
                        entry: entry.clone(),
                        target,
                    })
                    .await?;
                SourceTransferOutcome::Sent
            }
            Err(error) => {
                error!("[Sender Remote] read_symlink {:?}: {error}", entry.get_relative_path());
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason: error.to_string(),
                        ndx: Some(ndx),
                    })
                    .await?;
                SourceTransferOutcome::SourceFailed
            }
        }
    } else {
        let (mut chunks, read_join) = StorageEnum::read_chunk_stream(src_storage, entry, None, qos.cloned(), true, 8);
        transport
            .send(SenderMsg::FileBegin {
                ndx,
                entry: entry.clone(),
            })
            .await?;
        while let Some(chunk) = chunks.recv().await {
            transport
                .send(SenderMsg::FileData {
                    entry: entry.clone(),
                    chunk,
                })
                .await?;
        }
        let read_result = match read_join.await {
            Ok(inner) => inner.map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match read_result {
            Ok(hasher) => {
                transport
                    .send(SenderMsg::EndOfFile {
                        ndx,
                        entry: entry.clone(),
                        source_hash: hasher.map(ConsistencyCheck::finalize),
                    })
                    .await?;
                SourceTransferOutcome::Sent
            }
            Err(reason) => {
                error!("[Sender Remote] read file {:?}: {reason}", entry.get_relative_path());
                transport
                    .send(SenderMsg::EntryError {
                        path: entry.get_relative_path().to_path_buf(),
                        reason,
                        ndx: Some(ndx),
                    })
                    .await?;
                SourceTransferOutcome::SourceFailed
            }
        }
    };
    if outcome == SourceTransferOutcome::Sent {
        send_acl_if_enabled(transport, src_storage, entry, enable_acl).await;
    }
    Ok(outcome)
}

/// ACL 跨进程传输：仅在启用且 entry 不是符号链接时发送。
pub(super) async fn send_acl_if_enabled(
    transport: &(dyn SenderTransport + 'static), src_storage: &Arc<StorageEnum>, entry: &Arc<EntryEnum>,
    enable_acl: bool,
) {
    if enable_acl
        && !entry.get_is_symlink()
        && let Ok(Some(acl_data)) = src_storage.get_acl_bytes(entry.get_relative_path()).await
    {
        let _ = transport
            .send(SenderMsg::SetAcl {
                entry: entry.clone(),
                acl_data: bytes::Bytes::from(acl_data),
            })
            .await;
    }
}

fn stage_error(stage: &'static str, source: AppError) -> AppError {
    AppError::SenderSessionStage {
        stage,
        source: Box::new(source),
    }
}
