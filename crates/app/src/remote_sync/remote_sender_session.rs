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
use data_mover::{StorageEnum, WalkDirAsyncIterator2};
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
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

fn stage_error(stage: &'static str, source: AppError) -> AppError {
    AppError::SenderSessionStage {
        stage,
        source: Box::new(source),
    }
}
