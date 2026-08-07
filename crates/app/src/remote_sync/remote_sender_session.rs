//! 协商完成后的 Remote Sender 会话 seam。
//!
//! 握手、鉴权、`SessionConfig`、依赖构造和资源关闭由外层 orchestration 负责；本模块
//! 从文件列表发布开始运行到 Receiver 返回 `AllDone`。后续迁移会逐步把会话状态与转换
//! 收入此模块，调用者与测试始终只依赖这里的 interface。

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use data_mover::qos::QosManager;
use data_mover::{StorageEnum, WalkDirAsyncIterator2};
use tokio::sync::Mutex as AsyncMutex;
use transport::message::NdxTable;
use transport::traits::SenderTransport;

use super::{Result, StatisticConsumer, process_requests_and_acks, send_file_list_phase};

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
}

impl<'a> RemoteSenderSession<'a> {
    pub(super) fn new(deps: RemoteSenderSessionDeps<'a>) -> Self {
        Self { deps }
    }

    pub(super) async fn run(self, completed_paths: &mut HashSet<String>) -> Result<RemoteSenderSessionSummary> {
        let ndx_table = Mutex::new(NdxTable::new());
        let (page_count, (transfer_count, success_count, error_count)) = tokio::try_join!(
            send_file_list_phase(
                self.deps.transport,
                self.deps.walkdir_iter,
                &ndx_table,
                self.deps.stats_consumer,
            ),
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
        )?;

        Ok(RemoteSenderSessionSummary {
            page_count,
            advertised_entries: ndx_table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            transfer_count,
            success_count,
            error_count,
        })
    }
}
