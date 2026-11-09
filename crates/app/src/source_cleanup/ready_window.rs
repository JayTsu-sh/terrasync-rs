//! 连接逐项对象清理与整轮成功证明；其他路线保持有界窗口及数据库收尾，不落本地磁盘。
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, OnceLock},
};

use data_mover::model::{EntryKind, ModelValueError, ObservedEntry};
use task_model::StorageEntryMessage;
use tokio_util::task::AbortOnDropHandle;

use super::ready_files::can_stream_files;
use super::{CompletedChecks, ReadyFileCleanup, SourceCleanupCheck, remove_entry};
use crate::broadcast::BroadcastForwarder;
use crate::copy_execution::CopyDatabaseExecution;
use crate::error::{AppError, CopyStep, Result, StorageError};
use crate::file_copy::selection::unrepresentable_object;
use crate::storage_factory::StorageContext;

const MAX_ENTRIES: usize = 64;
const MAX_BYTES: usize = 8 * 1024 * 1024;

type ConnectedStorages = (Arc<StorageContext>, Arc<StorageContext>);
pub(crate) type CleanupConnections = Arc<OnceLock<ConnectedStorages>>;

pub(crate) struct FullCleanupStage {
    connections: CleanupConnections,
    collector: AbortOnDropHandle<ReadyWindow>,
    ready_files: Option<Arc<ReadyFileCleanup>>,
}

#[derive(Default)]
pub(crate) struct EarlyCleanup {
    pub(crate) connections: Option<ConnectedStorages>,
    pub(crate) checked: CompletedChecks,
    pub(crate) streamed_files: bool,
}

impl FullCleanupStage {
    pub(crate) fn start(
        broadcaster: &mut BroadcastForwarder<StorageEntryMessage>, concurrency: usize,
        streaming: Option<(SourceCleanupCheck, CopyDatabaseExecution)>,
    ) -> Self {
        let connections = Arc::new(OnceLock::<ConnectedStorages>::new());
        let ready_files = streaming.map(|(check, execution)| Arc::new(ReadyFileCleanup::new(check, execution)));
        let can_stream = ready_files.is_some();
        let observed_connections = connections.clone();
        let mut receiver = broadcaster.subscribe();
        let collector = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut window = ReadyWindow::new(concurrency);
            let mut streaming = None;
            while let Some(message) = receiver.recv().await {
                if streaming.is_none() {
                    streaming = observed_connections
                        .get()
                        .map(|(source, target)| can_stream && can_stream_files(source, target));
                }
                if streaming == Some(true) {
                    window.disable();
                } else {
                    window.observe(message);
                }
            }
            window
        }));
        Self {
            connections,
            collector,
            ready_files,
        }
    }

    pub(crate) fn connections(&self) -> CleanupConnections {
        self.connections.clone()
    }

    pub(crate) fn ready_files(&self) -> Option<Arc<ReadyFileCleanup>> {
        self.ready_files.clone()
    }

    /// 必须先关闭广播，并证明扫描、复制及逐项清理成功；数据库消费者可以仍在冲刷。
    pub(crate) async fn complete(self, copy_ready: bool, check: SourceCleanupCheck) -> Result<EarlyCleanup> {
        let mut window = self.collector.await.map_err(AppError::from)?;
        if let Some(error) = self.ready_files.as_ref().and_then(|cleanup| cleanup.failure()) {
            return Err(error);
        }
        if let Some(error) = window.failure.take() {
            return Err(error);
        }
        if copy_ready
            && self.ready_files.is_some()
            && let Some((source, target)) = self
                .connections
                .get()
                .filter(|(source, target)| can_stream_files(source, target))
        {
            // 全部普通文件 ACK 已经过同一个就绪清理入口；无需按总量常驻记录已完成路径。
            return Ok(EarlyCleanup {
                connections: Some((source.clone(), target.clone())),
                streamed_files: true,
                ..Default::default()
            });
        }
        if !copy_ready || window.disabled || window.entries.is_empty() {
            return Ok(EarlyCleanup::default());
        }
        let (source, target) = self
            .connections
            .get()
            .ok_or_else(|| AppError::ConfigError("已复制条目缺少本执行存储上下文，保留源端".into()))?;
        let mut completed = EarlyCleanup {
            connections: Some((source.clone(), target.clone())),
            ..Default::default()
        };
        for (entry, _) in window.entries {
            if entry.path().as_str().is_empty() || unrepresentable_object(&entry, target).is_some() {
                continue;
            }
            // 首个失败后不发新删除；在途只有本项，其未知结果仍带原始路径返回。
            remove_entry(source, target, &entry, &check, None)
                .await
                .map_err(|source| {
                    AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(entry.path().as_str()), source)
                })?;
            completed.checked.insert(entry.path().clone(), entry.identity_key());
        }
        Ok(completed)
    }
}

struct ReadyWindow {
    entries: VecDeque<(Arc<ObservedEntry>, usize)>,
    bytes: usize,
    limit: usize,
    attempts: usize,
    disabled: bool,
    failure: Option<AppError>,
}

impl ReadyWindow {
    fn new(concurrency: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            limit: concurrency.clamp(1, MAX_ENTRIES),
            attempts: 0,
            disabled: false,
            failure: None,
        }
    }

    fn observe(&mut self, message: StorageEntryMessage) {
        if self.disabled {
            return;
        }
        let item = match message {
            StorageEntryMessage::New(item) => item,
            StorageEntryMessage::CopyEntryCompleted { .. }
            | StorageEntryMessage::CopyItemCompleted { .. }
            | StorageEntryMessage::CopyEntryBoundary(_) => {
                self.disable();
                return;
            }
            _ => return,
        };
        if item.entry.kind() == EntryKind::Directory || item.entry.history().is_some() || item.version_index.is_some() {
            self.disable();
            return;
        }
        // 只对固定窗口尝试编码；普通大迁移不会逐 entry 增加一次序列化。
        if self.attempts >= self.limit {
            return;
        }
        self.attempts += 1;
        let weight = match observation_weight(&item.entry) {
            Ok(weight) => weight,
            Err(error) => {
                self.failure = Some(AppError::copy_entry_failure(
                    CopyStep::SourceCleanup,
                    Path::new(item.entry.path().as_str()),
                    StorageError::from(error),
                ));
                self.disable();
                return;
            }
        };
        if self.bytes.saturating_add(weight) > MAX_BYTES {
            return;
        }
        self.bytes += weight;
        self.entries.push_back((item.entry, weight));
    }

    fn disable(&mut self) {
        self.disabled = true;
        self.entries.clear();
        self.bytes = 0;
    }
}

/// 复用不透明快照接口计入全部原值，保守预留容器余量；临时编码立即释放，不保留或落盘。
fn observation_weight(entry: &ObservedEntry) -> std::result::Result<usize, ModelValueError> {
    Ok(entry
        .try_encode_snapshot()?
        .as_bytes()
        .len()
        .saturating_mul(4)
        .saturating_add(1024))
}

#[cfg(test)]
#[path = "ready_window_tests.rs"]
mod tests;
