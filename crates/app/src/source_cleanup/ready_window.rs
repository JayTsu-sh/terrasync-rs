//! 无目录收尾时先清理有界就绪窗口；其他事实继续由数据库流式收尾，不暂存扫描到本地磁盘。
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, OnceLock},
};

use data_mover::model::{EntryKind, ModelValueError, ObservedEntry};
use task_model::StorageEntryMessage;
use tokio_util::task::AbortOnDropHandle;

use super::{CompletedChecks, SourceCleanupCheck, remove_entry};
use crate::broadcast::BroadcastForwarder;
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
}

#[derive(Default)]
pub(crate) struct EarlyCleanup {
    pub(crate) connections: Option<ConnectedStorages>,
    pub(crate) checked: CompletedChecks,
}

impl FullCleanupStage {
    pub(crate) fn start(broadcaster: &mut BroadcastForwarder<StorageEntryMessage>, concurrency: usize) -> Self {
        let mut receiver = broadcaster.subscribe();
        let collector = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut window = ReadyWindow::new(concurrency);
            while let Some(message) = receiver.recv().await {
                window.observe(message);
            }
            window
        }));
        Self {
            connections: Arc::new(OnceLock::new()),
            collector,
        }
    }

    pub(crate) fn connections(&self) -> CleanupConnections {
        self.connections.clone()
    }

    /// 必须先关闭广播，并证明扫描及全部复制成功；数据库消费者可以仍在冲刷。
    pub(crate) async fn complete(self, copy_ready: bool, check: SourceCleanupCheck) -> Result<EarlyCleanup> {
        let mut window = self.collector.await.map_err(AppError::from)?;
        if let Some(error) = window.failure.take() {
            return Err(error);
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
            remove_entry(source, target, &entry, &check).await.map_err(|source| {
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
