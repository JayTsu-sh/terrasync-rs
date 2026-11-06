use crate::error::storage::{Result, StorageError, operation};
use crate::storage_factory::path;
use data_mover::model::*;
use data_mover::storage::Storage;
use std::path::{Path, PathBuf};
use task_model::ObservedEntryExt;
use tokio_util::sync::CancellationToken;

use crate::task_stream::AsyncReceiver;
use data_mover::storage::{
    DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeTerminalFailure, StorageRoleFailure, delete_tree,
};
use std::num::NonZeroUsize;
pub async fn delete_dir_all_with_progress(
    storage: &Storage, sub: Option<&Path>, concurrency: usize,
) -> Result<AsyncReceiver<DeleteEvent>> {
    let mut session = delete_tree(
        storage,
        DeleteTreeRequest {
            root: path(sub.unwrap_or(Path::new("")))?,
            delete_root: true,
            max_inflight_operations: NonZeroUsize::new(concurrency).unwrap_or(NonZeroUsize::MIN),
            max_buffered_items: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
            cancel: CancellationToken::new(),
        },
    )
    .map_err(StorageError::from)?;
    let (tx, rx) = async_channel::bounded(128);
    tokio::spawn(async move {
        while let Some(item) = session.next_item().await {
            let event = match item {
                DeleteTreeItem::Deleted { path, kind } => DeleteEvent {
                    relative_path: path.as_str().into(),
                    is_dir: kind == EntryKind::Directory,
                    error: None,
                    failure: None,
                },
                DeleteTreeItem::EntryFailure(f) => DeleteEvent {
                    relative_path: f.path().as_str().into(),
                    is_dir: false,
                    error: Some(f.to_string()),
                    failure: Some(DeleteEventFailure::Entry(f)),
                },
            };
            if tx.send(event).await.is_err() {
                return;
            }
        }
        let result = session.finish().await;
        if !matches!(&result, Ok(DeleteTreeOutcome::Completed(_))) {
            let _ = tx
                .send(DeleteEvent {
                    relative_path: PathBuf::new(),
                    is_dir: false,
                    error: Some("recursive deletion did not complete".into()),
                    failure: match result {
                        Ok(DeleteTreeOutcome::Cancelled) => Some(DeleteEventFailure::Cancelled),
                        Err(source) => Some(DeleteEventFailure::Terminal(source)),
                        Ok(DeleteTreeOutcome::Completed(_)) => None,
                    },
                })
                .await;
        }
    });
    Ok(AsyncReceiver::new(rx))
}

pub async fn delete_dir_all(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    let events = delete_dir_all_with_progress(storage, Some(entry.get_relative_path()), 4).await?;
    let mut failure = None;
    while let Some(event) = events.next().await {
        if let Err(error) = event.into_deleted() {
            failure.get_or_insert(error);
        }
    }
    // channel EOF 在 native finish 之后，所有在途删除均已收束。
    failure.map_or(Ok(()), Err)
}

#[derive(Debug, Clone)]
pub struct DeleteEvent {
    pub relative_path: PathBuf,
    pub is_dir: bool,
    pub error: Option<String>,
    // 保留删除引擎的有类型原因，供任务收尾判定。
    failure: Option<DeleteEventFailure>,
}

#[derive(Debug, Clone)]
enum DeleteEventFailure {
    Cancelled,
    Entry(EntryOperationFailure),
    Terminal(DeleteTreeTerminalFailure),
}

impl DeleteEvent {
    /// 只把实际删除证据交给进度计数；失败保留原生原因并阻止成功收尾。
    pub(crate) fn into_deleted(self) -> Result<(PathBuf, bool)> {
        match self.failure {
            Some(DeleteEventFailure::Entry(source)) => return Err(StorageRoleFailure::Entry(source).into()),
            Some(DeleteEventFailure::Terminal(source)) => return Err(source.into()),
            Some(DeleteEventFailure::Cancelled) => return Err(StorageError::Cancelled),
            None => {}
        }
        if let Some(error) = self.error {
            return Err(operation(error));
        }
        Ok((self.relative_path, self.is_dir))
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
