//! 无目录枚举／属性屏障的对象文件：复制 ACK 后受保护清理，不等待数据库冲刷。
use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

use data_mover::model::{EntryKind, ObservedEntry};
use tokio::sync::Mutex;

use super::{SourceCleanupCheck, remove_entry};
use crate::{
    copy_execution::CopyDatabaseExecution,
    error::{AppError, CopyStep, Result, StorageError},
    file_copy::CopiedPublication,
    storage_factory::StorageContext,
};

pub(crate) struct ReadyFileCleanup {
    check: SourceCleanupCheck,
    execution: CopyDatabaseExecution,
    serial: Mutex<()>,
    failure: OnceLock<Arc<AppError>>,
}

pub(crate) fn can_stream_files(source: &StorageContext, destination: &StorageContext) -> bool {
    source.storage().holds_directory_objects()
        && destination.storage().holds_directory_objects()
        && source
            .storage()
            .namespace()
            .is_ok_and(|namespace| namespace.allows_paged_delete())
}

pub(super) fn ordinary_file(entry: &ObservedEntry) -> bool {
    entry.kind() == EntryKind::File && entry.history().is_none()
}

impl ReadyFileCleanup {
    pub(super) fn new(check: SourceCleanupCheck, execution: CopyDatabaseExecution) -> Self {
        Self {
            check,
            execution,
            serial: Mutex::new(()),
            failure: OnceLock::new(),
        }
    }

    pub(super) fn failure(&self) -> Option<AppError> {
        self.failure
            .get()
            .map(|source| AppError::CopyExecutionStopped { source: source.clone() })
    }

    pub(crate) async fn remove(
        &self, source: &Arc<StorageContext>, destination: &Arc<StorageContext>, entry: &Arc<ObservedEntry>,
        published: Option<CopiedPublication>,
    ) -> Result<()> {
        if !ordinary_file(entry) || !can_stream_files(source, destination) {
            return Ok(());
        }
        // 延续既有清理链的一项在途上限；不新增完成路径集合、待删除队列或逐项数据库回执。
        // guard 位于执行包装外，先保存最初类型并取消兄弟任务，再唤醒下一个等待者。
        let _guard = tokio::select! {
            biased;
            () = self.check.cancel.cancelled() => return Err(StorageError::Cancelled.into()),
            guard = self.serial.lock() => guard,
        };
        self.execution
            .run_child(async {
                if let Err(source) = remove_entry(source, destination, entry, &self.check, published).await {
                    let error =
                        AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(entry.path().as_str()), source);
                    // 全局最初原因可能来自数据库；单独保留唯一在途清理的实际原因及未知结果。
                    let source = self.failure.get_or_init(|| Arc::new(error)).clone();
                    return Err(AppError::CopyExecutionStopped { source });
                }
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
#[path = "ready_files_tests.rs"]
mod tests;
