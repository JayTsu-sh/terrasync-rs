//! 复制轮次收尾：目录元数据全部确认后才允许数据库提交。

use crate::file_copy::metadata as copy_metadata;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle as ThreadHandle;

use crate::storage_factory::StorageContext;
use data_mover::model::{BackendKind, ObservedEntry};
use db::ClickHouseDatabase;
use db::catalogue::CopyCatalogueEntry;
use task_model::{ErrorEvent, ObservedEntryExt, StorageEntryMessage};
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::{Id, JoinError, JoinHandle, JoinSet};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{Instrument, info, info_span, trace, warn};

use crate::consumer::{TaskStatistics, stats::DirectoryMetadataProgressBar};
use crate::copy_execution::CopyDatabaseExecution;
use crate::copy_root::CopyRootMetadata;
use crate::error::{AppError, Result};

const DIRECTORY_CONCURRENCY: usize = 32;

struct DirectoryProgress {
    bar: DirectoryMetadataProgressBar,
    reporter: Option<ThreadHandle<()>>,
}

impl DirectoryProgress {
    fn new() -> Self {
        let bar = DirectoryMetadataProgressBar::new();
        let reporter = Some(bar.start());
        Self { bar, reporter }
    }
}

impl Drop for DirectoryProgress {
    fn drop(&mut self) {
        self.bar.finish();
        if let Some(reporter) = self.reporter.as_ref() {
            reporter.thread().unpark();
        }
        if let Some(reporter) = self.reporter.take()
            && let Err(error) = reporter.join()
        {
            warn!(?error, "Directory metadata progress reporter panicked");
        }
    }
}

pub(crate) async fn complete(
    database: ClickHouseDatabase, destination: Arc<StorageContext>, statistics: Option<TaskStatistics>,
    execution: Option<CopyDatabaseExecution>, root: Option<&CopyRootMetadata>,
) -> Result<()> {
    if let Some(execution) = &execution
        && execution.token().is_cancelled()
    {
        return Err(execution.stopped_error());
    }
    if let Some(root) = root {
        root.verify_target_binding(&destination).await?;
    }
    if destination.storage().kind() != BackendKind::S3 {
        update_directories(database.clone(), destination.clone(), statistics, execution.clone()).await?;
    }
    if let Some(execution) = &execution
        && execution.token().is_cancelled()
    {
        return Err(execution.stopped_error());
    }
    if let Some(root) = root {
        root.finish(&destination).await?;
    }
    database.commit_scan_generation().await?;
    Ok(())
}

async fn update_directories(
    database: ClickHouseDatabase, destination: Arc<StorageContext>, statistics: Option<TaskStatistics>,
    execution: Option<CopyDatabaseExecution>,
) -> Result<()> {
    let (sender, receiver) = mpsc::channel(1000);
    let query_execution = execution.clone();
    let query = tokio::spawn(
        async move {
            CopyDatabaseExecution::run_optional_child(query_execution, async move {
                database
                    .query_current_copy_entries(Some(true), None, None, sender)
                    .await?;
                info!("Query selected current-copy directories completed");
                Ok(())
            })
            .await
        }
        .instrument(info_span!("directory_metadata_query")),
    );
    apply_directories(query, receiver, destination, statistics, execution).await
}

async fn apply_directories(
    query: JoinHandle<Result<()>>, mut receiver: Receiver<CopyCatalogueEntry>, destination: Arc<StorageContext>,
    statistics: Option<TaskStatistics>, execution: Option<CopyDatabaseExecution>,
) -> Result<()> {
    // 调用方取消整个收尾 future 时，查询与进度线程也随所属执行域释放。
    let cancel = execution
        .as_ref()
        .map(|execution| execution.token().clone())
        .unwrap_or_default();
    let mut query = AbortOnDropHandle::new(query);
    let mut updates = DirectoryUpdates::new(destination, statistics, cancel.clone());
    let mut depth = None;
    loop {
        let completion = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                receiver.close();
                query.abort();
                break;
            }
            completion = receiver.recv() => match completion {
                Some(completion) => completion,
                None => break,
            }
        };
        let entry = completion.completion.item.entry;
        let entry_depth = entry.path().as_str().split('/').count();
        // 查询按深度倒序；父目录收紧权限前，必须完成所有更深目录。
        if depth.is_some_and(|previous| previous != entry_depth) {
            updates.drain().await;
        }
        depth = Some(entry_depth);
        updates.push(entry).await;
    }
    updates.drain().await;
    let query_result = tokio::select! {
        biased;
        () = cancel.cancelled() => {
            query.abort();
            match query.await {
                Err(error) if error.is_cancelled() => Ok(Ok(())),
                result => result,
            }
        }
        result = &mut query => result,
    };
    query_result.map_err(AppError::from)??;
    if let Some(execution) = &execution
        && cancel.is_cancelled()
    {
        return Err(execution.stopped_error());
    }
    updates.finish()
}

enum DirectoryUpdate {
    Applied,
    NotStarted,
}

/// 目录更新的在途状态和逐项诊断；不持有查询或消费者生命周期。
struct DirectoryUpdates {
    failure: Option<AppError>,
    paths: HashMap<Id, Arc<ObservedEntry>>,
    tasks: JoinSet<Result<DirectoryUpdate>>,
    progress: DirectoryProgress,
    destination: Arc<StorageContext>,
    statistics: Option<TaskStatistics>,
    cancel: CancellationToken,
}

impl DirectoryUpdates {
    fn new(destination: Arc<StorageContext>, statistics: Option<TaskStatistics>, cancel: CancellationToken) -> Self {
        let progress = DirectoryProgress::new();
        let tasks = JoinSet::new();
        // 与在途任务同生命周期，panic 也能定位目录；数量不超过并发上限。
        let paths = HashMap::with_capacity(DIRECTORY_CONCURRENCY);
        Self {
            failure: None,
            paths,
            tasks,
            progress,
            destination,
            statistics,
            cancel,
        }
    }

    async fn push(&mut self, entry: Arc<ObservedEntry>) {
        // 及时回收已结束任务，任务集合连同完成结果也保持有界。
        while self.tasks.len() >= DIRECTORY_CONCURRENCY {
            self.join_next().await;
        }
        // 等待深度屏障或并发槽期间可能取消；后续目录不再进入存储操作。
        if self.cancel.is_cancelled() {
            return;
        }
        let cancel = self.cancel.clone();
        let target = self.destination.clone();
        let task_entry = entry.clone();
        let task = self.tasks.spawn(async move {
            if cancel.is_cancelled() {
                return Ok(DirectoryUpdate::NotStarted);
            }
            trace!(path = ?entry.get_relative_path(), "Updating directory metadata");
            copy_metadata::set_entry_metadata(target.storage(), &entry, Some(cancel)).await?;
            Ok(DirectoryUpdate::Applied)
        });
        self.paths.insert(task.id(), task_entry);
    }

    async fn join_next(&mut self) -> bool {
        if let Some(result) = self.tasks.join_next_with_id().await {
            self.record_update(result).await;
            true
        } else {
            false
        }
    }

    async fn drain(&mut self) {
        while self.join_next().await {}
    }

    fn finish(self) -> Result<()> {
        self.failure.map_or(Ok(()), |source| {
            Err(if self.statistics.is_some() {
                AppError::ReportedCopyFailure {
                    source: Box::new(source),
                    data_failed: false,
                }
            } else {
                source
            })
        })
    }

    async fn record_update(&mut self, result: std::result::Result<(Id, Result<DirectoryUpdate>), JoinError>) {
        let (id, result) = match result {
            Ok((id, result)) => (id, result),
            Err(error) => (error.id(), Err(error.into())),
        };
        let entry = self.paths.remove(&id);
        match result {
            Ok(DirectoryUpdate::Applied) => {
                self.progress.bar.increment_dir_count();
            }
            Ok(DirectoryUpdate::NotStarted) => {}
            Err(error) => {
                // 所有最终失败逐项交付；返回值仍保留首个有类型原因，不累计整轮明细。
                if let (Some(statistics), Some(entry)) = (&self.statistics, entry) {
                    let backend = self.destination.storage().kind();
                    statistics
                        .record_statistics([StorageEntryMessage::Error {
                            event: ErrorEvent::Copy,
                            path: entry.get_relative_path().to_path_buf(),
                            entry: Some(entry),
                            reason: format!("backend={backend:?} phase=directory_metadata_finalize: {error}"),
                        }])
                        .await;
                }
                self.failure.get_or_insert(error);
            }
        }
    }
}

#[cfg(test)]
#[path = "copy_completion_tests.rs"]
mod tests;
