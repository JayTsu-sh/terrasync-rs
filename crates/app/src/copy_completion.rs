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
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info, info_span, trace, warn};

use crate::consumer::{TaskStatistics, stats::DirectoryMetadataProgressBar};
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
        if let Some(reporter) = self.reporter.take()
            && let Err(error) = reporter.join()
        {
            warn!(?error, "Directory metadata progress reporter panicked");
        }
    }
}

pub(crate) async fn complete(
    database: ClickHouseDatabase, destination: Arc<StorageContext>, statistics: Option<TaskStatistics>,
) -> Result<()> {
    if destination.storage().kind() != BackendKind::S3 {
        update_directories(database.clone(), destination, statistics).await?;
    }
    database.commit_scan_generation().await?;
    Ok(())
}

async fn update_directories(
    database: ClickHouseDatabase, destination: Arc<StorageContext>, statistics: Option<TaskStatistics>,
) -> Result<()> {
    let (sender, receiver) = mpsc::channel(1000);
    let query = tokio::spawn(
        async move {
            database
                .query_current_copy_entries(Some(true), None, None, sender)
                .await?;
            info!("Query selected current-copy directories completed");
            Ok(())
        }
        .instrument(info_span!("directory_metadata_query")),
    );
    apply_directories(query, receiver, destination, statistics).await
}

async fn apply_directories(
    query: JoinHandle<Result<()>>, mut receiver: Receiver<CopyCatalogueEntry>, destination: Arc<StorageContext>,
    statistics: Option<TaskStatistics>,
) -> Result<()> {
    // 调用方取消整个收尾 future 时，查询与进度线程也随所属执行域释放。
    let query = AbortOnDropHandle::new(query);
    let progress = DirectoryProgress::new();
    let mut tasks = JoinSet::new();
    // 与在途任务同生命周期，panic 也能定位目录；数量不超过并发上限。
    let mut paths = HashMap::with_capacity(DIRECTORY_CONCURRENCY);
    let mut failure = None;
    let mut depth = None;
    while let Some(completion) = receiver.recv().await {
        let entry = completion.completion.item.entry;
        let entry_depth = entry.path().as_str().split('/').count();
        // 查询按深度倒序；父目录收紧权限前，必须完成所有更深目录。
        if depth.is_some_and(|previous| previous != entry_depth) {
            while let Some(result) = tasks.join_next_with_id().await {
                record_update(
                    result,
                    &mut paths,
                    &mut failure,
                    &progress.bar,
                    destination.storage().kind(),
                    statistics.as_ref(),
                )
                .await;
            }
        }
        depth = Some(entry_depth);
        // 及时回收已结束任务，任务集合连同完成结果也保持有界。
        while tasks.len() >= DIRECTORY_CONCURRENCY {
            if let Some(result) = tasks.join_next_with_id().await {
                record_update(
                    result,
                    &mut paths,
                    &mut failure,
                    &progress.bar,
                    destination.storage().kind(),
                    statistics.as_ref(),
                )
                .await;
            }
        }
        let target = destination.clone();
        let task_entry = entry.clone();
        let task = tasks.spawn(async move {
            trace!(path = ?entry.get_relative_path(), "Updating directory metadata");
            copy_metadata::set_entry_metadata(target.storage(), &entry, None).await?;
            Ok(())
        });
        paths.insert(task.id(), task_entry);
    }
    while let Some(result) = tasks.join_next_with_id().await {
        record_update(
            result,
            &mut paths,
            &mut failure,
            &progress.bar,
            destination.storage().kind(),
            statistics.as_ref(),
        )
        .await;
    }
    let query_result = query.await.map_err(AppError::from).and_then(|result| result);
    query_result?;
    failure.map_or(Ok(()), |source| {
        Err(if statistics.is_some() {
            AppError::ReportedCopyFailure {
                source: Box::new(source),
                data_failed: false,
            }
        } else {
            source
        })
    })
}

async fn record_update(
    result: std::result::Result<(Id, Result<()>), JoinError>, paths: &mut HashMap<Id, Arc<ObservedEntry>>,
    failure: &mut Option<AppError>, progress: &DirectoryMetadataProgressBar, backend: BackendKind,
    statistics: Option<&TaskStatistics>,
) {
    let (id, result) = match result {
        Ok((id, result)) => (id, result),
        Err(error) => (error.id(), Err(error.into())),
    };
    let entry = paths.remove(&id);
    match result {
        Ok(()) => {
            progress.increment_dir_count();
        }
        Err(error) => {
            // 所有最终失败逐项交付；返回值仍保留首个有类型原因，不累计整轮明细。
            if let (Some(statistics), Some(entry)) = (statistics, entry) {
                statistics
                    .record_statistics([StorageEntryMessage::Error {
                        event: ErrorEvent::Copy,
                        path: entry.get_relative_path().to_path_buf(),
                        entry: Some(entry),
                        reason: format!("backend={backend:?} phase=directory_metadata_finalize: {error}"),
                    }])
                    .await;
            }
            failure.get_or_insert(error);
        }
    }
}

#[cfg(test)]
#[path = "copy_completion_tests.rs"]
mod tests;
