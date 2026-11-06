//! 复制轮次收尾：目录元数据全部确认后才允许数据库提交。

use std::sync::Arc;
use std::thread::JoinHandle as ThreadHandle;

use data_mover::model::BackendKind;
use db::ClickHouseDatabase;
use storage_adapter::{ObservedEntry, StorageContext};
use task_model::ObservedEntryExt;
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info, info_span, trace, warn};

use crate::consumer::stats::DirectoryMetadataProgressBar;
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

pub(crate) async fn complete(database: ClickHouseDatabase, destination: Arc<StorageContext>) -> Result<()> {
    if destination.storage().kind() != BackendKind::S3 {
        update_directories(database.clone(), destination).await?;
    }
    database.commit_scan_generation().await?;
    Ok(())
}

async fn update_directories(database: ClickHouseDatabase, destination: Arc<StorageContext>) -> Result<()> {
    let (sender, receiver) = mpsc::channel(1000);
    let query = tokio::spawn(
        async move {
            database.query_storage_entry(Some(true), None, None, sender).await?;
            info!("Query all directories completed");
            Ok(())
        }
        .instrument(info_span!("directory_metadata_query")),
    );
    apply_directories(query, receiver, destination).await
}

async fn apply_directories(
    query: JoinHandle<Result<()>>, mut receiver: Receiver<ObservedEntry>, destination: Arc<StorageContext>,
) -> Result<()> {
    // 调用方取消整个收尾 future 时，查询与进度线程也随所属执行域释放。
    let query = AbortOnDropHandle::new(query);
    let progress = DirectoryProgress::new();
    let mut tasks = JoinSet::new();
    let mut failure = None;
    while let Some(entry) = receiver.recv().await {
        // 及时回收已结束任务，任务集合连同完成结果也保持有界。
        while tasks.len() >= DIRECTORY_CONCURRENCY {
            if let Some(result) = tasks.join_next().await {
                record_update(result, &mut failure, &progress.bar);
            }
        }
        let target = destination.clone();
        tasks.spawn(async move {
            trace!(path = ?entry.get_relative_path(), "Updating directory metadata");
            target.set_entry_metadata(&entry).await?;
            Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        record_update(result, &mut failure, &progress.bar);
    }
    let query_result = query.await.map_err(AppError::from).and_then(|result| result);
    query_result?;
    failure.map_or(Ok(()), Err)
}

fn record_update(
    result: std::result::Result<Result<()>, JoinError>, failure: &mut Option<AppError>,
    progress: &DirectoryMetadataProgressBar,
) {
    match result {
        Ok(Ok(())) => {
            progress.increment_dir_count();
        }
        Ok(Err(error)) => {
            failure.get_or_insert(error);
        }
        Err(error) => {
            failure.get_or_insert(error.into());
        }
    }
}

#[cfg(test)]
#[path = "copy_completion_tests.rs"]
mod tests;
