//! 单进程文件复制的任务参数与字节进度收尾。

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use data_mover::model::{BackendKind, ObservedEntry};
use task_model::{ObservedEntryExt, TransferMode};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::warn;

use super::{CopiedPublication, CopyOptions, HdfsCopyContext, copy_file, transfer};
use crate::byte_resume::{ByteResumeStore, CommitCallback};
use crate::error::{AppError, Result};
use crate::storage_factory::StorageContext;

/// 全量和增量沿用同一传输策略以及任务进度收尾。
pub(crate) async fn copy_file_for_job(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, options: CopyOptions,
    job_id: &str, job_dir: &str, regular_copy_error: impl FnOnce(AppError) -> AppError,
) -> Result<Option<CopiedPublication>> {
    if destination.storage().kind() == BackendKind::Hdfs {
        let mode = options.transfer_mode;
        let mut hdfs = HdfsCopyContext::from_job(job_id, mode)?;
        let recorder = if mode == TransferMode::Checkpoint {
            let recorder = CommitRecorder::start(job_dir, entry, source.block_size()).await?;
            hdfs = hdfs.with_commit_callback(recorder.callback());
            Some(recorder)
        } else {
            None
        };
        let result = copy_file(source, destination, entry, options, Some(&hdfs)).await;
        return match recorder {
            Some(recorder) => recorder.finish(result).await,
            None => result,
        };
    }
    if should_record_resume(options.transfer_mode, job_dir, entry, source, destination) {
        return copy_file_with_resume(entry, source, destination, options, job_dir).await;
    }
    copy_file(source, destination, entry, options, None)
        .await
        .map_err(regular_copy_error)
}

pub(crate) struct CommitRecorder {
    store: Arc<ByteResumeStore>,
    callback: CommitCallback,
    drain: JoinHandle<()>,
    shutdown: oneshot::Sender<()>,
    dropped: Arc<AtomicU64>,
    path: PathBuf,
}

impl CommitRecorder {
    pub(crate) async fn start(job_dir: &str, entry: &ObservedEntry, block_size: u64) -> Result<Self> {
        if job_dir.trim().is_empty() {
            return Err(AppError::ConfigError(
                "recoverable copy requires a persistent job directory".to_string(),
            ));
        }
        let path = entry.get_relative_path().to_path_buf();
        let store = ByteResumeStore::open(job_dir, &path, entry.get_size(), entry.get_mtime(), block_size).await?;
        const CAPACITY: usize = 65_536;
        let (tx, mut rx) = mpsc::channel::<(u64, u64)>(CAPACITY);
        let store_task = store.clone();
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let drain = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        rx.close();
                        while let Some((offset, length)) = rx.recv().await {
                            if let Err(error) = store_task.mark_committed(offset, length).await {
                                warn!("byte-resume mark_committed failed: {error}");
                            }
                        }
                        break;
                    }
                    message = rx.recv() => match message {
                        Some((offset, length)) => {
                            if let Err(error) = store_task.mark_committed(offset, length).await {
                                warn!("byte-resume mark_committed failed: {error}");
                            }
                        }
                        None => break,
                    }
                }
            }
        });
        let dropped = Arc::new(AtomicU64::new(0));
        let callback_dropped = dropped.clone();
        let callback = Arc::new(move |offset, length| {
            if tx.try_send((offset, length)).is_err() {
                callback_dropped.fetch_add(1, Ordering::Relaxed);
            }
        });
        Ok(Self {
            store,
            callback,
            drain,
            shutdown,
            dropped,
            path,
        })
    }

    pub(crate) fn callback(&self) -> CommitCallback {
        self.callback.clone()
    }

    pub(crate) async fn finish<T>(self, result: Result<T>) -> Result<T> {
        drop(self.callback);
        let _ = self.shutdown.send(());
        let _ = self.drain.await;
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            warn!(
                "byte-resume: {dropped} commit records were dropped for {}; retry remains conservative",
                self.path.display()
            );
        }
        match result {
            Ok(value) => {
                self.store.finalize_done().await?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.store.flush().await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod commit_recorder_tests {
    use crate::scan::observe_path;
    use std::path::Path;
    use std::time::Duration;

    use crate::storage_factory::{CreateStorageOptions, create_storage};

    use super::CommitRecorder;

    #[tokio::test]
    async fn finish_does_not_wait_for_external_callback_clones() {
        let source_root = tempfile::tempdir().unwrap_or_else(|error| panic!("source tempdir: {error}"));
        let job_root = tempfile::tempdir().unwrap_or_else(|error| panic!("job tempdir: {error}"));
        tokio::fs::write(source_root.path().join("payload.bin"), b"payload")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let source = create_storage(
            source_root
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("source path is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create source storage: {error}"));
        let entry = observe_path(source.storage(), Path::new("payload.bin"))
            .await
            .unwrap_or_else(|error| panic!("source metadata: {error}"));
        let recorder = CommitRecorder::start(
            job_root
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("job path is not UTF-8")),
            &entry,
            4,
        )
        .await
        .unwrap_or_else(|error| panic!("start recorder: {error}"));
        let external_callback = recorder.callback();
        external_callback(0, 4);

        tokio::time::timeout(Duration::from_secs(1), recorder.finish(Ok(())))
            .await
            .unwrap_or_else(|error| panic!("finish must use explicit shutdown: {error}"))
            .unwrap_or_else(|error| panic!("finish recorder: {error}"));

        drop(external_callback);
    }

    #[tokio::test]
    async fn start_rejects_missing_persistent_job_directory() {
        let source_root = tempfile::tempdir().unwrap_or_else(|error| panic!("source tempdir: {error}"));
        tokio::fs::write(source_root.path().join("payload.bin"), b"payload")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let source = create_storage(
            source_root
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("source path is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create source storage: {error}"));
        let entry = observe_path(source.storage(), Path::new("payload.bin"))
            .await
            .unwrap_or_else(|error| panic!("source metadata: {error}"));

        assert!(CommitRecorder::start("", &entry, 4).await.is_err());
        assert!(CommitRecorder::start("   ", &entry, 4).await.is_err());
    }
}

/// 该存储是否为 S3。
fn is_s3(storage: &StorageContext) -> bool {
    storage.storage().kind() == BackendKind::S3
}

/// 该 entry 是否应走字节级断点续传：启用 + 多块大文件（全后端：Local/NFS/CIFS/S3）。
///
/// - **源端 S3**：data-mover `read_data_intervals` 以 Range GET 只读缺失区间。
/// - **目标端 S3**：走 multipart part 粒度续传——进度真值是目标端 in-progress
///   multipart upload（ListParts 反推缺失区间），本地状态文件仅作进度参考；
///   额外要求 size > 目标端 block_size（part 大小下限），单 part 文件续传无意义
///   （中断即全丢），走普通拷贝即可。
/// - **S3 多版本路径**：版本化源固定 version_id，并遵循同一显式策略。
fn should_record_resume(
    mode: TransferMode, job_dir: &str, entry: &ObservedEntry, src: &StorageContext, dest: &StorageContext,
) -> bool {
    mode == TransferMode::Checkpoint
        && !job_dir.is_empty()
        && !entry.get_is_dir()
        && !entry.get_is_symlink()
        && entry.get_size() > src.block_size()
        && (!is_s3(dest) || entry.get_size() > dest.block_size())
}

/// 以原生 Checkpoint 策略复制大文件；恢复路径与已确认范围只由目标引擎发现。
/// 本地记录器保留现有产品进度收尾，不向引擎提供 `.part` 路径或缺失区间。
async fn copy_file_with_resume(
    entry: &Arc<ObservedEntry>, src_storage: &StorageContext, dest_storage: &StorageContext, options: CopyOptions,
    job_dir: &str,
) -> Result<Option<CopiedPublication>> {
    let recorder = CommitRecorder::start(job_dir, entry, src_storage.block_size()).await?;

    let res = transfer::copy_checkpointed(
        src_storage,
        dest_storage,
        entry,
        CopyOptions {
            transfer_mode: TransferMode::Checkpoint,
            ..options
        },
    )
    .await;

    let result = res.map_err(AppError::from);
    recorder.finish(result).await
}
