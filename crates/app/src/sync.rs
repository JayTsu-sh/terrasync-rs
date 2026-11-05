//! 文件复制模块
//!
//! 该模块提供文件复制和增量复制的核心功能，包括：
//! 1. 全量文件复制
//! 2. 增量文件复制（基于数据库记录）
//! 3. 存储资源管理
//! 4. 并发复制控制
//! 5. 重试机制集成

use data_mover::model::BackendKind;
// 标准库
use crate::file_copy::{HdfsCopyContext, copy_file, copy_versions, prepare_directory};
use std::iter::once;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use task_model::ObservedEntryExt;
use task_model::observation::ObservedItem;
use task_model::{EntryChange, TransferMode};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

// 外部crate
use storage_adapter::qos::QosManager;
use tracing::{Instrument, debug, error, info, info_span, instrument, trace, warn};
#[cfg(windows)]
use windows_sys::Win32::Foundation::CloseHandle;
#[cfg(windows)]
use windows_sys::Win32::Security::GetTokenInformation;
#[cfg(windows)]
use windows_sys::Win32::Security::TOKEN_QUERY;
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
use {
    storage_adapter::{CopyOptions, ObservedEntry, StorageContext, redact_storage_url},
    task_model::{ErrorEvent, StorageEntryMessage},
};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::byte_resume::{ByteResumeStore, part_path_for};
use crate::error::{AppError, Result};
use crate::storage_factory::{StorageRole, create_storage_for_role};

/// 存储资源管理器 - 统一管理源和目标存储实例
///
/// 该结构体封装了源存储和目标存储的创建、管理和访问功能，
/// 提供了简洁的接口来获取存储实例和并发配置。
pub struct StoragePair {
    /// 源存储实例，用于读取数据
    src_storage: Arc<StorageContext>,
    /// 目标存储实例，用于写入数据
    dest_storage: Arc<StorageContext>,
}

impl StoragePair {
    /// 创建存储对实例
    ///
    /// 根据源路径和目标路径创建存储对，用于后续的文件复制操作。
    /// 该方法会初始化源和目标的存储池，并配置指定的并发度。
    ///
    /// # 参数
    /// - `src_path`: 源存储路径，指定从哪里读取数据
    /// - `dest_path`: 目标存储路径，指定将数据写入到哪里
    /// - `concurrency`: 存储操作的并发度配置
    ///
    /// # 返回值
    /// - 成功时返回初始化好的 `StoragePair` 实例
    /// - 失败时返回错误信息
    pub async fn new(src_path: &str, dest_path: &str, block_size: Option<usize>) -> Result<Self> {
        let block_size_u64 = block_size.map(|s| s as u64);
        // 根据传入的src_path创建storage pool
        let src_storage =
            Arc::new(create_storage_for_role(src_path, block_size_u64, false, StorageRole::Source).await?);
        // 根据传入的dest_path创建storage pool，如果目标目录不存在则自动创建
        let dest_storage =
            Arc::new(create_storage_for_role(dest_path, block_size_u64, true, StorageRole::Destination).await?);
        info!("Created source and destination storage, block_size: {:?}", block_size);

        Ok(Self {
            src_storage,
            dest_storage,
        })
    }

    /// 获取源存储实例
    ///
    /// 返回源存储的引用，用于从源位置读取数据。
    ///
    /// # 返回值
    /// - 源存储实例的Arc引用
    pub fn get_src_storage(&self) -> &Arc<StorageContext> {
        &self.src_storage
    }

    /// 获取目标存储实例
    ///
    /// 返回目标存储的引用，用于将数据写入目标位置。
    ///
    /// # 返回值
    /// - 目标存储实例的Arc引用
    pub fn get_dest_storage(&self) -> &Arc<StorageContext> {
        &self.dest_storage
    }
}

pub(crate) fn parse_size(size: &str) -> Result<usize> {
    // 去除字符串中的空格，转为小写以便统一处理
    let size = size.replace(' ', "").to_lowercase();

    // 定义支持的单位及其对应的字节数
    let units = [
        ("mib", 1024 * 1024),        // mebibytes
        ("mb", 1024 * 1024),         // megabytes
        ("gib", 1024 * 1024 * 1024), // gibibytes
        ("gb", 1024 * 1024 * 1024),  // gigabytes
        ("kib", 1024),               // kibibytes
        ("kb", 1024),                // kilobytes
    ];

    // 尝试匹配每个单位
    for (unit, multiplier) in &units {
        if size.ends_with(unit) {
            // 提取数字部分
            let number_str = &size[0..size.len() - unit.len()];
            // 确保数字部分不为空
            if number_str.is_empty() {
                continue;
            }
            // 解析数字
            let Ok(number) = number_str.parse::<f64>() else {
                continue; // 尝试下一个单位
            };

            // 计算最终的bps值
            #[allow(clippy::cast_precision_loss, clippy::cast_lossless)]
            let bytes = (number * *multiplier as f64).round() as usize;
            return Ok(bytes);
        }
    }

    // 如果没有匹配到任何单位，尝试直接解析为数字（假设单位为字节）
    if let Ok(bytes) = size.parse::<usize>() {
        return Ok(bytes);
    }

    Err(AppError::ConfigError(
        "无效的带宽格式，请使用如'1GiB/s'或'200MiB/s'的格式".to_string(),
    ))
}

/// 执行文件复制操作
///
/// 该函数是文件复制功能的主入口，负责：
/// 1. 初始化应用配置和扫描配置
/// 2. 初始化消费者配置和管理器
/// 3. 创建通信通道
/// 4. 启动目录扫描
/// 5. 处理扫描结果并执行复制
/// 6. 管理重试机制
/// 7. 验证文件一致性
///
/// # 参数
/// - `config`: 同步任务的完整配置，详见 [`SyncJobConfig`](crate::config::SyncJobConfig)
///
/// # 返回值
/// - 成功时返回`Ok(())`
/// - 失败时返回包含错误信息的`Err`
#[instrument(skip_all, fields(
    job_id = %config.job_id,
    src = %redact_storage_url(&config.src_path),
    dest = %redact_storage_url(&config.dest_path),
))]
pub async fn sync(config: crate::config::SyncJobConfig) -> Result<()> {
    crate::orchestrator::SyncOrchestrator::new_local(config).run().await
}

/// 执行增量文件复制操作
///
/// 该函数执行增量复制操作，基于数据库中记录的上次扫描结果，只复制变更的文件。
/// 主要流程包括：
/// 1. 加载应用配置
/// 2. 初始化扫描配置
/// 3. 初始化消费者配置和管理器
/// 4. 创建通信通道
/// 5. 启动目录扫描（只扫描变更文件）
/// 6. 处理扫描结果并执行复制
/// 7. 管理重试机制
/// 8. 验证文件一致性
///
/// # 参数
/// - `config`: 同步任务的完整配置，详见 [`SyncJobConfig`](crate::config::SyncJobConfig)
///
/// # 返回值
/// - 成功时返回`Ok(())`
/// - 失败时返回包含错误信息的`Err`
#[instrument(skip_all, fields(
    job_id = %config.job_id,
    src = %redact_storage_url(&config.src_path),
    dest = %redact_storage_url(&config.dest_path),
))]
pub async fn incremental_sync(config: crate::config::SyncJobConfig) -> Result<()> {
    crate::orchestrator::SyncOrchestrator::new_local(config).run().await
}

#[cfg(windows)]
pub(crate) fn check_admin_privileges() -> Result<bool> {
    #[allow(unsafe_code)]
    unsafe {
        let mut token_handle = std::ptr::null_mut();

        // 打开当前进程的令牌
        let result = OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token_handle);

        if result == 0 {
            return Err(AppError::CopyError("无法打开进程令牌，可能是权限不足".to_string()));
        }

        let mut elevation = [0u8; 4];
        let mut return_length: u32 = 0;

        const TOKEN_ELEVATION: i32 = 20;

        let result = GetTokenInformation(
            token_handle,
            TOKEN_ELEVATION,
            elevation.as_mut_ptr() as *mut _,
            elevation.len() as u32,
            &mut return_length,
        );

        let _ = CloseHandle(token_handle);

        if result != 0 {
            let is_elevated = u32::from_ne_bytes(elevation) != 0;
            Ok(is_elevated)
        } else {
            Err(AppError::CopyError("无法获取令牌信息，可能是权限不足".to_string()))
        }
    }
}

pub(crate) async fn process_versioned_entry(
    item: &ObservedItem, src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>,
    broadcaster: BroadcastForwarder<StorageEntryMessage>, qos_manager: Option<QosManager>,
    enable_integrity_check: bool, enable_acl: bool, is_source_reserved: bool, bytes_counter: Option<Arc<AtomicU64>>,
    resume: ResumeOpts, change: EntryChange,
) -> Result<()> {
    if item.entry.history().is_some() {
        return copy_versions(
            once(item.clone()),
            &src_storage,
            &dest_storage,
            CopyOptions {
                transfer_mode: resume.transfer_mode,
                qos: qos_manager,
                enable_integrity_check,
                is_source_reserved,
                bytes_counter,
                cancel: None,
            },
            &resume.job_id,
            &broadcaster,
            change,
        )
        .await;
    }
    process_entry(
        &item.entry,
        src_storage,
        dest_storage,
        enable_integrity_check,
        enable_acl,
        is_source_reserved,
        qos_manager,
        bytes_counter,
        &broadcaster,
        resume,
    )
    .await?;
    broadcaster.broadcast(change.message(item.clone())).await;
    Ok(())
}

/// `process_entry` / `process_entry_inner` 的标志位组合
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)] // 标志位结构按调用现场天然分组，封装为枚举会引入调用点繁杂匹配
pub(crate) struct ProcessEntryOpts {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    /// 仅同步元数据（chmod/chown 场景），跳过 `copy_file`
    pub skip_data_copy: bool,
    /// 本次同步的写入模式与恢复上下文。
    pub resume: ResumeOpts,
}

#[allow(clippy::too_many_arguments)] // 公共入口需要逐项透传调用方上下文，封装结构会破坏 API 形态
pub(crate) async fn process_entry(
    entry: &Arc<ObservedEntry>, src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>,
    enable_integrity_check: bool, enable_acl: bool, is_source_reserved: bool, qos_manager: Option<QosManager>,
    bytes_counter: Option<Arc<AtomicU64>>, broadcaster: &BroadcastForwarder<StorageEntryMessage>, resume: ResumeOpts,
) -> Result<()> {
    let opts = ProcessEntryOpts {
        enable_integrity_check,
        enable_acl,
        is_source_reserved,
        skip_data_copy: false,
        resume,
    };
    process_entry_inner(
        entry,
        src_storage,
        dest_storage,
        opts,
        qos_manager,
        bytes_counter,
        broadcaster,
    )
    .await
}

/// 仅同步元数据：mode/uid/gid 变了但 size/mtime 未变（chmod/chown），跳过 `copy_file` 以节省带宽。
///
/// 目录 / symlink 行为与 `process_entry` 完全一致；普通文件跳过 `copy_file`，
/// 仍执行 `set_entry_metadata` + ACL / xattr 复制。
pub(crate) async fn process_metadata_only_entry(
    entry: &Arc<ObservedEntry>, src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>, enable_acl: bool,
    is_source_reserved: bool, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    let opts = ProcessEntryOpts {
        enable_integrity_check: false,
        enable_acl,
        is_source_reserved,
        skip_data_copy: true,
        resume: ResumeOpts::default(),
    };
    process_entry_inner(entry, src_storage, dest_storage, opts, None, None, broadcaster).await
}

async fn process_entry_inner(
    entry: &Arc<ObservedEntry>, src_storage: Arc<StorageContext>, dest_storage: Arc<StorageContext>,
    opts: ProcessEntryOpts, qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    let ProcessEntryOpts {
        enable_integrity_check,
        enable_acl,
        is_source_reserved,
        skip_data_copy,
        resume,
    } = opts;
    let relative_path = entry.get_relative_path();
    let span = info_span!("process_entry", path = %relative_path.display());

    async {
    trace!(
        "Processing entry: {:?} with parameters: enable_integrity_check: {:?}, enable_acl: {:?}, is_source_reserved: {:?}",
        relative_path, enable_integrity_check, enable_acl, is_source_reserved
    );

    if entry.history().is_some() && !skip_data_copy {
        return Err(AppError::ConfigError("history must use the ordered version-copy workflow".into()));
    }
    if prepare_directory(entry, &dest_storage).await? {
        trace!("Created directory: {:?}", relative_path);
    } else if entry.get_is_symlink() {
        match src_storage.read_symlink(entry).await {
            Ok(target_path) => {
                dest_storage.create_symlink(entry, &target_path).await?;

                if !is_source_reserved {
                    trace!("Is not source reserved, removing symlink {:?}", relative_path);
                    if let Err(e) = src_storage.delete_file(entry).await {
                        error!("Failed to remove existing symlink {:?}: {}", relative_path, e);
                        broadcaster
                            .broadcast(StorageEntryMessage::Error {
                                event: ErrorEvent::SymlinkOp,
                                path: relative_path.to_path_buf(),
                                entry: Some(Arc::clone(entry)),
                                reason: format!("{e}"),
                            })
                            .await;
                        return Err(e.into());
                    }
                }
            }
            Err(e) => {
                error!("Failed to read symlink {:?}: {}", relative_path, e);
                broadcaster
                    .broadcast(StorageEntryMessage::Error {
                        event: ErrorEvent::SymlinkOp,
                        path: relative_path.to_path_buf(),
                        entry: Some(Arc::clone(entry)),
                        reason: format!("{e}"),
                    })
                    .await;
                return Err(e.into());
            }
        }
    } else if skip_data_copy {
        // MetadataOnly 变更：chmod/chown 导致 mode/uid/gid 变了但内容未变，跳过 copy_file
        trace!("Skipping data copy (metadata-only change): {:?}", relative_path);
        dest_storage
            .set_entry_metadata(entry)
            .await?;
    } else if dest_storage.as_ref().storage().kind() == BackendKind::Hdfs {
        let hdfs = HdfsCopyContext::from_job(&resume.job_id, resume.transfer_mode)?;
        let recorder = if resume.transfer_mode == TransferMode::Checkpoint {
            Some(CommitRecorder::start(&resume.job_dir, entry, src_storage.block_size()).await?)
        } else { None };
        let hdfs = recorder.as_ref().map_or(hdfs.clone(), |recorder| hdfs.with_commit_callback(recorder.callback()));
        let result = copy_file(
            &src_storage,
            &dest_storage,
            entry,
            storage_adapter::CopyOptions {
                transfer_mode: resume.transfer_mode,
                qos: qos_manager,
                enable_integrity_check,
                is_source_reserved,
                bytes_counter,
                ..Default::default()
            },
            Some(&hdfs),
        )
        .await
        .map(|_| ());
        if let Some(recorder) = recorder { recorder.finish(result).await?; } else { result?; }
        debug!("Copied file through HDFS recoverable lifecycle {:?}", relative_path);
    } else if should_resume(&resume, entry, &src_storage, &dest_storage) {
        // 多块大文件：字节级断点续传（全量建 .part / 增量续 .part）
        debug!("Copying file (resumable) {:?}", relative_path);
        copy_file_with_resume(
            entry,
            &src_storage,
            &dest_storage,
            enable_integrity_check,
            is_source_reserved,
            qos_manager,
            bytes_counter,
            &resume.job_dir,
        )
        .await?;

        // data-mover 在发布文件前应用协商后的元数据。

        debug!("Copied file (resumable) {:?}", relative_path);
    } else {
        debug!("Copying file {:?}", relative_path);

        // 调用 data_mover 的 copy_file（处理 QoS、完整性检查、源文件删除）
        StorageContext::copy_file(
            &src_storage,
            &dest_storage,
            entry,
            storage_adapter::CopyOptions {
                transfer_mode: resume.transfer_mode,
                qos: qos_manager,
                enable_integrity_check,
                is_source_reserved,
                bytes_counter,
                ..Default::default()
            },
        )
        .await?;

        // data-mover 在发布文件前应用协商后的元数据。

        debug!("Copied file {:?}", relative_path);
    }

    // ACL 复制（目录和文件均需要，symlink 除外）
    // 支持 Local→Local（Windows）、CIFS→CIFS（跨平台）、NFS→NFS（NFSv4+），其他组合静默跳过
    if enable_acl && !entry.get_is_symlink() {
        if let Err(err) = StorageContext::copy_acl(&src_storage, &dest_storage, relative_path).await {
            error!("Failed to copy ACL for {:?}: {}", relative_path, err);
            broadcaster
                .broadcast(StorageEntryMessage::Error {
                    event: ErrorEvent::CopyAcl,
                    path: relative_path.to_path_buf(),
                    entry: Some(Arc::clone(entry)),
                    reason: format!("{err}"),
                })
                .await;
            return Err(err.into());
        }

        // xattr 复制（仅 NFSv4.1→NFSv4.1，其他组合静默跳过）
        if let Err(err) = StorageContext::copy_xattr(&src_storage, &dest_storage, relative_path).await {
            error!("Failed to copy xattr for {:?}: {}", relative_path, err);
            broadcaster
                .broadcast(StorageEntryMessage::Error {
                    event: ErrorEvent::CopyXattr,
                    path: relative_path.to_path_buf(),
                    entry: Some(Arc::clone(entry)),
                    reason: format!("{err}"),
                })
                .await;
            return Err(err.into());
        }
    }

    Ok(())
    }.instrument(span).await
}

/// 字节级断点续传的可调参数（全量/增量两条路径共用）。
#[derive(Debug, Clone, Default)]
pub(crate) struct ResumeOpts {
    /// 持久化任务身份；HDFS 用它绑定跨进程可恢复状态。
    pub job_id: String,
    /// 任务目录；续传状态存于 `<job_dir>/byte_resume/`。为空则禁用续传。
    pub job_dir: String,
    /// 显式选定的写入模式。
    pub transfer_mode: TransferMode,
}

pub(crate) struct CommitRecorder {
    store: Arc<ByteResumeStore>,
    callback: storage_adapter::CommitCallback,
    drain: JoinHandle<()>,
    shutdown: oneshot::Sender<()>,
    dropped: Arc<AtomicU64>,
    path: std::path::PathBuf,
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

    pub(crate) fn callback(&self) -> storage_adapter::CommitCallback {
        self.callback.clone()
    }

    pub(crate) async fn finish(self, result: Result<()>) -> Result<()> {
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
            Ok(()) => self.store.finalize_done().await,
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
    use std::path::Path;
    use std::time::Duration;

    use storage_adapter::{CreateStorageOptions, create_storage};

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
        let entry = source
            .get_metadata(Path::new("payload.bin"))
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
        let entry = source
            .get_metadata(Path::new("payload.bin"))
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
pub(crate) fn should_resume(
    resume: &ResumeOpts, entry: &ObservedEntry, src: &StorageContext, dest: &StorageContext,
) -> bool {
    resume.transfer_mode == TransferMode::Checkpoint
        && !resume.job_dir.is_empty()
        && !entry.get_is_dir()
        && !entry.get_is_symlink()
        && entry.get_size() > src.block_size()
        && (!is_s3(dest) || entry.get_size() > dest.block_size())
}

/// 字节级断点续传复制单个大文件（全量建 `.part` / 增量续 `.part` 共用）。
///
/// 加载/创建 `<job_dir>/byte_resume/` 下的进度状态，把缺失区间交给
/// `copy_file_resumable`；每个 chunk 确认落盘的回调经有界 channel 投递到收敛 task
/// 异步持久化区间（不阻塞写管道热路径）。成功后清理状态文件；失败（中断）保留状态供续传。
///
/// - NAS 目标端：续写到 `<dest>.terrasync-part`，完成后由 data-mover 原子 rename。
/// - S3 目标端：multipart part 粒度续传，进度真值是目标端 in-progress upload
///   （data-mover 以 ListParts 反推缺失区间，忽略此处传入的 `missing`）；
///   本地状态文件仍随 part 回调更新，仅作进度参考。
pub(crate) async fn copy_file_with_resume(
    entry: &Arc<ObservedEntry>, src_storage: &StorageContext, dest_storage: &StorageContext,
    enable_integrity_check: bool, is_source_reserved: bool, qos_manager: Option<QosManager>,
    bytes_counter: Option<Arc<AtomicU64>>, job_dir: &str,
) -> Result<()> {
    let dest_rel = entry.get_relative_path();
    let recorder = CommitRecorder::start(job_dir, entry, src_storage.block_size()).await?;
    let missing = recorder.store.missing_intervals().await;

    let resume = storage_adapter::ResumeContext {
        part_relative_path: part_path_for(dest_rel),
        missing_intervals: missing,
        on_committed: recorder.callback(),
    };

    let res = StorageContext::copy_file_resumable(
        src_storage,
        dest_storage,
        entry,
        storage_adapter::CopyOptions {
            transfer_mode: TransferMode::Checkpoint,
            qos: qos_manager,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
            ..Default::default()
        },
        resume,
    )
    .await;

    let result = res.map_err(AppError::from);
    recorder.finish(result).await
}

/// 处理单个文件或目录的复制
pub(crate) async fn process_rename_entry(
    from_entry: Arc<ObservedEntry>, to_entry: Arc<ObservedEntry>, src_storage: Arc<StorageContext>,
    dest_storage: Arc<StorageContext>, is_source_reserved: bool,
) -> Result<()> {
    let from_path = from_entry.get_relative_path();
    let to_path = to_entry.get_relative_path();
    let span = info_span!("process_rename", from = %from_path.display(), to = %to_path.display());

    async {
        trace!("rename file: {:?} to {:?}", from_path, to_path);

        dest_storage.rename(from_path, to_path).await?;

        if !is_source_reserved {
            // 如果源文件不是保留文件，删除源文件
            src_storage.delete_file(to_entry.as_ref()).await?;
        }

        Ok(())
    }
    .instrument(span)
    .await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod native_completion_tests {
    use super::*;
    use crate::config::JobType;
    use crate::consumer::stats::FullStats;
    use crate::observed_fixtures::observation;
    use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory};

    fn history(latest_is_delete: bool) -> Arc<ObservedEntry> {
        let observations: Vec<_> = [
            ("old", 7, false, false),
            ("null", 11, false, false),
            ("latest", 19, true, latest_is_delete),
        ]
        .into_iter()
        .map(|(id, size, latest, deleted)| {
            observation(
                BackendKind::S3,
                "object".into(),
                EntryKind::File,
                size,
                [0, 1, 0],
                None,
                None,
                None,
                Some((if id == "null" { None } else { Some(id.into()) }, latest, deleted)),
            )
        })
        .collect();
        let versions = observations
            .iter()
            .flat_map(|entry| entry.history().unwrap().records().to_vec())
            .collect();
        let current = observations.into_iter().last().unwrap();
        Arc::new(
            current
                .with_history(Some(Box::new(
                    ObjectHistory::whole(versions, HistoryOrder::Server).unwrap(),
                )))
                .unwrap(),
        )
    }

    #[test]
    fn completion_shares_history_and_counts_each_selected_version_once() {
        let entry = history(false);
        let mut stats = FullStats::new(JobType::Copy, "selected".into(), String::new(), String::new());
        let completed: Vec<_> = crate::file_copy::selected_copy_items(&entry, true).collect();
        assert_eq!(completed.len(), 3);
        let mut ids = Vec::new();
        for item in completed {
            assert!(Arc::ptr_eq(&item.entry, &entry));
            ids.push(item.view().version_id().to_owned());
            let event = StorageEntryMessage::New(item);
            stats.update_from_message(&event);
        }
        assert_eq!(stats.regular_file_count, 3);
        assert_eq!(stats.regular_file_size, 37);
        assert_eq!(ids, ["old", "null", "latest"]);

        let selected: Vec<_> = crate::file_copy::selected_copy_items(&entry, false).collect();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].view().version_id(), "latest");
    }

    #[test]
    fn a_latest_delete_marker_has_no_file_completion_on_a_nonversioned_target() {
        let entry = history(true);
        assert_eq!(crate::file_copy::selected_copy_items(&entry, false).count(), 0);
        assert_eq!(crate::file_copy::selected_copy_items(&entry, true).count(), 3);
    }
}
