//! 文件复制模块
//!
//! 该模块提供文件复制和增量复制的核心功能，包括：
//! 1. 全量文件复制
//! 2. 增量文件复制（基于数据库记录）
//! 3. 存储资源管理
//! 4. 并发复制控制
//! 5. 重试机制集成

// 标准库
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// 外部crate
use dashmap::DashMap;
use data_mover::qos::QosManager;
use data_mover::{
    EntryEnum, ErrorEvent, StorageEntryMessage, StorageEnum, create_storage, create_storage_for_dest,
    redact_storage_url,
};
use tracing::{Instrument, debug, error, info, info_span, instrument, trace};
#[cfg(windows)]
use windows_sys::Win32::Foundation::CloseHandle;
#[cfg(windows)]
use windows_sys::Win32::Security::GetTokenInformation;
#[cfg(windows)]
use windows_sys::Win32::Security::TOKEN_QUERY;
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::error::{AppError, Result};

/// 存储资源管理器 - 统一管理源和目标存储实例
///
/// 该结构体封装了源存储和目标存储的创建、管理和访问功能，
/// 提供了简洁的接口来获取存储实例和并发配置。
pub struct StoragePair {
    /// 源存储实例，用于读取数据
    src_storage: Arc<StorageEnum>,
    /// 目标存储实例，用于写入数据
    dest_storage: Arc<StorageEnum>,
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
        let src_storage = Arc::new(create_storage(src_path, block_size_u64).await?);
        // 根据传入的dest_path创建storage pool，如果目标目录不存在则自动创建
        let dest_storage = Arc::new(create_storage_for_dest(dest_path, block_size_u64).await?);
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
    pub fn get_src_storage(&self) -> &Arc<StorageEnum> {
        &self.src_storage
    }

    /// 获取目标存储实例
    ///
    /// 返回目标存储的引用，用于将数据写入目标位置。
    ///
    /// # 返回值
    /// - 目标存储实例的Arc引用
    pub fn get_dest_storage(&self) -> &Arc<StorageEnum> {
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_versioned_entry(
    entry: &Arc<EntryEnum>, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>,
    broadcaster: BroadcastForwarder<StorageEntryMessage>, qos_manager: Option<QosManager>,
    object_buffers: Arc<DashMap<String, Vec<Arc<EntryEnum>>>>, enable_integrity_check: bool, enable_acl: bool,
    is_source_reserved: bool, bytes_counter: Option<Arc<AtomicU64>>,
) {
    // 版本化对象，使用per-object缓冲区机制
    trace!("Processing versioned entry with buffer: {:?}", entry);

    // 按对象分组
    let object_key = match &**entry {
        EntryEnum::S3(s3_entry) => s3_entry.relative_path.clone(),
        EntryEnum::NAS(_) => {
            error!("Expected S3Entry for versioned processing, got {:?}", entry);
            return;
        }
    };

    // 使用 DashMap per-key 细粒度锁：只锁住当前 object_key，不阻塞其他对象
    // push + 集齐判定 + take 在同一个锁 scope 内完成，消除多 worker 下的 TOCTOU 竞态
    let object_versions = {
        let mut entry_ref = object_buffers.entry(object_key.clone()).or_default();
        entry_ref.push(entry.clone());

        // 检查是否已收集所有版本（无版本信息时视为单版本，立即处理）
        let total = entry.get_version_count().unwrap_or(1) as usize;
        if entry_ref.len() < total {
            // 未集齐，drop RefMut 自动释放锁
            return;
        }

        // 集齐 → 在持锁期间 take 走数据（留空 Vec）
        std::mem::take(entry_ref.value_mut())
    };
    // RefMut 已 drop，安全清理空壳 entry
    object_buffers.remove(&object_key);

    // 已集齐所有版本，开始处理
    trace!("All versions of object {} collected, starting processing", object_key);

    // 启动处理任务
    let span = info_span!("version_processor", path = %object_key, object_key = %object_key);
    drop(tokio::spawn(async move {
        let mut task_versions = object_versions;

        // 按mtime从旧到新排序版本
        task_versions.sort_by_key(|a| a.get_mtime());

        trace!("Sorted versions: {:?}", task_versions);

        if dest_storage.is_bucket_versioned() {
            let version_count = task_versions.len() as u32;
            debug!("Processing object {} with {} versions", object_key, version_count);

            // 处理每个版本
            for mut entry in task_versions {
                // 设置版本计数
                Arc::make_mut(&mut entry).set_version_count(version_count);

                // 记录开始处理的版本信息
                trace!(
                    "Processing versioned entry: relative_path={:?}, version_id={:?}, is_latest={}, is_delete_marker={}, mtime={:?}",
                    entry.get_relative_path(),
                    entry.get_version_id(),
                    entry.get_is_latest(),
                    entry.get_is_delete_marker(),
                    entry.get_mtime()
                );

                // 处理条目
                match process_entry(
                    entry.as_ref(),
                    Arc::clone(&src_storage),
                    Arc::clone(&dest_storage),
                    enable_integrity_check,
                    enable_acl,
                    is_source_reserved,
                    qos_manager.clone(),
                    bytes_counter.clone(),
                    &broadcaster,
                )
                .await
                {
                    Ok(()) => {
                        // 记录处理成功的版本信息
                        debug!(
                            "Successfully processed versioned entry: relative_path={:?}, version_id={:?}, is_latest={}, is_delete_marker={}, mtime={:?}",
                            entry.get_relative_path(),
                            entry.get_version_id(),
                            entry.get_is_latest(),
                            entry.get_is_delete_marker(),
                            entry.get_mtime()
                        );

                        // 复制成功后才广播结果
                        broadcaster.broadcast(StorageEntryMessage::New(entry.clone())).await;
                    }
                    Err(e) => {
                        error!(
                            "Failed to process versioned entry {:?}: {}",
                            entry.get_relative_path(),
                            e
                        );
                        broadcaster
                            .broadcast(StorageEntryMessage::Error {
                                event: ErrorEvent::Copy,
                                path: entry.get_relative_path().to_path_buf(),
                                reason: format!("{e}"),
                            })
                            .await;
                    }
                }
            }
        } else {
            // 目标不支持版本化，仅复制最新版本（最后一个，因为已按mtime升序排列）
            if let Some(entry) = task_versions.last() {
                let entry = entry.clone();
                debug!(
                    "Destination not versioned, copying only latest version of object {}: version_id={:?}, mtime={:?}",
                    object_key,
                    entry.get_version_id(),
                    entry.get_mtime()
                );

                match process_entry(
                    entry.as_ref(),
                    Arc::clone(&src_storage),
                    Arc::clone(&dest_storage),
                    enable_integrity_check,
                    enable_acl,
                    is_source_reserved,
                    qos_manager.clone(),
                    bytes_counter.clone(),
                    &broadcaster,
                )
                .await
                {
                    Ok(()) => {
                        debug!(
                            "Successfully processed latest version of object {}: version_id={:?}",
                            object_key,
                            entry.get_version_id()
                        );
                        broadcaster.broadcast(StorageEntryMessage::New(entry)).await;
                    }
                    Err(e) => {
                        error!("Failed to process latest version of object {}: {}", object_key, e);
                        broadcaster
                            .broadcast(StorageEntryMessage::Error {
                                event: ErrorEvent::Copy,
                                path: object_key.clone().into(),
                                reason: format!("{e}"),
                            })
                            .await;
                    }
                }
            }
        }
    }.instrument(span)));
}

/// `process_entry` / `process_entry_inner` 的标志位组合
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProcessEntryOpts {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub is_source_reserved: bool,
    /// 仅同步元数据（chmod/chown 场景），跳过 `copy_file`
    pub skip_data_copy: bool,
}

pub(crate) async fn process_entry(
    entry: &EntryEnum, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>, enable_integrity_check: bool,
    enable_acl: bool, is_source_reserved: bool, qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    let opts = ProcessEntryOpts {
        enable_integrity_check,
        enable_acl,
        is_source_reserved,
        skip_data_copy: false,
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
    entry: &EntryEnum, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>, enable_acl: bool,
    is_source_reserved: bool, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    let opts = ProcessEntryOpts {
        enable_integrity_check: false,
        enable_acl,
        is_source_reserved,
        skip_data_copy: true,
    };
    process_entry_inner(entry, src_storage, dest_storage, opts, None, None, broadcaster).await
}

async fn process_entry_inner(
    entry: &EntryEnum, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>, opts: ProcessEntryOpts,
    qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    let ProcessEntryOpts {
        enable_integrity_check,
        enable_acl,
        is_source_reserved,
        skip_data_copy,
    } = opts;
    let relative_path = entry.get_relative_path();
    let span = info_span!("process_entry", path = %relative_path.display());

    async {
    trace!(
        "Processing entry: {:?} with parameters: enable_integrity_check: {:?}, enable_acl: {:?}, is_source_reserved: {:?}",
        relative_path, enable_integrity_check, enable_acl, is_source_reserved
    );

    if entry.get_is_dir() {
        trace!("Calling create_dir_all: {:?}", relative_path);

        // 创建目标目录
        match dest_storage.create_dir_all(entry).await {
            Ok(()) => debug!("Created directory: {:?}", relative_path),
            Err(e) => {
                error!("Error creating directory: {:?}, {:?}", relative_path, e);
                broadcaster
                    .broadcast(StorageEntryMessage::Error {
                        event: ErrorEvent::Copy,
                        path: relative_path.to_path_buf(),
                        reason: format!("{e}"),
                    })
                    .await;
                // 目录创建失败，不返回错误，继续尝试设置元数据和ACL
            }
        }

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
                                reason: format!("{e}"),
                            })
                            .await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to read symlink {:?}: {}", relative_path, e);
                broadcaster
                    .broadcast(StorageEntryMessage::Error {
                        event: ErrorEvent::SymlinkOp,
                        path: relative_path.to_path_buf(),
                        reason: format!("{e}"),
                    })
                    .await;
            }
        }
    } else if skip_data_copy {
        // MetadataOnly 变更：chmod/chown 导致 mode/uid/gid 变了但内容未变，跳过 copy_file
        trace!("Skipping data copy (metadata-only change): {:?}", relative_path);
        dest_storage
            .set_entry_metadata(entry)
            .await
            .map_err(|e| AppError::CopyError(format!("Failed to set metadata for {}: {e}", relative_path.display())))?;
    } else {
        debug!("Copying file {:?}", relative_path);

        // 调用 data_mover 的 copy_file（处理 QoS、完整性检查、源文件删除）
        StorageEnum::copy_file(
            &src_storage,
            &dest_storage,
            entry,
            qos_manager,
            enable_integrity_check,
            is_source_reserved,
            bytes_counter,
        )
        .await
        .map_err(|e| AppError::CopyError(format!("Failed to copy {}: {e}", relative_path.display())))?;

        // 设置目标文件元数据（时间戳、权限）
        dest_storage
            .set_entry_metadata(entry)
            .await
            .map_err(|e| AppError::CopyError(format!("Failed to set metadata for {}: {e}", relative_path.display())))?;

        debug!("Copied file {:?}", relative_path);
    }

    // ACL 复制（目录和文件均需要，symlink 除外）
    // 支持 Local→Local（Windows）、CIFS→CIFS（跨平台）、NFS→NFS（NFSv4+），其他组合静默跳过
    if enable_acl && !entry.get_is_symlink() {
        if let Err(err) = StorageEnum::copy_acl(&src_storage, &dest_storage, relative_path).await {
            error!("Failed to copy ACL for {:?}: {}", relative_path, err);
            broadcaster
                .broadcast(StorageEntryMessage::Error {
                    event: ErrorEvent::CopyAcl,
                    path: relative_path.to_path_buf(),
                    reason: format!("{err}"),
                })
                .await;
        }

        // xattr 复制（仅 NFSv4.1→NFSv4.1，其他组合静默跳过）
        if let Err(err) = StorageEnum::copy_xattr(&src_storage, &dest_storage, relative_path).await {
            error!("Failed to copy xattr for {:?}: {}", relative_path, err);
            broadcaster
                .broadcast(StorageEntryMessage::Error {
                    event: ErrorEvent::CopyXattr,
                    path: relative_path.to_path_buf(),
                    reason: format!("{err}"),
                })
                .await;
        }
    }

    Ok(())
    }.instrument(span).await
}

/// 处理单个文件或目录的复制
pub(crate) async fn process_rename_entry(
    from_entry: Arc<EntryEnum>, to_entry: Arc<EntryEnum>, src_storage: Arc<StorageEnum>,
    dest_storage: Arc<StorageEnum>, is_source_reserved: bool,
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
