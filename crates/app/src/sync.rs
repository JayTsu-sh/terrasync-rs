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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// 外部crate
use dashmap::DashMap;
use storage_v2::qos::QosManager;
use storage_v2::{EntryEnum, ErrorEvent, StorageEntryMessage, StorageEnum, create_storage, create_storage_for_dest};
use tokio::sync::Mutex;
use tracing::{Instrument, debug, error, info, info_span, instrument, trace, warn};
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
use crate::config::{JobType, ScanConfig, initialize_consumer_config};
use crate::consumer::ConsumerManager;
use crate::dir_walker;
use crate::error::{AppError, Result};
use crate::scan::ScanType;

/// 广播通道容量
const BROADCAST_CHANNEL_CAPACITY: usize = 1000;

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
/// - `job_id`: 任务ID，用于标识当前复制任务
/// - `job_dir`: 任务目录，用于存储任务相关数据
/// - `src_path`: 源路径，指定从哪里读取文件
/// - `dest_path`: 目标路径，指定将文件写入到哪里
/// - `enable_integrity_check`: 是否启用完整性检查
/// - `enable_acl`: 是否启用ACL（访问控制列表）复制
/// - `scan_type`: 扫描类型（全量或增量）
/// - `r#match`: 匹配表达式，用于过滤要复制的文件
/// - `exclude`: 排除表达式，用于排除不需要复制的文件
/// - `qos`: QoS配置，用于流量控制
/// - `peak_qos_rate`: 峰值 `QoS` 速率
/// - `block_size`: 块大小配置
/// - `file_list`: 可选的文件列表路径，指定只复制列表中的文件
/// - `iops`: 可选的 IOPS 限制
/// - `packaged`: 是否以打包模式处理
/// - `package_depth`: 打包深度
/// - `raw_command_line`: 原始命令行，用于记录和调试
/// - `progress_callback_url`: 进度回调 URL（web 层传入，CLI 传 `None`）
///
/// # 返回值
/// - 成功时返回`Ok(())`
/// - 失败时返回包含错误信息的`Err`
#[instrument(skip_all, fields(job_id = %job_id, src = %src_path, dest = %dest_path))]
#[allow(clippy::too_many_arguments)]
pub async fn sync(
    job_id: String, job_dir: String, job_dir_pre_existing: bool, src_path: &str, dest_path: &str,
    enable_integrity_check: bool, enable_acl: bool, r#match: &Option<String>, exclude: &Option<String>,
    qos: &Option<String>, peak_qos_rate: f32, block_size: &Option<String>, file_list: &Option<String>,
    iops: Option<u32>, packaged: bool, package_depth: usize, raw_command_line: String,
    progress_callback_url: Option<String>,
) -> Result<()> {
    let config = crate::config::SyncJobConfig {
        job_id,
        job_dir,
        job_dir_pre_existing,
        src_path: src_path.to_string(),
        dest_path: dest_path.to_string(),
        enable_integrity_check,
        enable_acl,
        r#match: r#match.clone(),
        exclude: exclude.clone(),
        qos: qos.clone(),
        peak_qos_rate,
        block_size: block_size.clone(),
        file_list: file_list.clone(),
        iops,
        packaged,
        package_depth,
        raw_command_line,
        progress_callback_url,
    };
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
/// - `job_id`: 任务ID，用于标识当前复制任务
/// - `job_dir`: 任务目录，用于存储任务相关数据
/// - `src_path`: 源路径，指定从哪里读取文件
/// - `dest_path`: 目标路径，指定将文件写入到哪里
/// - `enable_integrity_check`: 是否启用完整性检查
/// - `enable_acl`: 是否启用ACL（访问控制列表）复制
/// - `scan_type`: 扫描类型（全量或增量）
/// - `r#match`: 匹配表达式，用于过滤要复制的文件
/// - `exclude`: 排除表达式，用于排除不需要复制的文件
/// - `qos`: QoS配置，用于流量控制
/// - `peak_qos_rate`: 峰值 `QoS` 速率
/// - `block_size`: 块大小配置
/// - `iops`: 可选的 IOPS 限制
/// - `packaged`: 是否以打包模式处理
/// - `package_depth`: 打包深度
/// - `raw_command_line`: 原始命令行，用于记录和调试
/// - `progress_callback_url`: 进度回调 URL（web 层传入，CLI 传 `None`）
///
/// # 返回值
/// - 成功时返回`Ok(())`
/// - 失败时返回包含错误信息的`Err`
#[instrument(skip_all, fields(job_id = %job_id, src = %src_path, dest = %dest_path))]
#[allow(clippy::too_many_arguments)]
pub async fn incremental_sync(
    job_id: String, job_dir: String, job_dir_pre_existing: bool, src_path: &str, dest_path: &str,
    enable_integrity_check: bool, enable_acl: bool, r#match: &Option<String>, exclude: &Option<String>,
    qos: &Option<String>, peak_qos_rate: f32, block_size: &Option<String>, iops: Option<u32>, packaged: bool,
    package_depth: usize, raw_command_line: String, progress_callback_url: Option<String>,
) -> Result<()> {
    let config = crate::config::SyncJobConfig {
        job_id,
        job_dir,
        job_dir_pre_existing,
        src_path: src_path.to_string(),
        dest_path: dest_path.to_string(),
        enable_integrity_check,
        enable_acl,
        r#match: r#match.clone(),
        exclude: exclude.clone(),
        qos: qos.clone(),
        peak_qos_rate,
        block_size: block_size.clone(),
        file_list: None,
        iops,
        packaged,
        package_depth,
        raw_command_line,
        progress_callback_url,
    };
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_entry(
    entry: &EntryEnum, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>, enable_integrity_check: bool,
    enable_acl: bool, is_source_reserved: bool, qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
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
    } else {
        debug!("Copying file {:?}", relative_path);

        // 调用 storage_v2 的 copy_file（处理 QoS、完整性检查、源文件删除）
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

// 执行完整性检查，验证源路径和目标路径下文件的一致性
//
// 该函数负责验证源路径和目标路径下文件的完整性，确保所有文件的内容完全相同。
// 主要流程包括：
// 1. 扫描源路径下的所有文件
// 2. 对每个文件，计算源文件和目标文件的哈希值
// ─────────────────────────────────────────────────
// Integrity Check 数据模型
// ─────────────────────────────────────────────────

/// 不一致条目类型
#[derive(Debug, Clone)]
pub enum IssueKind {
    /// 目标不存在
    Missing,
    /// 元数据/内容不匹配
    Mismatch,
}

/// Auto-fix 结果
#[derive(Debug, Clone)]
pub enum FixStatus {
    /// `auto_fix` 关闭，未尝试修复
    NotAttempted,
    /// 修复成功，无遗留问题
    Fixed,
    /// 元数据已修，但内容（size/hash）仍不匹配
    PartiallyFixed,
    /// `set_metadata` 调用失败
    FixFailed,
}

/// 单条 integrity check 不一致记录
#[derive(Debug, Clone)]
pub struct IntegrityIssue {
    /// 相对路径
    pub path: String,
    /// 条目类型："file" / "dir" / "symlink"
    pub entry_type: &'static str,
    /// Missing 或 Mismatch
    pub kind: IssueKind,
    /// 不匹配的字段标签，例如 `["size", "mtime", "uid"]`
    pub mismatches: Vec<String>,
    /// 修复状态
    pub fix_status: FixStatus,
}

/// 从 "size: src=100, dest=200" 格式的 mismatch 描述中提取标签 "size"
fn extract_mismatch_labels(mismatches: &[String]) -> Vec<String> {
    mismatches
        .iter()
        .map(|m| m.split(':').next().unwrap_or(m.as_str()).trim().to_string())
        .collect()
}

/// 格式化 entry 元数据，用于不一致告警日志。
fn format_entry_metadata(entry: &EntryEnum) -> String {
    format!(
        "size={}, mtime={}, uid={}, gid={}, mode={}",
        entry.get_size(),
        entry.get_mtime(),
        entry.get_uid().map_or("-".to_string(), |v| v.to_string()),
        entry.get_gid().map_or("-".to_string(), |v| v.to_string()),
        entry.get_mode().map_or("-".to_string(), |v| format!("{v:#o}")),
    )
}

/// 比较两个 entry 的 mtime/uid/gid，以及可选的 mode，收集不一致描述。
fn collect_metadata_mismatches(src: &EntryEnum, dest: &EntryEnum, check_mode: bool) -> Vec<String> {
    let mut mismatches = Vec::new();

    let src_mtime = src.get_mtime();
    let dest_mtime = dest.get_mtime();
    if src_mtime != dest_mtime {
        mismatches.push(format!("mtime: src={src_mtime}, dest={dest_mtime}"));
    }

    if let (Some(src_uid), Some(dest_uid)) = (src.get_uid(), dest.get_uid())
        && src_uid != dest_uid
    {
        mismatches.push(format!("uid: src={src_uid}, dest={dest_uid}"));
    }

    if let (Some(src_gid), Some(dest_gid)) = (src.get_gid(), dest.get_gid())
        && src_gid != dest_gid
    {
        mismatches.push(format!("gid: src={src_gid}, dest={dest_gid}"));
    }

    if check_mode
        && let (Some(src_mode), Some(dest_mode)) = (src.get_mode(), dest.get_mode())
        && src_mode != dest_mode
    {
        mismatches.push(format!("mode: src={src_mode:#o}, dest={dest_mode:#o}"));
    }

    mismatches
}

/// 执行完整性检查（哈希比对）
///
/// 扫描源路径的所有文件，逐一与目标路径对比哈希值，报告不一致的文件。
///
/// # 参数
/// - `job_id`: 任务ID
/// - `job_dir`: 任务目录
/// - `src_path`: 源路径，指定要检查的源文件位置
/// - `dest_path`: 目标路径，指定要检查的目标文件位置
/// - `quick`: 是否启用快速模式（跳过内容哈希，只比对元数据）
/// - `auto_fix`: 是否自动修复不一致的文件（重新复制）
/// - `raw_command_line`: 原始命令行，用于记录和调试
/// - `progress_callback_url`: 进度回调 URL（web 层传入，CLI 传 `None`）
///
/// # 返回值
/// - 所有文件验证一致时返回`Ok(())`
/// - 任何文件哈希值不匹配或发生错误时返回包含错误信息的`Err`
#[instrument(skip_all, fields(job_id = %job_id, src = %src_path, dest = %dest_path))]
#[allow(clippy::too_many_arguments)]
pub async fn integrity_check(
    job_id: String, job_dir: String, src_path: &str, dest_path: &str, quick: bool, auto_fix: bool,
    raw_command_line: String, progress_callback_url: Option<String>,
) -> Result<()> {
    info!("Starting integrity check between {} and {}", src_path, dest_path);

    // 加载应用配置
    let app_config = utils::app_config::AppConfig::fetch()?;

    // 初始化消费者配置
    let consumer_config = Arc::new(initialize_consumer_config(
        &job_id,
        &job_dir,
        JobType::IntegrityCheck,
        raw_command_line,
        &app_config,
        progress_callback_url,
    )?);

    // 创建广播转发器
    let mut broadcaster = BroadcastForwarder::new(BROADCAST_CHANNEL_CAPACITY);

    // 初始化消费者管理器并启动消费者
    let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
    let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;

    // 初始化扫描配置（扫描并发从 scan 配置读取）
    let scan_config = ScanConfig {
        job_id: job_id.clone(),
        path: src_path.to_string(),
        depth: 0,
        scan_type: ScanType::Full,
        concurrency: app_config.scan.concurrency,
        r#match: String::new(),
        exclude: String::new(),
        match_expressions: None,
        exclude_expressions: None,
        include_tags: false,
        file_list: None,
        packaged: false,
        package_depth: 0,
    };

    // 校验并发数从 integrity_check 配置读取
    let check_concurrency = app_config.integrity_check.concurrency;
    info!(
        "Integrity check configuration: scan_concurrency={}, check_concurrency={}",
        app_config.scan.concurrency, check_concurrency
    );

    // 开始扫描源路径
    let walkdir_iter = match dir_walker::walkdir(scan_config).await {
        Ok(iter) => iter,
        Err(e) => {
            error!("Failed to start directory walker: {}", e);
            return Err(AppError::ScanError(format!("Failed to start directory walker: {e}")));
        }
    };

    // 并发安全的不一致记录收集容器
    let issues: Arc<Mutex<Vec<IntegrityIssue>>> = Arc::new(Mutex::new(Vec::new()));

    // 已检查条目计数器（用于最终汇总统计）
    let checked_files = Arc::new(AtomicUsize::new(0));
    let checked_dirs = Arc::new(AtomicUsize::new(0));
    let checked_symlinks = Arc::new(AtomicUsize::new(0));

    // 创建 worker 线程池（与 sync 相同的竞争消费模式）
    let mut worker_handles = Vec::new();

    for worker_id in 0..check_concurrency {
        let src_path = src_path.to_string();
        let dest_path = dest_path.to_string();
        let walkdir_iter = walkdir_iter.clone();
        let broadcaster = broadcaster.clone();
        let issues = issues.clone();
        let checked_files = checked_files.clone();
        let checked_dirs = checked_dirs.clone();
        let checked_symlinks = checked_symlinks.clone();

        let span = info_span!("integrity_check_worker", worker_id = worker_id);
        let handle = tokio::spawn(
            async move {
                // 每个 worker 创建独立的 storage 连接
                let src_storage = match create_storage(&src_path, None).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        error!("Worker {}: Failed to create src storage: {}", worker_id, e);
                        return;
                    }
                };
                let dest_storage = match create_storage(&dest_path, None).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        error!("Worker {}: Failed to create dest storage: {}", worker_id, e);
                        return;
                    }
                };

                while let Some(msg) = walkdir_iter.next().await {
                    match msg {
                        StorageEntryMessage::Scanned(entry) => {
                            if !entry.get_is_dir() && !entry.get_is_symlink() {
                                let relative_path = entry.get_relative_path();
                                let size = entry.get_size();
                                debug!("Worker {}: Checking integrity for file: {:?}", worker_id, relative_path);

                                if quick {
                                    // 快速模式：比较文件大小、mtime，以及 uid/gid/mode（如果有）
                                    match dest_storage.get_metadata(relative_path).await {
                                        Ok(dest_entry) => {
                                            let dest_size = dest_entry.get_size();
                                            let mut mismatches = if size == dest_size {
                                                Vec::new()
                                            } else {
                                                vec![format!("size: src={size}, dest={dest_size}")]
                                            };
                                            mismatches.extend(collect_metadata_mismatches(&entry, &dest_entry, true));

                                            if mismatches.is_empty() {
                                                debug!("Quick integrity check passed for {:?}", relative_path);
                                            } else {
                                                error!(
                                                    "Metadata mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
                                                    relative_path,
                                                    mismatches.join(", "),
                                                    format_entry_metadata(&entry),
                                                    format_entry_metadata(&dest_entry)
                                                );
                                                let labels = extract_mismatch_labels(&mismatches);
                                                // size 不一致无法通过 auto_fix 修复，需要重新同步
                                                let src_mtime = entry.get_mtime();
                                                let dest_mtime = dest_entry.get_mtime();
                                                let has_content_mismatch = size != dest_size || src_mtime != dest_mtime;
                                                if auto_fix {
                                                    if let Err(e) = dest_storage
                                                        .set_metadata(
                                                            relative_path,
                                                            None,
                                                            None,
                                                            entry.get_uid(),
                                                            entry.get_gid(),
                                                            entry.get_mode(),
                                                        )
                                                        .await
                                                    {
                                                        error!("Auto-fix failed for file {:?}: {}", relative_path, e);
                                                        issues.lock().await.push(IntegrityIssue {
                                                            path: relative_path.display().to_string(),
                                                            entry_type: "file",
                                                            kind: IssueKind::Mismatch,
                                                            mismatches: labels,
                                                            fix_status: FixStatus::FixFailed,
                                                        });
                                                    } else {
                                                        info!(
                                                            "Auto-fix applied uid/gid/mode for file {:?}",
                                                            relative_path
                                                        );
                                                        if has_content_mismatch {
                                                            issues.lock().await.push(IntegrityIssue {
                                                                path: relative_path.display().to_string(),
                                                                entry_type: "file",
                                                                kind: IssueKind::Mismatch,
                                                                mismatches: labels,
                                                                fix_status: FixStatus::PartiallyFixed,
                                                            });
                                                        } else {
                                                            issues.lock().await.push(IntegrityIssue {
                                                                path: relative_path.display().to_string(),
                                                                entry_type: "file",
                                                                kind: IssueKind::Mismatch,
                                                                mismatches: labels,
                                                                fix_status: FixStatus::Fixed,
                                                            });
                                                        }
                                                    }
                                                } else {
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "file",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::NotAttempted,
                                                    });
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("File missing in destination {:?}: {}", relative_path, e);
                                            issues.lock().await.push(IntegrityIssue {
                                                path: relative_path.display().to_string(),
                                                entry_type: "file",
                                                kind: IssueKind::Missing,
                                                mismatches: vec![],
                                                fix_status: FixStatus::NotAttempted,
                                            });
                                        }
                                    }
                                } else {
                                    // 完整模式：先获取目标文件元数据（确认存在并取得其实际大小），再并发计算哈希
                                    match dest_storage.get_metadata(relative_path).await {
                                        Err(e) => {
                                            error!("File missing in destination {:?}: {}", relative_path, e);
                                            issues.lock().await.push(IntegrityIssue {
                                                path: relative_path.display().to_string(),
                                                entry_type: "file",
                                                kind: IssueKind::Missing,
                                                mismatches: vec![],
                                                fix_status: FixStatus::NotAttempted,
                                            });
                                        }
                                        Ok(dest_entry) => {
                                            let dest_size = dest_entry.get_size();
                                            let (src_hash_result, dest_hash_result) = tokio::join!(
                                                StorageEnum::compute_hash(&src_storage, relative_path, size),
                                                StorageEnum::compute_hash(&dest_storage, relative_path, dest_size)
                                            );

                                            let mut mismatches = Vec::new();
                                            match (src_hash_result, dest_hash_result) {
                                                (Ok(src_hash), Ok(dest_hash)) => {
                                                    if src_hash != dest_hash {
                                                        mismatches
                                                            .push(format!("hash: src={src_hash}, dest={dest_hash}",));
                                                    }
                                                }
                                                (Err(e), _) => {
                                                    mismatches.push(format!("src hash error: {e}"));
                                                }
                                                (_, Err(e)) => {
                                                    mismatches.push(format!("dest hash error: {e}"));
                                                }
                                            }
                                            mismatches.extend(collect_metadata_mismatches(&entry, &dest_entry, true));

                                            if mismatches.is_empty() {
                                                debug!("Integrity check passed for {:?}", relative_path);
                                            } else {
                                                error!(
                                                    "Integrity check failed for {:?}: {}\n  src:  {}\n  dest: {}",
                                                    relative_path,
                                                    mismatches.join(", "),
                                                    format_entry_metadata(&entry),
                                                    format_entry_metadata(&dest_entry)
                                                );
                                                let labels = extract_mismatch_labels(&mismatches);
                                                // hash 不一致或 hash 计算错误无法通过 auto_fix 修复
                                                let has_content_mismatch = mismatches
                                                    .iter()
                                                    .any(|m| m.starts_with("hash") || m.contains("hash error"));
                                                if auto_fix {
                                                    if let Err(e) = dest_storage
                                                        .set_metadata(
                                                            relative_path,
                                                            None,
                                                            None,
                                                            entry.get_uid(),
                                                            entry.get_gid(),
                                                            entry.get_mode(),
                                                        )
                                                        .await
                                                    {
                                                        error!("Auto-fix failed for file {:?}: {}", relative_path, e);
                                                        issues.lock().await.push(IntegrityIssue {
                                                            path: relative_path.display().to_string(),
                                                            entry_type: "file",
                                                            kind: IssueKind::Mismatch,
                                                            mismatches: labels,
                                                            fix_status: FixStatus::FixFailed,
                                                        });
                                                    } else {
                                                        info!(
                                                            "Auto-fix applied uid/gid/mode for file {:?}",
                                                            relative_path
                                                        );
                                                        if has_content_mismatch {
                                                            issues.lock().await.push(IntegrityIssue {
                                                                path: relative_path.display().to_string(),
                                                                entry_type: "file",
                                                                kind: IssueKind::Mismatch,
                                                                mismatches: labels,
                                                                fix_status: FixStatus::PartiallyFixed,
                                                            });
                                                        } else {
                                                            issues.lock().await.push(IntegrityIssue {
                                                                path: relative_path.display().to_string(),
                                                                entry_type: "file",
                                                                kind: IssueKind::Mismatch,
                                                                mismatches: labels,
                                                                fix_status: FixStatus::Fixed,
                                                            });
                                                        }
                                                    }
                                                } else {
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "file",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::NotAttempted,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }

                                checked_files.fetch_add(1, Ordering::Relaxed);
                                broadcaster
                                    .broadcast(StorageEntryMessage::IntegrityChecked(entry))
                                    .await;
                            } else if entry.get_is_dir() {
                                let relative_path = entry.get_relative_path();
                                debug!("Worker {}: Checking integrity for dir: {:?}", worker_id, relative_path);

                                match dest_storage.get_metadata(relative_path).await {
                                    Ok(dest_entry) => {
                                        let mismatches = collect_metadata_mismatches(&entry, &dest_entry, true);
                                        if mismatches.is_empty() {
                                            debug!("Dir integrity check passed for {:?}", relative_path);
                                        } else {
                                            error!(
                                                "Dir metadata mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
                                                relative_path,
                                                mismatches.join(", "),
                                                format_entry_metadata(&entry),
                                                format_entry_metadata(&dest_entry)
                                            );
                                            let labels = extract_mismatch_labels(&mismatches);
                                            if auto_fix {
                                                let src_mtime = entry.get_mtime();
                                                if let Err(e) = dest_storage
                                                    .set_metadata(
                                                        relative_path,
                                                        Some(src_mtime),
                                                        Some(src_mtime),
                                                        entry.get_uid(),
                                                        entry.get_gid(),
                                                        entry.get_mode(),
                                                    )
                                                    .await
                                                {
                                                    error!("Auto-fix failed for dir {:?}: {}", relative_path, e);
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "dir",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::FixFailed,
                                                    });
                                                } else {
                                                    info!(
                                                        "Auto-fix applied mtime/uid/gid/mode for dir {:?}",
                                                        relative_path
                                                    );
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "dir",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::Fixed,
                                                    });
                                                }
                                            } else {
                                                issues.lock().await.push(IntegrityIssue {
                                                    path: relative_path.display().to_string(),
                                                    entry_type: "dir",
                                                    kind: IssueKind::Mismatch,
                                                    mismatches: labels,
                                                    fix_status: FixStatus::NotAttempted,
                                                });
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Dir missing in destination {:?}: {}", relative_path, e);
                                        issues.lock().await.push(IntegrityIssue {
                                            path: relative_path.display().to_string(),
                                            entry_type: "dir",
                                            kind: IssueKind::Missing,
                                            mismatches: vec![],
                                            fix_status: FixStatus::NotAttempted,
                                        });
                                    }
                                }

                                checked_dirs.fetch_add(1, Ordering::Relaxed);
                                broadcaster
                                    .broadcast(StorageEntryMessage::IntegrityChecked(entry))
                                    .await;
                            } else if entry.get_is_symlink() {
                                let relative_path = entry.get_relative_path();
                                debug!(
                                    "Worker {}: Checking integrity for symlink: {:?}",
                                    worker_id, relative_path
                                );

                                match dest_storage.get_metadata(relative_path).await {
                                    Ok(dest_entry) => {
                                        let mismatches = collect_metadata_mismatches(&entry, &dest_entry, false);
                                        if mismatches.is_empty() {
                                            debug!("Symlink integrity check passed for {:?}", relative_path);
                                        } else {
                                            error!(
                                                "Symlink metadata mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
                                                relative_path,
                                                mismatches.join(", "),
                                                format_entry_metadata(&entry),
                                                format_entry_metadata(&dest_entry)
                                            );
                                            let labels = extract_mismatch_labels(&mismatches);
                                            if auto_fix {
                                                let src_mtime = entry.get_mtime();
                                                if let Err(e) = dest_storage
                                                    .set_metadata(
                                                        relative_path,
                                                        Some(src_mtime),
                                                        Some(src_mtime),
                                                        entry.get_uid(),
                                                        entry.get_gid(),
                                                        None,
                                                    )
                                                    .await
                                                {
                                                    error!("Auto-fix failed for symlink {:?}: {}", relative_path, e);
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "symlink",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::FixFailed,
                                                    });
                                                } else {
                                                    info!(
                                                        "Auto-fix applied mtime/uid/gid for symlink {:?}",
                                                        relative_path
                                                    );
                                                    issues.lock().await.push(IntegrityIssue {
                                                        path: relative_path.display().to_string(),
                                                        entry_type: "symlink",
                                                        kind: IssueKind::Mismatch,
                                                        mismatches: labels,
                                                        fix_status: FixStatus::Fixed,
                                                    });
                                                }
                                            } else {
                                                issues.lock().await.push(IntegrityIssue {
                                                    path: relative_path.display().to_string(),
                                                    entry_type: "symlink",
                                                    kind: IssueKind::Mismatch,
                                                    mismatches: labels,
                                                    fix_status: FixStatus::NotAttempted,
                                                });
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Symlink missing in destination {:?}: {}", relative_path, e);
                                        issues.lock().await.push(IntegrityIssue {
                                            path: relative_path.display().to_string(),
                                            entry_type: "symlink",
                                            kind: IssueKind::Missing,
                                            mismatches: vec![],
                                            fix_status: FixStatus::NotAttempted,
                                        });
                                    }
                                }

                                checked_symlinks.fetch_add(1, Ordering::Relaxed);
                                broadcaster
                                    .broadcast(StorageEntryMessage::IntegrityChecked(entry))
                                    .await;
                            }
                        }
                        StorageEntryMessage::Error { path, reason, .. } => {
                            error!(
                                "[IntegrityCheck] Worker {}: Walkdir error for {}: {}",
                                worker_id,
                                path.display(),
                                reason
                            );
                        }
                        _ => {}
                    }
                }
            }
            .instrument(span),
        );

        worker_handles.push(handle);
    }

    // 等待所有 worker 完成
    for handle in worker_handles {
        if let Err(e) = handle.await {
            error!("Integrity check worker panicked: {}", e);
        }
    }

    // 提取结果
    let issues = Arc::try_unwrap(issues)
        .map_err(|_| AppError::ScanError("Failed to unwrap issues".to_string()))?
        .into_inner();
    let checked_file_count = checked_files.load(Ordering::Relaxed);
    let checked_dir_count = checked_dirs.load(Ordering::Relaxed);
    let checked_symlink_count = checked_symlinks.load(Ordering::Relaxed);

    // 关闭广播通道，确保所有消费者任务完成（consumer 会自动打印 Job Summary + Scan Statistics）
    drop(broadcaster);

    // 等待所有消费者任务完成
    let mut all_successful = true;
    for handle in consumer_handles {
        match handle.await {
            Ok(Ok(())) => debug!("Consumer task completed successfully"),
            Ok(Err(e)) => {
                error!("Consumer task failed: {}", e);
                all_successful = false;
            }
            Err(e) => {
                error!("Consumer task panicked: {}", e);
                all_successful = false;
            }
        }
    }

    if all_successful {
        info!("All consumer tasks completed successfully");
    } else {
        warn!("Some consumer tasks failed or panicked");
    }

    // 打印 integrity check 校验结果
    let total_checked = checked_file_count + checked_dir_count + checked_symlink_count;
    crate::consumer::stats::print_integrity_check_result(
        &issues,
        total_checked,
        checked_file_count,
        checked_dir_count,
        checked_symlink_count,
        quick,
        auto_fix,
    );

    info!("Integrity check completed");
    Ok(())
}

/// 远程存储时间校验
///
/// 在目标远程存储上写入临时文件 → 读取 mtime → 删除 → 和 license_clock 比较差值。
/// 存储端 mtime 由服务端设置，客户端无法伪造，可作为可信外部时钟源。
/// 仅对远程存储（NFS/S3/CIFS）启用，本地存储自动跳过。
#[cfg(feature = "license")]
pub(crate) async fn verify_storage_time(dest_path: &str) -> Result<()> {
    use chrono::DateTime;
    use storage_v2::storage_enum::{StorageType, detect_storage_type};

    // 本地存储 mtime 等于系统时钟，无校验意义
    if matches!(detect_storage_type(dest_path), StorageType::Local) {
        return Ok(());
    }

    let license = match licensing::get_global_license() {
        Ok(l) => l,
        Err(_) => return Ok(()),
    };

    let license_clock = match license.license_clock {
        Some(clock) => clock,
        None => return Ok(()), // 无时钟值（旧格式），跳过
    };

    // 创建目标存储实例
    let dest_storage = match create_storage(dest_path, None).await {
        Ok(s) => s,
        Err(e) => {
            warn!("Storage time check: failed to create storage: {}", e);
            return Ok(()); // 存储不可用时不阻断，后续 sync 会报错
        }
    };

    // 探测存储服务端时间（写临时文件 → 读 mtime → 删除）
    let server_time = match dest_storage.probe_server_time().await {
        Ok(Some(mtime)) => match DateTime::from_timestamp(mtime, 0) {
            Some(dt) => dt,
            None => return Ok(()),
        },
        Ok(None) => return Ok(()), // 本地存储，跳过
        Err(e) => {
            warn!("Storage time check: probe failed: {}", e);
            return Ok(()); // 探测失败不阻断
        }
    };

    // 比较差值：T_storage vs license_clock
    let gap = server_time.signed_duration_since(license_clock);
    let threshold = chrono::Duration::minutes(5);

    if gap > threshold || gap < -threshold {
        warn!("License verification failed");
        return Err(AppError::LicenseError(licensing::error::LicenseError::ClockRegression));
    }

    Ok(())
}
