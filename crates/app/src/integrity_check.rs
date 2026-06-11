//! 完整性检查模块
//!
//! 该模块提供源/目标存储之间的一致性验证：
//! 1. 扫描源路径全部条目
//! 2. 对每个条目在目标端查 metadata（含瞬时错误重试）
//! 3. 比对 size/mtime/uid/gid/mode（dest 是 S3 时跳过元数据，仅依赖 size）
//! 4. quick 模式跳过哈希比对，full 模式做内容哈希校验
//! 5. 可选 auto-fix：仅修可修的元数据字段（mode/uid/gid 等）
//!
//! 与 [`crate::sync`] 解耦：此处不依赖 sync 主流程，仅复用通用 storage / broadcast / consumer 设施。
//!
//! S3 dest 的特殊性集中在三处：
//! - [`collect_metadata_mismatches`] 通过 `dest_is_s3` 跳过全部元数据比较
//! - quick 模式 `has_content_mismatch` 改用 `mismatches` 内容判定
//! - 目录 entry 整体跳过 `get_metadata`，因为 S3 prefix 没有真实对象

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use data_mover::error::StorageError;
use data_mover::{EntryEnum, StorageEntryMessage, StorageEnum, create_storage, redact_storage_url};
use tracing::{Instrument, debug, error, info, info_span, instrument, warn};

use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, ScanConfig, initialize_consumer_config};
use crate::consumer::ConsumerManager;
use crate::dir_walker;
use crate::error::{AppError, Result};
use crate::scan::ScanType;

// ─────────────────────────────────────────────────
// Integrity Check 数据模型
// ─────────────────────────────────────────────────

/// 不一致条目类型
#[derive(Debug, Clone)]
pub enum IssueKind {
    /// 目标不存在（确认 ENOENT）
    Missing,
    /// 元数据/内容不匹配
    Mismatch,
    /// 无法验证（瞬时 NFS 错误，如 `NFS4ERR_DELAY` 重试耗尽、连接断开）。
    /// 与 `Missing` 区分：源端列出该条目，但因服务端瞬时故障无法确认目标端是否存在。
    /// 不能直接判定"缺失"——很可能文件存在，只是这次 LOOKUP 没成功。
    Error,
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
    /// Missing / Mismatch / Error
    pub kind: IssueKind,
    /// 不匹配的字段标签，例如 `["size", "mtime", "uid"]`
    pub mismatches: Vec<String>,
    /// 修复状态
    pub fix_status: FixStatus,
}

/// integrity-check 期间针对瞬时错误的应用层重试间隔（毫秒）。
///
/// 与 nfs-rs 层的 RPC 重试是**两层互补的防线**：
/// - nfs-rs：RPC 级重试 `NFS4ERR_DELAY`，带 jitter，~50s 总等待。
/// - 应用层：单条 `get_metadata` 失败后的二次尝试，间隔更长（2s/5s/10s），
///   覆盖服务端因大量并发查询持续繁忙的场景（thundering herd 余波）。
const INTEGRITY_TRANSIENT_RETRY_DELAYS_MS: [u64; 3] = [2000, 5000, 10000];

/// 跨后端 mode 比较时用的掩码。
///
/// 不同后端返回的 mode 格式不一致：
/// - NFSv3：原始 `fattr3.mode`，含 `S_IFREG`(0o100000)/`S_IFDIR`(0o040000) 等文件类型高位（e.g. `0o100644`）
/// - NFSv4.1：RFC 5661 §5.8.2.16 `mode4` 仅为权限位，文件类型由独立 attr 表达（e.g. `0o644`）
/// - Local on Unix：`MetadataExt::mode()` 返回完整 `st_mode`，含文件类型位
/// - Local on Windows / CIFS：基于 readonly 属性合成的权限位（e.g. `0o644` / `0o755`），无类型位
/// - S3：始终 `None`，由 `dest_is_s3` 短路跳过元数据比较
///
/// 直接相等比较会在混合后端 / 跨 NFS 协议版本场景下持续误报 mode 不匹配，
/// 因此比较前用 0o777 归一化到 user/group/other 三组 rwx。
/// 已知 trade-off：suid(0o4000)/sgid(0o2000)/sticky(0o1000) 也会被丢弃。
const POSIX_MODE_COMPARE_MASK: u32 = 0o777;

/// 判断 integrity-check 期间 dest 元数据查询失败是否为"目标确实不存在"。
///
/// 仅当错误为 `FileNotFound` / `DirectoryNotFound` 时返回 true（对应 `NFS4ERR_NOENT`
/// 在 data-mover 层重试耗尽后的转换结果）。其他错误（NFS 瞬时繁忙、连接断开等）
/// 应归为 [`IssueKind::Error`]，避免误报为 `Missing`。
fn is_truly_missing(err: &StorageError) -> bool {
    matches!(err, StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_))
}

/// 带瞬时错误重试的 metadata 查询。
///
/// 返回路径：
/// - `Ok(entry)`：首次或重试中成功
/// - `Err(NotFound)`：确认对象不存在，**立即返回不重试**
/// - `Err(other)`：所有重试耗尽后仍为瞬时错误，返回**最后一次**失败的错误
async fn get_metadata_with_transient_retry(
    storage: &Arc<StorageEnum>, path: &Path,
) -> std::result::Result<EntryEnum, StorageError> {
    let mut attempt = 0usize;
    loop {
        match storage.get_metadata(path).await {
            Ok(entry) => return Ok(entry),
            Err(e) if is_truly_missing(&e) => return Err(e),
            Err(e) => {
                if attempt >= INTEGRITY_TRANSIENT_RETRY_DELAYS_MS.len() {
                    return Err(e);
                }
                warn!(
                    "Transient error on get_metadata({:?}): {} — retry {}/{} after {}ms",
                    path,
                    e,
                    attempt + 1,
                    INTEGRITY_TRANSIENT_RETRY_DELAYS_MS.len(),
                    INTEGRITY_TRANSIENT_RETRY_DELAYS_MS[attempt]
                );
                tokio::time::sleep(Duration::from_millis(INTEGRITY_TRANSIENT_RETRY_DELAYS_MS[attempt])).await;
                attempt += 1;
            }
        }
    }
}

/// 跨后端 mode 比较：仅比较 0o777 部分，丢弃文件类型位与特殊位。
/// 参见 [`POSIX_MODE_COMPARE_MASK`] 的注释了解 trade-off。
fn modes_equivalent(a: u32, b: u32) -> bool {
    a & POSIX_MODE_COMPARE_MASK == b & POSIX_MODE_COMPARE_MASK
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
///
/// 当 `dest_is_s3` 为 true 时跳过全部元数据比较：
/// - S3 的 `LastModified` 由服务端写入，PUT/`CopyObject` 都不允许客户端指定，源 mtime
///   无法保留；S3-to-S3 sync 时 dest 的 `LastModified` 是 `CopyObject` 执行时间。
/// - S3 对象不存在 POSIX uid/gid/mode 概念（S3Entry 中这三项始终为 None）。
fn collect_metadata_mismatches(src: &EntryEnum, dest: &EntryEnum, check_mode: bool, dest_is_s3: bool) -> Vec<String> {
    if dest_is_s3 {
        return Vec::new();
    }

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
        && !modes_equivalent(src_mode, dest_mode)
    {
        let src_perm = src_mode & POSIX_MODE_COMPARE_MASK;
        let dest_perm = dest_mode & POSIX_MODE_COMPARE_MASK;
        mismatches.push(format!(
            "mode: src={src_perm:#o} (raw {src_mode:#o}), dest={dest_perm:#o} (raw {dest_mode:#o})"
        ));
    }

    mismatches
}

// ─────────────────────────────────────────────────
// Auto-fix 与 issue 构造辅助函数
// ─────────────────────────────────────────────────

/// 决定 mismatch 是否含「内容差异」（不可通过 `set_metadata` 修复）。
fn detect_content_mismatch(mismatches: &[String]) -> bool {
    mismatches
        .iter()
        .any(|m| m.starts_with("size:") || m.starts_with("mtime:") || m.starts_with("hash") || m.contains("hash error"))
}

/// 把瞬时错误 / `NotFound` 错误归类为 `IssueKind` 并记录 error log。
fn classify_lookup_error(entry_type: &'static str, relative_path: &Path, err: &StorageError) -> IssueKind {
    if is_truly_missing(err) {
        error!("{} missing in destination {:?}: {}", entry_type, relative_path, err);
        IssueKind::Missing
    } else {
        error!(
            "Transient error checking {} {:?}: {} (not counted as Missing)",
            entry_type, relative_path, err
        );
        IssueKind::Error
    }
}

/// 对单个条目尝试 auto-fix；返回最终 `FixStatus` 和 issue 应填充的 mismatches 标签。
async fn apply_auto_fix(
    dest_storage: &StorageEnum, relative_path: &Path, entry_type: &'static str, src: &EntryEnum,
    has_content_mismatch: bool,
) -> FixStatus {
    // 文件 auto-fix 只改 uid/gid/mode；目录额外尝试改 mtime；symlink 改 mtime/uid/gid（mode 无效）。
    let (atime, mtime, mode) = match entry_type {
        "file" => (None, None, src.get_mode()),
        "dir" => (Some(src.get_mtime()), Some(src.get_mtime()), src.get_mode()),
        "symlink" => (Some(src.get_mtime()), Some(src.get_mtime()), None),
        _ => (None, None, None),
    };

    if let Err(e) = dest_storage
        .set_metadata(relative_path, atime, mtime, src.get_uid(), src.get_gid(), mode)
        .await
    {
        error!("Auto-fix failed for {} {:?}: {}", entry_type, relative_path, e);
        FixStatus::FixFailed
    } else {
        info!("Auto-fix applied metadata for {} {:?}", entry_type, relative_path);
        if has_content_mismatch {
            FixStatus::PartiallyFixed
        } else {
            FixStatus::Fixed
        }
    }
}

/// 构造一条 mismatch issue（按 `auto_fix` 决定 `FixStatus`）。
async fn build_mismatch_issue(
    dest_storage: &StorageEnum, relative_path: &Path, entry_type: &'static str, src: &EntryEnum,
    mismatches: Vec<String>, auto_fix: bool,
) -> IntegrityIssue {
    let labels = extract_mismatch_labels(&mismatches);
    let has_content_mismatch = detect_content_mismatch(&mismatches);
    let fix_status = if auto_fix {
        apply_auto_fix(dest_storage, relative_path, entry_type, src, has_content_mismatch).await
    } else {
        FixStatus::NotAttempted
    };
    IntegrityIssue {
        path: relative_path.display().to_string(),
        entry_type,
        kind: IssueKind::Mismatch,
        mismatches: labels,
        fix_status,
    }
}

/// 构造一条 missing/error issue。
fn build_lookup_issue(entry_type: &'static str, relative_path: &Path, err: &StorageError) -> IntegrityIssue {
    IntegrityIssue {
        path: relative_path.display().to_string(),
        entry_type,
        kind: classify_lookup_error(entry_type, relative_path, err),
        mismatches: vec![],
        fix_status: FixStatus::NotAttempted,
    }
}

// ─────────────────────────────────────────────────
// Worker 上下文 + 单条目处理函数
// ─────────────────────────────────────────────────

/// 单个 worker 的运行时上下文（避免 16 个 Arc 在每个 helper 之间逐个传递）。
struct CheckContext {
    src_storage: Arc<StorageEnum>,
    dest_storage: Arc<StorageEnum>,
    issues: Arc<Mutex<Vec<IntegrityIssue>>>,
    checked_files: Arc<AtomicUsize>,
    checked_dirs: Arc<AtomicUsize>,
    checked_symlinks: Arc<AtomicUsize>,
    quick: bool,
    auto_fix: bool,
}

impl CheckContext {
    fn push_issue(&self, issue: IntegrityIssue) {
        // 锁竞争极短（仅 push），unwrap 仅在 Mutex 被 poison 时触发——此时进程已无意义继续。
        if let Ok(mut guard) = self.issues.lock() {
            guard.push(issue);
        } else {
            error!("Issue mutex poisoned; dropping issue record");
        }
    }

    /// 文件条目处理：quick 模式比 size + 元数据；full 模式额外做 hash 比对。
    async fn process_file(&self, entry: &EntryEnum, relative_path: &Path) {
        let dest_entry = match get_metadata_with_transient_retry(&self.dest_storage, relative_path).await {
            Ok(e) => e,
            Err(e) => {
                self.push_issue(build_lookup_issue("file", relative_path, &e));
                return;
            }
        };

        let mut mismatches = Vec::new();
        let size = entry.get_size();
        let dest_size = dest_entry.get_size();
        if size != dest_size {
            mismatches.push(format!("size: src={size}, dest={dest_size}"));
        }

        if !self.quick {
            // 完整模式：补上 hash 比对
            let (src_hash_result, dest_hash_result) = tokio::join!(
                StorageEnum::compute_hash(&self.src_storage, relative_path, size),
                StorageEnum::compute_hash(&self.dest_storage, relative_path, dest_size)
            );
            match (src_hash_result, dest_hash_result) {
                (Ok(src_hash), Ok(dest_hash)) if src_hash != dest_hash => {
                    mismatches.push(format!("hash: src={src_hash}, dest={dest_hash}"));
                }
                (Ok(_), Ok(_)) => {}
                (Err(e), _) => mismatches.push(format!("src hash error: {e}")),
                (_, Err(e)) => mismatches.push(format!("dest hash error: {e}")),
            }
        }

        mismatches.extend(collect_metadata_mismatches(
            entry,
            &dest_entry,
            true,
            !self.dest_storage.has_real_directory_objects(),
        ));

        if mismatches.is_empty() {
            debug!("Integrity check passed for {:?}", relative_path);
        } else {
            error!(
                "Mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
                relative_path,
                mismatches.join(", "),
                format_entry_metadata(entry),
                format_entry_metadata(&dest_entry)
            );
            let issue = build_mismatch_issue(
                &self.dest_storage,
                relative_path,
                "file",
                entry,
                mismatches,
                self.auto_fix,
            )
            .await;
            self.push_issue(issue);
        }
    }

    /// 目录条目处理：S3 dest 直接跳过；其他后端比对元数据。
    async fn process_dir(&self, entry: &EntryEnum, relative_path: &Path) {
        // S3 没有真实目录对象，prefix 存在性由其下文件隐式保证；
        // 跳过 dir 元数据校验，避免 GetObject 404 误判为 transient。
        if !self.dest_storage.has_real_directory_objects() {
            return;
        }

        let dest_entry = match get_metadata_with_transient_retry(&self.dest_storage, relative_path).await {
            Ok(e) => e,
            Err(e) => {
                self.push_issue(build_lookup_issue("dir", relative_path, &e));
                return;
            }
        };

        let mismatches = collect_metadata_mismatches(
            entry,
            &dest_entry,
            true,
            !self.dest_storage.has_real_directory_objects(),
        );
        if mismatches.is_empty() {
            debug!("Dir integrity check passed for {:?}", relative_path);
            return;
        }
        error!(
            "Dir mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
            relative_path,
            mismatches.join(", "),
            format_entry_metadata(entry),
            format_entry_metadata(&dest_entry)
        );
        let issue = build_mismatch_issue(
            &self.dest_storage,
            relative_path,
            "dir",
            entry,
            mismatches,
            self.auto_fix,
        )
        .await;
        self.push_issue(issue);
    }

    /// 符号链接条目处理：mode 不参与比对（平台限制）。
    async fn process_symlink(&self, entry: &EntryEnum, relative_path: &Path) {
        let dest_entry = match get_metadata_with_transient_retry(&self.dest_storage, relative_path).await {
            Ok(e) => e,
            Err(e) => {
                self.push_issue(build_lookup_issue("symlink", relative_path, &e));
                return;
            }
        };

        let mismatches = collect_metadata_mismatches(
            entry,
            &dest_entry,
            false,
            !self.dest_storage.has_real_directory_objects(),
        );
        if mismatches.is_empty() {
            debug!("Symlink integrity check passed for {:?}", relative_path);
            return;
        }
        error!(
            "Symlink mismatch for {:?}: {}\n  src:  {}\n  dest: {}",
            relative_path,
            mismatches.join(", "),
            format_entry_metadata(entry),
            format_entry_metadata(&dest_entry)
        );
        let issue = build_mismatch_issue(
            &self.dest_storage,
            relative_path,
            "symlink",
            entry,
            mismatches,
            self.auto_fix,
        )
        .await;
        self.push_issue(issue);
    }

    /// 处理一条 walkdir 消息：根据 entry 类型分派到对应 helper，并增加计数 + 广播。
    async fn dispatch(&self, entry: Arc<EntryEnum>, broadcaster: &BroadcastForwarder<StorageEntryMessage>) {
        let relative_path = entry.get_relative_path().to_path_buf();
        if entry.get_is_symlink() {
            self.process_symlink(entry.as_ref(), &relative_path).await;
            self.checked_symlinks.fetch_add(1, Ordering::Relaxed);
        } else if entry.get_is_dir() {
            self.process_dir(entry.as_ref(), &relative_path).await;
            self.checked_dirs.fetch_add(1, Ordering::Relaxed);
        } else {
            self.process_file(entry.as_ref(), &relative_path).await;
            self.checked_files.fetch_add(1, Ordering::Relaxed);
        }
        broadcaster
            .broadcast(StorageEntryMessage::IntegrityChecked(entry))
            .await;
    }
}

// ─────────────────────────────────────────────────
// 公开入口 integrity_check
// ─────────────────────────────────────────────────

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
#[instrument(skip_all, fields(
    job_id = %job_id,
    src = %redact_storage_url(src_path),
    dest = %redact_storage_url(dest_path),
))]
#[allow(clippy::too_many_arguments)]
pub async fn integrity_check(
    job_id: String, job_dir: String, src_path: &str, dest_path: &str, quick: bool, auto_fix: bool,
    raw_command_line: String, progress_callback_url: Option<String>,
) -> Result<()> {
    info!(
        "Starting integrity check between {} and {}",
        redact_storage_url(src_path),
        redact_storage_url(dest_path)
    );

    let app_config = utils::app_config::AppConfig::fetch()?;

    let consumer_config = Arc::new(initialize_consumer_config(
        &job_id,
        &job_dir,
        JobType::IntegrityCheck,
        raw_command_line,
        &app_config,
        progress_callback_url,
    )?);

    let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
    let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
    consumer_manager.begin_lifecycle().await;
    let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;

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

    let check_concurrency = app_config.integrity_check.concurrency;
    info!(
        "Integrity check configuration: scan_concurrency={}, check_concurrency={}",
        app_config.scan.concurrency, check_concurrency
    );

    let walkdir_iter = match dir_walker::walkdir(scan_config).await {
        Ok(iter) => iter,
        Err(e) => {
            error!("Failed to start directory walker: {}", e);
            return Err(AppError::ScanError(format!("Failed to start directory walker: {e}")));
        }
    };

    let issues: Arc<Mutex<Vec<IntegrityIssue>>> = Arc::new(Mutex::new(Vec::new()));
    let checked_files = Arc::new(AtomicUsize::new(0));
    let checked_dirs = Arc::new(AtomicUsize::new(0));
    let checked_symlinks = Arc::new(AtomicUsize::new(0));

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
                let src_storage = match create_storage(&src_path, None, false).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        error!("Worker {}: Failed to create src storage: {}", worker_id, e);
                        return;
                    }
                };
                let dest_storage = match create_storage(&dest_path, None, false).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        error!("Worker {}: Failed to create dest storage: {}", worker_id, e);
                        return;
                    }
                };
                let ctx = CheckContext {
                    src_storage,
                    dest_storage,
                    issues,
                    checked_files,
                    checked_dirs,
                    checked_symlinks,
                    quick,
                    auto_fix,
                };

                while let Some(msg) = walkdir_iter.next().await {
                    match msg {
                        StorageEntryMessage::Scanned(entry) => ctx.dispatch(entry, &broadcaster).await,
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

    for handle in worker_handles {
        if let Err(e) = handle.await {
            error!("Integrity check worker panicked: {}", e);
        }
    }

    let issues = Arc::try_unwrap(issues)
        .map_err(|_| AppError::ScanError("Failed to unwrap issues".to_string()))?
        .into_inner()
        .map_err(|e| AppError::ScanError(format!("Issue mutex poisoned: {e}")))?;
    let checked_file_count = checked_files.load(Ordering::Relaxed);
    let checked_dir_count = checked_dirs.load(Ordering::Relaxed);
    let checked_symlink_count = checked_symlinks.load(Ordering::Relaxed);

    drop(broadcaster);

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

    consumer_manager.end_lifecycle().await;

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
    use data_mover::storage_enum::{StorageType, detect_storage_type};

    if matches!(detect_storage_type(dest_path), StorageType::Local) {
        return Ok(());
    }

    let license = match licensing::get_global_license() {
        Ok(l) => l,
        Err(_) => return Ok(()),
    };

    let license_clock = match license.license_clock {
        Some(clock) => clock,
        None => return Ok(()),
    };

    let dest_storage = match create_storage(dest_path, None, false).await {
        Ok(s) => s,
        Err(e) => {
            warn!("Storage time check: failed to create storage: {}", e);
            return Ok(());
        }
    };

    let server_time = match dest_storage.probe_server_time().await {
        Ok(Some(mtime)) => match DateTime::from_timestamp(mtime, 0) {
            Some(dt) => dt,
            None => return Ok(()),
        },
        Ok(None) => return Ok(()),
        Err(e) => {
            warn!("Storage time check: probe failed: {}", e);
            return Ok(());
        }
    };

    let gap = server_time.signed_duration_since(license_clock);
    let threshold = chrono::Duration::minutes(5);

    if gap > threshold || gap < -threshold {
        warn!("License verification failed");
        return Err(AppError::LicenseError(licensing::error::LicenseError::ClockRegression));
    }

    Ok(())
}

// ─────────────────────────────────────────────────
// 单元测试（H4：纯函数 helpers）
// ─────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn extract_mismatch_labels_strips_values() {
        let raw = vec![
            "size: src=100, dest=200".to_string(),
            "mtime: src=1000, dest=1001".to_string(),
            "uid: src=0, dest=1000".to_string(),
        ];
        let labels = extract_mismatch_labels(&raw);
        assert_eq!(labels, vec!["size", "mtime", "uid"]);
    }

    #[test]
    fn extract_mismatch_labels_handles_missing_colon() {
        let raw = vec!["just_a_word".to_string()];
        assert_eq!(extract_mismatch_labels(&raw), vec!["just_a_word"]);
    }

    #[test]
    fn detect_content_mismatch_recognizes_size_mtime_hash() {
        assert!(detect_content_mismatch(&["size: src=1, dest=2".to_string()]));
        assert!(detect_content_mismatch(&["mtime: src=1, dest=2".to_string()]));
        assert!(detect_content_mismatch(&["hash: src=a, dest=b".to_string()]));
        assert!(detect_content_mismatch(&["src hash error: io".to_string()]));
    }

    #[test]
    fn detect_content_mismatch_ignores_metadata_only() {
        assert!(!detect_content_mismatch(&["uid: src=0, dest=1".to_string()]));
        assert!(!detect_content_mismatch(&["mode: src=0o644, dest=0o755".to_string()]));
        assert!(!detect_content_mismatch(&[]));
    }

    #[test]
    fn is_truly_missing_classifies_errors() {
        assert!(is_truly_missing(&StorageError::FileNotFound("/tmp/x".into())));
        assert!(is_truly_missing(&StorageError::DirectoryNotFound("/tmp/x".into())));
        assert!(!is_truly_missing(&StorageError::OperationError("nfs busy".into())));
    }

    #[test]
    fn modes_equivalent_ignores_file_type_bits() {
        // NFSv3 / Local Unix raw st_mode vs NFSv4.1 / CIFS / Local Windows 合成权限位。
        // 下划线分隔 <类型位>_<权限位>。
        assert!(modes_equivalent(0o100_644, 0o000_644));
        assert!(modes_equivalent(0o040_755, 0o000_755));
        assert!(modes_equivalent(0o120_777, 0o000_777));
    }

    #[test]
    fn modes_equivalent_detects_real_permission_diff() {
        assert!(!modes_equivalent(0o100_644, 0o100_755));
        assert!(!modes_equivalent(0o100_644, 0o000_755));
    }

    #[test]
    fn modes_equivalent_masks_special_bits() {
        // 已知 trade-off：0o777 掩码丢弃 suid/sgid/sticky。
        // 此测试钉住该行为，未来若改为 0o7777 必须同步更新。
        assert!(modes_equivalent(0o104_755, 0o100_755)); // suid bit difference ignored
        assert!(modes_equivalent(0o102_755, 0o100_755)); // sgid bit difference ignored
        assert!(modes_equivalent(0o101_755, 0o100_755)); // sticky bit difference ignored
    }

    #[test]
    fn retry_delays_are_increasing() {
        // 防回归：缩短/打乱重试间隔会失去"覆盖 thundering-herd 余波"的语义。
        let delays = INTEGRITY_TRANSIENT_RETRY_DELAYS_MS;
        for w in delays.windows(2) {
            assert!(w[0] < w[1], "retry delays must be strictly increasing: {delays:?}");
        }
        let total: u64 = delays.iter().sum();
        assert!(total >= 15_000, "total retry budget should be ~17s, got {total}ms");
    }
}
