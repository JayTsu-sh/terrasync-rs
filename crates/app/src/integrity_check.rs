//! 完整性检查模块
//!
//! 该模块提供源/目标存储之间的一致性验证：
//! 1. 扫描源路径全部条目
//! 2. 由 data-mover 统一解析目标 metadata、重试并归因错误
//! 3. 使用结构化 mismatch 比对类型、size、mtime、uid、gid、mode
//! 4. quick 模式跳过内容读取，full 模式流式逐字节比较并报告偏移
//! 5. 可选 auto-fix：仅修可修的元数据字段（mode/uid/gid 等）
//!
//! 与 [`crate::sync`] 解耦：此处不依赖 sync 主流程，仅复用通用 storage / broadcast / consumer 设施。
//!
//! S3 目标没有真实目录对象，因此目录 entry 跳过目标 metadata 查询；文件的
//! POSIX 元数据跳过策略由 data-mover 统一处理。

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use data_mover::error::StorageError;
use data_mover::{
    EntryEnum, IntegrityCheck, IntegrityCheckMode, IntegrityCheckOptions, MismatchDataField, MismatchMetaField,
    MtimePrecision, StorageEntryMessage, StorageEnum, redact_storage_url,
};
use tracing::{Instrument, debug, error, info, info_span, instrument, warn};

use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, ScanConfig, initialize_consumer_config};
use crate::consumer::ConsumerManager;
use crate::dir_walker;
use crate::error::{AppError, Result};
use crate::scan::ScanType;
use crate::storage_factory::{StorageRole, create_storage_for_role};

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

/// 判断 integrity-check 期间 dest 元数据查询失败是否为"目标确实不存在"。
///
/// 仅当错误为 `FileNotFound` / `DirectoryNotFound` 时返回 true（对应 `NFS4ERR_NOENT`
/// 在 data-mover 层重试耗尽后的转换结果）。其他错误（NFS 瞬时繁忙、连接断开等）
/// 应归为 [`IssueKind::Error`]，避免误报为 `Missing`。
fn is_truly_missing(err: &StorageError) -> bool {
    matches!(err, StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_))
}

// ─────────────────────────────────────────────────
// Auto-fix 与 issue 构造辅助函数
// ─────────────────────────────────────────────────

/// 决定 mismatch 是否含「内容差异」（不可通过 `set_metadata` 修复）。
fn detect_content_mismatch(mismatches: &[String]) -> bool {
    mismatches.iter().any(|m| {
        matches!(
            m.as_str(),
            "entry_kind" | "size" | "read_length" | "stream_offset" | "content" | "mtime"
        )
    })
}

fn structured_mismatch_labels(error: &StorageError) -> Option<Vec<String>> {
    match error {
        StorageError::MismatchData(fields) => Some(
            fields
                .iter()
                .map(|field| match field {
                    MismatchDataField::EntryKind { .. } => "entry_kind",
                    MismatchDataField::Size { .. } => "size",
                    MismatchDataField::ReadLength { .. } => "read_length",
                    MismatchDataField::StreamOffset { .. } => "stream_offset",
                    MismatchDataField::Content { .. } => "content",
                })
                .map(str::to_string)
                .collect(),
        ),
        StorageError::MismatchMeta(fields) => Some(
            fields
                .iter()
                .map(|field| match field {
                    MismatchMetaField::Mtime { .. } => "mtime",
                    MismatchMetaField::Uid { .. } => "uid",
                    MismatchMetaField::Gid { .. } => "gid",
                    MismatchMetaField::Mode { .. } => "mode",
                    MismatchMetaField::HdfsOwner { .. } => "hdfs_owner",
                    MismatchMetaField::HdfsGroup { .. } => "hdfs_group",
                })
                .map(str::to_string)
                .collect(),
        ),
        _ => None,
    }
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
        mismatches,
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
    options: IntegrityCheckOptions,
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

    async fn process_entry(&self, entry: &EntryEnum, relative_path: &Path, entry_type: &'static str) -> Option<String> {
        match IntegrityCheck::check_with_source_entry_and_options(
            &self.src_storage,
            &self.dest_storage,
            entry,
            self.options,
            None,
        )
        .await
        {
            Ok(()) => {
                debug!("Integrity check passed for {:?}", relative_path);
                None
            }
            Err(error) => {
                if let Some(labels) = structured_mismatch_labels(&error) {
                    error!("Integrity mismatch for {:?}: {:?}", relative_path, error);
                    let issue = build_mismatch_issue(
                        &self.dest_storage,
                        relative_path,
                        entry_type,
                        entry,
                        labels,
                        self.auto_fix,
                    )
                    .await;
                    self.push_issue(issue);
                    None
                } else if matches!(error, StorageError::Cancelled) {
                    Some("integrity check cancelled".to_string())
                } else {
                    self.push_issue(build_lookup_issue(entry_type, relative_path, &error));
                    Some(error.to_string())
                }
            }
        }
    }

    /// 处理一条 walkdir 消息：根据 entry 类型分派到对应 helper，并增加计数 + 广播。
    async fn dispatch(&self, entry: Arc<EntryEnum>, broadcaster: &BroadcastForwarder<StorageEntryMessage>) {
        let relative_path = entry.get_relative_path().to_path_buf();
        let error_reason = if entry.get_is_symlink() {
            let result = self.process_entry(entry.as_ref(), &relative_path, "symlink").await;
            self.checked_symlinks.fetch_add(1, Ordering::Relaxed);
            result
        } else if entry.get_is_dir() {
            let result = if self.dest_storage.has_real_directory_objects() {
                self.process_entry(entry.as_ref(), &relative_path, "dir").await
            } else {
                None
            };
            self.checked_dirs.fetch_add(1, Ordering::Relaxed);
            result
        } else {
            let result = self.process_entry(entry.as_ref(), &relative_path, "file").await;
            self.checked_files.fetch_add(1, Ordering::Relaxed);
            result
        };
        if let Some(reason) = error_reason {
            broadcaster
                .broadcast(StorageEntryMessage::Error {
                    event: data_mover::ErrorEvent::IntegrityCheck,
                    path: relative_path,
                    entry: Some(entry),
                    reason,
                })
                .await;
        } else {
            broadcaster
                .broadcast(StorageEntryMessage::IntegrityChecked(entry))
                .await;
        }
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
/// - `auto_fix`: 是否自动修复可修复的元数据；内容差异仅标记为 partially fixed
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
    let mtime_precision = match app_config.integrity_check.mtime_precision {
        utils::app_config::IntegrityMtimePrecision::Exact => MtimePrecision::Exact,
        utils::app_config::IntegrityMtimePrecision::Auto => MtimePrecision::Auto,
    };
    let check_options = IntegrityCheckOptions::new(if quick {
        IntegrityCheckMode::Quick
    } else {
        IntegrityCheckMode::Full
    })
    .with_mtime_precision(mtime_precision)
    .with_mtime_tolerance(Duration::from_millis(app_config.integrity_check.mtime_tolerance_ms));
    info!(
        "Integrity check configuration: scan_concurrency={}, check_concurrency={}, mtime_precision={:?}, mtime_tolerance_ms={}",
        app_config.scan.concurrency, check_concurrency, mtime_precision, app_config.integrity_check.mtime_tolerance_ms
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
                let src_storage = match create_storage_for_role(&src_path, None, false, StorageRole::Source).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        error!("Worker {}: Failed to create src storage: {}", worker_id, e);
                        return;
                    }
                };
                let dest_storage =
                    match create_storage_for_role(&dest_path, None, false, StorageRole::Destination).await {
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
                    options: check_options,
                    auto_fix,
                };

                while let Some(msg) = walkdir_iter.next().await {
                    match msg {
                        StorageEntryMessage::Scanned(entry) => ctx.dispatch(entry, &broadcaster).await,
                        StorageEntryMessage::Error {
                            path, entry, reason, ..
                        } => {
                            error!(
                                "[IntegrityCheck] Worker {}: Walkdir error for {}: {}",
                                worker_id,
                                path.display(),
                                reason
                            );
                            broadcaster
                                .broadcast(StorageEntryMessage::Error {
                                    event: data_mover::ErrorEvent::IntegrityCheck,
                                    path,
                                    entry,
                                    reason,
                                })
                                .await;
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

    let dest_storage = match create_storage_for_role(dest_path, None, false, StorageRole::Destination).await {
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
    fn structured_data_mismatches_map_to_stable_labels() {
        let error = StorageError::MismatchData(vec![
            MismatchDataField::Size { src: 1, dest: 2 },
            MismatchDataField::Content { offset: 7 },
        ]);
        assert_eq!(structured_mismatch_labels(&error).unwrap(), vec!["size", "content"]);
    }

    #[test]
    fn structured_metadata_mismatches_map_to_stable_labels() {
        let error = StorageError::MismatchMeta(vec![
            MismatchMetaField::Mtime { src: 1, dest: 2 },
            MismatchMetaField::Mode {
                src: 0o644,
                dest: 0o755,
            },
        ]);
        assert_eq!(structured_mismatch_labels(&error).unwrap(), vec!["mtime", "mode"]);
    }

    #[test]
    fn detect_content_mismatch_ignores_metadata_only() {
        assert!(detect_content_mismatch(&["size".to_string()]));
        assert!(detect_content_mismatch(&["content".to_string()]));
        assert!(!detect_content_mismatch(&["uid".to_string()]));
        assert!(!detect_content_mismatch(&["mode".to_string()]));
        assert!(!detect_content_mismatch(&[]));
    }

    #[test]
    fn is_truly_missing_classifies_errors() {
        assert!(is_truly_missing(&StorageError::FileNotFound("/tmp/x".into())));
        assert!(is_truly_missing(&StorageError::DirectoryNotFound("/tmp/x".into())));
        assert!(!is_truly_missing(&StorageError::OperationError("nfs busy".into())));
    }
}
