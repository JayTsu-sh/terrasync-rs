//! 本地迁移入口及其共用的挂载、统计和限速机制。

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use data_mover::qos::QosManager;
use task_model::migration::CopyGenerationPhase;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, warn};
use utils::app_config::AppConfig;

use crate::config::{JobType, SyncJobConfig, initialize_consumer_config};
use crate::consumer::ConsumerManager;
use crate::error::{AppError, Result, StorageError};
use crate::source_cleanup;
use crate::sync::StoragePair;
use data_mover::traversal::ScanFailureSignal;
use tokio::task::JoinSet;

use crate::receiver::LocalCopyExecutor;

mod full;
mod incremental;
mod local;

#[cfg(test)]
#[path = "orchestrator_absence_tests.rs"]
mod absence_tests;
#[path = "orchestrator_incremental_worker.rs"]
mod incremental_worker;
#[path = "orchestrator_target_reconciliation.rs"]
mod target_reconciliation;

#[cfg(test)]
#[path = "orchestrator_rename_tests.rs"]
mod rename_tests;

#[cfg(test)]
#[path = "orchestrator_retention_tests.rs"]
mod retention_tests;

#[cfg(all(test, unix))]
#[path = "orchestrator_controlled_tests.rs"]
mod controlled_tests;

#[cfg(all(test, unix))]
#[path = "orchestrator_controlled_real_tests.rs"]
mod controlled_real_tests;

#[cfg(all(test, unix))]
#[path = "orchestrator_root_error_tests.rs"]
mod root_error_tests;

/// `StoragePair` 创建失败后的最大重试次数
const STORAGE_PAIR_MAX_RETRIES: usize = 3;

/// 同时创建 NFS `StoragePair`（mount）的最大并发数。
/// 设为 2：nfs-rs 在 Windows 上绑定特权端口（<1024）时，并发量过大会导致端口
/// 竞争和 `TIME_WAIT` 积累，最终 WSAEADDRINUSE；限制并发可显著降低冲突概率。
const NFS_MOUNT_CONCURRENCY: usize = 2;

/// 非 NFS 协议（CIFS/SMB、S3、本地）的 mount/connect 阶段并发上限。
/// SMB 无特权端口约束，S3 无握手开销，应远大于典型 `copy_concurrency` 以让所有 worker
/// 的握手并行完成（SMB 协议设计支持上千客户端同时连接，不会被 server 视作压力）。
const NON_NFS_MOUNT_CONCURRENCY: usize = 32;

/// 统计 reporter 周期（首条 tick 之前若 mount 尚未完成，仅打 INFO 不打 WARN）。
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// 根据 storage URL 返回 mount/connect 阶段的最大并发数。
///
/// URL scheme 比较按 RFC 3986 是 case-insensitive 的（"NFS://"、"Nfs://"
/// 与 "nfs://" 等价），所以 prefix 比较走 ASCII 不区分大小写，避免上层
/// 配置或 URL 解析器返回非小写时误走 `NON_NFS` 并发上限，进而触发 nfs-rs
/// 特权端口 `TIME_WAIT` 拥塞。
fn mount_concurrency_for(path: &str) -> usize {
    const NFS_PREFIX: &str = "nfs://";
    if path.len() >= NFS_PREFIX.len() && path[..NFS_PREFIX.len()].eq_ignore_ascii_case(NFS_PREFIX) {
        NFS_MOUNT_CONCURRENCY
    } else {
        NON_NFS_MOUNT_CONCURRENCY
    }
}

/// 返回 src 和 dest 的 mount 并发上限的较小值（双方都受限时按更紧的那一侧走）。
fn mount_concurrency_for_pair(src_path: &str, dest_path: &str) -> usize {
    std::cmp::min(mount_concurrency_for(src_path), mount_concurrency_for(dest_path))
}

/// 等待所有 worker 完成 mount 初始化，输出存活汇总
///
/// 使用 countdown latch：每个 worker mount 完成（无论成功/失败）后 `done_counter` +1，
/// 等到所有 pool 的 done 计数都达到 expected 后汇总。
async fn wait_for_mount_init(pools: &[(&AtomicUsize, &AtomicUsize, &str)], expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let all_done = pools
            .iter()
            .all(|(done, _, _)| done.load(Ordering::Acquire) >= expected);
        if all_done || tokio::time::Instant::now() > deadline {
            let mut all_alive = true;
            for &(_, alive, label) in pools {
                let count = alive.load(Ordering::Acquire);
                if count < expected {
                    warn!("{} startup degraded: {}/{} alive", label, count, expected);
                    all_alive = false;
                } else {
                    info!("All {} started successfully: {}/{}", label, count, expected);
                }
            }
            if all_alive && pools.len() > 1 {
                info!("All workers started successfully");
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 带限流和退避重试的 `StoragePair` 创建
///
/// 通过 semaphore 限制同时进行 NFS mount 的并发数，
/// 失败时指数退避重试，避免 portmapper 被突发请求打满。
async fn create_storage_pair_with_retry(
    semaphore: &Semaphore, src_path: &str, dest_path: &str, block_size: Option<usize>, worker_label: &str,
    cancel: Option<&CancellationToken>,
) -> Result<StoragePair> {
    let initialize = async {
        let mut last_error = None;

        for attempt in 0..=STORAGE_PAIR_MAX_RETRIES {
            let permit = semaphore
                .acquire()
                .await
                .map_err(|_| AppError::ConfigError("Mount semaphore closed".to_string()))?;

            match StoragePair::new(src_path, dest_path, block_size).await {
                Ok(pair) => {
                    if attempt > 0 {
                        info!(
                            "{}: StoragePair created successfully after {} retries",
                            worker_label, attempt
                        );
                    }
                    return Ok(pair);
                }
                Err(e) => {
                    warn!(
                        "{}: StoragePair creation failed (attempt {}/{}): {}",
                        worker_label,
                        attempt + 1,
                        STORAGE_PAIR_MAX_RETRIES + 1,
                        e
                    );
                    last_error = Some(e);
                    // 显式释放 permit 后再进入退避等待，避免退避期间占用 mount 槽位
                    drop(permit);

                    if attempt < STORAGE_PAIR_MAX_RETRIES {
                        // 使用较长的退避：nfs-rs 绑定特权端口失败后端口进入 TIME_WAIT，
                        // 过短的退避（200ms）无法避开同一端口被再次选中。
                        // 2s/4s/8s 给 nfs-rs 更大概率在下次尝试时随机到可用端口。
                        let backoff_ms = 2000u64 * (1u64 << attempt);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            AppError::ConfigError(format!("{worker_label}: StoragePair creation exhausted all retries"))
        }))
    };
    match cancel {
        Some(cancel) => tokio::select! {
            biased;
            () = cancel.cancelled() => Err(StorageError::Cancelled.into()),
            result = initialize => result,
        },
        None => initialize.await,
    }
}

/// 收束已启动的接收端初始化；源故障必须能打断挂载等待。
async fn collect_controlled_receivers(
    mut mounts: JoinSet<Result<LocalCopyExecutor>>, source_guard: &ScanFailureSignal, task_cancel: &CancellationToken,
) -> (Vec<LocalCopyExecutor>, Option<AppError>) {
    let mut executors = Vec::new();
    let mut failure = None;
    let mut source_seen = false;
    while !mounts.is_empty() {
        tokio::select! {
            biased;
            () = source_guard.failed(), if !source_seen => {
                source_seen = true;
                task_cancel.cancel();
            }
            result = mounts.join_next() => {
                let Some(result) = result else { break };
                match result.unwrap_or_else(|error| Err(error.into())) {
                    Ok(executor) => executors.push(executor),
                    // 同一共享取消产生的派生错误不冒充新的挂载原因。
                    Err(error) if error.storage_error().is_some_and(StorageError::is_cancelled) => {}
                    Err(error) => { failure.get_or_insert(error); task_cancel.cancel(); }
                }
            }
        }
    }
    // 即使源故障早于任何mount的准入，也保留同一共享取消事实。
    if source_guard.failure().is_some() {
        task_cancel.cancel();
    }
    (executors, failure)
}

/// 按已提交基线和复制策略编排单进程全量或增量迁移。
pub(crate) struct SyncOrchestrator {
    config: SyncJobConfig,
    #[cfg(test)]
    filter_time: Option<i64>,
}

impl SyncOrchestrator {
    #[cfg(test)]
    pub(crate) fn with_filter_time(mut self, epoch: i64) -> Self {
        self.filter_time = Some(epoch);
        self
    }
    /// 创建单进程模式的编排器
    pub(crate) fn new_local(config: SyncJobConfig) -> Self {
        Self {
            config,
            #[cfg(test)]
            filter_time: None,
        }
    }

    /// 按数据库基线和复制策略执行本地迁移。
    pub(crate) async fn run(&self) -> Result<()> {
        let app_config = AppConfig::fetch()?;
        let c = &self.config;
        let mut notifications = initialize_consumer_config(
            &c.job_id,
            &c.job_dir,
            JobType::Copy,
            c.raw_command_line.clone(),
            &app_config,
            c.progress_callback_url.clone(),
        )?;
        // 此时尚未连接存储或改变基线；准入失败也使用同一最终报告通道。
        let (mut consumers, initialization_error) = match ConsumerManager::statistics_only(&notifications) {
            Ok(consumers) => (consumers, None),
            Err(error) => {
                // 数据库配置无效时仍通知外部；原错误返回，不把无数据库当成迁移降级。
                notifications.db_config.enabled = false;
                (ConsumerManager::statistics_only(&notifications)?, Some(error))
            }
        };
        let result = match initialization_error {
            Some(error) => Err(error),
            None => self.run_local_prepared(&app_config, &mut consumers).await,
        };
        let result = source_cleanup::report_failure(result, &consumers).await;
        consumers
            .finish_unpersisted_migration(if result.is_ok() {
                CopyGenerationPhase::Completed
            } else {
                CopyGenerationPhase::Failed
            })
            .await;
        if let Err(error) = &result {
            consumers.record_migration_error(error).await;
        }
        consumers.end_lifecycle().await;
        result
    }

    /// 启动统计 reporter task
    ///
    /// `mount_completed` 在所有 worker 完成 mount 初始化后由 orchestrator 置位；
    /// 之前 `active_tasks` 必然小于 `copy_concurrency`（worker 还在握手中），
    /// 不构成异常，仅打 INFO 避免把 "Processed 0 entries in 10s" 误报为 WARN。
    fn spawn_stats_reporter(
        active_entry_counter: Arc<AtomicUsize>, entry_counter: Arc<AtomicUsize>, size_counter: Arc<AtomicU64>,
        active_tokio_task_counter: Arc<AtomicUsize>, copy_concurrency: usize, mount_completed: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let span = info_span!("stats_reporter");
        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(STATS_REPORT_INTERVAL);
                loop {
                    interval.tick().await;
                    let active_entries = active_entry_counter.load(Ordering::Relaxed);
                    let count = entry_counter.swap(0, Ordering::Relaxed);
                    let total_size = size_counter.swap(0, Ordering::Relaxed);
                    let active_tasks = active_tokio_task_counter.load(Ordering::Relaxed);
                    let mount_ok = mount_completed.load(Ordering::Acquire);
                    if active_tasks == copy_concurrency || !mount_ok {
                        info!(
                            "Processed {} entries ({} bytes) in 10s, active_tasks: {}, active_entries: {}",
                            count, total_size, active_tasks, active_entries
                        );
                    } else {
                        warn!(
                            "Processed {} entries ({} bytes) in 10s, active_tasks: {}, active_entries: {}",
                            count, total_size, active_tasks, active_entries
                        );
                    }
                }
            }
            .instrument(span),
        )
    }
}

/// 创建 `QoS` 管理器（crate 内共享，供 orchestrator 和 `remote_sync` 调用）
pub(crate) fn create_qos_manager(qos: Option<&String>, peak_qos_rate: f32, iops: Option<u32>) -> Option<QosManager> {
    match qos {
        Some(qos_str) => match QosManager::try_new(Some(qos_str), peak_qos_rate, iops) {
            Ok(mgr) => Some(mgr),
            Err(e) => {
                error!("Failed to parse QoS configuration: {}", e);
                None
            }
        },
        None => match iops {
            Some(iops_val) if iops_val > 0 => match QosManager::try_new(None, peak_qos_rate, Some(iops_val)) {
                Ok(mgr) => Some(mgr),
                Err(e) => {
                    error!("Failed to create IOPS-only QoS: {}", e);
                    None
                }
            },
            _ => None,
        },
    }
}

#[cfg(test)]
#[path = "orchestrator_cancellation_tests.rs"]
mod cancellation_tests;

#[cfg(test)]
mod mount_concurrency_tests {
    use super::{NFS_MOUNT_CONCURRENCY, NON_NFS_MOUNT_CONCURRENCY, mount_concurrency_for, mount_concurrency_for_pair};

    #[test]
    fn nfs_path_uses_nfs_quota() {
        assert_eq!(
            mount_concurrency_for("nfs://10.0.0.1:2049/export"),
            NFS_MOUNT_CONCURRENCY
        );
    }

    #[test]
    fn smb_path_uses_non_nfs_quota() {
        assert_eq!(
            mount_concurrency_for("smb://user:pwd@10.0.0.1/share"),
            NON_NFS_MOUNT_CONCURRENCY
        );
    }

    #[test]
    fn s3_and_local_paths_use_non_nfs_quota() {
        assert_eq!(
            mount_concurrency_for("s3://ak:sk@bucket.host/p"),
            NON_NFS_MOUNT_CONCURRENCY
        );
        assert_eq!(mount_concurrency_for("C:\\path\\to\\dir"), NON_NFS_MOUNT_CONCURRENCY);
        assert_eq!(mount_concurrency_for("/abs/path"), NON_NFS_MOUNT_CONCURRENCY);
    }

    #[test]
    fn nfs_scheme_is_case_insensitive() {
        // RFC 3986: scheme is case-insensitive. Accept upper/mixed case so a
        // config file with "NFS://…" doesn't silently bypass the portmapper guard.
        assert_eq!(mount_concurrency_for("NFS://10.0.0.1/export"), NFS_MOUNT_CONCURRENCY);
        assert_eq!(mount_concurrency_for("Nfs://10.0.0.1/export"), NFS_MOUNT_CONCURRENCY);
        assert_eq!(mount_concurrency_for("nFs://10.0.0.1/export"), NFS_MOUNT_CONCURRENCY);
    }

    #[test]
    fn pair_uses_minimum_of_both_sides() {
        // 同协议：取该协议容量
        assert_eq!(
            mount_concurrency_for_pair("nfs://a/x", "nfs://b/y"),
            NFS_MOUNT_CONCURRENCY
        );
        assert_eq!(
            mount_concurrency_for_pair("smb://u:p@a/x", "smb://u:p@b/y"),
            NON_NFS_MOUNT_CONCURRENCY
        );
        // 混合：取较紧的一侧（NFS）
        assert_eq!(
            mount_concurrency_for_pair("nfs://a/x", "smb://u:p@b/y"),
            NFS_MOUNT_CONCURRENCY
        );
        assert_eq!(
            mount_concurrency_for_pair("smb://u:p@a/x", "nfs://b/y"),
            NFS_MOUNT_CONCURRENCY
        );
    }
}
