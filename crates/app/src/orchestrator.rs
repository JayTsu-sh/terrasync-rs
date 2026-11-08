//! Sync 编排器
//!
//! SyncOrchestrator 封装 sync/incremental_sync 的核心逻辑，
//! 通过 Transport 抽象层连接 Sender 和 Receiver，支持单进程和双进程两种模式。

use crate::error::StorageError;
use crate::file_copy::transfer as copy_transfer;
// 标准库
use crate::copy_dispatch::{CopyQueues, partition_scan};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use task_model::ObservedEntryExt;
use task_model::TransferMode;

/// RAII guard：构造时 +1，Drop 时 -1，确保计数器在 panic/early-return 时也能正确回收。
struct AtomicCounterGuard<'a>(&'a AtomicUsize);

impl<'a> AtomicCounterGuard<'a> {
    fn increment(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for AtomicCounterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

// 外部 crate
use crate::storage_factory::StorageContext;
#[cfg(windows)]
use crate::storage_factory::backend_kind_from_location;
use data_mover::qos::QosManager;
use data_mover::storage::NamespaceReadPolicy;
use db::ClickHouseDatabase;
use db::{self, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use futures::FutureExt;
use task_model::migration::{
    CopyGenerationPhase, CopyGenerationPlan, CopyRequestDescriptor, MigrationMode, MigrationReason, MigrationReport,
    TargetCleanupPolicy,
};
use task_model::{StorageEntryMessage, TargetRetentionReason};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{Instrument, error, info, info_span, trace, warn};
use transport::in_process::create_in_process_pair;
use transport::message::SenderMsg;
use transport::traits::{ReceiverTransport, SenderTransport};
use utils::app_config::AppConfig;

// 内部模块
use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, SyncJobConfig, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::copy_completion;
use crate::copy_execution::{CopyDatabaseExecution, CopyLease, begin_copy_catalogue, compile_copy_request};
use crate::copy_history::{HistoryCompletion, HistoryCopyPolicy};
use crate::dir_walker::DirectoryWalker;
use crate::error::{AppError, Result};
use crate::file_copy::{CopyOptions, VersionFailureContext};
#[cfg(feature = "license")]
use crate::integrity_check::verify_storage_time;
use crate::migration_plan::MigrationPlan;
use crate::receiver::{ReceiverConfig, process_entry_on_receiver};
use crate::scan::ScanType;
use crate::sender::{SenderWorkerConfig, sender_worker};
use crate::source_cleanup::{self, SourceCleanupCheck, remove_confirmed_sources};
use crate::storage_factory::{StorageRole, create_storage_for_role};
#[cfg(windows)]
use crate::sync::check_admin_privileges;
use crate::sync::{StoragePair, parse_size};
use crate::sync_catalog::batch_processing_to_generate_message;
use crate::target_absence::SourceAbsenceCheck;
use crate::task_stream::finish_phase;
use tokio_util::sync::CancellationToken;

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

/// 大文件日志阈值（512 MiB）
const LARGE_FILE_LOG_THRESHOLD: u64 = 512 * 1024 * 1024;

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

/// Sync 编排器
///
/// 统一管理 sync 和 `incremental_sync` 的执行流程。
/// 单进程模式下通过 `InProcessTransport` 连接 sender 和 receiver task。
/// 双进程模式下通过 QUIC Transport 连接远端 Receiver（Phase 3）。
/// Sync 模式
enum SyncMode {
    /// 单进程：src+dest 在同一进程
    Local,
    /// 双进程：通过 QUIC 连接远端 Receiver
    Remote {
        remote_addr: String,
        /// 服务端 DER 证书（来自 `serve --tls-cert-out`），None 时跳过验证（输出 WARNING）
        tls_server_cert: Option<Vec<u8>>,
        /// Token 鉴权（与 `serve --token` 配对），None 时发送空 token
        auth_token: Option<String>,
    },
}

pub struct SyncOrchestrator {
    config: SyncJobConfig,
    mode: SyncMode,
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
    pub fn new_local(config: SyncJobConfig) -> Self {
        Self {
            config,
            mode: SyncMode::Local,
            #[cfg(test)]
            filter_time: None,
        }
    }

    /// 创建双进程模式的编排器（Sender 侧，连接远端 Receiver）
    ///
    /// `tls_server_cert`: 服务端 DER 证书字节（来自 `serve --tls-cert-out`），
    /// 传入 `None` 时跳过 TLS 验证（输出 WARNING）。
    /// `auth_token`: 与 Receiver `serve --token` 配对的鉴权 token，`None` 时发送空 token。
    pub fn new_remote(
        config: SyncJobConfig, remote_addr: &str, tls_server_cert: Option<Vec<u8>>, auth_token: Option<String>,
    ) -> Self {
        Self {
            config,
            mode: SyncMode::Remote {
                remote_addr: remote_addr.to_string(),
                tls_server_cert,
                auth_token,
            },
            #[cfg(test)]
            filter_time: None,
        }
    }

    /// 执行 sync 管线
    ///
    /// 根据已提交数据库基线和复制策略选择；启用数据库时构造失败直接上报。
    pub async fn run(&self) -> Result<()> {
        match &self.mode {
            SyncMode::Local => self.run_local().await,
            SyncMode::Remote {
                remote_addr,
                tls_server_cert,
                auth_token,
            } => {
                self.run_sync_remote(remote_addr, tls_server_cert.as_deref(), auth_token.as_deref())
                    .await
            }
        }
    }

    async fn run_local(&self) -> Result<()> {
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
            consumers.record_task_error(error).await;
        }
        consumers.end_lifecycle().await;
        result
    }

    async fn run_local_prepared(&self, app_config: &AppConfig, consumers: &mut ConsumerManager) -> Result<()> {
        let c = &self.config;
        info!(transfer_mode = ?c.transfer_mode, "Sync transfer mode selected");
        if c.packaged && c.transfer_mode != TransferMode::Direct {
            return Err(AppError::ConfigError(
                "Packaged sync supports Direct only; generated archives do not support --resume".into(),
            ));
        }
        let (observation_plan, request) = {
            let destination = create_storage_for_role(&c.dest_path, None, true, StorageRole::Destination).await?;
            let source = create_storage_for_role(&c.src_path, None, false, StorageRole::Source).await?;
            if !c.source_cleanup.retains_source() && source.storage().identity() == destination.storage().identity() {
                return Err(AppError::ConfigError("移动任务的源端和目标端不能是同一存储根".into()));
            }
            copy_transfer::validate_transfer_mode(&destination, c.transfer_mode)?;
            let plan = MigrationPlan::compile(&source, &destination, c.enable_acl).await?;
            #[cfg(test)]
            let plan = {
                let mut plan = plan;
                if let Some(epoch) = self.filter_time {
                    plan.filter_time_anchor = epoch;
                }
                plan
            };
            plan.log();
            let request = compile_copy_request(
                c,
                &source,
                &destination,
                &plan,
                app_config,
                consumers.copy_attempt_id().await?,
            )
            .await?;
            (Some(plan), request)
        };

        // 模式选择与代次准备共用独占权，避免读取旧策略后被另一执行切换代次。
        let _lease = CopyLease::acquire(&app_config.database.clickhouse, c)?;
        if !c.source_cleanup.retains_source() && !app_config.database.enabled {
            return Err(AppError::ConfigError(
                "当前移动任务要求启用 ClickHouse 目录和代次记录".into(),
            ));
        }
        if c.packaged && !c.source_cleanup.retains_source() {
            return Err(AppError::ConfigError(
                "打包移动需要逐项清单确认与校验，当前拒绝提前删除源目录".into(),
            ));
        }
        // database.enabled=true 时必须成功构造；false 时忽略错误（未启用），走文件系统兜底
        let db_instance = if app_config.database.enabled {
            Some(ClickHouseDatabase::connect(&app_config.database.clickhouse, &c.job_id).await?)
        } else {
            None
        };
        let mut pipeline_database = db_instance.clone();
        let execution = pipeline_database
            .as_mut()
            .map(|database| CopyDatabaseExecution::prepare(database, CancellationToken::new()))
            .transpose()?;
        consumers.set_copy_database_execution(execution.clone())?;
        // 任务边界堆存 future，避免编排状态机与数据库恢复层的 poll 栈叠加。
        let work = self
            .run_planned_local(observation_plan, &request, app_config, consumers, pipeline_database)
            .boxed_local();
        let mut result = match execution {
            Some(execution) => execution.run(work).await,
            None => work.await,
        };
        if let Some(database) = db_instance {
            let phase = if result.is_ok() {
                CopyGenerationPhase::Completed
            } else {
                CopyGenerationPhase::Failed
            };
            let finalization = database.finish_copy_generation(phase).await;
            if let Ok(generation) = database.current_copy_generation().await {
                consumers.set_migration_report(generation.report()).await;
                if let Err(source) = result {
                    result = match consumers
                        .record_target_retention_range(
                            TargetRetentionReason::IncompleteEnumeration,
                            "本轮迁移未完整收尾，未展开的旧目标范围全部保留，未逐项确认源缺失".into(),
                        )
                        .await
                    {
                        Ok(()) => Err(source),
                        Err(reporting) => Err(AppError::MigrationReportingFailed {
                            source: Box::new(source),
                            reporting: Box::new(reporting),
                        }),
                    };
                }
            }
            return match (result, finalization) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(source), Ok(())) => Err(source),
                (Ok(()), Err(source)) => Err(source.into()),
                (Err(source), Err(finalization)) => Err(AppError::MigrationFinalizationFailed {
                    source: Box::new(source),
                    finalization,
                }),
            };
        }
        result
    }

    async fn run_planned_local(
        &self, observation_plan: Option<MigrationPlan>, request: &CopyRequestDescriptor, app_config: &AppConfig,
        consumers: &mut ConsumerManager, database: Option<ClickHouseDatabase>,
    ) -> Result<()> {
        let report = if let Some(database) = &database {
            begin_copy_catalogue(database, request).await?;
            database.current_copy_generation().await?.report()
        } else {
            MigrationReport {
                generation: None,
                execution_id: request.execution_id.clone(),
                filter_evaluated_at: request.filter_evaluated_at,
                source_cleanup: request.source_cleanup,
                target_history: request.binding.target_history,
                plan: CopyGenerationPlan::decide(request, None, false)?,
                phase: CopyGenerationPhase::Prepared,
                baseline_prepared: false,
            }
        };
        info!(mode=?report.plan.mode, target_cleanup=?report.plan.target_cleanup, reasons=?report.plan.reasons,
            filter_evaluated_at=report.filter_evaluated_at, generation=?report.generation, "Migration execution plan");
        consumers.set_migration_report(report.clone()).await;
        if report.plan.target_cleanup == TargetCleanupPolicy::Preserve {
            let reason = if !request.source_cleanup.retains_source()
                || report.plan.reasons.contains(&MigrationReason::PreviousSourceDeleted)
            {
                TargetRetentionReason::SourceCleanupPolicy
            } else if request.selection.is_filtered()
                || request.selection.relative_time
                || report.plan.reasons.contains(&MigrationReason::SelectionChanged)
            {
                TargetRetentionReason::FilteredSelection
            } else {
                TargetRetentionReason::BaselineUnavailable
            };
            consumers
                .record_target_retention_range(
                    reason,
                    format!("本轮采用 Preserve，原因 {:?}；未展开的旧目标范围全部保留，不逐项回源查询，也不推断缺失删除或破坏性 rename", report.plan.reasons),
                )
                .await?;
        }
        // 任务边界隔离管线状态机的布局，避免继续嵌套到 CLI／Web 入口。
        match report.plan.mode {
            MigrationMode::Full => {
                self.run_sync(observation_plan, app_config, consumers, database)
                    .boxed_local()
                    .await
            }
            MigrationMode::Incremental => {
                self.run_incremental_sync(
                    observation_plan,
                    app_config,
                    consumers,
                    database.ok_or_else(|| AppError::ConfigError("增量迁移需要已准备的复制目录".into()))?,
                )
                .boxed_local()
                .await
            }
        }
    }

    /// 全量同步 — 通过 transport 连接 sender workers 和 receiver tasks
    async fn run_sync(
        &self, observation_plan: Option<MigrationPlan>, app_config: &AppConfig, consumer_manager: &mut ConsumerManager,
        database: Option<ClickHouseDatabase>,
    ) -> Result<()> {
        let c = &self.config;

        // ── License 校验 ──
        #[cfg(feature = "license")]
        {
            if let Ok(license) = licensing::get_global_license() {
                licensing::verify::quick_verify(license)?;
            }
            verify_storage_time(&c.dest_path).await?;
        }

        info!(
            "Starting sync via orchestrator: job_id({}), src({}), dest({})",
            c.job_id,
            data_mover::redact_storage_url(&c.src_path),
            data_mover::redact_storage_url(&c.dest_path)
        );

        // ── 1. 加载配置 ──
        let is_source_reserved = c.source_cleanup.retains_source();
        let copy_concurrency = c.concurrency.unwrap_or(app_config.sync.concurrency);

        let block_size = match &c.block_size {
            Some(s) => Some(parse_size(s)?),
            None => None,
        };

        // ── 2. 初始化扫描配置 ──
        let mut scan_config = initialize_scan_config(
            &c.job_id,
            &c.src_path,
            0,
            ScanType::Full,
            &c.r#match,
            &c.exclude,
            c.concurrency.unwrap_or(app_config.scan.concurrency),
            false,
            &c.file_list,
            c.packaged,
            c.package_depth,
        )?;

        scan_config.filter_time_anchor = observation_plan.as_ref().map(|plan| plan.filter_time_anchor);
        scan_config.observation_plan = observation_plan.as_ref().map(|plan| plan.observation);
        let target_history = observation_plan.as_ref().map(|plan| !plan.scope.latest_version_only);
        scan_config.metadata_scope = observation_plan.map(|plan| plan.scope).unwrap_or_default();

        // ── 3. 初始化消费者配置 + 数据库 ──
        let consumer_config = Arc::new(initialize_consumer_config(
            &c.job_id,
            &c.job_dir,
            JobType::Copy,
            c.raw_command_line.clone(),
            app_config,
            c.progress_callback_url.clone(),
        )?);

        // 先建立枚举，再启动后台消费者；连接失败时没有待收尾的 worker。
        let walkdir_iter = DirectoryWalker::new(scan_config)
            .with_cancellation(
                consumer_manager
                    .copy_database_execution()
                    .map(|execution| execution.token().clone()),
            )
            .with_file_list_read_policy(NamespaceReadPolicy {
                operation_timeout: Duration::from_secs(app_config.scan.operation_timeout_secs),
                recovery_window: Duration::from_secs(app_config.scan.recovery_window_secs),
                retry_interval: Duration::from_millis(100),
            })
            .walk()
            .await?;

        // ── 4. 广播器 + 消费者 ──
        let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        consumer_manager
            .prepare_copy_consumers(consumer_config.as_ref(), database.clone())
            .await?;
        consumer_manager.begin_lifecycle().await;
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;
        let bytes_tracker = consumer_manager.get_bytes_tracker().await;

        // ── 5. 解析 block_size + QoS ──

        // Windows 管理员权限检查
        #[cfg(windows)]
        {
            if matches!(backend_kind_from_location(&c.src_path), BackendKind::Local)
                && matches!(backend_kind_from_location(&c.dest_path), BackendKind::Local)
            {
                match check_admin_privileges() {
                    Ok(true) => println!("✓ Running with administrator privileges\n"),
                    Ok(false) => {
                        eprintln!("⚠ Warning: Not running with administrator privileges");
                        eprintln!("   It is recommended to rerun this program as an administrator\n");
                    }
                    Err(e) => eprintln!("⚠ Unable to check privilege status: {}", e),
                }
            }
        }

        let qos_manager = create_qos_manager(c.qos.as_ref(), c.peak_qos_rate, c.iops);

        // ── 6. 原子计数器 + 统计 reporter ──
        let active_entry_counter = Arc::new(AtomicUsize::new(0));
        let entry_counter = Arc::new(AtomicUsize::new(0));
        let size_counter = Arc::new(AtomicU64::new(0));
        let active_tokio_task_counter = Arc::new(AtomicUsize::new(0));
        // mount 完成标志：reporter 在 mount 期间仅打 INFO，避免误报 WARN。
        let mount_completed = Arc::new(AtomicBool::new(false));

        let stats_handle = Self::spawn_stats_reporter(
            active_entry_counter.clone(),
            entry_counter.clone(),
            size_counter.clone(),
            active_tokio_task_counter.clone(),
            copy_concurrency,
            mount_completed.clone(),
        );

        // ── 8. 创建 Transport ──
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let sender_transport: Arc<dyn SenderTransport> = Arc::new(sender_transport);
        let receiver_transport = Arc::new(receiver_transport);

        let receiver_config = Arc::new(ReceiverConfig {
            job_id: c.job_id.clone(),
            enable_integrity_check: c.enable_integrity_check,
            enable_acl: c.enable_acl,
            is_source_reserved,
            job_dir: c.job_dir.clone(),
            transfer_mode: c.transfer_mode,
            cancel: consumer_manager
                .copy_database_execution()
                .map(|execution| execution.token().clone()),
        });

        // ── 8.5. 创建 mount 限流信号量 + countdown latch 计数器（Sender + Receiver 共享） ──
        // 容量按 storage 类型选择：仅 NFS 因 portmapper / 特权端口冲突需要限速，
        // CIFS/SMB / S3 / 本地放开到 NON_NFS_MOUNT_CONCURRENCY 让 16 个握手并行完成。
        let mount_semaphore = Arc::new(Semaphore::new(mount_concurrency_for_pair(&c.src_path, &c.dest_path)));
        let alive_senders = Arc::new(AtomicUsize::new(0));
        let alive_receivers = Arc::new(AtomicUsize::new(0));
        let mount_done_senders = Arc::new(AtomicUsize::new(0));
        let mount_done_receivers = Arc::new(AtomicUsize::new(0));

        // ── 9. 启动 N 个 Receiver worker（每个创建自己的 StoragePair，和原始 sync_inner 一致） ──
        let mut receiver_handles = Vec::new();
        for recv_id in 0..copy_concurrency {
            let rt = receiver_transport.clone();
            let src_path = c.src_path.clone();
            let dest_path = c.dest_path.clone();

            let cfg = receiver_config.clone();
            let qos = qos_manager.clone();
            let bt = bytes_tracker.clone();
            let bc = broadcaster.clone();
            let task_counter = active_tokio_task_counter.clone();
            let mount_sem = mount_semaphore.clone();
            let alive_counter = alive_receivers.clone();
            let done_counter = mount_done_receivers.clone();
            let execution = consumer_manager.copy_database_execution();

            let span = info_span!("receiver_worker", recv_id = recv_id);
            let handle = tokio::spawn(
                CopyDatabaseExecution::run_optional_child(execution, async move {
                    // 每个 worker 创建自己的 StoragePair（带限流重试，防止 portmapper 过载）
                    let storage_pair = match create_storage_pair_with_retry(
                        &mount_sem,
                        &src_path,
                        &dest_path,
                        block_size,
                        &format!("Receiver {recv_id}"),
                        cfg.cancel.as_ref(),
                    )
                    .await
                    {
                        Ok(pair) => pair,
                        Err(e) => {
                            error!(
                                "Receiver {}: Failed to create StoragePair after retries: {}",
                                recv_id, e
                            );
                            done_counter.fetch_add(1, Ordering::Release);
                            return Err(e);
                        }
                    };
                    alive_counter.fetch_add(1, Ordering::Relaxed);
                    done_counter.fetch_add(1, Ordering::Release);
                    let src = storage_pair.get_src_storage().clone();
                    let dest = storage_pair.get_dest_storage().clone();

                    // 从 transport 拉消息处理（MPMC，N 个 worker 竞争消费）
                    let mut failure = None;
                    while let Some(msg) = rt.recv().await {
                        match msg {
                            SenderMsg::CopyEntry { entry } => {
                                let _guard = AtomicCounterGuard::increment(&task_counter);
                                match process_entry_on_receiver(&entry, &src, &dest, &cfg, qos.clone(), bt.clone())
                                    .await
                                {
                                    Ok(()) => {
                                        bc.broadcast(StorageEntryMessage::New(entry.into())).await?;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Receiver {}: CopyEntry failed {:?}: {}",
                                            recv_id,
                                            entry.get_relative_path(),
                                            e
                                        );
                                        let context = e.storage_error().map_or_else(
                                            || VersionFailureContext {
                                                phase: "copy_entry".into(),
                                                backend: dest.storage().kind(),
                                                metadata_only: false,
                                            },
                                            |error| {
                                                VersionFailureContext::for_error(
                                                    error,
                                                    src.storage().kind(),
                                                    dest.storage().kind(),
                                                )
                                            },
                                        );
                                        let data_failed = !context.metadata_only;
                                        bc.broadcast(StorageEntryMessage::CopyVersionFailed {
                                            entry: entry.clone().into(),
                                            phase: context.phase,
                                            backend: context.backend,
                                            reason: e.to_string(),
                                            first_in_entry: true,
                                        })
                                        .await?;
                                        failure.get_or_insert(AppError::ReportedCopyFailure {
                                            source: Box::new(e),
                                            data_failed,
                                        });
                                    }
                                }
                            }
                            SenderMsg::TarPacked {
                                tar_entry,
                                manifest_entries,
                            } => {
                                let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                                bc.broadcast(StorageEntryMessage::New(tar_entry.into())).await?;
                                bc.broadcast(StorageEntryMessage::TarManifest {
                                    tar_path,
                                    entries: manifest_entries,
                                })
                                .await?;
                            }
                            _ => {}
                        }
                    }
                    // recv() 返回 None 时自然退出（sender_transport 被 drop 后 channel 关闭）
                    info!("Receiver {}: Completed", recv_id);
                    match failure {
                        Some(error) => Err(error),
                        None => Ok(()),
                    }
                })
                .instrument(span),
            );
            receiver_handles.push(handle);
        }

        // 只有实际 worker 持有接收端；全部挂载失败时生产端能收到关闭错误。
        drop(receiver_transport);

        // ── 10. 启动 N 个 Sender worker（S1） ──
        let sender_config = Arc::new(SenderWorkerConfig {
            target_history,
            enable_integrity_check: c.enable_integrity_check,
            enable_acl: c.enable_acl,
            is_source_reserved,
            large_file_log_threshold: LARGE_FILE_LOG_THRESHOLD,
        });

        let (sender_inputs, source_dispatch) = partition_scan(
            walkdir_iter.clone(),
            copy_concurrency,
            DEFAULT_CHANNEL_CAPACITY / copy_concurrency.max(1),
        );
        let mut sender_handles = Vec::new();

        for (worker_id, wi) in sender_inputs.into_iter().enumerate() {
            let src_path = c.src_path.clone();
            let dest_path = c.dest_path.clone();
            let transport = sender_transport.clone();
            let bc = broadcaster.clone();
            let cfg = sender_config.clone();
            let copy_cfg = receiver_config.clone();
            let qos = qos_manager.clone();
            let bt = bytes_tracker.clone();
            let aec = active_entry_counter.clone();
            let ec = entry_counter.clone();
            let sc = size_counter.clone();
            let mount_sem = mount_semaphore.clone();
            let alive_counter = alive_senders.clone();
            let done_counter = mount_done_senders.clone();
            let execution = consumer_manager.copy_database_execution();

            let span = info_span!("sender_worker", worker_id = worker_id);
            let handle = tokio::spawn(
                CopyDatabaseExecution::run_optional_child(execution, async move {
                    // 每个 Sender worker 创建自己的 StoragePair（带限流重试，防止 portmapper 过载）
                    let storage_pair = match create_storage_pair_with_retry(
                        &mount_sem,
                        &src_path,
                        &dest_path,
                        block_size,
                        &format!("Sender {worker_id}"),
                        copy_cfg.cancel.as_ref(),
                    )
                    .await
                    {
                        Ok(pair) => pair,
                        Err(e) => {
                            error!(
                                "Sender {}: Failed to create StoragePair after retries: {}",
                                worker_id, e
                            );
                            done_counter.fetch_add(1, Ordering::Release);
                            return Err(e);
                        }
                    };
                    alive_counter.fetch_add(1, Ordering::Relaxed);
                    done_counter.fetch_add(1, Ordering::Release);
                    let src = storage_pair.get_src_storage().clone();
                    let dest = storage_pair.get_dest_storage().clone();
                    sender_worker(
                        worker_id, wi, src, dest, transport, &bc, &cfg, qos, bt, &copy_cfg, aec, ec, sc,
                    )
                    .await
                })
                .instrument(span),
            );
            sender_handles.push(handle);
        }

        // ── 10.5. 等待所有 worker 完成 mount 初始化，输出存活汇总 ──
        wait_for_mount_init(
            &[
                (&mount_done_senders, &alive_senders, "Sender"),
                (&mount_done_receivers, &alive_receivers, "Receiver"),
            ],
            copy_concurrency,
        )
        .await;
        // 通知 stats_reporter：mount 期已结束，后续 active_tasks 不足才视为异常。
        mount_completed.store(true, Ordering::Release);

        // ── 11. 等待 Sender workers 完成 ──
        let senders_result = finish_phase(sender_handles).await;
        walkdir_iter.close();
        let source_dispatch_result = finish_phase(vec![source_dispatch]).await;
        walkdir_iter.wait_for_producer().await;

        // ── 12. 关闭管线（和原始 sync_inner 一致） ──
        // Sender workers 全部完成 → 不再发 CopyEntry
        // drop sender_transport → S→R channel 关闭 → 所有 Receiver 的 recv() 返回 None → 退出
        drop(sender_transport);

        // 等所有 Receiver worker 退出（退出时 broadcaster clone 也 drop）
        let receivers_result = finish_phase(receiver_handles).await;

        // ── 13. Cleanup ──
        stats_handle.abort();
        let _ = stats_handle.await;

        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        drop(broadcaster);
        let consumers_result = finish_phase(consumer_handles).await;
        let source_result = walkdir_iter.take_error().map_or(Ok(()), |error| Err(error.into()));
        let result: Result<()> = async {
            // 消费者关闭会使 worker 返回广播错误；优先保留导致关闭的真实原因。
            consumers_result
                .and(senders_result)
                .and(receivers_result)
                .and(source_dispatch_result)
                .and(source_result)?;
            if let Some(database) = &database {
                let destination = Arc::new(
                    create_storage_for_role(
                        &c.dest_path,
                        block_size.map(|size| size as u64),
                        false,
                        StorageRole::Destination,
                    )
                    .await?,
                );
                copy_completion::complete(database.clone(), destination.clone()).await?;
                if !is_source_reserved {
                    // 全量文件已在传输阶段执行所选校验；增量跳过项由增量清理入口补齐数据校验。
                    let source = Arc::new(
                        create_storage_for_role(
                            &c.src_path,
                            block_size.map(|size| size as u64),
                            false,
                            StorageRole::Source,
                        )
                        .await?,
                    );
                    remove_confirmed_sources(
                        database.clone(),
                        source,
                        destination,
                        SourceCleanupCheck::new(
                            Duration::from_secs(app_config.scan.operation_timeout_secs),
                            Duration::from_secs(app_config.scan.recovery_window_secs),
                            consumer_manager
                                .copy_database_execution()
                                .map(|execution| execution.token().clone())
                                .unwrap_or_default(),
                        ),
                    )
                    .await?;
                }
            }
            Ok(())
        }
        .await;
        result?;

        info!("Copy job {} completed successfully via orchestrator", c.job_id);
        Ok(())
    }

    /// 增量同步 — 双阶段流水线：
    /// - Phase A：scan workers → DB 比对 → handler workers 拷贝 → 通过 `broadcaster_a` 喂给 consumers
    /// - Barrier：`drop(broadcaster_a)` + `await phase_a_handles`（base 表冲刷完毕）
    /// - Phase B：`detect_deleted_items` → 执行 dest 物理删除/重命名 → 通过 `broadcaster_b`
    ///   喂给同一组 consumers（`DatabaseConsumer` 自动完成 incremental 表写入 + base 表修正）
    async fn run_incremental_sync(
        &self, observation_plan: Option<MigrationPlan>, app_config: &AppConfig, consumer_manager: &mut ConsumerManager,
        database: ClickHouseDatabase,
    ) -> Result<()> {
        let c = &self.config;

        // ── License 校验 ──
        #[cfg(feature = "license")]
        {
            if let Ok(license) = licensing::get_global_license() {
                licensing::verify::quick_verify(license)?;
            }
            verify_storage_time(&c.dest_path).await?;
        }

        info!(
            "Starting incremental sync via orchestrator: job_id({}), src({}), dest({})",
            c.job_id,
            data_mover::redact_storage_url(&c.src_path),
            data_mover::redact_storage_url(&c.dest_path)
        );

        // ── 1. 加载配置 ──
        let is_source_reserved = c.source_cleanup.retains_source();
        let copy_concurrency = c.concurrency.unwrap_or(app_config.sync.concurrency);

        let block_size = match &c.block_size {
            Some(s) => Some(parse_size(s)?),
            None => None,
        };

        // ── 2. 初始化扫描配置（增量不支持 file_list） ──
        let mut scan_config = initialize_scan_config(
            &c.job_id,
            &c.src_path,
            0,
            ScanType::Incremental,
            &c.r#match,
            &c.exclude,
            c.concurrency.unwrap_or(app_config.scan.concurrency),
            false,
            &c.file_list,
            c.packaged,
            c.package_depth,
        )?;

        scan_config.filter_time_anchor = observation_plan.as_ref().map(|plan| plan.filter_time_anchor);
        scan_config.observation_plan = observation_plan.as_ref().map(|plan| plan.observation);
        scan_config.metadata_scope = observation_plan.map(|plan| plan.scope).unwrap_or_default();

        // ── 3. 初始化消费者配置 + 数据库 ──
        let consumer_config = Arc::new(initialize_consumer_config(
            &c.job_id,
            &c.job_dir,
            JobType::IncrementalCopy,
            c.raw_command_line.clone(),
            app_config,
            c.progress_callback_url.clone(),
        )?);

        database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
        let generation = database.current_copy_generation().await?;
        let preserve_target = generation.plan.target_cleanup == TargetCleanupPolicy::Preserve;
        let target_history = generation.request.binding.target_history;

        let batch_size = consumer_config.db_config.batch_size as usize;

        // 枚举初始化失败时尚未启动 Phase A 的后台任务。
        let walkdir_iter = DirectoryWalker::new(scan_config)
            .with_cancellation(
                consumer_manager
                    .copy_database_execution()
                    .map(|execution| execution.token().clone()),
            )
            .with_file_list_read_policy(NamespaceReadPolicy {
                operation_timeout: Duration::from_secs(app_config.scan.operation_timeout_secs),
                recovery_window: Duration::from_secs(app_config.scan.recovery_window_secs),
                retry_interval: Duration::from_millis(100),
            })
            .walk()
            .await?;

        // ── 4. 消费者管理器 + 共享生命周期（跨 Phase A/B 共用同一份统计累积） ──
        consumer_manager
            .prepare_copy_consumers(consumer_config.as_ref(), Some(database.clone()))
            .await?;
        consumer_manager.begin_lifecycle().await;
        let bytes_tracker = consumer_manager.get_bytes_tracker().await;

        // ── 4.1. Phase A 广播器 + 消费者任务 ──
        let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;

        // ── 5. 检测广播器 + MPMC 通道 ──
        // scan workers → detect_broadcaster → dispatch_task → async_channel(MPMC) → N handler workers → broadcaster
        let mut detect_broadcaster = BroadcastForwarder::<StorageEntryMessage>::new(DEFAULT_CHANNEL_CAPACITY);
        let mut file_op_rx = detect_broadcaster.subscribe();
        let (work_queues, handler_inputs) =
            CopyQueues::new(copy_concurrency, DEFAULT_CHANNEL_CAPACITY / copy_concurrency.max(1));

        // ── 6. 并发度 + block_size ──
        let incremental_scan_concurrency = c.concurrency.unwrap_or(app_config.scan.concurrency);

        info!(
            "Using scan concurrency: {}, copy concurrency: {}, block_size: {:?}",
            incremental_scan_concurrency, copy_concurrency, block_size
        );

        // ── 7. Windows 管理员权限检查 ──
        #[cfg(windows)]
        {
            if matches!(backend_kind_from_location(&c.src_path), BackendKind::Local)
                && matches!(backend_kind_from_location(&c.dest_path), BackendKind::Local)
            {
                match check_admin_privileges() {
                    Ok(true) => println!("✓ Running with administrator privileges\n"),
                    Ok(false) => {
                        eprintln!("⚠ Warning: Not running with administrator privileges");
                        eprintln!("   It is recommended to rerun this program as an administrator\n");
                    }
                    Err(e) => eprintln!("⚠ Unable to check privilege status: {}", e),
                }
            }
        }

        // ── 8. QoS + 版本化对象缓冲 ──
        let qos_manager = create_qos_manager(c.qos.as_ref(), c.peak_qos_rate, c.iops);

        // ── 10. 启动 dispatch task：detect_broadcaster → async_channel（MPMC） ──
        let dispatch_span = info_span!("inc_dispatch");
        let dispatch_handle: JoinHandle<Result<()>> = tokio::spawn(
            async move {
                while let Some(msg) = file_op_rx.recv().await {
                    if work_queues.send(msg).await.is_err() {
                        return Err(AppError::ConsumerError(
                            "Incremental sync handler channel closed".into(),
                        ));
                    }
                }
                info!("Dispatch task completed");
                Ok(())
            }
            .instrument(dispatch_span),
        );

        let handler_handles = incremental_worker::spawn_handlers(
            c,
            handler_inputs,
            broadcaster.clone(),
            CopyOptions {
                transfer_mode: c.transfer_mode,
                qos: qos_manager.clone(),
                bytes_counter: bytes_tracker.clone(),
                enable_integrity_check: c.enable_integrity_check,
                cancel: consumer_manager
                    .copy_database_execution()
                    .map(|execution| execution.token().clone()),
            },
            block_size,
            HistoryCopyPolicy {
                active_entry_limit: incremental_scan_concurrency,
                target_history: Some(target_history),
                completion: HistoryCompletion::Incremental,
                recovering: database.copy_generation_recovering().await,
            },
        )
        .await;

        // ── 13. 启动 N 个 scan worker（walkdir → batch → DB compare → detect_broadcaster） ──
        let database_clone = database.clone();
        let mut scan_worker_handles = Vec::new();

        let (scan_inputs, scan_dispatch) = partition_scan(
            walkdir_iter.clone(),
            incremental_scan_concurrency,
            DEFAULT_CHANNEL_CAPACITY / incremental_scan_concurrency.max(1),
        );
        for (worker_id, worker_walkdir_iter) in scan_inputs.into_iter().enumerate() {
            let database_clone = database_clone.clone();
            let detect_broadcaster = detect_broadcaster.clone();
            let broadcaster = broadcaster.clone();
            let execution = consumer_manager.copy_database_execution();

            let span = info_span!("scan_worker", worker_id = worker_id);
            let handle = tokio::spawn(
                CopyDatabaseExecution::run_optional_child(execution, async move {
                    info!("Scan worker {}: Started processing", worker_id);

                    let mut current_batch = Vec::with_capacity(batch_size);

                    while let Some(msg) = worker_walkdir_iter.next().await {
                        match msg {
                            StorageEntryMessage::Scanned(ref entry) => {
                                trace!(
                                    "Scan worker {}: Received StorageEntry from walkdir: {:?}",
                                    worker_id, entry
                                );

                                broadcaster
                                    .broadcast(StorageEntryMessage::Scanned(entry.clone()))
                                    .await?;

                                current_batch.push(entry.clone());

                                if current_batch.len() >= batch_size {
                                    let batch = std::mem::take(&mut current_batch);
                                    let db = database_clone.clone();

                                    batch_processing_to_generate_message(
                                        &db,
                                        &batch,
                                        detect_broadcaster.clone(),
                                        false,
                                        false,
                                    )
                                    .await?;
                                }
                            }
                            StorageEntryMessage::Diagnostic { .. } => {
                                broadcaster.broadcast(msg.clone()).await?;
                            }
                            StorageEntryMessage::Error {
                                ref path, ref reason, ..
                            } => {
                                error!(
                                    "Scan worker {}: Walkdir error for {}: {}",
                                    worker_id,
                                    path.display(),
                                    reason
                                );
                                broadcaster.broadcast(msg.clone()).await?;
                            }
                            _ => {}
                        }
                    }

                    // 处理最后一批数据
                    if !current_batch.is_empty() {
                        let db = database_clone.clone();
                        let batch = current_batch;

                        batch_processing_to_generate_message(&db, &batch, detect_broadcaster, true, false).await?;
                    }

                    info!("Scan worker {}: Completed processing", worker_id);
                    Ok(())
                })
                .instrument(span),
            );

            scan_worker_handles.push(handle);
        }

        // ── 14. 等待 scan workers 完成 ──
        let scan_result = finish_phase(scan_worker_handles).await;
        walkdir_iter.close();
        let scan_dispatch_result = finish_phase(vec![scan_dispatch]).await;
        walkdir_iter.wait_for_producer().await;

        // ── 15. 关闭 detect_broadcaster → dispatch task 的 file_op_rx 耗尽 ──
        // scan workers 已全部退出，其 detect_broadcaster clone 均已 drop；
        // 主 detect_broadcaster clone drop 触发 dispatch task EOF。
        drop(detect_broadcaster);

        // ── 16. 等待 dispatch 与 handler 全部结束，再检查阶段结果 ──
        let dispatch_result = match dispatch_handle.await {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        let handler_result = finish_phase(handler_handles).await;
        info!("All handler workers completed");

        // ── 18. 关闭 Phase A 广播器 → 等待 Phase A 消费者完成（同步屏障） ──
        // handler workers 已全部退出（其 bc clone 均已 drop）；
        // 主 broadcaster clone drop → 消费者通道 EOF → DatabaseConsumer 刷入所有
        // Phase A 的 New/Changed 条目 → base 表完整，detect_deleted_items 才有完整依据。
        drop(broadcaster);
        let durable_result = finish_phase(consumer_handles).await;
        let enumeration_result = walkdir_iter.take_error().map_or(Ok(()), |error| Err(error.into()));
        // 未完整比对或未持久化的条目不能作为删除依据。
        // 元数据读取诊断已随观察交付；优先返回 worker 已计数的同项失败，避免再记任务错误。
        let (enumeration_result, handler_result) =
            if matches!(
                &enumeration_result,
                Err(AppError::StorageError(StorageError::TraversalObservation(_)))
            ) && handler_result.as_ref().is_err_and(AppError::is_reported_copy_failure)
            {
                (Ok(()), handler_result)
            } else {
                (enumeration_result, handler_result)
            };
        let phase_a = durable_result
            .and(enumeration_result)
            .and(scan_result)
            .and(handler_result)
            .and(dispatch_result)
            .and(scan_dispatch_result);
        if let Err(error) = phase_a {
            if let Some(ref qos_mgr) = qos_manager {
                qos_mgr.shutdown();
            }
            return Err(error);
        }
        info!("Phase A complete; base table ready for delete/rename detection");

        // ── 19. Phase B：在 Phase A 屏障之后，起一轮新的 broadcaster + 同一组 consumer 任务 ──
        // `apply_deletions_and_renames` 只负责「检测 + 执行物理操作 + 广播结果消息」，
        // DatabaseConsumer 走 Deleted/Renamed 分支自动完成 base/incremental 表的批量写入。
        let mut broadcaster_b = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        let phase_b_handles = consumer_manager.start_consumers(&mut broadcaster_b).await?;

        // dest storage 提升到 async 块外，供后续 update_directory_metadata 复用，避免二次挂载
        let phase_b_dest_storage: Result<Arc<StorageContext>> = async {
            let dest_storage_phase_b = Arc::new(
                create_storage_for_role(
                    &c.dest_path,
                    block_size.map(|s| s as u64),
                    false,
                    StorageRole::Destination,
                )
                .await?,
            );
            if !preserve_target {
                let src_storage_phase_b = Arc::new(
                    create_storage_for_role(&c.src_path, block_size.map(|s| s as u64), false, StorageRole::Source)
                        .await?,
                );

                Self::apply_deletions_and_renames(
                    &database,
                    src_storage_phase_b.clone(),
                    dest_storage_phase_b.clone(),
                    is_source_reserved,
                    &broadcaster_b,
                    c.enable_integrity_check,
                    qos_manager.clone(),
                    bytes_tracker.clone(),
                    &c.job_id,
                    &c.job_dir,
                    c.transfer_mode,
                    SourceAbsenceCheck::new(
                        Duration::from_secs(app_config.scan.operation_timeout_secs),
                        Duration::from_secs(app_config.scan.recovery_window_secs),
                    ),
                )
                .await?;
            }

            Ok(dest_storage_phase_b)
        }
        .await;

        // ── 21. 关闭 Phase B 广播器 → 等待 Phase B 消费者完成（incremental 表冲刷完毕） ──
        drop(broadcaster_b);
        let phase_b_consumers = finish_phase(phase_b_handles).await;

        // QoS 在两阶段都走完之后才关闭，保留 Phase B 物理操作期间的限速效果
        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        let result: Result<()> = async {
            let destination = phase_b_dest_storage?;
            phase_b_consumers?;
            copy_completion::complete(database.clone(), destination.clone()).await?;
            if !is_source_reserved {
                let source = Arc::new(
                    create_storage_for_role(
                        &c.src_path,
                        block_size.map(|size| size as u64),
                        false,
                        StorageRole::Source,
                    )
                    .await?,
                );
                remove_confirmed_sources(
                    database.clone(),
                    source,
                    destination,
                    SourceCleanupCheck::new(
                        Duration::from_secs(app_config.scan.operation_timeout_secs),
                        Duration::from_secs(app_config.scan.recovery_window_secs),
                        consumer_manager
                            .copy_database_execution()
                            .map(|execution| execution.token().clone())
                            .unwrap_or_default(),
                    )
                    .with_content_verification(c.enable_integrity_check),
                )
                .await?;
            }
            Ok(())
        }
        .await;
        result?;

        info!(
            "Incremental sync job {} completed successfully via orchestrator",
            c.job_id
        );
        Ok(())
    }

    /// 双进程全量同步 — Sender 侧：`directory_pages` 分页 → QUIC → Receiver 比较 + 写入
    async fn run_sync_remote(
        &self, remote_addr: &str, tls_cert_bytes: Option<&[u8]>, auth_token: Option<&str>,
    ) -> Result<()> {
        crate::remote_sync::run(&self.config, remote_addr, tls_cert_bytes, auth_token).await
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
