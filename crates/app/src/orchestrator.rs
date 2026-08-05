//! Sync 编排器
//!
//! SyncOrchestrator 封装 sync/incremental_sync 的核心逻辑，
//! 通过 Transport 抽象层连接 Sender 和 Receiver，支持单进程和双进程两种模式。

// 标准库
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

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
use dashmap::DashMap;
use data_mover::error::StorageError;
use data_mover::qos::QosManager;
#[cfg(windows)]
use data_mover::storage_enum::{StorageType, detect_storage_type};
use data_mover::{ChangeKind, EntryEnum, ErrorEvent, StorageEntryMessage, StorageEnum, create_storage};
use db::factory::DatabaseFactory;
use db::traits::Database;
use db::{self, DeletionStatus, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::in_process::create_in_process_pair;
use transport::message::SenderMsg;
use transport::traits::{ReceiverTransport, SenderTransport};
use utils::app_config::AppConfig;

// 内部模块
use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, SyncJobConfig, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::consumer::stats::DirectoryMetadataProgressBar;
use crate::error::{AppError, Result};
#[cfg(feature = "license")]
use crate::integrity_check::verify_storage_time;
use crate::receiver::{ReceiverConfig, process_entry_on_receiver};
use crate::scan::{ScanType, batch_processing_to_generate_message, determine_scan_type};
use crate::sender::{SenderWorkerConfig, sender_worker};
#[cfg(windows)]
use crate::sync::check_admin_privileges;
use crate::sync::{
    ResumeOpts, StoragePair, parse_size, process_entry, process_metadata_only_entry, process_rename_entry,
    process_versioned_entry,
};
use crate::{dir_walker, tar_pack};

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
) -> Result<StoragePair> {
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
}

impl SyncOrchestrator {
    /// 创建单进程模式的编排器
    pub fn new_local(config: SyncJobConfig) -> Self {
        Self {
            config,
            mode: SyncMode::Local,
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
        }
    }

    /// 执行 sync 管线
    ///
    /// `ScanType` 自动判定：查数据库 base 表是否存在，失败 fallback 到文件系统。
    pub async fn run(&self) -> Result<()> {
        // 自动判定 ScanType
        let c = &self.config;
        let app_config = AppConfig::fetch()?;
        let db_config = db::config::DatabaseConfig::from_app_config(&app_config.database);
        // database.enabled=true 时必须成功构造；false 时忽略错误（未启用），走文件系统兜底
        let db_instance = if app_config.database.enabled {
            Some(
                DatabaseFactory::new_database(&db_config, &c.job_id)
                    .await
                    .map_err(AppError::DatabaseError)?,
            )
        } else {
            None
        };
        let scan_type = determine_scan_type(db_instance.as_deref(), &c.job_id, c.job_dir_pre_existing).await;

        info!("SyncOrchestrator: determined scan_type={}", scan_type);

        match &self.mode {
            SyncMode::Local => match scan_type {
                ScanType::Full => self.run_sync().await,
                ScanType::Incremental => self.run_incremental_sync().await,
            },
            // 双进程模式本就是 compare-based（DestIndex 实时比对目标端），Full/Incremental
            // 在 wire protocol 和执行路径上是同一套代码，ScanType 对 Remote 是 no-op
            // （issue #23：不存在需要新写的"远端增量模式"）。
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

    /// 全量同步 — 通过 transport 连接 sender workers 和 receiver tasks
    async fn run_sync(&self) -> Result<()> {
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
            c.job_id, c.src_path, c.dest_path
        );

        // ── 1. 加载配置 ──
        let app_config = AppConfig::fetch()?;
        let is_source_reserved = app_config.sync.is_source_reserved;
        let copy_concurrency = app_config.sync.concurrency;

        // ── 2. 初始化扫描配置 ──
        let scan_config = initialize_scan_config(
            &c.job_id,
            &c.src_path,
            0,
            ScanType::Full,
            &c.r#match,
            &c.exclude,
            app_config.scan.concurrency,
            app_config.scan.include_tags,
            &c.file_list,
            c.packaged,
            c.package_depth,
        )?;

        // ── 3. 初始化消费者配置 + 数据库 ──
        let consumer_config = Arc::new(initialize_consumer_config(
            &c.job_id,
            &c.job_dir,
            JobType::Copy,
            c.raw_command_line.clone(),
            &app_config,
            c.progress_callback_url.clone(),
        )?);

        let database = DatabaseFactory::new_database(&consumer_config.db_config, &c.job_id)
            .await
            .map_err(AppError::DatabaseError)?;

        // ── 4. 广播器 + 消费者 ──
        let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
        consumer_manager.begin_lifecycle().await;
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;
        let bytes_tracker = consumer_manager.get_bytes_tracker().await;

        // ── 5. 解析 block_size + QoS ──
        let block_size = match &c.block_size {
            Some(s) => Some(parse_size(s)?),
            None => None,
        };

        // Windows 管理员权限检查
        #[cfg(windows)]
        {
            if matches!(detect_storage_type(&c.src_path), StorageType::Local)
                && matches!(detect_storage_type(&c.dest_path), StorageType::Local)
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

        // ── 7. 启动 walkdir ──
        let walkdir_iter = dir_walker::walkdir(scan_config)
            .await
            .map_err(|e| AppError::ScanError(format!("Start directory walker failed: {e}")))?;

        // ── 8. 创建 Transport ──
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let sender_transport: Arc<dyn SenderTransport> = Arc::new(sender_transport);
        let receiver_transport = Arc::new(receiver_transport);

        let receiver_config = Arc::new(ReceiverConfig {
            enable_integrity_check: c.enable_integrity_check,
            enable_acl: c.enable_acl,
            is_source_reserved,
            job_dir: c.job_dir.clone(),
            no_resume: c.no_resume,
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

            let span = info_span!("receiver_worker", recv_id = recv_id);
            let handle = tokio::spawn(
                async move {
                    // 每个 worker 创建自己的 StoragePair（带限流重试，防止 portmapper 过载）
                    let storage_pair = match create_storage_pair_with_retry(
                        &mount_sem,
                        &src_path,
                        &dest_path,
                        block_size,
                        &format!("Receiver {recv_id}"),
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
                            return;
                        }
                    };
                    alive_counter.fetch_add(1, Ordering::Relaxed);
                    done_counter.fetch_add(1, Ordering::Release);
                    let src = storage_pair.get_src_storage().clone();
                    let dest = storage_pair.get_dest_storage().clone();

                    // 从 transport 拉消息处理（MPMC，N 个 worker 竞争消费）
                    while let Some(msg) = rt.recv().await {
                        match msg {
                            SenderMsg::CopyEntry { entry } => {
                                let _guard = AtomicCounterGuard::increment(&task_counter);
                                match process_entry_on_receiver(&entry, &src, &dest, &cfg, qos.clone(), bt.clone())
                                    .await
                                {
                                    Ok(()) => {
                                        bc.broadcast(StorageEntryMessage::New(entry)).await;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Receiver {}: CopyEntry failed {:?}: {}",
                                            recv_id,
                                            entry.get_relative_path(),
                                            e
                                        );
                                        bc.broadcast(StorageEntryMessage::Error {
                                            event: ErrorEvent::Copy,
                                            path: entry.get_relative_path().to_path_buf(),
                                            entry: Some(entry.clone()),
                                            reason: format!("{e}"),
                                        })
                                        .await;
                                    }
                                }
                            }
                            SenderMsg::TarPacked {
                                tar_entry,
                                manifest_entries,
                            } => {
                                let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                                bc.broadcast(StorageEntryMessage::New(tar_entry)).await;
                                bc.broadcast(StorageEntryMessage::TarManifest {
                                    tar_path,
                                    entries: manifest_entries,
                                })
                                .await;
                            }
                            _ => {}
                        }
                    }
                    // recv() 返回 None 时自然退出（sender_transport 被 drop 后 channel 关闭）
                    info!("Receiver {}: Completed", recv_id);
                }
                .instrument(span),
            );
            receiver_handles.push(handle);
        }

        // ── 10. 启动 N 个 Sender worker（S1） ──
        let sender_config = Arc::new(SenderWorkerConfig {
            enable_integrity_check: c.enable_integrity_check,
            enable_acl: c.enable_acl,
            is_source_reserved,
            large_file_log_threshold: LARGE_FILE_LOG_THRESHOLD,
        });

        let object_buffers: Arc<DashMap<String, Vec<Arc<EntryEnum>>>> = Arc::new(DashMap::new());
        let mut sender_handles = Vec::new();

        for worker_id in 0..copy_concurrency {
            let wi = walkdir_iter.clone();
            let src_path = c.src_path.clone();
            let dest_path = c.dest_path.clone();
            let transport = sender_transport.clone();
            let bc = broadcaster.clone();
            let cfg = sender_config.clone();
            let qos = qos_manager.clone();
            let bt = bytes_tracker.clone();
            let ob = object_buffers.clone();
            let aec = active_entry_counter.clone();
            let ec = entry_counter.clone();
            let sc = size_counter.clone();
            let mount_sem = mount_semaphore.clone();
            let alive_counter = alive_senders.clone();
            let done_counter = mount_done_senders.clone();

            let span = info_span!("sender_worker", worker_id = worker_id);
            let handle = tokio::spawn(
                async move {
                    // 每个 Sender worker 创建自己的 StoragePair（带限流重试，防止 portmapper 过载）
                    let storage_pair = match create_storage_pair_with_retry(
                        &mount_sem,
                        &src_path,
                        &dest_path,
                        block_size,
                        &format!("Sender {worker_id}"),
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
                            return;
                        }
                    };
                    alive_counter.fetch_add(1, Ordering::Relaxed);
                    done_counter.fetch_add(1, Ordering::Release);
                    let src = storage_pair.get_src_storage().clone();
                    let dest = storage_pair.get_dest_storage().clone();
                    sender_worker(worker_id, wi, src, dest, transport, &bc, &cfg, qos, bt, ob, aec, ec, sc).await;
                }
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
        for handle in sender_handles {
            if let Err(e) = handle.await {
                error!("Sender worker task failed: {:?}", e);
            }
        }

        // ── 12. 关闭管线（和原始 sync_inner 一致） ──
        // Sender workers 全部完成 → 不再发 CopyEntry
        // drop sender_transport → S→R channel 关闭 → 所有 Receiver 的 recv() 返回 None → 退出
        drop(sender_transport);

        // 等所有 Receiver worker 退出（退出时 broadcaster clone 也 drop）
        for handle in receiver_handles {
            if let Err(e) = handle.await {
                error!("Receiver worker panicked: {:?}", e);
            }
        }

        // ── 13. Cleanup ──
        stats_handle.abort();

        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        drop(broadcaster);
        Self::await_consumers(consumer_handles).await;
        consumer_manager.end_lifecycle().await;

        // ── 14. 更新目录元数据（非 S3 目标端） ──
        let dest_storage_for_metadata =
            Arc::new(create_storage(&c.dest_path, block_size.map(|s| s as u64), false).await?);
        if !matches!(dest_storage_for_metadata.as_ref(), StorageEnum::S3(_)) {
            Self::update_directory_metadata(database, &dest_storage_for_metadata).await;
        }

        info!("Copy job {} completed successfully via orchestrator", c.job_id);
        Ok(())
    }

    /// 增量同步 — 双阶段流水线：
    /// - Phase A：scan workers → DB 比对 → handler workers 拷贝 → 通过 `broadcaster_a` 喂给 consumers
    /// - Barrier：`drop(broadcaster_a)` + `await phase_a_handles`（base 表冲刷完毕）
    /// - Phase B：`detect_deleted_items` → 执行 dest 物理删除/重命名 → 通过 `broadcaster_b`
    ///   喂给同一组 consumers（`DatabaseConsumer` 自动完成 incremental 表写入 + base 表修正）
    async fn run_incremental_sync(&self) -> Result<()> {
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
            c.job_id, c.src_path, c.dest_path
        );

        // ── 1. 加载配置 ──
        let app_config = AppConfig::fetch()?;
        let is_source_reserved = app_config.sync.is_source_reserved;
        let copy_concurrency = app_config.sync.concurrency;

        // ── 2. 初始化扫描配置（增量不支持 file_list） ──
        let scan_config = initialize_scan_config(
            &c.job_id,
            &c.src_path,
            0,
            ScanType::Incremental,
            &c.r#match,
            &c.exclude,
            app_config.scan.concurrency,
            app_config.scan.include_tags,
            &None,
            c.packaged,
            c.package_depth,
        )?;

        // ── 3. 初始化消费者配置 + 数据库 ──
        let consumer_config = Arc::new(initialize_consumer_config(
            &c.job_id,
            &c.job_dir,
            JobType::IncrementalCopy,
            c.raw_command_line.clone(),
            &app_config,
            c.progress_callback_url.clone(),
        )?);

        let database = DatabaseFactory::new_database(&consumer_config.db_config, &c.job_id)
            .await
            .map_err(AppError::DatabaseError)?;

        if let Err(e) = database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await {
            return Err(AppError::DatabaseError(e));
        }

        let batch_size = consumer_config.db_config.batch_size as usize;

        // ── 4. 消费者管理器 + 共享生命周期（跨 Phase A/B 共用同一份统计累积） ──
        let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
        consumer_manager.begin_lifecycle().await;
        let bytes_tracker = consumer_manager.get_bytes_tracker().await;

        // ── 4.1. Phase A 广播器 + 消费者任务 ──
        let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;

        // ── 5. 检测广播器 + MPMC 通道 ──
        // scan workers → detect_broadcaster → dispatch_task → async_channel(MPMC) → N handler workers → broadcaster
        let mut detect_broadcaster = BroadcastForwarder::<StorageEntryMessage>::new(DEFAULT_CHANNEL_CAPACITY);
        let mut file_op_rx = detect_broadcaster.subscribe();
        let (work_tx, work_rx) = async_channel::bounded::<StorageEntryMessage>(DEFAULT_CHANNEL_CAPACITY);

        // ── 6. 并发度 + block_size ──
        let incremental_scan_concurrency = app_config.scan.concurrency;

        let block_size = match &c.block_size {
            Some(s) => Some(parse_size(s)?),
            None => None,
        };

        info!(
            "Using scan concurrency: {}, copy concurrency: {}, block_size: {:?}",
            incremental_scan_concurrency, copy_concurrency, block_size
        );

        // ── 7. Windows 管理员权限检查 ──
        #[cfg(windows)]
        {
            if matches!(detect_storage_type(&c.src_path), StorageType::Local)
                && matches!(detect_storage_type(&c.dest_path), StorageType::Local)
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
        let object_buffers: Arc<DashMap<String, Vec<Arc<EntryEnum>>>> = Arc::new(DashMap::new());

        // ── 9. 切换数据库扫描状态 ──
        let db_clone = database.clone();
        if let Err(e) = db_clone.switch_scan_state().await {
            error!("Failed to switch scan state: {}", e);
            return Err(AppError::DatabaseError(e));
        }

        // ── 10. 启动 dispatch task：detect_broadcaster → async_channel（MPMC） ──
        let dispatch_span = info_span!("inc_dispatch");
        let dispatch_handle: JoinHandle<()> = tokio::spawn(
            async move {
                while let Some(msg) = file_op_rx.recv().await {
                    if work_tx.send(msg).await.is_err() {
                        break; // 所有 handler worker 已退出
                    }
                }
                info!("Dispatch task completed");
            }
            .instrument(dispatch_span),
        );

        // ── 10.5. 创建 mount 限流信号量 + 完成计数器 ──
        // 容量按 storage 类型选择（详见 sync 路径同名注释）。
        let mount_semaphore = Arc::new(Semaphore::new(mount_concurrency_for_pair(&c.src_path, &c.dest_path)));
        let alive_handlers = Arc::new(AtomicUsize::new(0));
        let mount_done_handlers = Arc::new(AtomicUsize::new(0));

        // ── 11. 启动 N 个 handler worker（MPMC 竞争消费，每个 worker 独立 StoragePair） ──
        let mut handler_handles = Vec::new();
        let src_path_str = c.src_path.clone();
        let dest_path_str = c.dest_path.clone();

        for handler_id in 0..copy_concurrency {
            let rx = work_rx.clone();
            let src_path_c = src_path_str.clone();
            let dest_path_c = dest_path_str.clone();
            let bc = broadcaster.clone();
            let ob = object_buffers.clone();
            let qos = qos_manager.clone();
            let bt = bytes_tracker.clone();
            let enable_integrity_check = c.enable_integrity_check;
            let enable_acl = c.enable_acl;
            let resume_opts = ResumeOpts {
                job_dir: c.job_dir.clone(),
                no_resume: c.no_resume,
            };
            let mount_sem = mount_semaphore.clone();
            let alive_counter = alive_handlers.clone();
            let done_counter = mount_done_handlers.clone();

            let span = info_span!("inc_handler_worker", handler_id);
            let handle = tokio::spawn(
                async move {
                    // 每个 worker 创建独立 StoragePair（带限流重试，防止 portmapper 过载）
                    let storage_pair = match create_storage_pair_with_retry(
                        &mount_sem,
                        &src_path_c,
                        &dest_path_c,
                        block_size,
                        &format!("Handler {handler_id}"),
                    )
                    .await
                    {
                        Ok(pair) => pair,
                        Err(e) => {
                            error!(
                                "Handler worker {}: Failed to create StoragePair after retries: {}",
                                handler_id, e
                            );
                            done_counter.fetch_add(1, Ordering::Release);
                            return;
                        }
                    };
                    alive_counter.fetch_add(1, Ordering::Relaxed);
                    done_counter.fetch_add(1, Ordering::Release);
                    let src = storage_pair.get_src_storage().clone();
                    let dest = storage_pair.get_dest_storage().clone();

                    while let Ok(message) = rx.recv().await {
                        match message {
                            StorageEntryMessage::New(ref entry) => {
                                if entry.get_version_id().is_some() {
                                    process_versioned_entry(
                                        entry,
                                        src.clone(),
                                        dest.clone(),
                                        bc.clone(),
                                        qos.clone(),
                                        ob.clone(),
                                        enable_integrity_check,
                                        enable_acl,
                                        is_source_reserved,
                                        bt.clone(),
                                    )
                                    .await;
                                } else {
                                    match process_entry(
                                        entry,
                                        src.clone(),
                                        dest.clone(),
                                        enable_integrity_check,
                                        enable_acl,
                                        is_source_reserved,
                                        qos.clone(),
                                        bt.clone(),
                                        &bc,
                                        resume_opts.clone(),
                                    )
                                    .await
                                    {
                                        Ok(()) => bc.broadcast(message.clone()).await,
                                        Err(e) => {
                                            error!(
                                                "Handler {}: Failed to process new entry {:?}: {}",
                                                handler_id,
                                                entry.get_relative_path(),
                                                e
                                            );
                                            bc.broadcast(StorageEntryMessage::Error {
                                                event: ErrorEvent::Copy,
                                                path: entry.get_relative_path().to_path_buf(),
                                                entry: Some(entry.clone()),
                                                reason: format!("{e}"),
                                            })
                                            .await;
                                        }
                                    }
                                }
                            }
                            StorageEntryMessage::Changed { ref entry, kind } => {
                                // MetadataOnly：chmod/chown 变更，跳过 copy_file 只同步属性
                                let result = if kind == data_mover::ChangeKind::MetadataOnly {
                                    process_metadata_only_entry(
                                        entry,
                                        src.clone(),
                                        dest.clone(),
                                        enable_acl,
                                        is_source_reserved,
                                        &bc,
                                    )
                                    .await
                                } else {
                                    process_entry(
                                        entry,
                                        src.clone(),
                                        dest.clone(),
                                        enable_integrity_check,
                                        enable_acl,
                                        is_source_reserved,
                                        qos.clone(),
                                        bt.clone(),
                                        &bc,
                                        resume_opts.clone(),
                                    )
                                    .await
                                };
                                match result {
                                    Ok(()) => bc.broadcast(message.clone()).await,
                                    Err(e) => {
                                        error!(
                                            "Handler {}: Failed to process changed entry {:?} (kind={}): {}",
                                            handler_id,
                                            entry.get_relative_path(),
                                            kind,
                                            e
                                        );
                                        bc.broadcast(StorageEntryMessage::Error {
                                            event: ErrorEvent::Copy,
                                            path: entry.get_relative_path().to_path_buf(),
                                            entry: Some(entry.clone()),
                                            reason: format!("{e}"),
                                        })
                                        .await;
                                    }
                                }
                            }
                            StorageEntryMessage::Deleted(ref entry) => {
                                // 删除操作幂等化：NotFound 视为成功
                                let result = if entry.get_is_dir() {
                                    trace!("Deleting directory: {:?}", entry.get_relative_path());
                                    dest.delete_dir_all(entry).await
                                } else {
                                    trace!("Deleting file: {:?}", entry.get_relative_path());
                                    dest.delete_file(entry).await
                                };
                                match result {
                                    Ok(()) => bc.broadcast(message.clone()).await,
                                    Err(StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_)) => {
                                        trace!("Already deleted, skipping: {:?}", entry.get_relative_path());
                                        bc.broadcast(message.clone()).await;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Handler {}: Failed to delete {:?}: {}",
                                            handler_id,
                                            entry.get_relative_path(),
                                            e
                                        );
                                        bc.broadcast(StorageEntryMessage::Error {
                                            event: ErrorEvent::Delete,
                                            path: entry.get_relative_path().to_path_buf(),
                                            entry: Some(entry.clone()),
                                            reason: format!("{e}"),
                                        })
                                        .await;
                                    }
                                }
                            }
                            StorageEntryMessage::Renamed((ref from_entry, ref to_entry)) => {
                                if from_entry.get_name() == to_entry.get_name() {
                                    // 同名 rename（父目录移动）：直接广播
                                    bc.broadcast(message.clone()).await;
                                } else {
                                    match process_rename_entry(
                                        from_entry.clone(),
                                        to_entry.clone(),
                                        src.clone(),
                                        dest.clone(),
                                        is_source_reserved,
                                    )
                                    .await
                                    {
                                        Ok(()) => bc.broadcast(message.clone()).await,
                                        Err(e) => {
                                            error!(
                                                "Handler {}: Failed to rename {:?} to {:?}: {}",
                                                handler_id,
                                                from_entry.get_relative_path(),
                                                to_entry.get_relative_path(),
                                                e
                                            );
                                            bc.broadcast(StorageEntryMessage::Error {
                                                event: ErrorEvent::Rename,
                                                path: to_entry.get_relative_path().to_path_buf(),
                                                entry: Some(to_entry.clone()),
                                                reason: format!("{e}"),
                                            })
                                            .await;
                                        }
                                    }
                                }
                            }
                            StorageEntryMessage::Packaged(ref entry) => {
                                info!("Packing directory {:?}", entry.get_relative_path());

                                match tar_pack::pack_directory(
                                    &src,
                                    &dest,
                                    entry,
                                    is_source_reserved,
                                    qos.clone(),
                                    bt.clone(),
                                )
                                .await
                                {
                                    Ok((tar_entry, manifest)) => {
                                        let tar_path = tar_entry.get_relative_path().to_string_lossy().to_string();
                                        bc.broadcast(StorageEntryMessage::New(Arc::new(tar_entry))).await;
                                        bc.broadcast(StorageEntryMessage::TarManifest {
                                            tar_path,
                                            entries: manifest,
                                        })
                                        .await;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Handler {}: Pack failed for {:?}: {}",
                                            handler_id,
                                            entry.get_relative_path(),
                                            e
                                        );
                                        bc.broadcast(StorageEntryMessage::Error {
                                            event: ErrorEvent::Pack,
                                            path: entry.get_relative_path().to_path_buf(),
                                            entry: Some(entry.clone()),
                                            reason: format!("{e}"),
                                        })
                                        .await;
                                    }
                                }
                            }
                            _ => {
                                // Scanned、Copyed、IntegrityChecked 不会经由 detect_broadcaster 到达此处
                            }
                        }
                    }

                    info!("Handler worker {} completed", handler_id);
                }
                .instrument(span),
            );
            handler_handles.push(handle);
        }
        // drop work_rx 本体（只有 clone 在 worker 手里）
        drop(work_rx);

        // ── 11.5. 等待所有 handler worker 完成 mount 初始化，输出存活汇总 ──
        wait_for_mount_init(&[(&mount_done_handlers, &alive_handlers, "Handler")], copy_concurrency).await;

        // ── 12. 启动 walkdir ──
        let walkdir_iter = dir_walker::walkdir(scan_config)
            .await
            .map_err(|e| AppError::ScanError(format!("Start directory walker failed: {e}")))?;

        // ── 13. 启动 N 个 scan worker（walkdir → batch → DB compare → detect_broadcaster） ──
        let database_clone = database.clone();
        let mut scan_worker_handles = Vec::new();

        for worker_id in 0..incremental_scan_concurrency {
            let worker_walkdir_iter = walkdir_iter.clone();
            let database_clone = database_clone.clone();
            let detect_broadcaster = detect_broadcaster.clone();
            let broadcaster = broadcaster.clone();

            let span = info_span!("scan_worker", worker_id = worker_id);
            let handle = tokio::spawn(
                async move {
                    info!("Scan worker {}: Started processing", worker_id);

                    let mut current_batch = Vec::with_capacity(batch_size);

                    while let Some(msg) = worker_walkdir_iter.next().await {
                        match msg {
                            StorageEntryMessage::Scanned(ref entry) => {
                                trace!(
                                    "Scan worker {}: Received StorageEntry from walkdir: {:?}",
                                    worker_id, entry
                                );

                                broadcaster.broadcast(StorageEntryMessage::Scanned(entry.clone())).await;

                                current_batch.push(entry.clone());

                                if current_batch.len() >= batch_size {
                                    let batch = std::mem::take(&mut current_batch);
                                    let db = database_clone.clone();

                                    if let Err(e) = batch_processing_to_generate_message(
                                        &db,
                                        &batch,
                                        detect_broadcaster.clone(),
                                        false,
                                        false,
                                    )
                                    .await
                                    {
                                        error!("Failed to process batch: {}", e);
                                    }
                                }
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
                                broadcaster.broadcast(msg.clone()).await;
                            }
                            _ => {}
                        }
                    }

                    // 处理最后一批数据
                    if !current_batch.is_empty() {
                        let db = database_clone.clone();
                        let batch = current_batch;

                        if let Err(e) =
                            batch_processing_to_generate_message(&db, &batch, detect_broadcaster, true, false).await
                        {
                            error!("Scan worker {}: Failed to process final batch: {}", worker_id, e);
                        }
                    }

                    info!("Scan worker {}: Completed processing", worker_id);
                }
                .instrument(span),
            );

            scan_worker_handles.push(handle);
        }

        // ── 14. 等待 scan workers 完成 ──
        for handle in scan_worker_handles {
            if let Err(e) = handle.await {
                error!("Scan worker failed: {:?}", e);
            }
        }

        // ── 15. 关闭 detect_broadcaster → dispatch task 的 file_op_rx 耗尽 ──
        // scan workers 已全部退出，其 detect_broadcaster clone 均已 drop；
        // 主 detect_broadcaster clone drop 触发 dispatch task EOF。
        drop(detect_broadcaster);

        // ── 16. 等待 dispatch task 退出 ──
        if let Err(e) = dispatch_handle.await {
            error!("Dispatch task failed: {:?}", e);
        }

        // ── 17. 等待所有 handler workers 退出（Phase A：New/Changed 拷贝完成） ──
        for handle in handler_handles {
            if let Err(e) = handle.await {
                error!("Handler worker failed: {:?}", e);
            }
        }
        info!("All handler workers completed");

        // ── 18. 关闭 Phase A 广播器 → 等待 Phase A 消费者完成（同步屏障） ──
        // handler workers 已全部退出（其 bc clone 均已 drop）；
        // 主 broadcaster clone drop → 消费者通道 EOF → DatabaseConsumer 刷入所有
        // Phase A 的 New/Changed 条目 → base 表完整，detect_deleted_items 才有完整依据。
        drop(broadcaster);
        Self::await_consumers(consumer_handles).await;
        info!("Phase A complete; base table ready for delete/rename detection");

        // ── 19. Phase B：在 Phase A 屏障之后，起一轮新的 broadcaster + 同一组 consumer 任务 ──
        // `apply_deletions_and_renames` 只负责「检测 + 执行物理操作 + 广播结果消息」，
        // DatabaseConsumer 走 Deleted/Renamed 分支自动完成 base/incremental 表的批量写入。
        let mut broadcaster_b = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        let phase_b_handles = consumer_manager.start_consumers(&mut broadcaster_b).await?;

        // dest storage 提升到 async 块外，供后续 update_directory_metadata 复用，避免二次挂载
        let phase_b_dest_storage: Result<Arc<StorageEnum>> = async {
            let dest_storage_phase_b =
                Arc::new(create_storage(&c.dest_path, block_size.map(|s| s as u64), false).await?);
            let src_storage_phase_b = Arc::new(create_storage(&c.src_path, block_size.map(|s| s as u64), false).await?);

            Self::apply_deletions_and_renames(
                &*db_clone,
                src_storage_phase_b.clone(),
                dest_storage_phase_b.clone(),
                is_source_reserved,
                &broadcaster_b,
                c.enable_integrity_check,
                qos_manager.clone(),
                bytes_tracker.clone(),
            )
            .await;

            Ok(dest_storage_phase_b)
        }
        .await;

        // ── 21. 关闭 Phase B 广播器 → 等待 Phase B 消费者完成（incremental 表冲刷完毕） ──
        drop(broadcaster_b);
        Self::await_consumers(phase_b_handles).await;

        // QoS 在两阶段都走完之后才关闭，保留 Phase B 物理操作期间的限速效果
        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        // stats spinner 必须先于目录元数据进度条收尾，避免两个 spinner 同行互相覆盖
        consumer_manager.end_lifecycle().await;

        // ── 22. 更新目录元数据（非 S3 目标端） ──
        let dest_storage_phase_b =
            phase_b_dest_storage.inspect_err(|e| error!("Phase B failed — reported statistics may be partial: {e}"))?;
        if !matches!(dest_storage_phase_b.as_ref(), StorageEnum::S3(_)) {
            Self::update_directory_metadata(database, &dest_storage_phase_b).await;
        }

        info!(
            "Incremental sync job {} completed successfully via orchestrator",
            c.job_id
        );
        Ok(())
    }

    /// 双进程全量同步 — Sender 侧：`walkdir_2` 分页 → QUIC → Receiver 比较 + 写入
    async fn run_sync_remote(
        &self, remote_addr: &str, tls_cert_bytes: Option<&[u8]>, auth_token: Option<&str>,
    ) -> Result<()> {
        crate::remote_sync::run(&self.config, remote_addr, tls_cert_bytes, auth_token).await
    }

    /// 等待所有消费者 task 完成并记录结果
    async fn await_consumers(consumer_handles: Vec<JoinHandle<Result<()>>>) {
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

    /// rename 成功后同步文件内容或元数据到目标端。
    ///
    /// 返回 `true` 表示同步成功，调用方可随后广播 `Changed` 消息。
    #[allow(clippy::too_many_arguments)]
    async fn sync_renamed_file_changes(
        kind: ChangeKind, entry: &EntryEnum, src: &StorageEnum, dest: &StorageEnum, qos: Option<QosManager>,
        integrity: bool, source_reserved: bool, bytes: Option<Arc<AtomicU64>>,
    ) -> bool {
        if kind != ChangeKind::MetadataOnly {
            // 内容变更：完整拷贝文件再设置元数据
            match StorageEnum::copy_file(
                src,
                dest,
                entry,
                data_mover::CopyOptions {
                    qos,
                    enable_integrity_check: integrity,
                    is_source_reserved: source_reserved,
                    bytes_counter: bytes,
                    ..Default::default()
                },
            )
            .await
            {
                Ok(()) => {}
                Err(e) => {
                    warn!(
                        "Phase B: renamed+data-changed content copy failed for {:?}: {}",
                        entry.get_relative_path(),
                        e
                    );
                    return false;
                }
            }
        }
        // 元数据同步（DataOnly 拷贝后也需要同步 mode/uid/gid/mtime）
        if let Err(e) = dest.set_entry_metadata(entry).await {
            warn!(
                "Phase B: renamed+changed metadata sync failed for {:?}: {}",
                entry.get_relative_path(),
                e
            );
            return false;
        }
        true
    }

    /// DB 写入由 [`DatabaseConsumer`](crate::consumer::DatabaseConsumer) 在收到 `Deleted` / `Renamed` /
    /// `Error` 消息后自动完成（走和 handler workers 同一套批量重试逻辑）；本函数只负责
    /// 「物理操作 + 广播结果消息」。目标端错误仅记录日志并广播 Error，不中断流程。
    ///
    /// # 两遍处理设计
    ///
    /// **第一遍**：收集所有 `DeletionStatus`，从中提取「目录名发生变化的 rename」
    /// 构建 `renamed_dirs` 映射（`old_dir_path` → `new_dir_path`）。
    ///
    /// **第二遍**：顺序执行物理操作。对每条 `Renamed(from, to)` 判断是否跳过：
    /// - `from.name != to.name`：目录/文件本身改了名字 → 必须调用 `process_rename_entry`
    /// - `from.name == to.name` 且 `from.parent` 在 `renamed_dirs` 中且映射到 `to.parent`：
    ///   父目录的 NFS RENAME 已传递性地移动了这个子条目 → 跳过物理操作
    /// - `from.name == to.name` 但父目录未在 `renamed_dirs` 中（跨父目录 move）：
    ///   没有父目录 rename 代为处理 → 必须调用 `process_rename_entry`
    ///
    /// 对于 rename 成功后检测到内容或元数据也同时变化的普通文件，额外执行内容拷贝
    /// 或元数据更新（处理「rename + data/metadata changed」复合场景）。
    #[allow(clippy::too_many_arguments)]
    async fn apply_deletions_and_renames(
        db: &dyn Database, src_storage: Arc<StorageEnum>, dest_storage: Arc<StorageEnum>, is_source_reserved: bool,
        broadcaster: &BroadcastForwarder<StorageEntryMessage>, enable_integrity_check: bool,
        qos_manager: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
    ) {
        let deletion_iter = match db.detect_deleted_items().await {
            Ok(iter) => iter,
            Err(e) => {
                error!("Failed to detect deleted items: {}", e);
                return;
            }
        };

        // 第一遍：一次性收集，构建「所有目录 rename」映射表。
        // 映射语义：renamed_dirs[old_dir_path] = new_dir_path
        // 收录所有 is_dir=true 的 Renamed 条目（包括同名跨父目录 move 如 d3/d3_4 → d4/d3_4）。
        // 用于判断某条目的父目录是否正在被 rename，若是则子条目无需单独执行物理操作。
        let statuses: Vec<DeletionStatus> = deletion_iter.collect();
        let renamed_dirs: HashMap<PathBuf, PathBuf> = statuses
            .iter()
            .filter_map(|s| {
                if let DeletionStatus::Renamed(from, to) = s
                    && from.get_is_dir()
                {
                    return Some((
                        from.get_relative_path().to_path_buf(),
                        to.get_relative_path().to_path_buf(),
                    ));
                }
                None
            })
            .collect();

        // 第二遍：按顺序执行物理操作
        for status in statuses {
            match status {
                DeletionStatus::Deleted(entry) => {
                    let entry_arc = Arc::new(entry);
                    let result = if entry_arc.get_is_dir() {
                        dest_storage.delete_dir_all(&entry_arc).await
                    } else {
                        dest_storage.delete_file(&entry_arc).await
                    };
                    match result {
                        Ok(()) | Err(StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_)) => {
                            broadcaster.broadcast(StorageEntryMessage::Deleted(entry_arc)).await;
                        }
                        Err(e) => {
                            error!("Phase B: Failed to delete {:?}: {}", entry_arc.get_relative_path(), e);
                            broadcaster
                                .broadcast(StorageEntryMessage::Error {
                                    event: ErrorEvent::Delete,
                                    path: entry_arc.get_relative_path().to_path_buf(),
                                    entry: Some(entry_arc.clone()),
                                    reason: format!("{e}"),
                                })
                                .await;
                        }
                    }
                }
                DeletionStatus::Renamed(from, to) => {
                    let from_arc = Arc::new(*from);
                    let to_arc = Arc::new(*to);

                    // 父目录正在被 rename 时，NFS RENAME 已传递性地移动了子条目，无需重复操作。
                    // from_parent 分配仅在同名情况下进行，避免对名字不同的 rename 做无效分配。
                    let skip_physical = from_arc.get_name() == to_arc.get_name() && {
                        let from_parent = from_arc.get_relative_path().parent().map(PathBuf::from);
                        let to_parent = to_arc.get_relative_path().parent().map(PathBuf::from);
                        match (from_parent, to_parent) {
                            (Some(fp), Some(tp)) => renamed_dirs.get(&fp).is_some_and(|d| d == &tp),
                            _ => false,
                        }
                    };

                    let dest_op_result = if skip_physical {
                        Ok(())
                    } else {
                        process_rename_entry(
                            from_arc.clone(),
                            to_arc.clone(),
                            src_storage.clone(),
                            dest_storage.clone(),
                            is_source_reserved,
                        )
                        .await
                    };

                    match dest_op_result {
                        Ok(()) => {
                            // Phase A 不再把路径已变的文件判为 Changed（Fix 2a），
                            // 此处补足 rename+changed 文件的内容/元数据同步，并广播 Changed 消息供消费者统计。
                            if !from_arc.get_is_dir()
                                && !from_arc.get_is_symlink()
                                && let Some(kind) = ChangeKind::from_entry_diff(&from_arc, &to_arc)
                                && Self::sync_renamed_file_changes(
                                    kind,
                                    &to_arc,
                                    &src_storage,
                                    &dest_storage,
                                    qos_manager.clone(),
                                    enable_integrity_check,
                                    is_source_reserved,
                                    bytes_counter.clone(),
                                )
                                .await
                            {
                                broadcaster
                                    .broadcast(StorageEntryMessage::Changed {
                                        entry: to_arc.clone(),
                                        kind,
                                    })
                                    .await;
                            }

                            broadcaster
                                .broadcast(StorageEntryMessage::Renamed((from_arc, to_arc)))
                                .await;
                        }
                        Err(e) => {
                            error!(
                                "Phase B: Failed to rename {:?} → {:?}: {}",
                                from_arc.get_relative_path(),
                                to_arc.get_relative_path(),
                                e
                            );
                            broadcaster
                                .broadcast(StorageEntryMessage::Error {
                                    event: ErrorEvent::Rename,
                                    path: to_arc.get_relative_path().to_path_buf(),
                                    entry: Some(to_arc.clone()),
                                    reason: format!("{e}"),
                                })
                                .await;
                        }
                    }
                }
            }
        }
    }

    /// 更新目录元数据（查询数据库，设置目标端目录的 mtime）
    async fn update_directory_metadata(database: Box<dyn db::traits::Database>, dest_storage: &Arc<StorageEnum>) {
        let mut metadata_pb = DirectoryMetadataProgressBar::new();
        let progress_handle = metadata_pb.start();

        let (dir_tx, mut dir_rx) = mpsc::channel::<EntryEnum>(1000);
        let span = info_span!("db_query");
        tokio::spawn(
            async move {
                if let Err(e) = database.query_storage_entry(Some(true), None, None, dir_tx).await {
                    eprintln!("Failed to query storage entries: {e:?}");
                }
                info!("Query all directories completed");
            }
            .instrument(span),
        );

        // 并发回写目录 mtime：串行调用对 NFS/CIFS 延迟叠加明显（N 目录 × 网络延迟）
        let dir_meta_concurrency: usize = 32;
        let sem = Arc::new(Semaphore::new(dir_meta_concurrency));
        let mut join_set = JoinSet::new();

        // 注意：acquire_owned 在 spawn 之前 await，确保 join_set 中在飞任务数 ≤ dir_meta_concurrency
        while let Some(dir_entry) = dir_rx.recv().await {
            let Ok(permit) = sem.clone().acquire_owned().await else {
                error!("Directory metadata semaphore unexpectedly closed");
                break;
            };
            let dest = dest_storage.clone();
            join_set.spawn(async move {
                let _permit = permit;
                trace!("Updating directory mtime for {:?}", dir_entry.get_relative_path());
                if let Err(e) = dest.set_entry_metadata(&dir_entry).await {
                    warn!(
                        "Failed to update directory mtime for {:?}: {}",
                        dir_entry.get_relative_path(),
                        e
                    );
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!("Directory metadata task panicked: {:?}", e);
            } else {
                metadata_pb.increment_dir_count();
            }
        }

        metadata_pb.finish();
        if let Err(e) = progress_handle.join() {
            error!("Progress bar thread panicked: {:?}", e);
        }
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
