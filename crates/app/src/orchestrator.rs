//! Sync 编排器
//!
//! SyncOrchestrator 封装 sync/incremental_sync 的核心逻辑，
//! 通过 Transport 抽象层连接 Sender 和 Receiver，支持单进程和双进程两种模式。

// 标准库
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
use db::factory::DatabaseFactory;
use db::{DeletionStatus, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use storage_v2::error::StorageError;
use storage_v2::qos::QosManager;
#[cfg(windows)]
use storage_v2::storage_enum::{StorageType, detect_storage_type};
use storage_v2::{EntryEnum, ErrorEvent, StorageEntryMessage, StorageEnum, create_storage};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};
use transport::in_process::create_in_process_pair;
use transport::message::SenderMsg;
use transport::traits::{ReceiverTransport, SenderTransport};
use utils::app_config::AppConfig;

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::config::{JobType, SyncJobConfig, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::consumer::stats::DirectoryMetadataProgressBar;
use crate::error::{AppError, Result};
use crate::receiver::{ReceiverConfig, process_entry_on_receiver};
use crate::scan::{ScanType, batch_processing_to_generate_message};
use crate::sender::{SenderWorkerConfig, sender_worker};
#[cfg(windows)]
use crate::sync::check_admin_privileges;
#[cfg(feature = "license")]
use crate::sync::verify_storage_time;
use crate::sync::{StoragePair, parse_size, process_entry, process_rename_entry, process_versioned_entry};
use crate::{dir_walker, tar_pack};

/// 广播通道容量
const BROADCAST_CHANNEL_CAPACITY: usize = 1000;

/// 大文件日志阈值（512 MiB）
const LARGE_FILE_LOG_THRESHOLD: u64 = 512 * 1024 * 1024;

/// StoragePair 创建失败后的最大重试次数
const STORAGE_PAIR_MAX_RETRIES: usize = 3;

/// 同时创建 StoragePair（NFS mount）的最大并发数。
/// 设为 2：nfs-rs 在 Windows 上绑定特权端口（<1024）时，并发量过大会导致端口
/// 竞争和 TIME_WAIT 积累，最终 WSAEADDRINUSE；限制并发可显著降低冲突概率。
const STORAGE_PAIR_MOUNT_CONCURRENCY: usize = 2;

/// 等待所有 worker 完成 mount 初始化，输出存活汇总
///
/// 使用 countdown latch：每个 worker mount 完成（无论成功/失败）后 done_counter +1，
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

/// 带限流和退避重试的 StoragePair 创建
///
/// 通过 semaphore 限制同时进行 NFS mount 的并发数，
/// 失败时指数退避重试，避免 portmapper 被突发请求打满。
async fn create_storage_pair_with_retry(
    semaphore: &Semaphore, src_path: &str, dest_path: &str, block_size: Option<usize>, worker_label: &str,
) -> Result<StoragePair> {
    let mut last_error = None;

    for attempt in 0..=STORAGE_PAIR_MAX_RETRIES {
        let _permit = semaphore
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
                drop(_permit);

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
        AppError::ConfigError(format!("{}: StoragePair creation exhausted all retries", worker_label))
    }))
}

/// Sync 编排器
///
/// 统一管理 sync 和 incremental_sync 的执行流程。
/// 单进程模式下通过 InProcessTransport 连接 sender 和 receiver task。
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
    pub fn new_remote(config: SyncJobConfig, remote_addr: &str, tls_server_cert: Option<Vec<u8>>) -> Self {
        Self {
            config,
            mode: SyncMode::Remote {
                remote_addr: remote_addr.to_string(),
                tls_server_cert,
            },
        }
    }

    /// 执行 sync 管线
    pub async fn run(&self) -> Result<()> {
        match (&self.mode, self.config.scan_type) {
            (SyncMode::Local, ScanType::Full) => self.run_sync().await,
            (SyncMode::Local, ScanType::Incremental) => self.run_incremental_sync().await,
            (
                SyncMode::Remote {
                    remote_addr,
                    tls_server_cert,
                },
                ScanType::Full,
            ) => self.run_sync_remote(remote_addr, tls_server_cert.as_deref()).await,
            (SyncMode::Remote { .. }, ScanType::Incremental) => {
                // 双进程增量同步后续 Phase 实现
                Err(AppError::CopyError(
                    "Remote incremental sync not yet implemented".into(),
                ))
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
            c.scan_type,
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
        let mut broadcaster = BroadcastForwarder::new(BROADCAST_CHANNEL_CAPACITY);
        let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
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

        let qos_manager = create_qos_manager(&c.qos, c.peak_qos_rate, c.iops);

        // ── 6. 原子计数器 + 统计 reporter ──
        let active_entry_counter = Arc::new(AtomicUsize::new(0));
        let entry_counter = Arc::new(AtomicUsize::new(0));
        let size_counter = Arc::new(AtomicU64::new(0));
        let active_tokio_task_counter = Arc::new(AtomicUsize::new(0));

        let stats_handle = Self::spawn_stats_reporter(
            active_entry_counter.clone(),
            entry_counter.clone(),
            size_counter.clone(),
            active_tokio_task_counter.clone(),
            copy_concurrency,
        );

        // ── 7. 启动 walkdir ──
        let walkdir_iter = dir_walker::walkdir(scan_config)
            .await
            .map_err(|e| AppError::ScanError(format!("Start directory walker failed: {}", e)))?;

        // ── 8. 创建 Transport ──
        let (sender_transport, receiver_transport) = create_in_process_pair();
        let sender_transport: Arc<dyn SenderTransport> = Arc::new(sender_transport);
        let receiver_transport = Arc::new(receiver_transport);

        let receiver_config = Arc::new(ReceiverConfig {
            enable_integrity_check: c.enable_integrity_check,
            enable_acl: c.enable_acl,
            is_source_reserved,
        });

        // ── 8.5. 创建 mount 限流信号量 + countdown latch 计数器（Sender + Receiver 共享） ──
        let mount_semaphore = Arc::new(Semaphore::new(STORAGE_PAIR_MOUNT_CONCURRENCY));
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
            let block_size = block_size;
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
                        &format!("Receiver {}", recv_id),
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
                                            reason: format!("{}", e),
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
                        &format!("Sender {}", worker_id),
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
                    sender_worker(worker_id, wi, src, dest, transport, &cfg, qos, bt, ob, aec, ec, sc).await;
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

        // ── 14. 更新目录元数据（非 S3 目标端） ──
        let dest_storage_for_metadata = Arc::new(create_storage(&c.dest_path, block_size.map(|s| s as u64)).await?);
        if !matches!(dest_storage_for_metadata.as_ref(), StorageEnum::S3(_)) {
            Self::update_directory_metadata(database, &dest_storage_for_metadata).await;
        }

        info!("Copy job {} completed successfully via orchestrator", c.job_id);
        Ok(())
    }

    /// 增量同步 — 双层广播管线：scan workers → DB batch → detect → handler workers → consumers
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
            c.scan_type,
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

        // ── 4. 广播器 + 消费者 ──
        let mut broadcaster = BroadcastForwarder::new(BROADCAST_CHANNEL_CAPACITY);
        let mut consumer_manager = ConsumerManager::new(consumer_config.as_ref()).await?;
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;
        let bytes_tracker = consumer_manager.get_bytes_tracker().await;

        // ── 5. 检测广播器 + MPMC 通道 ──
        // scan workers → detect_broadcaster → dispatch_task → async_channel(MPMC) → N handler workers → broadcaster
        let mut detect_broadcaster = BroadcastForwarder::<StorageEntryMessage>::new(BROADCAST_CHANNEL_CAPACITY);
        let mut file_op_rx = detect_broadcaster.subscribe();
        let (work_tx, work_rx) = async_channel::bounded::<StorageEntryMessage>(BROADCAST_CHANNEL_CAPACITY);

        // ── 6. 并发度 + block_size ──
        let incremental_scan_concurrency = match app_config.database.r#type.as_str() {
            "duckdb" => 1, // DuckDB 串行模式避免并发写入冲突
            _ => app_config.scan.concurrency,
        };

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
        let qos_manager = create_qos_manager(&c.qos, c.peak_qos_rate, c.iops);
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
        let mount_semaphore = Arc::new(Semaphore::new(STORAGE_PAIR_MOUNT_CONCURRENCY));
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
                        &format!("Handler {}", handler_id),
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
                                    )
                                    .await
                                    {
                                        Ok(_) => bc.broadcast(message.clone()).await,
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
                                                reason: format!("{}", e),
                                            })
                                            .await;
                                        }
                                    }
                                }
                            }
                            StorageEntryMessage::Changed(ref entry) => {
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
                                )
                                .await
                                {
                                    Ok(_) => bc.broadcast(message.clone()).await,
                                    Err(e) => {
                                        error!(
                                            "Handler {}: Failed to process changed entry {:?}: {}",
                                            handler_id,
                                            entry.get_relative_path(),
                                            e
                                        );
                                        bc.broadcast(StorageEntryMessage::Error {
                                            event: ErrorEvent::Copy,
                                            path: entry.get_relative_path().to_path_buf(),
                                            reason: format!("{}", e),
                                        })
                                        .await;
                                    }
                                }
                            }
                            StorageEntryMessage::Deleted(ref entry) => {
                                // 删除操作幂等化：NotFound 视为成功
                                let result = if !entry.get_is_dir() {
                                    trace!("Deleting file: {:?}", entry.get_relative_path());
                                    dest.delete_file(entry).await
                                } else {
                                    trace!("Deleting directory: {:?}", entry.get_relative_path());
                                    dest.delete_dir_all(entry).await
                                };
                                match result {
                                    Ok(_) => bc.broadcast(message.clone()).await,
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
                                            reason: format!("{}", e),
                                        })
                                        .await;
                                    }
                                }
                            }
                            StorageEntryMessage::Renamed((ref from_entry, ref to_entry)) => {
                                if from_entry.get_name() != to_entry.get_name() {
                                    match process_rename_entry(
                                        from_entry.clone(),
                                        to_entry.clone(),
                                        src.clone(),
                                        dest.clone(),
                                        is_source_reserved,
                                    )
                                    .await
                                    {
                                        Ok(_) => bc.broadcast(message.clone()).await,
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
                                                reason: format!("{}", e),
                                            })
                                            .await;
                                        }
                                    }
                                } else {
                                    // 同名 rename（父目录移动）：直接广播
                                    bc.broadcast(message.clone()).await;
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
                                            reason: format!("{}", e),
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
            .map_err(|e| AppError::ScanError(format!("Start directory walker failed: {}", e)))?;

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

        // ── 15. 检测删除/重命名 → detect_broadcaster ──
        match db_clone.detect_deleted_items().await {
            Ok(deletion_iter) => {
                for status in deletion_iter {
                    match status {
                        DeletionStatus::Deleted(entry) => {
                            detect_broadcaster
                                .broadcast(StorageEntryMessage::Deleted(Arc::new(entry)))
                                .await;
                        }
                        DeletionStatus::Renamed(from, to) => {
                            detect_broadcaster
                                .broadcast(StorageEntryMessage::Renamed((Arc::new(from), Arc::new(to))))
                                .await;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to detect deleted items: {}", e);
            }
        }
        // 关闭 detect_broadcaster → dispatch task 的 file_op_rx 耗尽
        drop(detect_broadcaster);

        // ── 16. 等待 dispatch task 退出 ──
        if let Err(e) = dispatch_handle.await {
            error!("Dispatch task failed: {:?}", e);
        }

        // ── 17. 等待所有 handler workers 退出 ──
        for handle in handler_handles {
            if let Err(e) = handle.await {
                error!("Handler worker failed: {:?}", e);
            }
        }
        info!("All handler workers completed");

        // ── 18. Cleanup ──
        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        drop(broadcaster);
        Self::await_consumers(consumer_handles).await;

        // ── 19. 更新目录元数据（非 S3 目标端） ──
        let dest_storage_for_metadata = Arc::new(create_storage(&c.dest_path, block_size.map(|s| s as u64)).await?);
        if !matches!(dest_storage_for_metadata.as_ref(), StorageEnum::S3(_)) {
            Self::update_directory_metadata(database, &dest_storage_for_metadata).await;
        }

        info!(
            "Incremental sync job {} completed successfully via orchestrator",
            c.job_id
        );
        Ok(())
    }

    /// 双进程全量同步 — Sender 侧：walkdir_2 分页 → QUIC → Receiver 比较 + 写入
    async fn run_sync_remote(&self, remote_addr: &str, tls_cert_bytes: Option<&[u8]>) -> Result<()> {
        crate::remote_sync::run(&self.config, remote_addr, tls_cert_bytes).await
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
    fn spawn_stats_reporter(
        active_entry_counter: Arc<AtomicUsize>, entry_counter: Arc<AtomicUsize>, size_counter: Arc<AtomicU64>,
        active_tokio_task_counter: Arc<AtomicUsize>, copy_concurrency: usize,
    ) -> JoinHandle<()> {
        let span = info_span!("stats_reporter");
        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    let active_entries = active_entry_counter.load(Ordering::Relaxed);
                    let count = entry_counter.swap(0, Ordering::Relaxed);
                    let total_size = size_counter.swap(0, Ordering::Relaxed);
                    let active_tasks = active_tokio_task_counter.load(Ordering::Relaxed);
                    if active_tasks != copy_concurrency {
                        warn!(
                            "Processed {} entries ({} bytes) in 10s, active_tasks: {}, active_entries: {}",
                            count, total_size, active_tasks, active_entries
                        );
                    } else {
                        info!(
                            "Processed {} entries ({} bytes) in 10s, active_tasks: {}, active_entries: {}",
                            count, total_size, active_tasks, active_entries
                        );
                    }
                }
            }
            .instrument(span),
        )
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
                    eprintln!("Failed to query storage entries: {:?}", e);
                }
                info!("Query all directories completed");
            }
            .instrument(span),
        );

        while let Some(dir_entry) = dir_rx.recv().await {
            trace!("Updating directory mtime for {:?}", dir_entry.get_relative_path());
            if let Err(e) = dest_storage.set_entry_metadata(&dir_entry).await {
                warn!(
                    "Failed to update directory mtime for {:?}: {}",
                    dir_entry.get_relative_path(),
                    e
                );
            }
            metadata_pb.increment_dir_count();
        }

        metadata_pb.finish();
        if let Err(e) = progress_handle.join() {
            error!("Progress bar thread panicked: {:?}", e);
        }
    }
}

/// 创建 QoS 管理器（crate 内共享，供 orchestrator 和 remote_sync 调用）
pub(crate) fn create_qos_manager(qos: &Option<String>, peak_qos_rate: f32, iops: Option<u32>) -> Option<QosManager> {
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
