//! 全量迁移的复制管线和早期源端清理。

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use data_mover::model::BackendKind;
use data_mover::storage::NamespaceReadPolicy;
use db::ClickHouseDatabase;
use task_model::migration::MigrationMode;
use tokio::sync::Semaphore;
use tracing::{Instrument, error, info, info_span};
use utils::app_config::AppConfig;

use super::{
    SyncOrchestrator, create_qos_manager, create_storage_pair_with_retry, local::RemainingSourceCleanup,
    mount_concurrency_for_pair, wait_for_mount_init,
};
use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::copy_dispatch::partition_scan;
use crate::copy_execution::CopyDatabaseExecution;
use crate::copy_root::CopyRootMetadata;
use crate::dir_walker::DirectoryWalker;
use crate::error::{AppError, Result};
#[cfg(feature = "license")]
use crate::integrity_check::verify_storage_time;
use crate::migration_plan::MigrationPlan;
use crate::receiver::{LocalCopyExecutor, LocalCopyReceiver, ReceiverConfig};
use crate::scan::ScanType;
use crate::sender::{SenderWorkerConfig, sender_worker};
use crate::source_cleanup::{ReadyCleanupStage, SourceCleanupCheck, finish_with_ready_cleanup};
#[cfg(windows)]
use crate::storage_factory::backend_kind_from_location;
use crate::storage_factory::{StorageRole, create_storage_for_role};
#[cfg(windows)]
use crate::sync::check_admin_privileges;
use crate::sync::parse_size;
use crate::task_stream::{combine_copy_results, finish_copy_phase};
use std::num::NonZeroUsize;

use tokio::task::JoinSet;

use super::collect_controlled_receivers;
use crate::error::StorageError;
use crate::scan::source::WalkOptions;
use crate::sender::controlled::{self, ControlledCopyCounters, DirectoryBudget};
use crate::sender::copy_queue::{CopyCommandSink, copy_queue};

/// 大文件日志阈值（512 MiB）
const LARGE_FILE_LOG_THRESHOLD: u64 = 512 * 1024 * 1024;

impl SyncOrchestrator {
    /// 全量同步 — 由本地有界队列调度 sender 和 receiver workers
    pub(super) async fn run_sync(
        &self, observation_plan: Option<MigrationPlan>, app_config: &AppConfig, consumer_manager: &mut ConsumerManager,
        database: Option<ClickHouseDatabase>, root_metadata: &CopyRootMetadata,
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

        let task_cancel = root_metadata.cancellation();
        let controlled_source = if database.is_none() && is_source_reserved && !c.packaged && c.file_list.is_none() {
            root_metadata.directory_bindings()
        } else {
            None
        };
        // 直接受控扫描没有安全的目录 cursor 续接；不套用 RestartRoot 恢复。
        let controlled_session = controlled_source
            .as_ref()
            .map(|(source, destination)| {
                // 本地文件系统可预取多个有界分支，保持复制 worker 数量独立。
                let window = if cfg!(unix)
                    && source.storage().kind() == BackendKind::Local
                    && destination.storage().kind() == BackendKind::Local
                {
                    copy_concurrency.saturating_mul(4)
                } else {
                    copy_concurrency
                };
                source.consumer_traversal(
                    &WalkOptions {
                        cancel: Some(task_cancel.clone()),
                        filter_time_anchor: scan_config.filter_time_anchor,
                        observation_plan: scan_config.observation_plan,
                        metadata_scope: scan_config.metadata_scope,
                        operation_timeout: Some(Duration::from_secs(app_config.scan.operation_timeout_secs)),
                        match_expressions: scan_config.match_expressions.clone(),
                        exclude_expressions: scan_config.exclude_expressions.clone(),
                        concurrency: scan_config.concurrency,
                        ..WalkOptions::default()
                    },
                    NonZeroUsize::MAX,
                    NonZeroUsize::new(window).unwrap_or(NonZeroUsize::MIN),
                )
            })
            .transpose()?;
        // 先建立枚举，再启动后台消费者；其他扫描场景保持现有流水线。
        let walkdir_iter = if controlled_session.is_none() {
            Some(
                DirectoryWalker::new(scan_config)
                    .with_cancellation(Some(task_cancel.clone()))
                    .with_file_list_read_policy(NamespaceReadPolicy {
                        operation_timeout: Duration::from_secs(app_config.scan.operation_timeout_secs),
                        recovery_window: Duration::from_secs(app_config.scan.recovery_window_secs),
                        retry_interval: Duration::from_millis(100),
                    })
                    .walk()
                    .await?,
            )
        } else {
            None
        };

        // ── 4. 广播器 + 消费者 ──
        let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);
        consumer_manager
            .prepare_copy_consumers(consumer_config.as_ref(), database.clone())
            .await?;
        consumer_manager.begin_lifecycle().await;
        let consumer_handles = consumer_manager.start_consumers(&mut broadcaster).await?;
        let cleanup_check = SourceCleanupCheck::new(
            Duration::from_secs(app_config.scan.operation_timeout_secs),
            Duration::from_secs(app_config.scan.recovery_window_secs),
            consumer_manager
                .copy_database_execution()
                .map(|execution| execution.token().clone())
                .unwrap_or_default(),
        );
        let early_cleanup = if !is_source_reserved && database.is_some() {
            let streaming = if c.packaged || c.file_list.is_some() {
                None
            } else {
                consumer_manager
                    .copy_database_execution()
                    .map(|execution| (cleanup_check.clone(), execution))
            };
            Some(ReadyCleanupStage::start(
                &mut broadcaster,
                copy_concurrency,
                MigrationMode::Full,
                streaming,
            ))
        } else {
            None
        };
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

        let pipeline_result = if let Some(session) = controlled_session {
            let receiver_config = Arc::new(ReceiverConfig {
                job_id: c.job_id.clone(),
                enable_integrity_check: c.enable_integrity_check,
                enable_acl: c.enable_acl,
                is_source_reserved,
                job_dir: c.job_dir.clone(),
                transfer_mode: c.transfer_mode,
                cancel: Some(task_cancel.clone()),
            });
            let (source, destination) = controlled_source.ok_or(AppError::CopyWorkProtocol {
                reason: "controlled source bindings missing",
            })?;
            let reporter = LocalCopyExecutor {
                id: 0,
                source,
                destination,
                config: receiver_config.clone(),
                qos: qos_manager.clone(),
                bytes: bytes_tracker.clone(),
                broadcaster: broadcaster.clone(),
                active: active_tokio_task_counter.clone(),
                cleanup: None,
            };
            let mount_semaphore = Arc::new(Semaphore::new(mount_concurrency_for_pair(&c.src_path, &c.dest_path)));
            let mut mounts = JoinSet::new();
            for id in 0..copy_concurrency.max(1) {
                if session.1.failure().is_some() {
                    task_cancel.cancel();
                }
                if task_cancel.is_cancelled() {
                    break;
                }
                let mount = mount_semaphore.clone();
                let source = c.src_path.clone();
                let target = c.dest_path.clone();
                let config = receiver_config.clone();
                let mut executor = reporter.clone();
                mounts.spawn(async move {
                    let pair = create_storage_pair_with_retry(
                        &mount,
                        &source,
                        &target,
                        block_size,
                        &format!("Receiver {id}"),
                        config.cancel.as_ref(),
                    )
                    .await?;
                    executor.id = id;
                    executor.source = pair.get_src_storage().clone();
                    executor.destination = pair.get_dest_storage().clone();
                    Result::Ok(executor)
                });
            }
            let (executors, mount_failure) = collect_controlled_receivers(mounts, &session.1, &task_cancel).await;
            mount_completed.store(true, Ordering::Release);
            let result = controlled::run(
                session,
                executors,
                reporter,
                ControlledCopyCounters {
                    entries: entry_counter.clone(),
                    bytes: size_counter.clone(),
                    active: active_entry_counter.clone(),
                },
                DirectoryBudget {
                    count: usize::MAX,
                    bytes: usize::MAX,
                },
                task_cancel.clone(),
            )
            .await;
            match (mount_failure, result) {
                (Some(startup), Err(source)) if !source.storage_error().is_some_and(StorageError::is_cancelled) => {
                    Err(AppError::CopyStartupAndTraversalFailed {
                        source: Box::new(source),
                        startup: Box::new(startup),
                    })
                }
                (Some(error), _) => Err(error),
                (None, result) => result,
            }
        } else {
            let walkdir_iter = walkdir_iter.ok_or(AppError::CopyWorkProtocol {
                reason: "missing ordinary full scan",
            })?;
            // ── 8. 创建本地工作队列 ──
            let (command_sender, command_receiver) = copy_queue();
            let command_sender: Arc<dyn CopyCommandSink> = Arc::new(command_sender);

            let receiver_config = Arc::new(ReceiverConfig {
                job_id: c.job_id.clone(),
                enable_integrity_check: c.enable_integrity_check,
                enable_acl: c.enable_acl,
                is_source_reserved,
                job_dir: c.job_dir.clone(),
                transfer_mode: c.transfer_mode,
                cancel: Some(task_cancel.clone()),
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
                let commands = command_receiver.clone();
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
                let cleanup_connections = early_cleanup.as_ref().map(ReadyCleanupStage::connections);
                let ready_files = early_cleanup.as_ref().and_then(ReadyCleanupStage::ready_files);

                let span = info_span!("receiver_worker", recv_id = recv_id);
                let handle = tokio::spawn(
                    CopyDatabaseExecution::run_optional_child(execution, "full_copy_receive", async move {
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
                        if let Some(connections) = cleanup_connections {
                            let _ = connections.set((src.clone(), dest.clone()));
                        }

                        LocalCopyReceiver {
                            commands,
                            executor: LocalCopyExecutor {
                                id: recv_id,
                                source: src,
                                destination: dest,
                                config: cfg,
                                qos,
                                bytes: bt,
                                broadcaster: bc,
                                active: task_counter,
                                cleanup: ready_files,
                            },
                        }
                        .run()
                        .await
                    })
                    .instrument(span),
                );
                receiver_handles.push(handle);
            }

            // 只有实际 worker 持有接收端；全部挂载失败时生产端能收到关闭错误。
            drop(command_receiver);

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
                let commands = command_sender.clone();
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
                    CopyDatabaseExecution::run_optional_child(execution, "full_copy_send", async move {
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
                            worker_id, wi, src, dest, commands, &bc, &cfg, qos, bt, &copy_cfg, aec, ec, sc,
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
            let senders_result = finish_copy_phase(sender_handles).await;
            walkdir_iter.close();
            let source_dispatch_result = finish_copy_phase(vec![source_dispatch]).await;
            walkdir_iter.wait_for_producer().await;

            // ── 12. 关闭管线（和原始 sync_inner 一致） ──
            // Sender workers 全部完成 → 不再发 CopyEntry
            // 释放最后一个发送端后，Receiver 排空队列并退出
            drop(command_sender);

            // 等所有 Receiver worker 退出（退出时 broadcaster clone 也 drop）
            let receivers_result = finish_copy_phase(receiver_handles).await;

            let source_result = walkdir_iter.take_error().map_or(Ok(()), |error| Err(error.into()));
            combine_copy_results(
                [
                    ("full_copy_send", senders_result),
                    ("full_copy_receive", receivers_result),
                    ("full_copy_dispatch", source_dispatch_result),
                    ("full_copy_enumerate", source_result),
                ]
                .map(|(phase, result)| result.map_err(|error| error.in_copy_phase(phase))),
            )
        };

        // ── 13. Cleanup ──
        stats_handle.abort();
        let _ = stats_handle.await;

        if let Some(ref qos_mgr) = qos_manager {
            qos_mgr.shutdown();
        }

        drop(broadcaster);
        let early = finish_with_ready_cleanup(
            consumer_handles,
            pipeline_result,
            early_cleanup,
            cleanup_check.clone(),
            root_metadata,
        )
        .await?;
        let result: Result<()> = async {
            if let Some(database) = &database {
                let destination = if let Some((_, destination)) = &early.connections {
                    destination.clone()
                } else {
                    Arc::new(
                        create_storage_for_role(
                            &c.dest_path,
                            block_size.map(|size| size as u64),
                            false,
                            StorageRole::Destination,
                        )
                        .await?,
                    )
                };
                // 全量已在传输时校验；沿用早期清理的连接和就绪证明。
                let cleanup = (!is_source_reserved).then(|| RemainingSourceCleanup {
                    source: early.connections.map(|(source, _)| source),
                    block_size,
                    check: cleanup_check.with_prechecked(early.checked, early.streamed_files),
                });
                self.complete_persisted(database, destination, consumer_manager, root_metadata, cleanup)
                    .await?;
            } else if root_metadata.enabled() {
                let destination = create_storage_for_role(&c.dest_path, None, false, StorageRole::Destination).await?;
                root_metadata.finish(&destination).await?;
            }
            Ok(())
        }
        .await;
        result?;

        info!("Copy job {} completed successfully via orchestrator", c.job_id);
        Ok(())
    }
}
