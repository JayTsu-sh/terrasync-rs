//! 增量迁移，先确认观察和更新，再判定目标删除与重命名。

use std::{sync::Arc, time::Duration};

#[cfg(windows)]
use data_mover::model::BackendKind;
use data_mover::storage::NamespaceReadPolicy;
use db::{ClickHouseDatabase, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use task_model::StorageEntryMessage;
use task_model::migration::{MigrationMode, TargetCleanupPolicy};
use tokio::task::JoinHandle;
use tracing::{Instrument, error, info, info_span, trace};
use utils::app_config::AppConfig;

use super::{SyncOrchestrator, create_qos_manager, incremental_worker, local::RemainingSourceCleanup};
use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{JobType, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::copy_dispatch::{CopyQueues, partition_scan};
use crate::copy_execution::CopyDatabaseExecution;
use crate::copy_history::{HistoryCompletion, HistoryCopyPolicy};
use crate::copy_root::CopyRootMetadata;
use crate::dir_walker::DirectoryWalker;
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::CopyOptions;
#[cfg(feature = "license")]
use crate::integrity_check::verify_storage_time;
use crate::migration_plan::MigrationPlan;
use crate::scan::ScanType;
use crate::source_cleanup::{ReadyCleanupStage, SourceCleanupCheck, finish_with_ready_cleanup};
#[cfg(windows)]
use crate::storage_factory::backend_kind_from_location;
use crate::storage_factory::{StorageContext, StorageRole, create_storage_for_role};
#[cfg(windows)]
use crate::sync::check_admin_privileges;
use crate::sync::parse_size;
use crate::sync_catalog::batch_processing_to_generate_message;
use crate::target_absence::SourceAbsenceCheck;
use crate::task_stream::finish_phase;

impl SyncOrchestrator {
    /// 增量同步 — 双阶段流水线：
    /// - Phase A：scan workers → DB 比对 → handler workers 拷贝 → 通过 `broadcaster_a` 喂给 consumers
    /// - Barrier：`drop(broadcaster_a)` + `await phase_a_handles`（base 表冲刷完毕）
    /// - Phase B：`detect_deleted_items` → 执行 dest 物理删除/重命名 → 通过 `broadcaster_b`
    ///   喂给同一组 consumers（`DatabaseConsumer` 自动完成 incremental 表写入 + base 表修正）
    pub(super) async fn run_incremental_sync(
        &self, observation_plan: Option<MigrationPlan>, app_config: &AppConfig, consumer_manager: &mut ConsumerManager,
        database: ClickHouseDatabase, root_metadata: &CopyRootMetadata,
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
        let cleanup_check = SourceCleanupCheck::new(
            Duration::from_secs(app_config.scan.operation_timeout_secs),
            Duration::from_secs(app_config.scan.recovery_window_secs),
            consumer_manager
                .copy_database_execution()
                .map(|execution| execution.token().clone())
                .unwrap_or_default(),
        )
        .with_content_verification(c.enable_integrity_check);
        let early_cleanup = (!is_source_reserved && !c.packaged && c.file_list.is_none())
            .then(|| ReadyCleanupStage::start(&mut broadcaster, copy_concurrency, MigrationMode::Incremental, None));

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
            early_cleanup.as_ref().map(ReadyCleanupStage::connections),
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
        let enumeration_result = walkdir_iter.take_error().map_or(Ok(()), |error| Err(error.into()));
        // 目标删除和重命名仍依赖完整比较及持久化。就绪窗口只处理本轮已复制的源端文件。
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
        let phase_a = enumeration_result
            .and(scan_result)
            .and(handler_result)
            .and(dispatch_result)
            .and(scan_dispatch_result);
        let early = match finish_with_ready_cleanup(
            consumer_handles,
            phase_a,
            early_cleanup,
            cleanup_check.clone(),
            root_metadata,
        )
        .await
        {
            Ok(completed) => completed,
            Err(error) => {
                if let Some(ref qos_mgr) = qos_manager {
                    qos_mgr.shutdown();
                }
                return Err(error);
            }
        };
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
                    )
                    .with_cancellation(
                        consumer_manager
                            .copy_database_execution()
                            .map(|execution| execution.token().clone())
                            .unwrap_or_default(),
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
            let destination = match (phase_b_dest_storage, phase_b_consumers) {
                (Ok(destination), Ok(())) => destination,
                (Err(source), Err(reporting)) => {
                    return Err(AppError::MigrationReportingFailed {
                        source: Box::new(source),
                        reporting: Box::new(reporting),
                    });
                }
                (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
            };
            // 增量跳过项由清理阶段补齐所选的数据校验。
            let cleanup = (!is_source_reserved).then(|| RemainingSourceCleanup {
                source: early.connections.map(|(source, _)| source),
                block_size,
                check: cleanup_check.with_prechecked(early.checked, early.streamed_files),
            });
            self.complete_persisted(&database, destination, consumer_manager, root_metadata, cleanup)
                .await?;
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
}
