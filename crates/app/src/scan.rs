//! 文件扫描模块
//!
//! 该模块提供文件扫描功能，包括：
//! 1. 全量扫描（扫描所有文件和目录）
//! 2. 增量扫描（只扫描变更的文件和目录）
//! 3. 扫描结果处理和分发
//! 4. 与数据库的交互，用于增量扫描

// 标准库
use std::sync::Arc;

// 外部crate
use data_mover::{EntryEnum, StorageEntryMessage, WalkDirAsyncIterator, canonicalize_path, redact_storage_url};
use db::factory::DatabaseFactory;
use db::traits::Database;
use db::{self, DeletionStatus, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, info_span, instrument, trace, warn};
use utils::app_config::AppConfig;

// 内部模块
use crate::broadcast::{BroadcastForwarder, DEFAULT_CHANNEL_CAPACITY};
use crate::config::{ConsumerConfig, JobType, initialize_consumer_config, initialize_scan_config};
use crate::consumer::ConsumerManager;
use crate::dir_walker;
use crate::error::{AppError, Result};

/// 扫描流水线，封装已初始化的 pipeline 各组件
struct ScanPipeline {
    app_config: AppConfig,
    consumer_config: Arc<ConsumerConfig>,
    broadcaster: BroadcastForwarder<StorageEntryMessage>,
    consumer_handles: Vec<JoinHandle<Result<()>>>,
    walkdir_iter: WalkDirAsyncIterator,
    /// 与 pipeline 同生命周期：`shutdown_pipeline` 在 await 完 consumer 任务后调用
    /// `end_lifecycle()` 打印统计报告。
    consumer_manager: ConsumerManager,
}

/// 扫描类型枚举
///
/// 定义系统支持的不同扫描策略
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScanType {
    /// 全量扫描 - 扫描所有文件和目录，不管是否之前已经扫描过
    Full,
    /// 增量扫描 - 只扫描变更的文件和目录，提高性能
    Incremental,
}

impl std::fmt::Display for ScanType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanType::Full => write!(f, "full"),
            ScanType::Incremental => write!(f, "incremental"),
        }
    }
}

/// 判定 ScanType：优先查数据库中 base 表是否存在，失败时 fallback 到文件系统快照
///
/// # 判定逻辑
/// 1. 尝试连接数据库并查询 `base_{sanitize(job_id)}` 表是否存在
/// 2. 数据库不可达或查询失败时，fallback 到调用方传入的 `job_dir_pre_existing`
///    快照（在创建 `job_dir` 之前捕获，避免被自身的 `create_dir_all` 污染）
pub async fn determine_scan_type(db: Option<&dyn Database>, job_id: &str, job_dir_pre_existing: bool) -> ScanType {
    if let Some(db) = db {
        let table_name = db::base_table_name_for_job(job_id);
        match db.table_exists(&table_name).await {
            Ok(true) => return ScanType::Incremental,
            Ok(false) => return ScanType::Full,
            Err(e) => {
                warn!("数据库查询失败，fallback 到文件系统快照判定: {}", e);
            }
        }
    }

    // Fallback：调用方注入的 job_dir 预存在快照
    if job_dir_pre_existing {
        ScanType::Incremental
    } else {
        ScanType::Full
    }
}

/// 初始化扫描流水线
///
/// 根据参数初始化配置、消费者管理器、广播转发器和目录遍历器。
///
/// `progress_callback_url`：HTTP 进度回调 URL，透传给 `ConsumerConfig`（web 层传入，CLI 传 `None`）
#[allow(clippy::too_many_arguments, clippy::ref_option)]
async fn setup_scan_pipeline(
    job_id: &str, job_dir: &str, job_type: JobType, path: &str, scan_type: ScanType, depth: u32,
    r#match: &Option<String>, exclude: &Option<String>, raw_command_line: String,
    progress_callback_url: Option<String>,
) -> Result<ScanPipeline> {
    // 1. 加载应用配置
    let app_config = AppConfig::fetch()?;
    debug!("[ScanPipeline] Loaded application configuration: {:?}", app_config);

    // 2. 规范化路径
    let src_path = canonicalize_path(path)?;

    // 3. 初始化扫描配置
    let scan_config = match initialize_scan_config(
        job_id,
        &src_path,
        depth,
        scan_type,
        r#match,
        exclude,
        app_config.scan.concurrency,
        app_config.scan.include_tags,
        &None,
        false, // packaged
        0,     // package_depth
    ) {
        Ok(config) => {
            info!(
                "Initialized scan configuration: {{id: {}, path: {}, depth: {}, concurrency: {}, include_tags: {}}}",
                config.job_id, config.path, config.depth, config.concurrency, config.include_tags
            );
            config
        }
        Err(e) => {
            error!("[ScanPipeline] Failed to initialize scan configuration: {}", e);
            return Err(AppError::ConfigError(format!(
                "Failed to initialize scan configuration: {e}"
            )));
        }
    };

    // 4. 初始化消费者配置
    let consumer_config = initialize_consumer_config(
        job_id,
        job_dir,
        job_type.clone(),
        raw_command_line,
        &app_config,
        progress_callback_url,
    )?;

    let consumer_config = Arc::new(consumer_config);

    // 5. 创建广播转发器
    let mut broadcaster = BroadcastForwarder::new(DEFAULT_CHANNEL_CAPACITY);

    // 6. 初始化消费者管理器
    let mut consumer_manager = match ConsumerManager::new(consumer_config.as_ref()).await {
        Ok(m) => m,
        Err(e) => {
            error!("[ScanPipeline] Failed to initialize consumer manager: {}", e);
            return Err(e);
        }
    };

    // 7. 启动生命周期（进度条 + HTTP 回调循环），再启动所有消费者
    consumer_manager.begin_lifecycle().await;
    let consumer_handles = match consumer_manager.start_consumers(&mut broadcaster).await {
        Ok(handles) => {
            info!("Started {} consumers successfully", handles.len());
            handles
        }
        Err(e) => {
            error!("[ScanPipeline] Failed to start consumers: {}", e);
            return Err(e);
        }
    };

    // 8. 启动目录扫描
    let walkdir_iter = match dir_walker::walkdir(scan_config).await {
        Ok(iter) => {
            debug!("Directory walker started successfully");
            iter
        }
        Err(e) => {
            error!("[ScanPipeline] Start directory walker failed: {}", e);
            return Err(AppError::ScanError(format!("Start directory walker failed: {e}")));
        }
    };

    Ok(ScanPipeline {
        app_config,
        consumer_config,
        broadcaster,
        consumer_handles,
        walkdir_iter,
        consumer_manager,
    })
}

/// 关闭流水线，等待所有消费者完成后收尾统计生命周期
async fn shutdown_pipeline(
    broadcaster: BroadcastForwarder<StorageEntryMessage>, consumer_handles: Vec<JoinHandle<Result<()>>>,
    mut consumer_manager: ConsumerManager,
) -> bool {
    drop(broadcaster);

    let mut all_successful = true;
    for (i, handle) in consumer_handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => debug!("Consumer task #{} completed successfully", i),
            Ok(Err(e)) => {
                error!("Consumer task #{} failed: {}", i, e);
                all_successful = false;
            }
            Err(e) => {
                error!("Consumer task #{} panicked: {:?}", i, e);
                all_successful = false;
            }
        }
    }
    if !all_successful {
        warn!("Some consumer tasks failed or panicked");
    }

    // 所有 consumer 任务已退出，此时 stats_consumer 不再有并发更新，安全打印合并报告
    consumer_manager.end_lifecycle().await;

    all_successful
}

/// 执行全量扫描（私有核心实现）
///
/// 由 `scan` 在 `ScanType::Full` 时调用。
/// `progress_callback_url`：透传给流水线，web 层传入，CLI 传 `None`。
#[instrument(skip_all, fields(job_id = %job_id, scan_type = %scan_type))]
#[allow(clippy::too_many_arguments, clippy::ref_option)]
async fn run_full_scan(
    job_id: String, job_dir: String, scan_type: ScanType, path: &str, depth: u32, r#match: &Option<String>,
    exclude: &Option<String>, raw_command_line: String, cancel_token: Option<CancellationToken>,
    progress_callback_url: Option<String>,
) -> Result<()> {
    info!(
        "Starting full scan: job_id={}, scan_type={}, path={}, depth={}, match={}, exclude={}",
        job_id,
        scan_type,
        redact_storage_url(path),
        depth,
        r#match.clone().unwrap_or_default(),
        exclude.clone().unwrap_or_default(),
    );

    let ScanPipeline {
        app_config: _,
        consumer_config: _,
        broadcaster,
        consumer_handles,
        walkdir_iter,
        consumer_manager,
    } = setup_scan_pipeline(
        &job_id,
        &job_dir,
        JobType::Scan,
        path,
        scan_type,
        depth,
        r#match,
        exclude,
        raw_command_line,
        progress_callback_url.clone(),
    )
    .await?;

    // 全量扫描主循环：walkdir → broadcast
    let mut processed_count = 0usize;
    let mut cancelled = false;
    loop {
        let msg = if let Some(ref token) = cancel_token {
            tokio::select! {
                msg = walkdir_iter.next() => msg,
                () = token.cancelled() => {
                    info!("[Scan] Cancelled via graceful shutdown");
                    cancelled = true;
                    None
                }
            }
        } else {
            walkdir_iter.next().await
        };
        match msg {
            Some(msg) => {
                if let StorageEntryMessage::Error {
                    ref path, ref reason, ..
                } = msg
                {
                    error!("[Scan] Error during directory walk for {}: {}", path.display(), reason);
                } else {
                    processed_count += 1;
                }
                broadcaster.broadcast(msg).await;
            }
            None => break,
        }
    }
    if cancelled {
        info!(
            "[Scan] Directory walk cancelled. Processed {} entries before shutdown.",
            processed_count
        );
    } else {
        info!(
            "[Scan] Directory walk completed. Processed {} entries.",
            processed_count
        );
    }

    shutdown_pipeline(broadcaster, consumer_handles, consumer_manager).await;

    info!("Scan job {} completed successfully", job_id);

    Ok(())
}

/// 执行增量扫描（私有核心实现）
///
/// 由 `scan` 在 `ScanType::Incremental` 时调用。
/// `progress_callback_url`：透传给流水线，web 层传入，CLI 传 `None`。
#[instrument(skip_all, fields(job_id = %job_id, scan_type = %scan_type))]
#[allow(clippy::too_many_arguments, clippy::ref_option)]
async fn run_incremental_scan(
    job_id: String, job_dir: String, scan_type: ScanType, path: &str, depth: u32, r#match: &Option<String>,
    exclude: &Option<String>, raw_command_line: String, cancel_token: Option<CancellationToken>,
    progress_callback_url: Option<String>,
) -> Result<()> {
    info!(
        "Starting incremental scan: job_id={}, scan_type={}, path={}, depth={}, match={}, exclude={}",
        job_id,
        scan_type,
        redact_storage_url(path),
        depth,
        r#match.clone().unwrap_or_default(),
        exclude.clone().unwrap_or_default(),
    );

    let ScanPipeline {
        app_config,
        consumer_config,
        broadcaster,
        consumer_handles,
        walkdir_iter,
        consumer_manager,
    } = setup_scan_pipeline(
        &job_id,
        &job_dir,
        JobType::IncrementalScan,
        path,
        scan_type,
        depth,
        r#match,
        exclude,
        raw_command_line,
        progress_callback_url,
    )
    .await?;

    // 初始化数据库（用于增量扫描比对）
    let database = match DatabaseFactory::new_database(&consumer_config.db_config, &job_id).await {
        Ok(database) => {
            info!("[DatabaseConsumer] Database initialized successfully");
            database
        }
        Err(e) => {
            error!("[DatabaseConsumer] Failed to initialize database: {}", e);
            return Err(AppError::DatabaseError(e));
        }
    };

    if let Err(e) = database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await {
        return Err(AppError::DatabaseError(e));
    }

    let batch_size = consumer_config.db_config.batch_size as usize;
    debug!("Using batch size: {}", batch_size);

    // 切换数据库扫描状态
    let db_clone = database.clone();
    if let Err(e) = db_clone.switch_scan_state().await {
        error!("Failed to switch scan state: {}", e);
        return Err(AppError::DatabaseError(e));
    }

    // 获取扫描并发度
    let incremental_scan_concurrency = app_config.scan.concurrency;

    // 创建工作线程池
    info!(
        "Starting directory walker to incremental scan the path: {}",
        redact_storage_url(path)
    );
    let mut worker_handles = Vec::new();

    for worker_id in 0..incremental_scan_concurrency {
        let worker_walkdir_iter = walkdir_iter.clone();
        let database = database.clone();
        let broadcaster = broadcaster.clone();
        let cancel_token = cancel_token.clone();

        let span = info_span!("scan_worker", worker_id = worker_id);
        let handle = tokio::spawn(
            async move {
                info!("Scan worker {}: Started processing", worker_id);

                let mut current_batch: Vec<Arc<EntryEnum>> = Vec::with_capacity(batch_size);
                let mut processing_tasks = Vec::new();

                loop {
                    let msg = if let Some(ref token) = cancel_token {
                        tokio::select! {
                            msg = worker_walkdir_iter.next() => msg,
                            () = token.cancelled() => {
                                info!("Scan worker {}: Cancelled via graceful shutdown", worker_id);
                                None
                            }
                        }
                    } else {
                        worker_walkdir_iter.next().await
                    };
                    let Some(msg) = msg else { break };

                    match msg {
                        StorageEntryMessage::Scanned(ref arc_entry) => {
                            trace!("Scan worker {}: Received entry: {:?}", worker_id, arc_entry);

                            current_batch.push(arc_entry.clone());

                            // Scanned 消息直接广播
                            broadcaster.broadcast(msg).await;

                            if current_batch.len() >= batch_size {
                                let batch = std::mem::take(&mut current_batch);
                                let db = database.clone();
                                let broadcaster = broadcaster.clone();
                                let batch_span = info_span!("batch_process");
                                let task = tokio::spawn(
                                    async move {
                                        if let Err(e) =
                                            batch_processing_to_generate_message(&db, &batch, broadcaster, false, true)
                                                .await
                                        {
                                            error!("Failed to process batch: {}", e);
                                        }
                                    }
                                    .instrument(batch_span),
                                );
                                processing_tasks.push(task);
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
                            broadcaster.broadcast(msg).await;
                        }
                        other => {
                            broadcaster.broadcast(other).await;
                        }
                    }
                }

                if !current_batch.is_empty() {
                    let db = database.clone();
                    if let Err(e) =
                        batch_processing_to_generate_message(&db, &current_batch, broadcaster, true, true).await
                    {
                        error!("Scan worker {}: Failed to process final batch: {}", worker_id, e);
                    }
                }

                for task in processing_tasks {
                    let _ = task.await;
                }

                info!("Scan worker {}: Completed processing", worker_id);
            }
            .instrument(span),
        );

        worker_handles.push(handle);
    }

    // 等待所有工作线程完成
    for handle in worker_handles {
        if let Err(e) = handle.await {
            error!("Scan worker failed: {:?}", e);
        }
    }
    info!("Scan results processor completed");

    // 检测已删除条目并直接广播
    match db_clone.detect_deleted_items().await {
        Ok(deletion_iter) => {
            for status in deletion_iter {
                match status {
                    DeletionStatus::Deleted(entry) => {
                        broadcaster
                            .broadcast(StorageEntryMessage::Deleted(Arc::new(entry)))
                            .await;
                    }
                    DeletionStatus::Renamed(from, to) => {
                        broadcaster
                            .broadcast(StorageEntryMessage::Renamed((Arc::new(*from), Arc::new(*to))))
                            .await;
                    }
                }
            }
        }
        Err(e) => {
            error!("[IncrementalScan] Failed to detect deleted items: {}", e);
            return Err(AppError::DatabaseError(e));
        }
    }
    info!("Detect deleted items completed");

    shutdown_pipeline(broadcaster, consumer_handles, consumer_manager).await;

    info!("Incremental scan job {} completed successfully", job_id);

    Ok(())
}

/// 执行文件扫描操作（基础扫描接口）
///
/// 该函数是基础扫描场景的公开入口。
/// 根据 `scan_type` 自动分发到全量或增量扫描。
///
/// # 参数
/// - `job_id`: 任务ID
/// - `job_dir`: 任务目录
/// - `path`: 要扫描的路径
/// - `depth`: 扫描深度，0表示无限深度
/// - `r#match`: 匹配表达式
/// - `exclude`: 排除表达式
/// - `raw_command_line`: 原始命令行
/// - `cancel_token`: 可选的取消令牌，触发后优雅停止扫描
/// - `progress_callback_url`: 进度回调 URL（web 层传入，CLI 传 `None`）
///
/// `ScanType` 自动判定：优先查数据库中 base 表是否存在，失败时 fallback 到文件系统。
///
/// # 返回值
/// - 成功时返回 `Ok(())`
/// - 失败时返回包含错误信息的 `Err`
#[allow(clippy::too_many_arguments, clippy::ref_option)]
pub async fn scan(
    job_id: String, job_dir: String, job_dir_pre_existing: bool, path: &str, depth: u32, r#match: &Option<String>,
    exclude: &Option<String>, raw_command_line: String, cancel_token: Option<CancellationToken>,
    progress_callback_url: Option<String>,
) -> Result<()> {
    // License 二次校验
    #[cfg(feature = "license")]
    {
        if let Ok(license) = licensing::get_global_license() {
            licensing::verify::quick_verify(license)?;
        }
    }

    // 自动判定 ScanType：尝试连接数据库查询 base 表，失败 fallback 到文件系统
    let app_config = AppConfig::fetch()?;
    let db_config = db::config::DatabaseConfig::from_app_config(&app_config.database);
    let db_instance = DatabaseFactory::new_database(&db_config, &job_id).await.ok();
    let scan_type = determine_scan_type(db_instance.as_deref(), &job_id, job_dir_pre_existing).await;

    info!(
        "Starting scan: job_id={}, scan_type={}, path={}",
        job_id, scan_type, path
    );
    match scan_type {
        ScanType::Full => {
            run_full_scan(
                job_id,
                job_dir,
                scan_type,
                path,
                depth,
                r#match,
                exclude,
                raw_command_line,
                cancel_token,
                progress_callback_url,
            )
            .await
        }
        ScanType::Incremental => {
            run_incremental_scan(
                job_id,
                job_dir,
                scan_type,
                path,
                depth,
                r#match,
                exclude,
                raw_command_line,
                cancel_token,
                progress_callback_url,
            )
            .await
        }
    }
}

/// 处理增量扫描批次数据并生成消息
///
/// 该函数是增量扫描的核心辅助函数，负责：
/// 1. 处理扫描生成的批次数据
/// 2. 将批次数据插入临时记录表
/// 3. 比较新旧记录，生成变更消息
/// 4. 将变更消息发送到消息通道
/// 5. 更新数据库中的扫描记录
///
/// # 参数
/// - `database`: 数据库实例，用于存储和查询扫描记录
/// - `batch`: 扫描生成的存储条目批次
/// - `message_tx`: 消息发送器，用于发送变更消息
/// - `is_final_batch`: 是否为最后一批数据
/// - `keep_item`: 是否保留原始条目
///
/// # 返回值
/// - 成功时返回`Ok(())`
/// - 失败时返回包含错误信息的`Err`
#[allow(clippy::borrowed_box)]
pub async fn batch_processing_to_generate_message(
    database: &Box<dyn Database>, batch: &[Arc<EntryEnum>], broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) -> Result<()> {
    // 由于database是不可变引用，但我们需要可变引用，这里进行clone
    let mut db_mut = database.clone();

    // 使用现有的batch_insert_temp_record辅助函数处理批次插入
    if !batch_insert_temp_record(0, &mut db_mut, batch, is_final_batch).await {
        error!("Failed to process batch for incremental scan");
        return Err(AppError::ScanError(
            "Failed to process batch for incremental scan".to_string(),
        ));
    }

    // 使用现有的generate_storage_entry_message_events辅助函数生成事件
    generate_storage_entry_message_events(0, &mut db_mut, broadcaster, is_final_batch, keep_item).await;

    // 清理临时表
    trace!("Dropping temporary table for batch processing");
    if let Err(e) = db_mut.drop_scan_temporary_table().await {
        error!("Failed to drop temporary table: {}", e);
        // 不返回错误，因为主要工作已经完成
    }

    Ok(())
}

/// 处理批次数据的辅助函数: 创建临时表，插入当前批次的数据到临时表里
async fn batch_insert_temp_record(
    task_id: usize, database: &mut Box<dyn Database>, batch: &[Arc<EntryEnum>], is_final_batch: bool,
) -> bool {
    // 根据是否是最后一批数据，设置不同的日志消息
    let batch_type = if is_final_batch { "final batch" } else { "batch" };

    trace!("Task {} creating temporary table for {}", task_id, batch_type);
    if let Err(e) = database.create_scan_temporary_table().await {
        error!(
            "Task {} failed to create temporary table for {}: {}",
            task_id, batch_type, e
        );
        return false;
    }

    trace!(
        "Task {} batch inserting {} temporary records, batch size is {}",
        task_id,
        batch_type,
        batch.len()
    );
    if let Err(e) = database.batch_insert_temp_record(batch).await {
        error!(
            "Task {} failed to batch insert {} temporary records: {}",
            task_id, batch_type, e
        );
        // 清理临时表
        let _ = database.drop_scan_temporary_table().await;
        return false;
    }

    true
}

/// 处理查询结果的辅助函数
async fn generate_storage_entry_message_events(
    task_id: usize, database: &mut Box<dyn Database>, broadcaster: BroadcastForwarder<StorageEntryMessage>,
    is_final_batch: bool, keep_item: bool,
) {
    // 根据是否是最后一批数据，设置不同的日志消息
    let batch_type = if is_final_batch { "final batch" } else { "batch" };

    let mut excluded_paths: Vec<(String, String)> = Vec::new();

    // 查询新增文件
    trace!("Task {} querying new items for {}", task_id, batch_type);
    match database.detect_new_items().await {
        Ok(new_items) => {
            let items: Vec<EntryEnum> = new_items.collect();
            if !keep_item {
                for item in &items {
                    excluded_paths.push((
                        item.get_relative_path().to_string_lossy().into_owned(),
                        item.get_version_id().unwrap_or_default().to_string(),
                    ));
                }
            }
            for item in items {
                broadcaster.broadcast(StorageEntryMessage::New(Arc::new(item))).await;
            }
        }
        Err(e) => error!("Task {} failed to query new items: {}", task_id, e),
    }

    // 查询修改文件（区分 Data/Metadata/Both 三种变更）
    trace!("Task {} querying changed items for {}", task_id, batch_type);
    match database.detect_changed_items().await {
        Ok(changed_items) => {
            let items: Vec<(EntryEnum, data_mover::ChangeKind)> = changed_items.collect();
            if !keep_item {
                for (item, _) in &items {
                    excluded_paths.push((
                        item.get_relative_path().to_string_lossy().into_owned(),
                        item.get_version_id().unwrap_or_default().to_string(),
                    ));
                }
            }
            for (item, kind) in items {
                broadcaster
                    .broadcast(StorageEntryMessage::Changed {
                        entry: Arc::new(item),
                        kind,
                    })
                    .await;
            }
        }
        Err(e) => error!("Task {} failed to query changed items: {}", task_id, e),
    }

    // 将临时表数据插入到主表（带排除过滤）
    trace!(
        "Task {} inserting temporary table to base table for {} (excluded: {} paths)",
        task_id,
        batch_type,
        excluded_paths.len()
    );
    if let Err(e) = database.insert_temp_to_base_table(&excluded_paths).await {
        error!("Task {} failed to insert temporary table to base table: {}", task_id, e);
    }
}
