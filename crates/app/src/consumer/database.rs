// 标准库
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

// 外部crate
use db::error::DatabaseError;
use db::factory::DatabaseFactory;
use db::traits::{Database, TarManifestRecord};
use storage_v2::{EntryEnum, StorageEntryMessage};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

// 内部模块
use crate::config::{ConsumerConfig, JobType};
use crate::consumer::traits::Consumer;
use crate::error::Result;

/// 默认重试次数
const MAX_RETRY_ATTEMPTS: u32 = 3;
/// 初始退避间隔（毫秒）
const INITIAL_BACKOFF_MS: u64 = 100;

/// 带重试和指数退避的数据库批量操作辅助函数
///
/// 在失败时按指数退避策略重试（100ms → 200ms → 400ms），重试耗尽后返回最后一次的错误。
async fn retry_batch_operation<F, Fut>(operation_name: &str, f: F) -> db::error::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = db::error::Result<()>>,
{
    let mut last_err = None;
    for attempt in 0..MAX_RETRY_ATTEMPTS {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let backoff = Duration::from_millis(INITIAL_BACKOFF_MS * 2u64.pow(attempt));
                warn!(
                    "[DatabaseConsumer] {} failed (attempt {}/{}): {}, retrying in {:?}",
                    operation_name,
                    attempt + 1,
                    MAX_RETRY_ATTEMPTS,
                    e,
                    backoff
                );
                last_err = Some(e);
                tokio::time::sleep(backoff).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| DatabaseError::QueryError("retry exhausted".to_string())))
}

/// 数据库消费者 - 将扫描结果异步存储到数据库的组件
///
/// 该组件负责接收扫描结果，将其转换为数据库记录，并批量插入到数据库中
#[derive(Clone)]
pub struct DatabaseConsumer {
    /// 数据库实例引用
    db: Box<dyn Database>,
    /// 批量插入的大小阈值
    batch_size: u32,
    job_type: JobType,
}

// 为StorageEntryMessage实现Consumer trait
#[async_trait::async_trait]
impl Consumer for DatabaseConsumer {
    /// 启动数据库消费者任务
    ///
    /// 该方法会创建一个异步任务，持续从通道接收者中获取消息，并将其处理后存储到数据库。
    /// 当所有发送端被丢弃时，`recv()` 返回 None，任务刷新剩余批次后退出。
    ///
    /// # 参数
    /// * `receiver` - mpsc 通道接收者，用于接收存储条目消息
    ///
    /// # 返回值
    /// 返回一个异步任务的句柄，该任务负责处理和存储扫描结果
    async fn start(
        &mut self, mut receiver: mpsc::Receiver<StorageEntryMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        // 获取数据库引用和批量大小的克隆
        let db = self.db.clone();
        let batch_size = self.batch_size;
        let job_type = self.job_type.clone();
        debug!("[DatabaseConsumer] Starting consumer with batch size: {}", batch_size);

        let span = info_span!("consumer", kind = "database");
        let handle = tokio::spawn(
            async move {
                // 预分配容量以优化性能
                let mut current_message_batch: Vec<StorageEntryMessage> = Vec::with_capacity(batch_size as usize);
                let mut current_insert_entry_batch: Vec<Arc<EntryEnum>> = Vec::with_capacity(batch_size as usize);
                let mut current_delete_entry_batch: Vec<String> = Vec::with_capacity(batch_size as usize);
                let mut processed_count = 0;
                let mut processed_errors: usize = 0;

                while let Some(message) = receiver.recv().await {
                    match message {
                        StorageEntryMessage::Scanned(entry) => {
                            trace!(path = %entry.get_relative_path().display(), "db: scanned");
                            // 全量扫描场景：将扫描到的条目插入基础表
                            // 其他场景（如增量扫描）Scanned 仅用于统计，无需入库
                            if job_type == JobType::Scan {
                                DatabaseConsumer::batch_insert_base_record(
                                    db.as_ref(),
                                    &entry,
                                    &mut current_insert_entry_batch,
                                    batch_size,
                                    &mut processed_errors,
                                )
                                .await;
                            }
                        }
                        StorageEntryMessage::New(entry) => {
                            trace!(path = %entry.get_relative_path().display(), "db: new entry");
                            let db_clone = Box::clone(&db);

                            // 全量拷贝(Copy)和增量拷贝(IncrementalCopy)都需要插入基础表；
                            // 增量扫描(IncrementalScan)无需插入基础表（临时表导入主表时已处理）。
                            if job_type == JobType::Copy || job_type == JobType::IncrementalCopy {
                                DatabaseConsumer::batch_insert_base_record(
                                    db.as_ref(),
                                    &entry,
                                    &mut current_insert_entry_batch,
                                    batch_size,
                                    &mut processed_errors,
                                )
                                .await;
                            }

                            // 全量拷贝(Copy)只需入基础表，不需要增量记录。
                            if job_type != JobType::Copy {
                                DatabaseConsumer::batch_insert_incremental_record(
                                    db_clone.as_ref(),
                                    &StorageEntryMessage::New(entry),
                                    &mut current_message_batch,
                                    batch_size,
                                )
                                .await;
                            }
                        }
                        StorageEntryMessage::Changed(entry) => {
                            trace!(path = %entry.get_relative_path().display(), "db: changed entry");

                            // 只有在增量拷贝场景才需要更新数据库里的记录
                            // 在增量扫描场景是不需要更新记录的,是因为在临时表导入主表时就已经更新了。
                            if job_type == JobType::IncrementalCopy {
                                DatabaseConsumer::update_base_record(db.as_ref(), &entry).await;
                            }

                            DatabaseConsumer::batch_insert_incremental_record(
                                db.as_ref(),
                                &StorageEntryMessage::Changed(entry),
                                &mut current_message_batch,
                                batch_size,
                            )
                            .await;
                        }
                        StorageEntryMessage::Renamed((from_entry, to_entry)) => {
                            trace!("[DatabaseConsumer] Renaming entry {:?} to {:?}", from_entry, to_entry);
                            // 在增量拷贝和增量扫描场景均需要更新数据库里的记录
                            DatabaseConsumer::batch_insert_base_record(
                                db.as_ref(),
                                &to_entry,
                                &mut current_insert_entry_batch,
                                batch_size,
                                &mut processed_errors,
                            )
                            .await;

                            DatabaseConsumer::batch_delete_base_record(
                                db.as_ref(),
                                &from_entry,
                                &mut current_delete_entry_batch,
                                batch_size,
                            )
                            .await;

                            DatabaseConsumer::batch_insert_incremental_record(
                                db.as_ref(),
                                &StorageEntryMessage::Renamed((from_entry, to_entry)),
                                &mut current_message_batch,
                                batch_size,
                            )
                            .await;
                        }
                        StorageEntryMessage::Deleted(entry) => {
                            trace!(path = %entry.get_relative_path().display(), "db: deleted entry");
                            // 在增量拷贝和增量扫描场景均需要删除数据库里的记录
                            DatabaseConsumer::batch_delete_base_record(
                                db.as_ref(),
                                &entry,
                                &mut current_delete_entry_batch,
                                batch_size,
                            )
                            .await;

                            DatabaseConsumer::batch_insert_incremental_record(
                                db.as_ref(),
                                &StorageEntryMessage::Deleted(entry),
                                &mut current_message_batch,
                                batch_size,
                            )
                            .await;
                        }
                        StorageEntryMessage::IntegrityChecked(_) | StorageEntryMessage::Packaged(_) => {
                            // 完整性检查/Packaged 操作，不做数据库操作
                        }
                        StorageEntryMessage::TarManifest {
                            ref tar_path,
                            ref entries,
                        } => {
                            trace!("[DatabaseConsumer] Received TarManifest for: {}", tar_path);
                            let manifest_records: Vec<TarManifestRecord> = entries
                                .iter()
                                .map(|e| TarManifestRecord::from_entry(e, tar_path))
                                .collect();
                            if let Err(e) = retry_batch_operation("insert_tar_manifest", || {
                                db.batch_insert_tar_manifest(&manifest_records)
                            })
                            .await
                            {
                                error!(
                                    "[DatabaseConsumer] Failed to insert tar manifest for {} after {} retries: {}",
                                    tar_path, MAX_RETRY_ATTEMPTS, e
                                );
                            }
                        }
                        StorageEntryMessage::Error {
                            ref path, ref reason, ..
                        } => {
                            trace!(
                                path = %path.display(),
                                reason = %reason,
                                "db: error entry, skipping"
                            );
                        }
                    }

                    processed_count += 1;
                }

                // 通道已关闭，刷新所有剩余批次
                info!("[DatabaseConsumer] Channel closed, flushing remaining batches...");
                if !current_insert_entry_batch.is_empty() {
                    debug!(
                        "[DatabaseConsumer] Flushing {} remaining base insertion records",
                        current_insert_entry_batch.len()
                    );
                    DatabaseConsumer::finalize_insert_base_record(db.as_ref(), &mut current_insert_entry_batch).await;
                }
                if !current_delete_entry_batch.is_empty() {
                    debug!(
                        "[DatabaseConsumer] Flushing {} remaining base deletion records",
                        current_delete_entry_batch.len()
                    );
                    DatabaseConsumer::finalize_delete_base_record(db.as_ref(), &mut current_delete_entry_batch).await;
                }
                if !current_message_batch.is_empty() {
                    debug!(
                        "[DatabaseConsumer] Flushing {} remaining incremental records",
                        current_message_batch.len()
                    );
                    DatabaseConsumer::finalize_insert_incremental_record(db.as_ref(), &mut current_message_batch).await;
                }

                info!(
                    "[DatabaseConsumer] Consumer task completed, total processed: {}, processed error: {}",
                    processed_count, processed_errors
                );
                Ok(())
            }
            .instrument(span),
        );

        info!("[DatabaseConsumer] Consumer task started successfully");
        Ok(handle)
    }

    /// 返回消费者名称
    fn name(&self) -> &'static str {
        "database_consumer"
    }
}

impl DatabaseConsumer {
    /// 创建数据库消费者实例
    ///
    /// 根据提供的配置初始化数据库连接并创建消费者实例
    ///
    /// # 参数
    /// * `config` - 消费者配置，包含数据库连接信息和批量大小
    ///
    /// # 返回值
    /// 返回初始化后的数据库消费者实例
    pub async fn try_new(config: &ConsumerConfig) -> Result<Self> {
        // 从配置中提取job_id（作为引用使用）
        let job_id = &config.job_id;

        info!("[DatabaseConsumer] Initializing database for job: {}", job_id);
        debug!(
            "[DatabaseConsumer] Database config: type={}, batch_size={}, enabled={}",
            config.db_config.db_type, config.db_config.batch_size, config.db_config.enabled
        );

        let database = match DatabaseFactory::new_database(&config.db_config, job_id).await {
            Ok(database) => {
                info!("[DatabaseConsumer] Create database successfully");
                database
            }
            Err(e) => {
                error!("[DatabaseConsumer] Failed to create database: {}", e);
                return Err(crate::error::AppError::DatabaseError(e));
            }
        };

        // 全量扫描和全量拷贝需要初始化基础表
        if config.job_type == JobType::Scan || config.job_type == JobType::Copy {
            match database.initialize().await {
                Ok(()) => {
                    info!("[DatabaseConsumer] Database initialized successfully");
                }
                Err(e) => {
                    error!("[DatabaseConsumer] Failed to initialize database: {}", e);
                    return Err(crate::error::AppError::DatabaseError(e));
                }
            }
        }

        // tar manifest 表仅在 Copy/IncrementalCopy 任务时创建（sync 过程中写入数据）
        if (config.job_type == JobType::Copy || config.job_type == JobType::IncrementalCopy)
            && let Err(e) = database.create_tar_manifest_table().await
        {
            error!("[DatabaseConsumer] Failed to create tar manifest table: {}", e);
        }

        Ok(Self {
            db: database,
            batch_size: config.db_config.batch_size,
            job_type: config.job_type.clone(),
        })
    }

    /// 获取数据库实例的引用
    ///
    /// 允许外部代码访问内部使用的数据库实例
    pub fn get_database(&self) -> Box<dyn Database> {
        Box::clone(&self.db)
    }

    /// 处理单个扫描结果
    ///
    /// 将存储条目转换为数据库记录，并在达到批量大小时插入数据库
    ///
    /// # 参数
    /// * `entry` - 存储条目，包含文件路径、大小、时间戳等信息
    /// * `database` - 数据库实例引用
    /// * `current_batch` - 当前批次的记录，新记录会被添加到这里
    /// * `batch_size` - 批量插入的大小阈值
    pub async fn batch_insert_base_record(
        database: &dyn Database, entry: &Arc<EntryEnum>, current_batch: &mut Vec<Arc<EntryEnum>>, batch_size: u32,
        processed_errors: &mut usize,
    ) {
        current_batch.push(entry.clone());

        // 达到批量大小则异步插入数据库并切换缓冲
        if current_batch.len() >= batch_size as usize {
            info!(
                "[DatabaseConsumer] Reached batch size threshold, inserting {} records",
                current_batch.len()
            );

            let batch_len = current_batch.len();
            if let Err(e) =
                retry_batch_operation("insert_base", || database.batch_insert_base_record(current_batch)).await
            {
                *processed_errors += batch_len;
                error!(
                    "[DatabaseConsumer] Failed to insert batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS, batch_len, e
                );
            } else {
                info!("[DatabaseConsumer] Batch inserted successfully");
            }

            current_batch.clear();
        }
    }

    /// 处理完成扫描后的剩余记录
    ///
    /// 当扫描完成时调用此方法，确保所有剩余的记录都被插入到数据库中
    ///
    /// # 参数
    /// * `database` - 数据库实例引用
    /// * `current_batch` - 当前批次的记录，处理后会被清空
    pub async fn finalize_insert_base_record(database: &dyn Database, current_batch: &mut Vec<Arc<EntryEnum>>) {
        if current_batch.is_empty() {
            info!("[DatabaseConsumer] No remaining records to flush");
        } else {
            info!(
                "[DatabaseConsumer] Inserting final batch of {} records",
                current_batch.len()
            );

            if let Err(e) = retry_batch_operation("finalize_insert_base", || {
                database.batch_insert_base_record(current_batch)
            })
            .await
            {
                error!(
                    "[DatabaseConsumer] Failed to insert final batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS,
                    current_batch.len(),
                    e
                );
            } else {
                info!("[DatabaseConsumer] Final batch inserted successfully");
            }
            current_batch.clear();
        }
    }

    /// 处理完成扫描后的剩余记录
    ///
    /// 当扫描完成时调用此方法，确保所有剩余的记录都被插入到数据库中
    ///
    /// # 参数
    /// * `database` - 数据库实例引用
    /// * `current_batch` - 当前批次的记录，处理后会被清空
    pub async fn finalize_delete_base_record(database: &dyn Database, current_batch: &mut Vec<String>) {
        if current_batch.is_empty() {
            info!("[DatabaseConsumer] No remaining records to flush");
        } else {
            info!(
                "[DatabaseConsumer] Deleting final batch of {} records.\n {:?}",
                current_batch.len(),
                current_batch
            );

            if let Err(e) = retry_batch_operation("finalize_delete_base", || {
                database.batch_delete_base_record(current_batch)
            })
            .await
            {
                error!(
                    "[DatabaseConsumer] Failed to delete final batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS,
                    current_batch.len(),
                    e
                );
            } else {
                info!("[DatabaseConsumer] Final batch deleted successfully");
            }
            current_batch.clear();
        }
    }

    pub async fn batch_delete_base_record(
        database: &dyn Database, entry: &Arc<EntryEnum>, current_batch: &mut Vec<String>, batch_size: u32,
    ) {
        current_batch.push(entry.get_relative_path().to_string_lossy().to_string());

        // 达到批量大小则异步删除数据库记录并切换缓冲
        if current_batch.len() >= batch_size as usize {
            let batch_len = current_batch.len();
            info!(
                "[DatabaseConsumer] Reached batch size threshold, delete {} base records",
                batch_len
            );

            if let Err(e) =
                retry_batch_operation("delete_base", || database.batch_delete_base_record(current_batch)).await
            {
                error!(
                    "[DatabaseConsumer] Failed to delete base batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS, batch_len, e
                );
            } else {
                info!("[DatabaseConsumer] Batch of base records deleted successfully");
            }

            current_batch.clear();
        }
    }

    /// 处理单个扫描结果
    ///
    /// 将存储条目转换为数据库记录，并在达到批量大小时插入数据库
    ///
    /// # 参数
    /// * `entry` - 存储条目，包含文件路径、大小、时间戳等信息
    /// * `database` - 数据库实例引用
    /// * `current_batch` - 当前批次的记录，新记录会被添加到这里
    /// * `batch_size` - 批量插入的大小阈值
    pub async fn batch_insert_incremental_record(
        database: &dyn Database, message: &StorageEntryMessage, current_batch: &mut Vec<StorageEntryMessage>,
        batch_size: u32,
    ) {
        current_batch.push(message.clone());

        // 达到批量大小则异步插入数据库并切换缓冲
        if current_batch.len() >= batch_size as usize {
            let batch_len = current_batch.len();
            info!(
                "[DatabaseConsumer] Reached batch size threshold, inserting {} incremental records",
                batch_len
            );

            if let Err(e) = retry_batch_operation("insert_incremental", || {
                database.batch_insert_incremental_record(current_batch)
            })
            .await
            {
                error!(
                    "[DatabaseConsumer] Failed to insert incremental batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS, batch_len, e
                );
            } else {
                info!("[DatabaseConsumer] Batch of incremental records inserted successfully");
            }

            current_batch.clear();
        }
    }

    /// 处理完成扫描后的剩余记录
    ///
    /// 当扫描完成时调用此方法，确保所有剩余的记录都被插入到数据库中
    ///
    /// # 参数
    /// * `database` - 数据库实例引用
    /// * `current_batch` - 当前批次的记录，处理后会被清空
    pub async fn finalize_insert_incremental_record(
        database: &dyn Database, current_batch: &mut Vec<StorageEntryMessage>,
    ) {
        if current_batch.is_empty() {
            info!("[DatabaseConsumer] No remaining incremental records to flush");
        } else {
            info!(
                "[DatabaseConsumer] Inserting final batch of {} incremental records",
                current_batch.len()
            );

            if let Err(e) = retry_batch_operation("finalize_insert_incremental", || {
                database.batch_insert_incremental_record(current_batch)
            })
            .await
            {
                error!(
                    "[DatabaseConsumer] Failed to insert final incremental batch after {} retries, discarding {} records: {}",
                    MAX_RETRY_ATTEMPTS,
                    current_batch.len(),
                    e
                );
            } else {
                info!("[DatabaseConsumer] Final batch of incremental records inserted successfully");
            }
            current_batch.clear();
        }
    }

    pub async fn update_base_record(database: &dyn Database, entry: &Arc<EntryEnum>) {
        if let Err(e) = database.update_base_record(entry).await {
            error!("[DatabaseConsumer] Failed to update base record: {}", e);
        } else {
            debug!("[DatabaseConsumer] Base record updated successfully");
        }
    }
}
