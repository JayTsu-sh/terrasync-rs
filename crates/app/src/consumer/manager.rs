//! 消费者管理器模块
//!
//! 该模块负责管理和协调多个消费者实例，提供统一的消息广播机制和消费者生命周期管理。
//! 回调逻辑已移至 StatisticConsumer 内部，此处只负责编排。

// 标准库
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use storage_v2::StorageEntryMessage;
use tokio::sync::Mutex;
// 外部crate
use tracing::{Instrument, debug, error, info_span, warn};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::config::{ConsumerConfig, JobType};
use crate::consumer::DatabaseConsumer;
use crate::consumer::stats::{FullStats, IncrementalStats, ProgressBar, StatisticConsumer, StatsKind};
use crate::consumer::traits::Consumer;
use crate::error::Result;

/// 消费者管理器 - 负责创建、启动和管理多个消费者实例
pub struct ConsumerManager {
    /// 消费者列表，存储所有已注册的消费者实例
    consumers: Vec<Box<dyn Consumer>>,
    /// 统计消费者，通过 Arc<Mutex> 持有以便任务完成后读取统计结果
    stats_consumer: Option<Arc<Mutex<StatisticConsumer>>>,
}

impl ConsumerManager {
    /// 创建新的消费者管理器实例
    pub async fn new(consumer_config: &ConsumerConfig) -> Result<Self> {
        let enable_db = consumer_config.db_config.enabled;

        let mut manager = Self {
            consumers: Vec::new(),
            stats_consumer: None,
        };

        // 根据配置初始化数据库消费者
        if enable_db {
            match DatabaseConsumer::try_new(consumer_config).await {
                Ok(db_consumer) => {
                    manager.add_consumer(Box::new(db_consumer));
                    debug!("Database consumer initialized successfully");
                }
                Err(e) => {
                    error!("Failed to initialize database consumer: {}", e);
                }
            }
        }

        // 根据 JobType 初始化对应的统计消费者
        {
            let job_type = consumer_config.job_type.clone();
            let job_id = consumer_config.job_id.clone();
            let command = consumer_config.console_config.raw_command_line.clone();
            let log_path = utils::logger::get_current_app_log_path();
            let job_dir = consumer_config.job_dir.clone();
            let callback_url = consumer_config.progress_callback_url.clone();

            let sc = match job_type {
                JobType::Scan | JobType::Copy | JobType::IntegrityCheck => StatisticConsumer {
                    stats: StatsKind::Full(FullStats::new(job_type.clone(), job_id, command, log_path)),
                    progress_bar: ProgressBar::new(job_type),
                    job_dir,
                    callback_url,
                },
                JobType::IncrementalScan | JobType::IncrementalCopy => StatisticConsumer {
                    stats: StatsKind::Incremental(IncrementalStats::new(job_type.clone(), job_id, command, log_path)),
                    progress_bar: ProgressBar::new(job_type),
                    job_dir,
                    callback_url,
                },
            };
            manager.stats_consumer = Some(Arc::new(Mutex::new(sc)));
            debug!("Statistic consumer initialized successfully");
        }

        // 验证至少有一个消费者被成功初始化
        if manager.consumers.is_empty() && manager.stats_consumer.is_none() {
            warn!("No consumers were successfully initialized");
        }

        Ok(manager)
    }

    /// 向消费者管理器添加一个消费者
    pub fn add_consumer(&mut self, consumer: Box<dyn Consumer + Send + Sync>) {
        self.consumers.push(consumer);
        debug!("Added consumer, total consumers: {}", self.consumers.len());
    }

    /// 启动所有已注册的消费者
    ///
    /// 为每个消费者通过 BroadcastForwarder 订阅独立的 mpsc 通道并启动异步任务。
    /// 统计消费者通过 Arc<Mutex> 持有，任务结束时自动调用 finalize()。
    pub async fn start_consumers(
        &mut self, broadcaster: &mut BroadcastForwarder<StorageEntryMessage>,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        debug!("Starting {} consumers", self.consumers.len());
        let mut handles = Vec::new();

        for consumer in &mut self.consumers {
            let receiver = broadcaster.subscribe();
            let consumer_handle = consumer.start(receiver).await?;
            handles.push(consumer_handle);
            debug!("Consumer started successfully");
        }

        // 统计消费者：生命周期完全由 StatisticConsumer::run() 管理
        if let Some(stats_consumer) = &self.stats_consumer {
            let stats_rx = broadcaster.subscribe();
            let stats_consumer_clone = stats_consumer.clone();
            let span = info_span!("consumer", kind = "statistics");
            let stats_handle = tokio::spawn(
                async move {
                    StatisticConsumer::run(stats_consumer_clone, stats_rx).await;
                    Ok(())
                }
                .instrument(span),
            );
            handles.push(stats_handle);
            debug!("Statistic consumer started successfully");
        }

        debug!("All consumers started successfully");
        Ok(handles)
    }

    /// 获取统计消费者的 Arc 引用，用于在所有消费者完成后读取统计结果
    pub fn stats_consumer(&self) -> Option<Arc<Mutex<StatisticConsumer>>> {
        self.stats_consumer.clone()
    }

    /// 获取 per-chunk 实时字节计数器，用于在 copy_file 中每个 chunk 写入后更新带宽统计
    pub async fn get_bytes_tracker(&self) -> Option<Arc<AtomicU64>> {
        match &self.stats_consumer {
            Some(sc) => Some(sc.lock().await.get_bytes_tracker()),
            None => None,
        }
    }

    /// 获取已注册的消费者数量
    pub fn get_consumer_count(&self) -> usize {
        self.consumers.len()
    }
}
