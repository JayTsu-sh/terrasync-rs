//! 消费者管理器模块
//!
//! 该模块负责管理和协调多个消费者实例，提供统一的消息广播机制和消费者生命周期管理。
//!
//! 生命周期拆分：
//! - [`ConsumerManager::begin_lifecycle`] / [`ConsumerManager::end_lifecycle`] 包裹整个 pipeline，
//!   启停进度条、HTTP 回调循环，并在最后打印合并统计报告。
//! - [`ConsumerManager::start_consumers`] 可多次调用：增量拷贝两阶段流水线（Phase A/B）共用同一个
//!   `StatisticConsumer` 实例，但每阶段各起一轮 `DatabaseConsumer::start` + `StatisticConsumer::process`。
//! - 单阶段调用方（scan / sync / integrity_check）仍然只调一次 `start_consumers`。

// 标准库
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// 外部crate
use data_mover::StorageEntryMessage;
use tokio::sync::Mutex;
use tracing::{Instrument, debug, error, info_span, warn};

// 内部模块
use crate::broadcast::BroadcastForwarder;
use crate::config::{ConsumerConfig, JobType};
use crate::consumer::DatabaseConsumer;
use crate::consumer::stats::{
    FullStats, IncrementalStats, ProgressBar, StatisticConsumer, StatsKind, consumer::CallbackGuard,
};
use crate::consumer::traits::Consumer;
use crate::error::Result;

/// 消费者管理器 - 负责创建、启动和管理多个消费者实例
pub struct ConsumerManager {
    /// 消费者列表，存储所有已注册的消费者实例
    consumers: Vec<Box<dyn Consumer>>,
    /// 统计消费者，通过 `Arc<Mutex>` 持有以便任务完成后读取统计结果
    stats_consumer: Option<Arc<Mutex<StatisticConsumer>>>,
    /// `begin_lifecycle()` 启动的周期 HTTP 回调守卫，在 `end_lifecycle()` 中 abort + 复用 client
    callback_guard: Option<CallbackGuard>,
}

impl ConsumerManager {
    /// 创建新的消费者管理器实例
    pub async fn new(consumer_config: &ConsumerConfig) -> Result<Self> {
        let enable_db = consumer_config.db_config.enabled;

        let mut manager = Self {
            consumers: Vec::new(),
            stats_consumer: None,
            callback_guard: None,
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
                    pb_handle: None,
                },
                JobType::IncrementalScan | JobType::IncrementalCopy => StatisticConsumer {
                    stats: StatsKind::Incremental(IncrementalStats::new(job_type.clone(), job_id, command, log_path)),
                    progress_bar: ProgressBar::new(job_type),
                    job_dir,
                    callback_url,
                    pb_handle: None,
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

    /// 启动一次性生命周期：进度条 + HTTP 回调循环。必须与 [`end_lifecycle`](Self::end_lifecycle) 成对调用。
    ///
    /// 多阶段 pipeline（如增量拷贝的 Phase A/B）只调用一次 begin + 一次 end；中间可多次
    /// [`start_consumers`](Self::start_consumers) 为每一阶段建立独立的 consumer 任务。
    pub async fn begin_lifecycle(&mut self) {
        if let Some(ref sc) = self.stats_consumer {
            self.callback_guard = StatisticConsumer::begin(sc.clone()).await;
        }
    }

    /// 启动所有已注册的消费者
    ///
    /// 为每个消费者通过 [`BroadcastForwarder`] 订阅独立的 mpsc 通道并启动异步任务。
    /// 可多次调用：每次给所有 `consumers` 重新 `start()`、给 `StatisticConsumer` 订阅新 rx + 启动
    /// [`StatisticConsumer::process`] 任务，共用同一个 `Arc<Mutex<StatisticConsumer>>` 累计统计。
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

        // 统计消费者：只负责消费消息累积统计，生命周期由 begin_lifecycle/end_lifecycle 管理
        if let Some(stats_consumer) = &self.stats_consumer {
            let stats_rx = broadcaster.subscribe();
            let stats_consumer_clone = stats_consumer.clone();
            let span = info_span!("consumer", kind = "statistics");
            let stats_handle = tokio::spawn(
                async move {
                    StatisticConsumer::process(stats_consumer_clone, stats_rx).await;
                    Ok(())
                }
                .instrument(span),
            );
            handles.push(stats_handle);
            debug!("Statistic consumer process task started successfully");
        }

        debug!("All consumers started successfully");
        Ok(handles)
    }

    /// 获取 per-chunk 实时字节计数器，用于在 `copy_file` 中每个 chunk 写入后更新带宽统计
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

    /// 结束一次性生命周期：abort HTTP 回调 → 发送 final 回调 → finalize（打印合并统计 + join 显示线程）。
    ///
    /// 必须在所有 `start_consumers` 产生的任务全部 await 完成之后调用（此时不会再有并发
    /// `update_statistics` 触发锁竞争）。无 `stats_consumer` 时为 no-op。
    pub async fn end_lifecycle(&mut self) {
        if let Some(sc) = self.stats_consumer.clone() {
            let callback = self.callback_guard.take();
            StatisticConsumer::end(sc, callback).await;
        }
    }
}

/// 兜底回收 callback 回调任务：若 `end_lifecycle` 因 panic/early-return 未被调用，
/// 这里必须 abort 掉周期性 HTTP 回调任务，避免 tokio task 泄漏（`JoinHandle` drop 不会 abort）。
impl Drop for ConsumerManager {
    fn drop(&mut self) {
        if let Some((handle, _client)) = self.callback_guard.take() {
            handle.abort();
            warn!("ConsumerManager dropped without end_lifecycle(); aborted callback loop task");
        }
    }
}
