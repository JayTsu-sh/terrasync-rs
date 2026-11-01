//! 扫描分发及检查点协调。数据库批次 B 写入时，下一批 A 在有界队列中积累。
use super::{flush_batch, input::ScanInput, workers};
use crate::{
    consumer::{
        ConsumerManager, ScanStatistics,
        observed::{Delivery, ObservedForwarder},
    },
    error::{AppError, Result},
};
use data_mover::{model::ObservedEntry, traversal::TraversalOutcome};
use db::{
    clickhouse::scan_resume::{FINALIZING, ScanResumeState},
    scan_store::ScanStore,
};
use std::{sync::Arc, time::Duration};
use storage_adapter::{ResumableScan, ResumableScanItem};
#[cfg(feature = "lab-fault-injection")]
use tokio::time::sleep;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{MissedTickBehavior, interval},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::info;

pub(super) const DISPATCH_BATCH: usize = 256;
pub(super) type Entries = Arc<[Arc<ObservedEntry>]>;
pub(super) struct SaveState {
    pub(super) state: ScanResumeState,
    pub(super) ack: oneshot::Sender<()>,
}

pub(super) async fn consume(
    source: ResumableScan, database: &mut ScanStore, state: &mut ScanResumeState, statistics: &ConsumerManager,
    batch_size: usize, cancel: CancellationToken,
) -> Result<()> {
    let mut forwarder = ObservedForwarder::<Entries>::new();
    let db_rx = forwarder.subscribe("clickhouse", batch_size.div_ceil(DISPATCH_BATCH));
    let stat_rx = forwarder.subscribe("statistics", 8);
    let (control_tx, control_rx) = mpsc::channel(1);
    // 结构化并发：任一分支失败都等待消费者退出，不遗留后台写入任务。
    let writer_state = state.clone();
    let template = writer_state.incremental.then(|| database.batch_writer()).flatten();
    let prefetch_capacity = template.as_ref().map(|_| batch_size.saturating_mul(workers::WORKERS));
    let mut producer_state = state.clone();
    let producer_statistics = statistics.scan_statistics();
    let count_statistics = producer_statistics.clone();
    let producer_cancel = cancel.clone();
    // SQL 行编码不能占住遍历生产者所在任务；退出时仍等待写入与统计屏障。
    let traversal_task = AbortOnDropHandle::new(tokio::spawn(async move {
        let result = produce(
            source,
            &mut producer_state,
            &producer_statistics,
            forwarder,
            control_tx,
            producer_cancel,
            prefetch_capacity,
        )
        .await;
        (producer_state, result)
    }));
    let statistics_task = AbortOnDropHandle::new(tokio::spawn(async move {
        count(&count_statistics, stat_rx).await;
    }));
    let writer_statistics = statistics.scan_statistics();
    let writer = async {
        let result = if let Some(template) = template {
            workers::consume(
                database,
                template,
                writer_state,
                writer_statistics,
                db_rx,
                control_rx,
                batch_size,
                cancel.clone(),
            )
            .await
        } else {
            write(
                database,
                writer_state,
                &writer_statistics,
                db_rx,
                control_rx,
                batch_size,
            )
            .await
        };
        if result.is_err() {
            cancel.cancel();
        }
        result
    };
    // producer 独占可变 state；writer 只需要运行初始值，禁止跨检查点共享可变游标。
    let (traversal_result, persisted, statistics_result) = tokio::join!(traversal_task, writer, statistics_task);
    statistics_result?;
    let (producer_state, traversal_outcome) = traversal_result?;
    *state = producer_state;
    persisted?;
    traversal_outcome
}

async fn count(statistics: &ScanStatistics, mut receiver: mpsc::Receiver<Delivery<Entries>>) {
    while let Some(message) = receiver.recv().await {
        match message {
            Delivery::Data(entries) => statistics.record_observations(&entries).await,
            Delivery::Barrier(ack) => {
                let _ = ack.send(());
            }
        }
    }
}
async fn write(
    database: &mut ScanStore, state: ScanResumeState, statistics: &ScanStatistics,
    mut receiver: mpsc::Receiver<Delivery<Entries>>, mut control: mpsc::Receiver<SaveState>, batch_size: usize,
) -> Result<()> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut control_open = true;
    loop {
        select! {
            command=control.recv(),if control_open=>match command {
                Some(command)=>{database.save_resume_state(&command.state).await?;let _=command.ack.send(());}
                None=>control_open=false,
            },
            delivery=receiver.recv()=>match delivery {
                Some(Delivery::Data(entries))=>{
                    for entry in entries.iter() {
                        batch.push(Arc::clone(entry));
                        if batch.len()>=batch_size{flush_batch(database,&state,statistics,&mut batch).await?;}
                    }
                }
                Some(Delivery::Barrier(ack))=>{
                    flush_batch(database,&state,statistics,&mut batch).await?;
                    let _=ack.send(());
                }
                None=>break,
            }
        }
    }
    flush_batch(database, &state, statistics, &mut batch).await
}
async fn deliver(forwarder: &ObservedForwarder<Entries>, pending: &mut Vec<Arc<ObservedEntry>>) -> Result<()> {
    if !pending.is_empty() {
        let entries = Arc::from(std::mem::replace(pending, Vec::with_capacity(DISPATCH_BATCH)));
        forwarder.send(entries).await?;
    }
    Ok(())
}
async fn save(control: &mpsc::Sender<SaveState>, state: &ScanResumeState) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    control
        .send(SaveState {
            state: state.clone(),
            ack: tx,
        })
        .await
        .map_err(|_| AppError::ConsumerError("ClickHouse checkpoint consumer stopped".into()))?;
    rx.await
        .map_err(|_| AppError::ConsumerError("ClickHouse checkpoint was not acknowledged".into()))
}
async fn produce(
    source: ResumableScan, state: &mut ScanResumeState, statistics: &ScanStatistics,
    forwarder: ObservedForwarder<Entries>, control: mpsc::Sender<SaveState>, cancel: CancellationToken,
    prefetch_capacity: Option<usize>,
) -> Result<()> {
    let mut input = ScanInput::new(source, prefetch_capacity);
    let result = produce_items(&mut input, state, statistics, forwarder, control, cancel.clone()).await;
    if result.is_err() {
        cancel.cancel();
        input.stop().await;
    }
    result
}

async fn produce_items(
    source: &mut ScanInput, state: &mut ScanResumeState, statistics: &ScanStatistics,
    forwarder: ObservedForwarder<Entries>, control: mpsc::Sender<SaveState>, cancel: CancellationToken,
) -> Result<()> {
    let mut pending = Vec::with_capacity(DISPATCH_BATCH);
    let mut observed = state.observed_entries;
    let mut tick = interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let item = select! {
            item=source.next()=>item,
            _=tick.tick(),if !pending.is_empty()=>{deliver(&forwarder,&mut pending).await?;continue;}
        };
        let Some(item) = item else { break };
        match item {
            Ok(ResumableScanItem::Entry(entry)) => {
                observed = observed.saturating_add(entry.history().map_or(1, |h| h.records().len() as u64));
                pending.push(Arc::from(entry));
                if pending.len() >= DISPATCH_BATCH {
                    deliver(&forwarder, &mut pending).await?;
                }
            }
            Ok(ResumableScanItem::Checkpoint(checkpoint)) => {
                deliver(&forwarder, &mut pending).await?;
                forwarder.barrier().await?;
                state.statistics = statistics.statistics_checkpoint().await?;
                state.checkpoint = checkpoint;
                state.observed_entries = observed;
                state.advance()?;
                save(&control, state).await?;
                info!(
                    version = state.version,
                    entries = observed,
                    "Scan checkpoint acknowledged by all consumers"
                );
                #[cfg(feature = "lab-fault-injection")]
                if let Ok(value) = std::env::var("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS")
                    && let Ok(value) = value.parse::<u64>()
                {
                    sleep(Duration::from_millis(value)).await;
                }
                #[cfg(feature = "lab-fault-injection")]
                if std::env::var_os("TERRASYNC_LAB_SCAN_STOP_AFTER_CHECKPOINT").is_some() {
                    cancel.cancel();
                }
            }
            Err(error) => {
                cancel.cancel();
                return Err(error);
            }
        }
    }
    deliver(&forwarder, &mut pending).await?;
    forwarder.barrier().await?;
    let outcome = source.finish().await?;
    if !matches!(outcome,TraversalOutcome::Completed(ref completion) if completion.totals.failed_children==0 && completion.totals.listing_failures==0)
    {
        return Err(AppError::ScanError(
            "Scan paused or incomplete; checkpoint retained, generation not committed".into(),
        ));
    }
    state.statistics = statistics.statistics_checkpoint().await?;
    state.phase = FINALIZING;
    state.observed_entries = observed;
    state.advance()?;
    save(&control, state).await
}
