//! 扫描分发及检查点协调。数据库批次 B 写入时，下一批 A 在有界队列中积累。
use super::{flush_batch, input::ScanInput, workers};
use crate::scan::cancellation::ScanCancellation;
use crate::scan::progress::ScanProgress;
use crate::{
    consumer::{
        ConsumerManager, ScanStatistics,
        observed::{Delivery, ObservedForwarder},
    },
    error::{AppError, Result},
};
use data_mover::{
    model::ObservedEntry,
    traversal::{TraversalOrder, TraversalOutcome},
};
use db::{
    clickhouse::scan_resume::{FINALIZING, ScanResumeState},
    scan_store::ScanStore,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use storage_adapter::{RemoteScanFailure, ResumableScan, ResumableScanItem, StorageError};
#[cfg(feature = "lab-fault-injection")]
use tokio::time::sleep;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    time::{MissedTickBehavior, interval},
};
use tokio_util::task::AbortOnDropHandle;
use tracing::info;

pub(super) const DISPATCH_BATCH: usize = 256;
pub(super) type Entries = Arc<[Arc<ObservedEntry>]>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SaveAction {
    Capture,
    Restore,
    ManifestOnly,
}

pub(super) struct SaveState {
    pub(super) state: ScanResumeState,
    pub(super) action: SaveAction,
    pub(super) ack: oneshot::Sender<ScanResumeState>,
}

impl SaveState {
    pub(super) async fn execute(mut self, database: &ScanStore) -> Result<()> {
        match self.action {
            SaveAction::Restore => database.restore_scan_checkpoint(&self.state).await?,
            SaveAction::Capture => database.seal_resume_state(&mut self.state).await?,
            SaveAction::ManifestOnly => database.save_resume_state(&self.state).await?,
        }
        let _ = self.ack.send(self.state);
        Ok(())
    }
}

pub(super) async fn consume(
    source: ResumableScan, database: &mut ScanStore, state: &mut ScanResumeState, statistics: &ConsumerManager,
    batch_size: usize, cancel: ScanCancellation, resume: bool,
) -> Result<()> {
    let failure = source.failure_signal();
    let initial = statistics
        .scan_statistics()
        .snapshot()
        .await
        .map_or_else(Default::default, |scan| scan.counts);
    let progress = ScanProgress::new(
        if resume {
            TraversalOrder::Blocks
        } else {
            TraversalOrder::Unordered
        },
        initial,
        (!state.checkpoint.is_empty()).then(|| state.checkpoint.clone()),
    );
    // 独立于数据库/输出背压停止查询；只监听防抖耗尽后的最终故障，闪断不会取消扫描。
    let _failure_watch = cancel.watch_storage_failure(failure.clone());
    let mut forwarder = ObservedForwarder::<Entries>::new();
    let db_rx = forwarder.subscribe("clickhouse", batch_size.div_ceil(DISPATCH_BATCH));
    let stat_rx = forwarder.subscribe("statistics", 8);
    let (control_tx, control_rx) = mpsc::channel(1);
    // 结构化并发：任一分支失败都等待消费者退出，不遗留后台写入任务。
    let writer_state = state.clone();
    let template = writer_state.incremental.then(|| database.batch_writer()).flatten();
    // 预读跟随实际写入并行度；全量和单机增量的串行写入只分配一个 worker 的预算。
    let writer_workers = if template.is_some() { workers::WORKERS } else { 1 };
    // 增量比较和目录块确认会暂停下游，用三个有界窗口覆盖在途批次及确认等待。
    let windows = if state.incremental || resume { 3 } else { 1 };
    let prefetch_capacity = Some(batch_size.saturating_mul(writer_workers).saturating_mul(windows));
    let mut producer_state = state.clone();
    let producer_statistics = statistics.scan_statistics();
    let recovery_progress = producer_statistics.watch_recovery(source.recovery_status()).await;
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
            progress,
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
            cancel
                .operation(write(
                    database,
                    writer_state,
                    &writer_statistics,
                    db_rx,
                    control_rx,
                    batch_size,
                    &cancel,
                ))
                .await
        };
        if result.is_err() {
            cancel.cancel();
        }
        result
    };
    // producer 独占可变 state；writer 只需要运行初始值，禁止跨检查点共享可变游标。
    let (traversal_result, persisted, statistics_result) = tokio::join!(traversal_task, writer, statistics_task);
    if let Some(progress) = recovery_progress {
        progress.finish().await;
    }
    statistics_result?;
    let (producer_state, traversal_outcome) = traversal_result?;
    *state = producer_state;
    merge_outcomes(persisted, traversal_outcome, failure.failure())
}

fn merge_outcomes(persisted: Result<()>, traversal: Result<()>, failure: Option<RemoteScanFailure>) -> Result<()> {
    if matches!(
        &persisted,
        Ok(()) | Err(AppError::ObservationScanCancelled | AppError::ScanWorkerStopped)
    ) && let Some(failure) = failure
    {
        return Err(StorageError::from(failure).into());
    }
    match persisted {
        Err(AppError::ObservationScanCancelled | AppError::ScanWorkerStopped) if traversal.is_err() => traversal,
        persisted => {
            persisted?;
            traversal
        }
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use db::DatabaseError;

    #[test]
    fn source_failure_is_not_masked_by_the_resulting_worker_cancellation() {
        let result = merge_outcomes(
            Err(AppError::ObservationScanCancelled),
            Err(AppError::ScanError("source failed".into())),
            None,
        );
        assert!(matches!(result, Err(AppError::ScanError(message)) if message=="source failed"));
        let result = merge_outcomes(
            Err(AppError::DatabaseError(DatabaseError::ConfigError(
                "database failed".into(),
            ))),
            Err(AppError::ObservationScanCancelled),
            None,
        );
        assert!(matches!(result, Err(AppError::DatabaseError(_))));
    }
    #[test]
    fn final_storage_failure_is_not_relabelled_by_query_cancellation() {
        use data_mover::model::{BackendKind, Operation, StoragePath};
        use storage_adapter::RemoteScanFailureKind;
        let failure = RemoteScanFailure {
            backend: BackendKind::Cifs,
            operation: Operation::Traverse,
            path: StoragePath::root(),
            kind: RemoteScanFailureKind::ConnectionLost,
            operation_timeout: Duration::from_secs(30),
            source: None,
        };
        let result = merge_outcomes(
            Err(AppError::ObservationScanCancelled),
            Err(AppError::ConsumerError("writer stopped".into())),
            Some(failure),
        );
        assert!(
            matches!(result, Err(AppError::StorageError(StorageError::RemoteScan(failure)))
            if failure.kind == RemoteScanFailureKind::ConnectionLost)
        );
    }
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
    cancel: &ScanCancellation,
) -> Result<()> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut control_open = true;
    loop {
        select! {
            biased;
            _=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
            command=control.recv(),if control_open=>match command {
                Some(command)=>command.execute(database).await?,
                None=>control_open=false,
            },
            delivery=receiver.recv()=>match delivery {
                Some(Delivery::Data(entries))=>{
                    for entry in entries.iter() {
                        batch.push(Arc::clone(entry));
                        if batch.len()>=batch_size{flush_batch(database,&state,statistics,&mut batch,cancel).await?;}
                    }
                }
                Some(Delivery::Barrier(ack))=>{
                    flush_batch(database,&state,statistics,&mut batch,cancel).await?;
                    let _=ack.send(());
                }
                None=>break,
            }
        }
    }
    flush_batch(database, &state, statistics, &mut batch, cancel).await
}
async fn deliver(forwarder: &ObservedForwarder<Entries>, pending: &mut Vec<Arc<ObservedEntry>>) -> Result<()> {
    if !pending.is_empty() {
        let entries = Arc::from(std::mem::replace(pending, Vec::with_capacity(DISPATCH_BATCH)));
        forwarder.send(entries).await?;
    }
    Ok(())
}
async fn save(control: &mpsc::Sender<SaveState>, state: &mut ScanResumeState, action: SaveAction) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    control
        .send(SaveState {
            state: state.clone(),
            action,
            ack: tx,
        })
        .await
        .map_err(|_| AppError::ConsumerError("ClickHouse checkpoint consumer stopped".into()))?;
    *state = rx
        .await
        .map_err(|_| AppError::ConsumerError("ClickHouse checkpoint was not acknowledged".into()))?;
    Ok(())
}
async fn produce(
    source: ResumableScan, state: &mut ScanResumeState, statistics: &ScanStatistics,
    forwarder: ObservedForwarder<Entries>, control: mpsc::Sender<SaveState>, cancel: ScanCancellation,
    prefetch_capacity: Option<usize>, progress: ScanProgress,
) -> Result<()> {
    let mut input = ScanInput::new(source, prefetch_capacity);
    let result = produce_items(
        &mut input,
        state,
        statistics,
        forwarder,
        control,
        cancel.clone(),
        progress,
    )
    .await;
    if result.is_err() {
        cancel.cancel();
        input.stop().await;
    }
    result
}

async fn produce_items(
    source: &mut ScanInput, state: &mut ScanResumeState, statistics: &ScanStatistics,
    forwarder: ObservedForwarder<Entries>, control: mpsc::Sender<SaveState>, cancel: ScanCancellation,
    mut progress: ScanProgress,
) -> Result<()> {
    let mut pending = Vec::with_capacity(DISPATCH_BATCH);
    // 仅在写入屏障和快照确认后置真；后续实际观察一旦交付，就必须重新封存事实。
    let mut snapshot_matches_facts = false;
    let mut tick = interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let item = select! {
            biased;
            ()=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
            item=source.next_batch(DISPATCH_BATCH)=>item,
            _=tick.tick(),if !pending.is_empty()=>{
                cancel.interrupt(deliver(&forwarder,&mut pending)).await??;continue;
            }
        };
        let Some(batch) = item else { break };
        for item in batch.items {
            match item {
                ResumableScanItem::Resumed(event) => {
                    pending.clear();
                    cancel.interrupt(forwarder.barrier()).await??;
                    cancel.interrupt(save(&control, state, SaveAction::Restore)).await??;
                    snapshot_matches_facts = true;
                    progress.resumed(&event, statistics).await?;
                    statistics.restore_scan_event_counts(&state.statistics).await?;
                }
                ResumableScanItem::Entry(entry) => {
                    snapshot_matches_facts = false;
                    pending.push(Arc::from(entry));
                    if pending.len() >= DISPATCH_BATCH {
                        cancel.interrupt(deliver(&forwarder, &mut pending)).await??;
                    }
                }
                ResumableScanItem::Statistics(value) => cancel.interrupt(progress.push(*value, statistics)).await??,
                ResumableScanItem::Checkpoint(checkpoint) => {
                    cancel.interrupt(deliver(&forwarder, &mut pending)).await??;
                    let before = source.prefetched_batches();
                    let started = Instant::now();
                    cancel.interrupt(forwarder.barrier()).await??;
                    info!(barrier_ms=started.elapsed().as_millis(),prefetched_batches_before=?before,
                        prefetched_batches_after=?source.prefetched_batches(),"Scan checkpoint consumer barrier finished");

                    state.checkpoint = checkpoint.clone();
                    let prefix = progress.checkpoint_counts()?.clone();
                    state.observed_entries = prefix.scanned.total();
                    state.advance()?;
                    state.statistics = statistics.checkpoint_for_scan_prefix(&prefix).await?;
                    let action = if snapshot_matches_facts {
                        SaveAction::ManifestOnly
                    } else {
                        SaveAction::Capture
                    };
                    cancel.interrupt(save(&control, state, action)).await??;
                    snapshot_matches_facts = true;
                    progress.confirm(checkpoint);
                    info!(
                        version = state.version,
                        entries = state.observed_entries,
                        "Scan checkpoint acknowledged by all consumers"
                    );
                    #[cfg(feature = "lab-fault-injection")]
                    if let Ok(value) = std::env::var("TERRASYNC_LAB_SCAN_PAUSE_AFTER_CHECKPOINT_MS")
                        && let Ok(value) = value.parse::<u64>()
                    {
                        cancel.interrupt(sleep(Duration::from_millis(value))).await?;
                    }
                    #[cfg(feature = "lab-fault-injection")]
                    if std::env::var_os("TERRASYNC_LAB_SCAN_STOP_AFTER_CHECKPOINT").is_some() {
                        cancel.cancel();
                    }
                }
            }
        }
        if let Some(error) = batch.failure {
            cancel.cancel();
            return Err(error);
        }
    }
    cancel.interrupt(deliver(&forwarder, &mut pending)).await??;
    cancel.interrupt(forwarder.barrier()).await??;

    let outcome = cancel.interrupt(source.finish()).await??;
    if !matches!(outcome,TraversalOutcome::Completed(ref completion) if completion.totals.failed_children==0 && completion.totals.listing_failures==0)
    {
        return Err(AppError::ScanError(
            "Scan paused or incomplete; checkpoint retained, generation not committed".into(),
        ));
    }
    progress.finish()?;
    statistics.complete_scan().await;
    state.phase = FINALIZING;
    state.observed_entries = statistics.scanned_entries().await;
    state.advance()?;
    state.statistics = statistics.statistics_checkpoint().await?;
    // 最后一次 checkpoint 后没有新观察时，只更新 FINALIZING 统计和阶段，复用已确认事实。
    let action = if snapshot_matches_facts {
        SaveAction::ManifestOnly
    } else {
        SaveAction::Capture
    };
    cancel.interrupt(save(&control, state, action)).await??;
    #[cfg(feature = "lab-fault-injection")]
    if std::env::var_os("TERRASYNC_LAB_SCAN_FAIL_AFTER_FINALIZING").is_some() {
        return Err(AppError::ScanError(
            "Lab failure after durable FINALIZING snapshot".into(),
        ));
    }
    Ok(())
}
