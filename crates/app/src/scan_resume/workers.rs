//! 按 SQL 路径分配有界批次 worker；同一路径串行，检查点等待所有 worker 提交。
use super::{
    flush_batch,
    pipeline::{DISPATCH_BATCH, Entries, SaveState},
};
use crate::scan::cancellation::ScanCancellation;
use crate::{
    consumer::{ScanStatistics, observed::Delivery},
    error::{AppError, Result},
};
use data_mover::model::ObservedEntry;
use db::{clickhouse::scan_resume::ScanResumeState, scan_store::ScanStore};
use rustc_hash::FxHasher;
use std::{hash::Hasher, sync::Arc};
use storage_adapter::observed_scan::ObservedView;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tracing::info;

pub(super) const WORKERS: usize = 4;

#[allow(clippy::too_many_arguments)]
pub(super) async fn consume(
    database: &mut ScanStore, template: ScanStore, state: ScanResumeState, statistics: ScanStatistics,
    receiver: mpsc::Receiver<Delivery<Entries>>, control: mpsc::Receiver<SaveState>, batch_size: usize,
    cancel: ScanCancellation,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let mut senders = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let writer = template.batch_writer().ok_or_else(worker_stopped)?;
        let (tx, rx) = mpsc::channel(batch_size.div_ceil(DISPATCH_BATCH).max(1));
        senders.push(tx);
        let state = state.clone();
        let statistics = statistics.clone();
        let cancel = cancel.clone();
        tasks.spawn(async move {
            let result = cancel
                .operation(work(writer, state, statistics, rx, batch_size, &cancel))
                .await;
            if result.is_err() {
                cancel.cancel();
            }
            result
        });
    }
    info!(workers = WORKERS, batch_size, "Using parallel database scan workers");
    let mut result = cancel
        .operation(route(database, receiver, control, &senders, &mut tasks, &cancel))
        .await;
    if result.is_err() {
        cancel.cancel();
    }
    drop(senders);
    // 错误时也等待其余 worker 退出，禁止任务返回后继续写入。
    while let Some(completed) = tasks.join_next().await {
        let completed = completed.map_err(AppError::from).and_then(|result| result);
        if let Err(error) = completed {
            cancel.cancel();
            // 队列关闭只是 worker 失败的后果，优先返回实际数据库或处理错误。
            if result.is_ok()
                || matches!(
                    result,
                    Err(AppError::ScanWorkerStopped | AppError::ObservationScanCancelled)
                )
            {
                result = Err(error);
            }
        }
    }
    result
}

async fn route(
    database: &ScanStore, mut receiver: mpsc::Receiver<Delivery<Entries>>, mut control: mpsc::Receiver<SaveState>,
    senders: &[mpsc::Sender<Delivery<Entries>>], tasks: &mut JoinSet<Result<()>>, cancel: &ScanCancellation,
) -> Result<()> {
    let mut control_open = true;
    let mut pending: Vec<_> = (0..senders.len()).map(|_| Vec::with_capacity(DISPATCH_BATCH)).collect();
    loop {
        select! {
            biased;
            _=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
            completed=tasks.join_next(),if !tasks.is_empty()=>{
                let completed = completed.ok_or_else(worker_stopped)?;
                completed??;
                return Err(worker_stopped());
            },
            command=control.recv(),if control_open=>match command {
                Some(command)=>{
                    command.execute(database).await?;
                }
                None=>control_open=false,
            },
            delivery=receiver.recv()=>match delivery {
                Some(Delivery::Data(entries))=>{
                    dispatch(senders, entries, &mut pending).await?;
                }
                Some(Delivery::Barrier(ack))=>{
                    flush_pending(senders, &mut pending).await?;
                    barrier(senders).await?;
                    let _=ack.send(());
                }
                None=>{
                    flush_pending(senders, &mut pending).await?;
                    return Ok(());
                }
            }
        }
    }
}

async fn dispatch(
    senders: &[mpsc::Sender<Delivery<Entries>>], entries: Entries, pending: &mut [Vec<Arc<ObservedEntry>>],
) -> Result<()> {
    for entry in entries.iter() {
        // 同一路径的版本段、重放条目固定进入一个 worker，保留源端交付顺序。
        let view = ObservedView {
            entry,
            version_index: entry.history().map(|_| 0),
        };
        let mut hash = FxHasher::default();
        hash.write(view.sql_path().as_bytes());
        let worker = hash.finish() as usize % senders.len();
        pending[worker].push(Arc::clone(entry));
        if pending[worker].len() >= DISPATCH_BATCH {
            deliver(&senders[worker], &mut pending[worker]).await?;
        }
    }
    Ok(())
}

async fn deliver(sender: &mpsc::Sender<Delivery<Entries>>, pending: &mut Vec<Arc<ObservedEntry>>) -> Result<()> {
    if !pending.is_empty() {
        let entries = Arc::from(std::mem::replace(pending, Vec::with_capacity(DISPATCH_BATCH)));
        sender
            .send(Delivery::Data(entries))
            .await
            .map_err(|_| worker_stopped())?;
    }
    Ok(())
}

async fn flush_pending(
    senders: &[mpsc::Sender<Delivery<Entries>>], pending: &mut [Vec<Arc<ObservedEntry>>],
) -> Result<()> {
    for (sender, pending) in senders.iter().zip(pending.iter_mut()) {
        deliver(sender, pending).await?;
    }
    Ok(())
}

async fn barrier(senders: &[mpsc::Sender<Delivery<Entries>>]) -> Result<()> {
    let mut acknowledgements = Vec::with_capacity(senders.len());
    for sender in senders {
        let (tx, rx) = oneshot::channel();
        sender.send(Delivery::Barrier(tx)).await.map_err(|_| worker_stopped())?;
        acknowledgements.push(rx);
    }
    for ack in acknowledgements {
        ack.await.map_err(|_| worker_stopped())?;
    }
    Ok(())
}

async fn work(
    mut database: ScanStore, state: ScanResumeState, statistics: ScanStatistics,
    mut receiver: mpsc::Receiver<Delivery<Entries>>, batch_size: usize, cancel: &ScanCancellation,
) -> Result<()> {
    let mut batch = Vec::with_capacity(batch_size);
    loop {
        let delivery = select! {
            biased;
            _=cancel.cancelled()=>return Err(AppError::ObservationScanCancelled),
            delivery=receiver.recv()=>delivery,
        };
        let Some(delivery) = delivery else { break };
        match delivery {
            Delivery::Data(entries) => {
                for entry in entries.iter() {
                    batch.push(Arc::clone(entry));
                    if batch.len() >= batch_size {
                        flush_batch(&mut database, &state, &statistics, &mut batch, cancel).await?;
                    }
                }
            }
            Delivery::Barrier(ack) => {
                flush_batch(&mut database, &state, &statistics, &mut batch, cancel).await?;
                let _ = ack.send(());
            }
        }
    }
    flush_batch(&mut database, &state, &statistics, &mut batch, cancel).await
}

fn worker_stopped() -> AppError {
    AppError::ScanWorkerStopped
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        config::{ConsoleConfig, ConsumerConfig, JobType},
        consumer::ConsumerManager,
    };
    use data_mover::model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath};
    use db::{ClickHouseConfig, DatabaseConfig};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep, timeout};
    use tokio_util::sync::CancellationToken;
    use utils::app_config::ClickHouseMode;

    #[tokio::test]
    async fn cancelled_scan_stops_database_workers_without_waiting_for_queue_closure() {
        let config = ConsumerConfig {
            job_id: "cancel_fixture".into(),
            job_dir: String::new(),
            job_type: JobType::IncrementalScan,
            db_config: DatabaseConfig {
                enabled: false,
                db_type: "clickhouse".into(),
                batch_size: 1,
                clickhouse: None,
            },
            console_config: ConsoleConfig {
                raw_command_line: String::new(),
            },
            progress_callback_url: None,
        };
        let manager = ConsumerManager::new(&config).await.unwrap();
        let mut database = ScanStore::new(
            &ClickHouseConfig {
                mode: ClickHouseMode::Cluster,
                cluster: Some("clickhouse".into()),
                dsn: "http://127.0.0.1:1".into(),
                database: "cancel_fixture".into(),
                username: "default".into(),
                password: None,
                dial_timeout: 30,
                read_timeout: 120,
            },
            "cancel_fixture",
        )
        .unwrap();
        let template = database.batch_writer().unwrap();
        let state = ScanResumeState {
            id: 1,
            schema_version: 1,
            version: 1,
            request_key: "fixture".into(),
            generation: 0,
            incremental: true,
            phase: 0,
            event_time: 1,
            observed_entries: 0,
            checkpoint: vec![],
            statistics: String::new(),
            baseline_snapshot: String::new(),
            checkpoint_snapshot: String::new(),
        };
        let (_input, receiver) = mpsc::channel(1);
        let (_control, commands) = mpsc::channel(1);
        let cancel = ScanCancellation::new(CancellationToken::new());
        cancel.cancel();
        let result = timeout(
            Duration::from_millis(200),
            consume(
                &mut database,
                template,
                state,
                manager.scan_statistics(),
                receiver,
                commands,
                100_000,
                cancel,
            ),
        )
        .await;
        assert!(
            result.is_ok(),
            "cancellation must stop workers even while the input queue remains open"
        );
        assert!(result.unwrap().is_err(), "cancelled scan must not report completion");
    }

    fn entry(path: &str, identity: &[u8], size: u64) -> Arc<ObservedEntry> {
        let backend = BackendIdentity::new(BackendKind::Nfs, "worker-routing").unwrap();
        let identity = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, identity).unwrap();
        Arc::new(
            ObservedEntry::new(
                StoragePath::new(path).unwrap(),
                EntryKind::File,
                Some(size),
                None,
                identity,
            )
            .unwrap(),
        )
    }

    fn offline_store() -> ScanStore {
        ScanStore::new(
            &ClickHouseConfig {
                mode: Default::default(),
                cluster: None,
                dsn: "http://127.0.0.1:1".into(),
                database: "routing_fixture".into(),
                username: "default".into(),
                password: None,
                dial_timeout: 1,
                read_timeout: 1,
            },
            "routing_fixture",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn path_reuse_stays_on_one_worker_in_source_order() {
        let mut senders: Vec<mpsc::Sender<Delivery<Entries>>> = Vec::with_capacity(WORKERS);
        let mut receivers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let (sender, receiver) = mpsc::channel(4);
            senders.push(sender);
            receivers.push(receiver);
        }
        let (input, receiver) = mpsc::channel(2);
        input
            .send(Delivery::Data(Arc::from(vec![
                entry("same", b"before", 10),
                entry("other", b"other", 3),
            ])))
            .await
            .unwrap();
        input
            .send(Delivery::Data(Arc::from(vec![entry("same", b"after", 11)])))
            .await
            .unwrap();
        drop(input);
        let (control, commands) = mpsc::channel(1);
        drop(control);
        route(
            &offline_store(),
            receiver,
            commands,
            &senders,
            &mut JoinSet::new(),
            &ScanCancellation::new(CancellationToken::new()),
        )
        .await
        .unwrap();
        let mut observations = Vec::with_capacity(2);
        let mut total = 0;
        for (worker, receiver) in receivers.iter_mut().enumerate() {
            while let Ok(Delivery::Data(entries)) = receiver.try_recv() {
                for entry in entries.iter() {
                    total += 1;
                    if entry.path().as_str() == "same" {
                        observations.push((worker, entry.size()));
                    }
                }
            }
        }
        assert_eq!(total, 3);
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].0, observations[1].0);
        assert_eq!(
            observations.iter().map(|(_, size)| *size).collect::<Vec<_>>(),
            vec![Some(10), Some(11)]
        );
    }

    #[tokio::test]
    async fn checkpoint_flushes_partial_router_chunks_before_acknowledging() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut senders: Vec<mpsc::Sender<Delivery<Entries>>> = Vec::with_capacity(WORKERS);
        let mut tasks = JoinSet::new();
        for _ in 0..WORKERS {
            let (sender, mut receiver) = mpsc::channel(4);
            senders.push(sender);
            let count = count.clone();
            tasks.spawn(async move {
                while let Some(delivery) = receiver.recv().await {
                    match delivery {
                        Delivery::Data(entries) => {
                            count.fetch_add(entries.len(), Ordering::SeqCst);
                        }
                        Delivery::Barrier(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
                Ok(())
            });
        }
        let (input, receiver) = mpsc::channel(2);
        input
            .send(Delivery::Data(Arc::from(vec![entry("partial", b"partial", 1)])))
            .await
            .unwrap();
        let (ack, confirmed) = oneshot::channel();
        input.send(Delivery::Barrier(ack)).await.unwrap();
        drop(input);
        let (control, commands) = mpsc::channel(1);
        drop(control);
        let database = offline_store();
        let cancel = ScanCancellation::new(CancellationToken::new());
        let verify = async {
            confirmed.await.unwrap();
            assert_eq!(count.load(Ordering::SeqCst), 1);
        };
        let (result, ()) = tokio::join!(
            route(&database, receiver, commands, &senders, &mut tasks, &cancel),
            verify
        );
        result.unwrap();
        drop(senders);
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn checkpoint_waits_for_the_slowest_worker() {
        let (a, mut first) = mpsc::channel(1);
        let (b, mut second) = mpsc::channel(1);
        let done = Arc::new(AtomicBool::new(false));
        let checkpoint = async {
            barrier(&[a, b]).await.unwrap();
            done.store(true, Ordering::SeqCst);
        };
        let consumers = async {
            let Some(Delivery::Barrier(first)) = first.recv().await else {
                panic!("missing first barrier")
            };
            let Some(Delivery::Barrier(second)) = second.recv().await else {
                panic!("missing second barrier")
            };
            first.send(()).unwrap();
            sleep(Duration::from_millis(20)).await;
            assert!(!done.load(Ordering::SeqCst));
            second.send(()).unwrap();
        };
        tokio::join!(checkpoint, consumers);
        assert!(done.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_missing_worker_cannot_acknowledge_a_checkpoint() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(barrier(&[sender]).await.is_err());
    }
}
