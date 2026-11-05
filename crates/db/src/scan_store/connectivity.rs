//! 一次扫描内共享的连通性恢复窗口；探测入口可达不等于失败操作已恢复。
#[cfg(test)]
mod tests;
use crate::{
    ClickHouseConfig, DatabaseError, Result,
    http_transport::{ScanHttpCancellation, build_client},
};
use clickhouse::{Client, query::Query};
use rustc_hash::FxHashSet;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    select,
    sync::{Mutex, Notify, watch},
    time::{Instant, sleep, sleep_until, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{debug, warn};

const RECOVERY_WINDOW: Duration = Duration::from_secs(3);
const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(50);
tokio::task_local! { static REQUEST_SCOPE: (ScanDatabaseExecution, u64); }

#[derive(Clone, Copy)]
pub(crate) enum RequestSafety {
    ReadOnly,
    IdempotentWrite,
    IsolatedRestart,
    UnconfirmedWrite,
}

#[derive(Default)]
struct Recovery {
    started: Option<Instant>,
    failed: FxHashSet<u64>,
    cause: Option<Arc<DatabaseError>>,
}

struct Inner {
    origin: Instant,
    last_ack_millis: AtomicU64,
    next_operation: AtomicU64,
    monitoring: AtomicBool,
    recovering: AtomicBool,
    expired: AtomicBool,
    recovery: Mutex<Recovery>,
    changed: Notify,
    terminal: watch::Sender<Option<Arc<DatabaseError>>>,
    stopping: CancellationToken,
    transport: ScanHttpCancellation,
    probe: Client,
    probe_cluster: Option<String>,
}

#[derive(Clone)]
pub struct ScanDatabaseExecution {
    inner: Arc<Inner>,
}

pub struct ConnectivityWatch {
    stop: CancellationToken,
    task: AbortOnDropHandle<()>,
}

struct MonitorExit {
    execution: ScanDatabaseExecution,
    done: CancellationToken,
}

impl Drop for MonitorExit {
    fn drop(&mut self) {
        if !self.done.is_cancelled()
            && !self.execution.inner.stopping.is_cancelled()
            && self.execution.failure().is_none()
        {
            self.execution
                .inner
                .terminal
                .send_replace(Some(Arc::new(DatabaseError::ConnectivityMonitorStopped)));
        }
    }
}

impl ConnectivityWatch {
    pub async fn finish(self) -> Result<()> {
        self.stop.cancel();
        self.task.await?;
        Ok(())
    }
}

impl ScanDatabaseExecution {
    pub(crate) fn new(config: &ClickHouseConfig) -> Result<Self> {
        let mut probe = config.clone();
        probe.dial_timeout = probe.dial_timeout.min(1);
        probe.read_timeout = probe.read_timeout.min(1);
        let (terminal, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(Inner {
                origin: Instant::now(),
                last_ack_millis: AtomicU64::new(0),
                next_operation: AtomicU64::new(1),
                monitoring: AtomicBool::new(false),
                recovering: AtomicBool::new(false),
                expired: AtomicBool::new(false),
                recovery: Mutex::new(Recovery::default()),
                changed: Notify::new(),
                terminal,
                stopping: CancellationToken::new(),
                transport: ScanHttpCancellation::default(),
                probe: build_client(&probe, None)?.with_validation(false),
                probe_cluster: config.cluster_name()?.map(str::to_owned),
            }),
        })
    }

    pub(crate) fn transport(&self) -> ScanHttpCancellation {
        self.inner.transport.clone()
    }
    pub fn begin_stop(&self) {
        self.inner.stopping.cancel();
        self.inner.transport.begin_stop();
        self.inner.changed.notify_waiters();
    }
    pub fn same_execution(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub fn failure(&self) -> Option<Arc<DatabaseError>> {
        self.inner.terminal.borrow().clone()
    }
    pub fn recovery_window(&self) -> Duration {
        if self.inner.expired.load(Ordering::Acquire) {
            RECOVERY_WINDOW
        } else {
            Duration::ZERO
        }
    }

    pub async fn failed(&self) -> Option<Arc<DatabaseError>> {
        let mut receiver = self.inner.terminal.subscribe();
        loop {
            if let Some(error) = receiver.borrow().clone() {
                return Some(error);
            }
            select! {
                () = self.inner.stopping.cancelled() => return self.failure(),
                changed = receiver.changed() => { if changed.is_err() { return None; } }
            }
        }
    }

    pub fn start_monitor(&self) -> Result<ConnectivityWatch> {
        if self.inner.monitoring.swap(true, Ordering::AcqRel) {
            return Err(DatabaseError::ConfigError(
                "ClickHouse execution already has a connectivity monitor".into(),
            ));
        }
        let stop = CancellationToken::new();
        let done = stop.clone();
        let execution = self.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let _exit = MonitorExit {
                execution: execution.clone(),
                done: done.clone(),
            };
            execution.monitor(done).await;
        }));
        Ok(ConnectivityWatch { stop, task })
    }

    fn acknowledged(&self) {
        self.inner.last_ack_millis.store(
            u64::try_from(self.inner.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    async fn recovered(&self, operation: u64) {
        self.acknowledged();
        if !self.inner.recovering.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.inner.recovery.lock().await;
        state.failed.remove(&operation);
        debug!(operation, unresolved_operations = ?state.failed, "ClickHouse request acknowledged during recovery");
        if state.failed.is_empty() {
            *state = Recovery::default();
            self.inner.recovering.store(false, Ordering::Release);
            self.inner.changed.notify_waiters();
        }
    }

    async fn failed_operation(&self, operation: u64, error: Arc<DatabaseError>) {
        let mut state = self.inner.recovery.lock().await;
        if state.started.is_none() {
            let (error_class, server_code) = error.diagnostic_class();
            warn!(
                operation,
                error_class, server_code, "ClickHouse connection recovery started"
            );
        }
        state.started.get_or_insert_with(Instant::now);
        state.failed.insert(operation);
        state.cause.get_or_insert(error);
        self.inner.recovering.store(true, Ordering::Release);
        self.inner.changed.notify_waiters();
    }

    async fn expire_if_due(&self) -> Option<Instant> {
        let state = self.inner.recovery.lock().await;
        let deadline = state.started.map(|start| start + RECOVERY_WINDOW);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline)
            && self.failure().is_none()
            && let Some(cause) = &state.cause
        {
            self.inner.expired.store(true, Ordering::Release);
            warn!(unresolved_operations = ?state.failed, "ClickHouse connection recovery window exhausted");
            self.inner.terminal.send_replace(Some(cause.clone()));
            self.inner.changed.notify_waiters();
        }
        deadline
    }

    async fn before_request(&self, operation: u64) -> Result<()> {
        loop {
            let changed = self.inner.changed.notified();
            if let Some(source) = self.failure() {
                return Err(DatabaseError::ScanRequestFailed { source });
            }
            if self.inner.stopping.is_cancelled() {
                return Err(DatabaseError::ScanCancelled);
            }
            if !self.inner.recovering.load(Ordering::Acquire)
                || self.inner.recovery.lock().await.failed.contains(&operation)
            {
                return Ok(());
            }
            select! { () = self.inner.stopping.cancelled() => return Err(DatabaseError::ScanCancelled), () = changed => {} }
        }
    }

    pub(crate) async fn request<T, F, Fut>(&self, safety: RequestSafety, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        // 子步骤立即报告故障，但由同一逻辑操作决定如何重试，不能另建互相等待的 ticket。
        let parent = REQUEST_SCOPE
            .try_with(|(execution, id)| self.same_execution(execution).then_some(*id))
            .ok()
            .flatten();
        if let Some(id) = parent {
            return match operation().await {
                Err(error) if error.is_transient_connectivity_failure() && !self.inner.stopping.is_cancelled() => {
                    let source = Arc::new(error);
                    self.failed_operation(id, source.clone()).await;
                    Err(DatabaseError::ScanRequestFailed { source })
                }
                result => result,
            };
        }
        let id = self.inner.next_operation.fetch_add(1, Ordering::Relaxed);
        loop {
            self.before_request(id).await?;
            match REQUEST_SCOPE.scope((self.clone(), id), operation()).await {
                Ok(result) => {
                    self.recovered(id).await;
                    return Ok(result);
                }
                Err(error) if error.is_transient_connectivity_failure() && !self.inner.stopping.is_cancelled() => {
                    let not_sent = error.request_was_not_sent();
                    let source = Arc::new(error);
                    self.failed_operation(id, source.clone()).await;
                    if matches!(safety, RequestSafety::UnconfirmedWrite) && !not_sent {
                        warn!(
                            operation = id,
                            "ClickHouse statement outcome is unconfirmed; automatic replay is disabled"
                        );
                        self.wait_for_end().await;
                        return Err(DatabaseError::ScanRequestFailed { source });
                    }
                    select! {
                        () = self.inner.stopping.cancelled() => return Err(DatabaseError::ScanRequestFailed { source }),
                        _ = sleep(RETRY_DELAY) => {},
                    }
                    self.expire_if_due().await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn settle_failure(&self, error: DatabaseError) -> DatabaseError {
        if !error.is_transient_connectivity_failure() || self.inner.stopping.is_cancelled() {
            return error;
        }
        let source = Arc::new(error);
        let id = self.inner.next_operation.fetch_add(1, Ordering::Relaxed);
        self.failed_operation(id, source.clone()).await;
        self.wait_for_end().await;
        DatabaseError::ScanRequestFailed { source }
    }

    async fn wait_for_end(&self) {
        loop {
            let changed = self.inner.changed.notified();
            let deadline = self.expire_if_due().await;
            if self.failure().is_some() || self.inner.stopping.is_cancelled() {
                return;
            }
            let deadline = deadline.unwrap_or_else(|| Instant::now() + RECOVERY_WINDOW);
            select! { () = self.inner.stopping.cancelled() => return, () = sleep_until(deadline) => {}, () = changed => {} }
        }
    }

    async fn monitor(&self, done: CancellationToken) {
        let mut next_probe = Instant::now() + PROBE_INTERVAL;
        loop {
            let deadline = self.expire_if_due().await;
            if self.failure().is_some() || self.inner.stopping.is_cancelled() {
                return;
            }
            let wake = deadline.map_or(next_probe, |deadline| deadline.min(next_probe));
            select! { () = done.cancelled() => return, () = self.inner.stopping.cancelled() => return, () = self.inner.changed.notified() => continue, () = sleep_until(wake) => {} }
            if Instant::now() < next_probe {
                continue;
            }
            self.expire_if_due().await;
            if self.failure().is_some() {
                return;
            }
            let elapsed = u64::try_from(self.inner.origin.elapsed().as_millis()).unwrap_or(u64::MAX);
            let idle = elapsed.saturating_sub(self.inner.last_ack_millis.load(Ordering::Relaxed));
            if Duration::from_millis(idle) < PROBE_INTERVAL && !self.inner.recovering.load(Ordering::Acquire) {
                next_probe = Instant::now() + PROBE_INTERVAL;
                continue;
            }
            let remaining = deadline.map_or(PROBE_TIMEOUT, |deadline| {
                deadline.saturating_duration_since(Instant::now()).min(PROBE_TIMEOUT)
            });
            let result = select! {
                () = done.cancelled() => return,
                () = self.inner.stopping.cancelled() => return,
                result = timeout(remaining, self.probe_query().fetch_one::<u8>()) => result,
            };
            match result {
                Ok(Ok(1)) => self.recovered(0).await,
                Ok(Ok(_)) => {
                    self.failed_operation(0, Arc::new(DatabaseError::ClusterProbeIncomplete))
                        .await
                }
                Ok(Err(error)) => {
                    let error = DatabaseError::from(error);
                    if !error.is_transient_connectivity_failure() {
                        self.inner.terminal.send_replace(Some(Arc::new(error)));
                        return;
                    }
                    self.failed_operation(0, Arc::new(error)).await;
                }
                Err(error) => {
                    self.failed_operation(0, Arc::new(DatabaseError::ProbeTimeout { source: error }))
                        .await
                }
            }
            next_probe = Instant::now() + PROBE_INTERVAL;
        }
    }

    fn probe_query(&self) -> Query {
        match &self.inner.probe_cluster {
            None => self.inner.probe.query("SELECT 1"),
            Some(cluster) => {
                // 当前快照发布使用全节点 DDL，任务最终确认本就要求全部配置节点可达。
                // 这里只读 system.one，不向业务表写探测数据；失败操作仍有独立 ticket。
                // 标量子查询保留 Nullable，必须消除空值标记再按 RowBinary UInt8 读取。
                self.inner.probe.query("SELECT ifNull(toUInt8((SELECT count() FROM clusterAllReplicas(?,system.one))=(SELECT count() FROM system.clusters WHERE cluster=?)),toUInt8(0)) SETTINGS skip_unavailable_shards=0,connect_timeout_with_failover_ms=250,receive_timeout=1,send_timeout=1,max_execution_time=1")
                    .bind(cluster).bind(cluster)
            }
        }
    }
}
