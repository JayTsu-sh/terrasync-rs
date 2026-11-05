//! 取消时持续终止本次执行的查询，直到本地工作任务全部退出。
use crate::error::{AppError, Result};
use data_mover::traversal::ScanFailureSignal;
use db::{
    DatabaseError,
    scan::{ScanDatabaseExecution, ScanQueryCancellation, with_scan_cancellation},
};
use std::result::Result as OperationResult;
use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    select,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct ScanCancellation {
    requested: CancellationToken,
    database: Arc<OnceLock<ScanDatabaseExecution>>,
}

impl ScanCancellation {
    pub(crate) fn new(requested: CancellationToken) -> Self {
        Self {
            requested: requested.child_token(),
            database: Arc::new(OnceLock::new()),
        }
    }
    pub(crate) fn watch_storage_failure(&self, failure: ScanFailureSignal) -> AbortOnDropHandle<()> {
        let control = self.clone();
        AbortOnDropHandle::new(tokio::spawn(async move {
            failure.failed().await;
            control.cancel();
        }))
    }
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.requested
    }
    pub(crate) fn cancel(&self) {
        self.requested.cancel();
    }
    pub(crate) async fn cancelled(&self) {
        self.requested.cancelled().await;
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.requested.is_cancelled() {
            Err(AppError::ObservationScanCancelled)
        } else {
            Ok(())
        }
    }
    /// 流式读取和输出等待可以直接中止；数据库写入仍使用 operation 确认完成。
    pub(crate) async fn interrupt<T>(&self, work: impl Future<Output = T>) -> Result<T> {
        self.check()?;
        select! {
            biased;
            () = self.cancelled() => Err(AppError::ObservationScanCancelled),
            result = work => Ok(result),
        }
    }
    /// 取消后禁止新语句和重试，已发出的请求必须等待响应，不能因空匹配 KILL 提前丢弃。
    pub(crate) async fn operation<T, E: Into<AppError>>(
        &self, work: impl Future<Output = OperationResult<T, E>>,
    ) -> Result<T> {
        self.check()?;
        let result = with_scan_cancellation(self.requested.clone(), self.database.get().cloned(), work)
            .await
            .map_err(Into::into);
        let result = match result {
            Err(AppError::DatabaseError(error)) if !self.requested.is_cancelled() => match self.database.get() {
                Some(database) => Err(AppError::DatabaseError(database.settle_failure(error).await)),
                None => Err(AppError::DatabaseError(error)),
            },
            result => result,
        };
        if matches!(&result, Err(AppError::DatabaseError(error)) if !error.is_confirmed_scan_cancellation()) {
            return result;
        }
        // 已发出的请求先确认结束，再报告取消，禁止把 KILL 的副作用冒充原始存储故障。
        self.check()?;
        match result {
            Err(AppError::DatabaseError(DatabaseError::ScanCancelled)) => Err(AppError::ObservationScanCancelled),
            result => result,
        }
    }
}

pub(crate) async fn scope<T>(
    queries: ScanQueryCancellation, cancel: ScanCancellation, work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let database = queries.execution();
    if let Err(other) = cancel.database.set(database.clone())
        && !cancel
            .database
            .get()
            .is_some_and(|current| current.same_execution(&other))
    {
        return Err(AppError::ConsumerError(
            "Scan cancellation is already bound to another database execution".into(),
        ));
    }
    let connectivity = database.start_monitor()?;
    let finished = CancellationToken::new();
    let done = finished.clone();
    let terminal = database.clone();
    let watcher = AbortOnDropHandle::new(tokio::spawn(async move {
        select! {
            biased;
            _=cancel.cancelled()=>queries.begin_cancellation(),
            _=done.cancelled()=>return,
            failure=terminal.failed()=>{
                if failure.is_none() { return; }
                cancel.cancel();
                queries.begin_cancellation();
            }
        }
        let mut warned = false;
        loop {
            // 不把管理请求的网络等待无限叠加到取消耗时；失败时仍等原请求自然退出。
            let response = timeout(Duration::from_secs(2), queries.cancel()).await;
            if !matches!(response, Ok(Ok(()))) && !warned {
                warn!("Query cancellation unavailable; waiting for current requests to complete");
                warned = true;
            }
            if done.is_cancelled() {
                break;
            }
            select! {
                _=done.cancelled()=>break,
                _=sleep(Duration::from_millis(100))=>{},
            }
        }
    }));
    let result = work.await;
    finished.cancel();
    // 所有业务请求均已确认结束，此时终止仅针对本执行的管理轮询。
    watcher.abort();
    let _ = watcher.await;
    let monitored = connectivity.finish().await;
    let failure = database.failure();
    let recovery_window = database.recovery_window();
    let result = fold_database_failure(result, failure, recovery_window);
    if result.is_ok() {
        monitored?;
    } else if let Err(error) = monitored {
        warn!(%error, "Connectivity monitor failed during scan cleanup");
    }
    result
}

fn fold_database_failure<T>(
    result: Result<T>, failure: Option<Arc<DatabaseError>>, recovery_window: Duration,
) -> Result<T> {
    match (result, failure) {
        (Ok(_), Some(source)) => Err(DatabaseError::ScanDatabaseUnavailable {
            recovery_window,
            source,
            termination: None,
        }
        .into()),
        (Err(AppError::ObservationScanCancelled), Some(source)) => Err(DatabaseError::ScanDatabaseUnavailable {
            recovery_window,
            source,
            termination: None,
        }
        .into()),
        (Err(AppError::DatabaseError(error)), Some(source)) => Err(DatabaseError::ScanDatabaseUnavailable {
            recovery_window,
            source,
            termination: Some(Box::new(error)),
        }
        .into()),
        (Err(AppError::ScanStorageAndDatabaseFailure { source, database }), Some(failure)) => {
            Err(AppError::ScanStorageAndDatabaseFailure {
                source,
                database: Box::new(DatabaseError::ScanDatabaseUnavailable {
                    recovery_window,
                    source: failure,
                    termination: Some(database),
                }),
            })
        }
        (Err(AppError::StorageError(source)), Some(failure)) => Err(AppError::ScanStorageAndDatabaseFailure {
            source: Box::new(source),
            database: Box::new(DatabaseError::ScanDatabaseUnavailable {
                recovery_window,
                source: failure,
                termination: None,
            }),
        }),
        (result, _) => result,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clickhouse::{Client, error::Error as ClickHouseError};
    use db::{ClickHouseConfig, DatabaseError, scan::ScanStore};
    use std::io::{Error as IoError, ErrorKind};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Notify, oneshot},
        task::JoinSet,
    };

    #[test]
    fn a_terminal_database_failure_cannot_be_hidden_by_late_success() {
        let source = Arc::new(DatabaseError::ClusterProbeIncomplete);
        assert!(matches!(
            fold_database_failure(Ok(()), Some(source), Duration::from_secs(3)),
            Err(AppError::DatabaseError(DatabaseError::ScanDatabaseUnavailable { .. }))
        ));
    }

    #[test]
    fn internal_storage_failure_cancellation_keeps_the_user_request_active() {
        let user = CancellationToken::new();
        let scope = ScanCancellation::new(user.clone());
        scope.cancel();
        assert!(scope.token().is_cancelled());
        assert!(
            !user.is_cancelled(),
            "a storage failure must not be classified as user cancellation"
        );
    }

    #[test]
    fn a_user_cancel_still_reaches_the_internal_scan_scope() {
        let user = CancellationToken::new();
        let scope = ScanCancellation::new(user.clone());
        user.cancel();
        assert!(scope.token().is_cancelled());
    }

    #[tokio::test]
    async fn empty_kill_does_not_finish_a_request_still_waiting_for_its_response() {
        assert_empty_kill_request_is_drained().await;
    }

    async fn assert_empty_kill_request_is_drained() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let received = Arc::new(Notify::new());
        let killed = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completed = Arc::new(AtomicBool::new(false));
        let (seen, kill_seen, allow, written) = (received.clone(), killed.clone(), release.clone(), completed.clone());
        let server = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (seen, kill_seen, allow, written) =
                    (seen.clone(), kill_seen.clone(), allow.clone(), written.clone());
                requests.spawn(async move {
                    let mut bytes = Vec::with_capacity(4096);
                    loop {
                        let mut chunk = [0; 4096];
                        let count = socket.read(&mut chunk).await.unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&chunk[..count]);
                        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&bytes[..end]);
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|value| value.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    if String::from_utf8_lossy(&bytes).contains("KILL") {
                        kill_seen.notify_one();
                    } else if String::from_utf8_lossy(&bytes).contains("INSERT") {
                        seen.notify_one();
                        allow.notified().await;
                        written.store(true, Ordering::SeqCst);
                    }
                    let body = Vec::<u8>::new();
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(header.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: url.clone(),
            database: "fixture".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let store = ScanStore::new(&config, "fixture").unwrap();
        let client = Client::default().with_url(url);
        let cancel = ScanCancellation::new(CancellationToken::new());
        let operation_cancel = cancel.clone();
        let work = scope(store.query_cancellation(), cancel.clone(), async move {
            operation_cancel
                .operation(async move {
                    client
                        .query("INSERT INTO fixture VALUES (1)")
                        .execute()
                        .await
                        .map_err(DatabaseError::from)
                })
                .await
        });
        tokio::pin!(work);
        select! {_=received.notified()=>{},result=&mut work=>panic!("request ended before being held: {result:?}")}
        cancel.cancel();
        select! {_=killed.notified()=>{},result=&mut work=>panic!("request ended before KILL: {result:?}")}
        assert!(
            timeout(Duration::from_millis(50), &mut work).await.is_err(),
            "empty KILL must not be treated as completion of an outstanding request"
        );
        assert!(!completed.load(Ordering::SeqCst));
        release.notify_one();
        assert!(matches!(
            timeout(Duration::from_secs(2), work).await.unwrap(),
            Err(AppError::ObservationScanCancelled)
        ));
        assert!(completed.load(Ordering::SeqCst));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn finished_work_does_not_wait_for_a_stalled_management_kill() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let received = Arc::new(Notify::new());
        let seen = received.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            seen.notify_one();
            std::future::pending::<()>().await;
        });
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: url,
            database: "fixture".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let store = ScanStore::new(&config, "fixture").unwrap();
        let user = CancellationToken::new();
        let cancel = ScanCancellation::new(user.clone());
        let stop = cancel.clone();
        let result = timeout(
            Duration::from_millis(200),
            scope(store.query_cancellation(), cancel, async move {
                stop.cancel();
                received.notified().await;
                Err::<(), _>(AppError::ScanError("all owned requests have finished".into()))
            }),
        )
        .await;
        server.abort();
        let _ = server.await;
        assert!(
            result.is_ok(),
            "management polling must not extend an already quiescent scan"
        );
        assert!(matches!(result.unwrap(), Err(AppError::ScanError(_))));
        assert!(!user.is_cancelled());
    }

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn pending_operation_is_not_dropped_when_cancellation_is_requested() {
        let cancel = ScanCancellation::new(CancellationToken::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = dropped.clone();
        let (complete, completed) = oneshot::channel();
        let work = async move {
            let _guard = Dropped(flag);
            completed.await.unwrap();
            Ok::<_, AppError>(())
        };
        let operation = cancel.operation(work);
        tokio::pin!(operation);
        assert!(timeout(Duration::from_millis(10), &mut operation).await.is_err());
        cancel.cancel();
        assert!(timeout(Duration::from_millis(10), &mut operation).await.is_err());
        assert!(!dropped.load(Ordering::SeqCst));
        complete.send(()).unwrap();
        let result = timeout(Duration::from_millis(200), operation).await.unwrap();
        assert!(matches!(result, Err(AppError::ObservationScanCancelled)));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn actual_read_timeout_is_not_erased_by_an_earlier_user_cancel() {
        let cancel = ScanCancellation::new(CancellationToken::new());
        let (release, ready) = oneshot::channel();
        let operation = cancel.operation(async {
            ready.await.unwrap();
            Err::<(), _>(DatabaseError::from(ClickHouseError::Network(
                IoError::new(ErrorKind::TimedOut, "response ACK was lost").into(),
            )))
        });
        tokio::pin!(operation);
        assert!(timeout(Duration::from_millis(10), &mut operation).await.is_err());
        cancel.cancel();
        release.send(()).unwrap();
        let result = operation.await;
        assert!(matches!(
            result,
            Err(AppError::DatabaseError(DatabaseError::RequestOutcomeUnconfirmed { .. }))
        ));
    }

    #[tokio::test]
    async fn requested_cancellation_prevents_starting_another_operation() {
        let cancel = ScanCancellation::new(CancellationToken::new());
        cancel.cancel();
        let mut started = false;
        let result = cancel
            .operation(async {
                started = true;
                Ok::<_, AppError>(())
            })
            .await;
        assert!(matches!(result, Err(AppError::ObservationScanCancelled)));
        assert!(!started);
    }
}
