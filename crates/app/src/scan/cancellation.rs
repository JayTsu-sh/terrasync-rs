//! 取消时持续终止本次执行的查询，直到本地工作任务全部退出。
use crate::error::{AppError, Result};
use data_mover::traversal::ScanFailureSignal;
use db::{
    DatabaseError,
    scan_store::{ScanQueryCancellation, with_scan_cancellation},
};
use std::result::Result as OperationResult;
use std::{future::Future, time::Duration};
use tokio::{
    select,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct ScanCancellation {
    requested: CancellationToken,
}

impl ScanCancellation {
    pub(crate) fn new(requested: CancellationToken) -> Self {
        Self {
            requested: requested.child_token(),
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
        let result = with_scan_cancellation(self.requested.clone(), work)
            .await
            .map_err(Into::into);
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
    let finished = CancellationToken::new();
    let done = finished.clone();
    let watcher = AbortOnDropHandle::new(tokio::spawn(async move {
        select! {
            biased;
            _=cancel.cancelled()=>{},
            _=done.cancelled()=>return,
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
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clickhouse::Client;
    use db::{
        ClickHouseConfig, DatabaseError,
        scan_store::{ScanDirectoryStatisticsRow, ScanStore},
    };
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
        assert_empty_kill_request_is_drained(false).await;
    }

    #[tokio::test]
    async fn directory_statistics_write_is_drained_before_cancelled_scope_returns() {
        assert_empty_kill_request_is_drained(true).await;
    }

    async fn assert_empty_kill_request_is_drained(directory_statistics: bool) {
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
                    let body = if String::from_utf8_lossy(&bytes).contains("DESCRIBE") {
                        directory_schema()
                    } else {
                        Vec::new()
                    };
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
                    if directory_statistics {
                        store
                            .write_directory_statistics(
                                1,
                                1,
                                &[ScanDirectoryStatisticsRow {
                                    directory: "a".into(),
                                    payload: "confirmed-contribution".into(),
                                }],
                            )
                            .await
                    } else {
                        client
                            .query("INSERT INTO fixture VALUES (1)")
                            .execute()
                            .await
                            .map_err(DatabaseError::from)
                    }
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

    fn directory_schema() -> Vec<u8> {
        let mut bytes = vec![7];
        let names = [
            "name",
            "type",
            "default_type",
            "default_expression",
            "comment",
            "codec_expression",
            "ttl_expression",
        ];
        for value in names.into_iter().chain(["String"; 7]) {
            bytes.push(value.len() as u8);
            bytes.extend_from_slice(value.as_bytes());
        }
        for (name, kind) in [
            ("job_id", "String"),
            ("round", "Int64"),
            ("version", "UInt64"),
            ("revision", "UInt64"),
            ("directory", "String"),
            ("payload", "String"),
        ] {
            for value in [name, kind, "", "", "", "", ""] {
                bytes.push(value.len() as u8);
                bytes.extend_from_slice(value.as_bytes());
            }
        }
        bytes
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
