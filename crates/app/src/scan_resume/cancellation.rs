//! 取消时持续终止本次执行的查询，直到本地工作任务全部退出。
use crate::error::{AppError, Result};
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
pub(super) struct ScanCancellation {
    requested: CancellationToken,
}

impl ScanCancellation {
    pub(super) fn new(requested: CancellationToken) -> Self {
        Self { requested }
    }
    pub(super) fn child(&self) -> Self {
        Self {
            requested: self.requested.child_token(),
        }
    }
    pub(super) fn token(&self) -> &CancellationToken {
        &self.requested
    }
    pub(super) fn cancel(&self) {
        self.requested.cancel();
    }
    pub(super) async fn cancelled(&self) {
        self.requested.cancelled().await;
    }
    pub(super) fn check(&self) -> Result<()> {
        if self.requested.is_cancelled() {
            Err(AppError::ObservationScanCancelled)
        } else {
            Ok(())
        }
    }
    /// 取消后禁止新语句和重试，已发出的请求必须等待响应，不能因空匹配 KILL 提前丢弃。
    pub(super) async fn operation<T, E: Into<AppError>>(
        &self, work: impl Future<Output = OperationResult<T, E>>,
    ) -> Result<T> {
        self.check()?;
        match with_scan_cancellation(self.requested.clone(), work)
            .await
            .map_err(Into::into)
        {
            Err(AppError::DatabaseError(DatabaseError::ScanCancelled)) => Err(AppError::ObservationScanCancelled),
            result => result,
        }
    }
}

pub(super) async fn scope<T>(
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
    watcher.await?;
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use clickhouse::Client;
    use db::{ClickHouseConfig, DatabaseError, scan_store::ScanStore};
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

    #[tokio::test]
    async fn empty_kill_does_not_finish_a_request_still_waiting_for_its_response() {
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
                    } else {
                        seen.notify_one();
                        allow.notified().await;
                        written.store(true, Ordering::SeqCst);
                    }
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                });
            }
        });
        let config = ClickHouseConfig {
            dsn: url.clone(),
            database: "fixture".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let store = ScanStore::new(&config, None, "fixture").unwrap();
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
        assert!(timeout(Duration::from_secs(2), work).await.unwrap().is_ok());
        assert!(completed.load(Ordering::SeqCst));
        server.abort();
        let _ = server.await;
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
        assert!(result.is_ok());
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
