//! ClickHouse HTTP 的连接、读写及执行内取消边界；不把本地断开当作远端操作已结束。
#[cfg(test)]
pub(crate) mod tests;
use crate::{ClickHouseConfig, DatabaseError, Result, error::ScanCancellationDeadline};
use clickhouse::Client;
use hyper::{
    Uri,
    rt::{Read, ReadBufCursor, Write},
};
use hyper_timeout::TimeoutConnector;
use hyper_util::{
    client::legacy::{
        Client as HyperClient,
        connect::{Connected, Connection, HttpConnector},
    },
    rt::TokioExecutor,
};
use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    result::Result as TransportResult,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Sleep, sleep_until};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tower_service::Service;

const STOP_GRACE: Duration = Duration::from_millis(250);
type TimedConnector = TimeoutConnector<HttpConnector>;
type TimedIo = <TimedConnector as Service<Uri>>::Response;

#[derive(Clone, Default)]
pub(crate) struct ScanHttpCancellation {
    requested: CancellationToken,
    deadline: Arc<OnceLock<Instant>>,
}

impl ScanHttpCancellation {
    pub(crate) fn begin_stop(&self) {
        let _ = self.deadline.set(Instant::now() + STOP_GRACE);
        self.requested.cancel();
    }

    async fn expired(&self) {
        self.requested.cancelled().await;
        sleep_until(self.deadline.get().copied().unwrap_or_else(Instant::now)).await;
    }
}

pub(crate) fn build_client(config: &ClickHouseConfig, cancellation: Option<ScanHttpCancellation>) -> Result<Client> {
    if config.dial_timeout == 0 || config.read_timeout == 0 {
        return Err(DatabaseError::ConfigError(
            "ClickHouse connection and read timeouts must be positive".into(),
        ));
    }
    let mut connector = HttpConnector::new();
    connector.set_keepalive(Some(Duration::from_secs(60)));
    let mut connector = TimeoutConnector::new(connector);
    connector.set_connect_timeout(Some(Duration::from_secs(u64::from(config.dial_timeout))));
    connector.set_read_timeout(Some(Duration::from_secs(u64::from(config.read_timeout))));
    connector.set_write_timeout(Some(Duration::from_secs(u64::from(config.read_timeout))));
    // 上传仍有进展时，不把尚未收到响应误作读取静默超时。
    connector.set_reset_reader_on_write(true);
    let connector = ScanConnector {
        inner: connector,
        cancellation,
    };
    let http = HyperClient::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(2))
        .build(connector);
    let mut client = Client::with_http_client(http)
        .with_url(&config.dsn)
        .with_database(config.database.clone())
        .with_user(config.username.clone())
        // 相同内容可能属于不同的正常写入；只有固定 token 的幂等入口才显式启用去重。
        .with_option("deduplicate_insert", "disable");
    if let Some(password) = &config.password {
        client = client.with_password(password);
    }
    Ok(client)
}

#[derive(Clone)]
struct ScanConnector {
    inner: TimedConnector,
    cancellation: Option<ScanHttpCancellation>,
}

impl Service<Uri> for ScanConnector {
    type Response = ScanIo;
    type Error = <TimedConnector as Service<Uri>>::Error;
    type Future = Pin<Box<dyn Future<Output = TransportResult<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<TransportResult<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connecting = self.inner.call(uri);
        let cancellation = self.cancellation.clone();
        Box::pin(async move {
            let io = if let Some(cancellation) = &cancellation {
                tokio::select! {
                    biased;
                    () = cancellation.expired() => return Err(io::Error::new(io::ErrorKind::TimedOut, ScanCancellationDeadline).into()),
                    result = connecting => result?,
                }
            } else {
                connecting.await?
            };
            Ok(ScanIo {
                inner: io,
                cancellation: cancellation.map(DeadlineWatcher::new),
            })
        })
    }
}

struct DeadlineWatcher {
    cancellation: ScanHttpCancellation,
    requested: Pin<Box<WaitForCancellationFutureOwned>>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl DeadlineWatcher {
    fn new(cancellation: ScanHttpCancellation) -> Self {
        Self {
            requested: Box::pin(cancellation.requested.clone().cancelled_owned()),
            cancellation,
            sleep: None,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> bool {
        if self.sleep.is_none() {
            if self.requested.as_mut().poll(cx).is_pending() {
                return false;
            }
            self.sleep = Some(Box::pin(sleep_until(
                self.cancellation.deadline.get().copied().unwrap_or_else(Instant::now),
            )));
        }
        self.sleep
            .as_mut()
            .is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready())
    }
}

struct ScanIo {
    inner: TimedIo,
    cancellation: Option<DeadlineWatcher>,
}

impl ScanIo {
    fn check_deadline(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancellation.as_mut().is_some_and(|watcher| watcher.poll(cx)) {
            Err(io::Error::new(io::ErrorKind::TimedOut, ScanCancellationDeadline))
        } else {
            Ok(())
        }
    }
}

impl Read for ScanIo {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: ReadBufCursor<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        Pin::new(&mut this.inner).poll_read(cx, buffer)
    }
}

impl Write for ScanIo {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        Pin::new(&mut this.inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        Pin::new(&mut this.inner).poll_write_vectored(cx, buffers)
    }
}

impl Connection for ScanIo {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}
