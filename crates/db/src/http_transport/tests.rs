#![allow(clippy::unwrap_used)]
use super::*;
use crate::scan_store::queries::idempotent_insert_client;
use clickhouse::{Compression, error::Error as ClickHouseError};
use serde::Serialize;
use std::{
    str,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::task::AbortOnDropHandle;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

pub(crate) struct Blackhole {
    pub(crate) config: ClickHouseConfig,
    pub(crate) received: Arc<Notify>,
    pub(crate) responsive: Arc<AtomicBool>,
    pub(crate) requests: Arc<AtomicUsize>,
    applied: Arc<AtomicUsize>,
    server: AbortOnDropHandle<()>,
}

fn complete_request(bytes: &[u8]) -> bool {
    let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
        return false;
    };
    let header = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
    let mut body = &bytes[end + 4..];
    if header.contains("transfer-encoding: chunked") {
        loop {
            let Some(end) = body.windows(2).position(|part| part == b"\r\n") else {
                return false;
            };
            let Some(size) = str::from_utf8(&body[..end])
                .ok()
                .and_then(|value| usize::from_str_radix(value.split(';').next().unwrap_or_default(), 16).ok())
            else {
                return false;
            };
            body = &body[end + 2..];
            if size == 0 {
                return body.starts_with(b"\r\n");
            }
            if body.len() < size + 2 {
                return false;
            }
            body = &body[size + 2..];
        }
    }
    let size = header
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    body.len() >= size
}

async fn receive(socket: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !complete_request(&bytes) {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&chunk[..count]);
    }
    bytes
}

impl Blackhole {
    pub(crate) async fn start(body_started: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let received = Arc::new(Notify::new());
        let applied = Arc::new(AtomicUsize::new(0));
        let responsive = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));
        let (respond, request_count) = (responsive.clone(), requests.clone());
        let seen = received.clone();
        let committed = applied.clone();
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (seen, committed) = (seen.clone(), committed.clone());
                let (respond, request_count) = (respond.clone(), request_count.clone());
                requests.spawn(async move {
                    let request = receive(&mut socket).await;
                    request_count.fetch_add(1, Ordering::SeqCst);
                    if respond.load(Ordering::SeqCst) {
                        socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n\x01")
                            .await
                            .unwrap();
                        seen.notify_one();
                        return;
                    }
                    if request.windows(6).any(|part| part == b"INSERT") {
                        committed.fetch_add(1, Ordering::SeqCst);
                    }
                    if body_started {
                        socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n\x01\x00\x00\x00")
                            .await
                            .unwrap();
                    }
                    seen.notify_one();
                    std::future::pending::<()>().await;
                });
            }
        }));
        Self {
            config: ClickHouseConfig {
                mode: ClickHouseMode::Single,
                cluster: None,
                dsn: format!("http://{address}"),
                database: "fixture".into(),
                username: "default".into(),
                password: None,
                dial_timeout: 1,
                read_timeout: 1,
            },
            received,
            responsive,
            requests,
            applied,
            server,
        }
    }

    fn client(&self, cancellation: Option<ScanHttpCancellation>) -> Client {
        build_client(&self.config, cancellation)
            .unwrap()
            .with_validation(false)
            .with_compression(Compression::None)
    }

    pub(crate) async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

#[tokio::test]
async fn configured_timeout_ends_a_silent_response_without_cancellation() {
    let server = Blackhole::start(false).await;
    let request = server.client(None).query("SELECT 1").execute();
    let started = Instant::now();
    let error = timeout(Duration::from_secs(3), request).await.unwrap().unwrap_err();
    assert!(matches!(error, ClickHouseError::Network(_)));
    assert!(started.elapsed() >= Duration::from_millis(800));
    assert!(started.elapsed() < Duration::from_secs(2));
    server.stop().await;
}

async fn cancellation_case(body_started: bool) {
    let server = Blackhole::start(body_started).await;
    let control = ScanHttpCancellation::default();
    let client = server.client(Some(control.clone()));
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        client
            .query("SELECT 1")
            .fetch_one::<u64>()
            .await
            .map_err(DatabaseError::from)
    }));
    server.received.notified().await;
    let started = Instant::now();
    control.begin_stop();
    let result = timeout(Duration::from_secs(1), &mut task).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(DatabaseError::ScanTerminationUnconfirmed { .. })),
        "{result:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    server.stop().await;
}

#[tokio::test]
async fn cancellation_ends_both_header_and_body_stalls_and_joins_the_request_owner() {
    cancellation_case(false).await;
    cancellation_case(true).await;
}

#[derive(Serialize, clickhouse::Row)]
struct Number {
    value: u64,
}

#[tokio::test]
#[ignore = "requires LAB_CLICKHOUSE_DSN pointing to a writable Single server"]
async fn ordinary_writes_do_not_inherit_scan_block_deduplication() -> Result<()> {
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: std::env::var("LAB_CLICKHOUSE_DSN").map_err(|error| DatabaseError::ConfigError(error.to_string()))?,
        database: "default".into(),
        username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
        dial_timeout: 5,
        read_timeout: 10,
    };
    let client = build_client(&config, None)?.with_validation(false);
    let table = format!("ts_http_dedup_scope_{}", Uuid::new_v4().simple());
    client.query(&format!("CREATE TABLE {table} (id UInt8,generation UInt8) ENGINE=ReplacingMergeTree ORDER BY id SETTINGS non_replicated_deduplication_window=10000")).execute().await?;
    let checked = async {
        // 物理行数用于验证真正的重发抑制，不能让后台合并掩盖重复插入。
        client.query(&format!("SYSTEM STOP MERGES {table}")).execute().await?;
        let mut states = Vec::new();
        for value in [0, 1, 0] {
            client
                .query(&format!("INSERT INTO {table} VALUES (1,{value})"))
                .execute()
                .await?;
            states.push(
                client
                    .query(&format!("SELECT generation FROM {table} FINAL WHERE id=1"))
                    .fetch_one::<u8>()
                    .await?,
            );
        }
        let retry = idempotent_insert_client(&client);
        for _ in 0..2 {
            retry
                .query(&format!("INSERT INTO {table} VALUES (2,7)"))
                .execute()
                .await?;
        }
        let retried_rows = client
            .query(&format!("SELECT count() FROM {table} WHERE id=2"))
            .fetch_one::<u64>()
            .await?;
        idempotent_insert_client(&client)
            .query(&format!("INSERT INTO {table} VALUES (2,7)"))
            .execute()
            .await?;
        let new_intent_rows = client
            .query(&format!("SELECT count() FROM {table} WHERE id=2"))
            .fetch_one::<u64>()
            .await?;
        Ok::<_, DatabaseError>((states, retried_rows, new_intent_rows))
    }
    .await;
    let cleanup = client
        .query(&format!("DROP TABLE IF EXISTS {table} SYNC"))
        .execute()
        .await;
    let (states, retried_rows, new_intent_rows) = checked?;
    cleanup?;
    assert_eq!(states, [0, 1, 0]);
    assert_eq!(
        retried_rows, 1,
        "one logical retry must not create another physical row"
    );
    assert_eq!(
        new_intent_rows, 2,
        "a new write with the same content must still execute"
    );
    Ok(())
}

#[derive(Serialize, clickhouse::Row)]
struct LargeRow {
    payload: String,
}

#[tokio::test]
async fn cancellation_unblocks_an_upload_when_the_peer_stops_reading() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(Notify::new());
    let seen = accepted.clone();
    let release = Arc::new(Notify::new());
    let closed = release.clone();
    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        seen.notify_one();
        closed.notified().await;
        drop(socket);
    }));
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: format!("http://{address}"),
        database: "fixture".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 5,
        read_timeout: 30,
    };
    let control = ScanHttpCancellation::default();
    let client = build_client(&config, Some(control.clone()))
        .unwrap()
        .with_validation(false)
        .with_compression(Compression::None);
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        let row = LargeRow {
            payload: "x".repeat(8192),
        };
        let mut insert = client.insert::<LargeRow>("facts").await?;
        for _ in 0..4096 {
            insert.write(&row).await?;
        }
        insert.end().await.map_err(DatabaseError::from)
    }));
    timeout(Duration::from_secs(2), accepted.notified()).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    control.begin_stop();
    let result = timeout(Duration::from_secs(1), &mut task).await.unwrap().unwrap();
    assert!(
        matches!(
            result,
            Err(DatabaseError::ScanTerminationUnconfirmed { .. } | DatabaseError::RequestOutcomeUnconfirmed { .. })
        ),
        "{result:?}"
    );
    release.notify_one();
    server.await.unwrap();
}

#[tokio::test]
async fn lost_insert_ack_returns_an_unconfirmed_outcome_without_resending() {
    let server = Blackhole::start(false).await;
    let control = ScanHttpCancellation::default();
    let client = server.client(Some(control.clone()));
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut insert = client.insert::<Number>("facts").await?;
        insert.write(&Number { value: 7 }).await?;
        insert.end().await.map_err(DatabaseError::from)
    }));
    server.received.notified().await;
    assert_eq!(server.applied.load(Ordering::SeqCst), 1);
    control.begin_stop();
    let result = timeout(Duration::from_secs(1), &mut task).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(DatabaseError::ScanTerminationUnconfirmed { .. })),
        "{result:?}"
    );
    assert_eq!(
        server.applied.load(Ordering::SeqCst),
        1,
        "unknown writes cannot be blindly resent"
    );
    server.stop().await;
}

#[tokio::test]
async fn all_connections_share_one_stop_deadline() {
    let server = Blackhole::start(false).await;
    let control = ScanHttpCancellation::default();
    let client = server.client(Some(control.clone()));
    let first = client.clone();
    let mut first = AbortOnDropHandle::new(tokio::spawn(async move {
        first.query("SELECT 1").execute().await.map_err(DatabaseError::from)
    }));
    server.received.notified().await;
    let started = Instant::now();
    control.begin_stop();
    sleep(Duration::from_millis(200)).await;
    let second = client.query("SELECT 2").execute().await.map_err(DatabaseError::from);
    assert!(matches!(second, Err(DatabaseError::ScanTerminationUnconfirmed { .. })));
    assert!(matches!(
        timeout(Duration::from_millis(200), &mut first).await.unwrap().unwrap(),
        Err(DatabaseError::ScanTerminationUnconfirmed { .. })
    ));
    assert!(started.elapsed() < Duration::from_millis(450));
    server.stop().await;
}
