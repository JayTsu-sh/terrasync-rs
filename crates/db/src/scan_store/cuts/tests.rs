//! 两个 HTTP CREATE 都到达后才放行，验证真实捕获链路并行且不会提前发布 token。
#![allow(clippy::unwrap_used)]

use super::ScanCuts;
use crate::ClickHouseConfig;
use crate::{
    http_transport::build_client,
    scan_store::{ScanDatabaseExecution, with_scan_cancellation},
};
use clickhouse::Compression;
use std::{
    net::SocketAddr,
    str::from_utf8,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Barrier, Notify},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use utils::app_config::ClickHouseMode;

struct Gates {
    creates: Barrier,
    events_attached: Notify,
    release_events: Notify,
    fail_base: bool,
}

async fn request(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let headers = from_utf8(&bytes[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return String::from_utf8(bytes).unwrap();
            }
        }
    }
}

async fn respond(mut socket: TcpStream, gates: Arc<Gates>) {
    let sql = request(&mut socket).await;
    let create = sql.contains("CREATE TABLE");
    if create {
        gates.creates.wait().await;
    }
    let failing = create && gates.fail_base && sql.contains("_base");
    if sql.contains("ALTER TABLE") && sql.contains("_events") {
        gates.events_attached.notify_one();
        gates.release_events.notified().await;
    }
    let body = if failing {
        b"capture failed".to_vec()
    } else if sql.contains("SELECT") {
        0_u64.to_le_bytes().to_vec()
    } else {
        Vec::new()
    };
    let status = if failing { 500 } else { 200 };
    let header = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
}

fn single_config(address: SocketAddr) -> ClickHouseConfig {
    ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: format!("http://{address}"),
        database: "capture_test".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    }
}

fn single_cuts(address: SocketAddr) -> ScanCuts {
    let mut cuts = ScanCuts::new(&single_config(address), "job", "owner").unwrap();
    cuts.client = cuts.client.with_validation(false).with_compression(Compression::None);
    cuts
}

async fn capture_case(fail_base: bool) {
    let gates = Arc::new(Gates {
        creates: Barrier::new(2),
        events_attached: Notify::new(),
        release_events: Notify::new(),
        fail_base,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_gates = gates.clone();
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut requests = JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            requests.spawn(respond(socket, server_gates.clone()));
        }
    }));
    let cuts = single_cuts(address);
    let mut capture = AbortOnDropHandle::new(tokio::spawn(async move { cuts.capture().await }));
    // 串行复制到不了 events ATTACH；提前返回 token 或提前丢弃另一复制也必须失败。
    timeout(Duration::from_secs(3), gates.events_attached.notified())
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(100), &mut capture).await.is_err());
    gates.release_events.notify_one();
    let result = timeout(Duration::from_secs(3), capture).await.unwrap().unwrap();
    if fail_base {
        assert!(result.is_err());
    } else {
        let token = result.unwrap();
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|value| value.is_ascii_hexdigit()));
    }
}

#[tokio::test]
async fn capture_waits_for_both_tables_before_returning_a_token() {
    capture_case(false).await;
}

#[tokio::test]
async fn capture_finishes_the_other_clone_before_returning_failure() {
    capture_case(true).await;
}

#[tokio::test]
async fn prior_execution_drain_is_shared_by_cuts_but_not_by_new_executions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let creates = Arc::new(AtomicUsize::new(0));
    let arrived = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server_reads = reads.clone();
    let server_creates = creates.clone();
    let server_arrived = arrived.clone();
    let server_release = release.clone();
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let reads = server_reads.clone();
            let creates = server_creates.clone();
            let arrived = server_arrived.clone();
            let release = server_release.clone();
            tasks.spawn(async move {
                let sql = request(&mut socket).await;
                if sql.contains("system.processes") {
                    reads.fetch_add(1, Ordering::SeqCst);
                    arrived.notify_one();
                    release.notified().await;
                }
                if sql.contains("CREATE TABLE") {
                    creates.fetch_add(1, Ordering::SeqCst);
                }
                let body = if sql.contains("SELECT") { vec![0; 8] } else { vec![] };
                let header = format!(
                    "HTTP/1.1 200 Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            });
        }
    }));
    let cuts = single_cuts(address);
    let execution = ScanDatabaseExecution::new(&single_config(address)).unwrap();
    let first = cuts.clone();
    let first_execution = execution.clone();
    let mut drain = AbortOnDropHandle::new(tokio::spawn(async move {
        with_scan_cancellation(CancellationToken::new(), Some(first_execution), first.capture()).await
    }));
    timeout(Duration::from_secs(1), arrived.notified()).await.unwrap();
    assert!(timeout(Duration::from_millis(20), &mut drain).await.is_err());
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    release.notify_one();
    drain.await.unwrap().unwrap();

    // 同一执行的后续检查点只克隆已排空的 working；旧执行检查不能再次阻塞它。
    let captured = timeout(
        Duration::from_millis(500),
        with_scan_cancellation(CancellationToken::new(), Some(execution), cuts.clone().capture()),
    )
    .await;
    assert!(captured.is_ok(), "a later checkpoint repeated the old-execution drain");
    captured.unwrap().unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(creates.load(Ordering::SeqCst), 4);

    // 同一 job 的新执行必须重新排空，不能跨执行复用成功证明。
    let fresh = cuts.clone();
    let next_execution = ScanDatabaseExecution::new(&single_config(address)).unwrap();
    let mut drain = AbortOnDropHandle::new(tokio::spawn(async move {
        with_scan_cancellation(CancellationToken::new(), Some(next_execution), fresh.capture()).await
    }));
    timeout(Duration::from_secs(1), arrived.notified()).await.unwrap();
    assert!(timeout(Duration::from_millis(20), &mut drain).await.is_err());
    release.notify_one();
    drain.await.unwrap().unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);

    // 无执行域的公共调用也不能使用前一个扫描的缓存。
    let mut drain = AbortOnDropHandle::new(tokio::spawn(async move { cuts.capture().await }));
    timeout(Duration::from_secs(1), arrived.notified()).await.unwrap();
    assert!(timeout(Duration::from_millis(20), &mut drain).await.is_err());
    release.notify_one();
    drain.await.unwrap().unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn failed_prior_execution_drain_is_not_cached() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let server_reads = reads.clone();
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let reads = server_reads.clone();
            tasks.spawn(async move {
                let sql = request(&mut socket).await;
                let failed = sql.contains("system.processes") && reads.fetch_add(1, Ordering::SeqCst) == 0;
                let (status, body) = if failed {
                    (500, b"Code: 516. AUTHENTICATION_FAILED".to_vec())
                } else {
                    (200, vec![0; 8])
                };
                let header = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            });
        }
    }));
    let cuts = single_cuts(address);
    let execution = ScanDatabaseExecution::new(&single_config(address)).unwrap();
    with_scan_cancellation(CancellationToken::new(), Some(execution), async {
        assert!(cuts.capture().await.is_err());
        cuts.capture().await.unwrap();
        cuts.clone().capture().await.unwrap();
    })
    .await;
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recovered_preflight_read_does_not_charge_healthy_capture_to_its_recovery_window() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let failed = Arc::new(AtomicBool::new(false));
    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let failed = failed.clone();
            tasks.spawn(async move {
                let sql = request(&mut socket).await;
                let first_read = sql.contains("system.processes") && !failed.swap(true, Ordering::SeqCst);
                if sql.contains("CREATE TABLE") {
                    sleep(Duration::from_millis(3100)).await;
                }
                let body = if first_read {
                    b"Code: 210. NETWORK_ERROR".to_vec()
                } else if sql.contains("SELECT 1") {
                    vec![1]
                } else if sql.contains("SELECT") {
                    vec![0; 8]
                } else {
                    vec![]
                };
                let status = if first_read { 503 } else { 200 };
                let header = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            });
        }
    }));
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: format!("http://{address}"),
        database: "capture_test".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 10,
    };
    let execution = ScanDatabaseExecution::new(&config).unwrap();
    let client = build_client(&config, Some(execution.transport()))
        .unwrap()
        .with_validation(false)
        .with_compression(Compression::None);
    let cuts = ScanCuts::with_client(&config, "job", "owner", client).unwrap();
    let monitor = execution.start_monitor().unwrap();
    let stop = execution.clone();
    let stopper = AbortOnDropHandle::new(tokio::spawn(async move {
        stop.failed().await;
        stop.begin_stop();
    }));
    let result = timeout(
        Duration::from_secs(6),
        with_scan_cancellation(CancellationToken::new(), Some(execution.clone()), cuts.capture()),
    )
    .await
    .unwrap();
    stopper.abort();
    let _ = stopper.await;
    monitor.finish().await.unwrap();
    server.abort();
    let _ = server.await;
    assert!(
        result.is_ok(),
        "a recovered read must not time out unrelated healthy capture work: {result:?}"
    );
    assert!(execution.failure().is_none());
}

struct CleanupGates {
    names: Vec<String>,
    keep: String,
    slow: String,
    failing: Option<String>,
    arrived: Barrier,
    slow_started: Notify,
    release: Notify,
    drops: AtomicUsize,
}

async fn cleanup_request(mut socket: TcpStream, gates: Arc<CleanupGates>) {
    let sql = request(&mut socket).await;
    let mut status = 200;
    let mut body = Vec::new();
    if sql.contains("SELECT") {
        for name in &gates.names {
            assert!(name.len() < 128);
            body.push(name.len() as u8);
            body.extend_from_slice(name.as_bytes());
        }
    } else {
        assert!(sql.contains("DROP TABLE"));
        assert!(!sql.contains(&gates.keep));
        gates.drops.fetch_add(1, Ordering::SeqCst);
        gates.arrived.wait().await;
        if sql.contains(&gates.slow) {
            gates.slow_started.notify_one();
            gates.release.notified().await;
        }
        if gates.failing.as_ref().is_some_and(|name| sql.contains(name)) {
            status = 500;
            body.extend_from_slice(b"cleanup failed");
        }
    }
    let header = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
}

async fn cleanup_case(fail: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cuts = single_cuts(listener.local_addr().unwrap());
    let keep = "a".repeat(32);
    let mut names = Vec::new();
    for token in ['a', 'b', 'c', 'd', 'e'] {
        for suffix in ["base", "events"] {
            names.push(format!("{}{}_{suffix}", cuts.prefix, token.to_string().repeat(32)));
        }
    }
    let gates = Arc::new(CleanupGates {
        slow: format!("{}_events", "c".repeat(32)),
        failing: fail.then(|| format!("{}_base", "b".repeat(32))),
        names,
        keep: keep.clone(),
        arrived: Barrier::new(4),
        slow_started: Notify::new(),
        release: Notify::new(),
        drops: AtomicUsize::new(0),
    });
    let server_gates = gates.clone();
    let _server = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut requests = JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            requests.spawn(cleanup_request(socket, server_gates.clone()));
        }
    }));
    let mut cleanup = AbortOnDropHandle::new(tokio::spawn(async move { cuts.cleanup(&[&keep]).await }));
    timeout(Duration::from_secs(3), gates.slow_started.notified())
        .await
        .unwrap();
    assert_eq!(gates.drops.load(Ordering::SeqCst), 4);
    assert!(timeout(Duration::from_millis(100), &mut cleanup).await.is_err());
    assert_eq!(
        gates.drops.load(Ordering::SeqCst),
        4,
        "next group must wait for every started DROP"
    );
    gates.release.notify_one();
    let result = timeout(Duration::from_secs(3), cleanup).await.unwrap().unwrap();
    assert_eq!(result.is_err(), fail);
    assert_eq!(gates.drops.load(Ordering::SeqCst), if fail { 4 } else { 8 });
}

#[tokio::test]
async fn cleanup_keeps_referenced_cuts_and_drops_unreferenced_tables_in_bounded_groups() {
    cleanup_case(false).await;
}

#[tokio::test]
async fn cleanup_waits_for_every_started_drop_before_returning_failure() {
    cleanup_case(true).await;
}
