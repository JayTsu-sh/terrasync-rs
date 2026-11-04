//! 两个 HTTP CREATE 都到达后才放行，验证真实捕获链路并行且不会提前发布 token。
#![allow(clippy::unwrap_used)]

use super::ScanCuts;
use crate::ClickHouseConfig;
use clickhouse::Compression;
use std::{str::from_utf8, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Barrier, Notify},
    task::JoinSet,
    time::timeout,
};
use tokio_util::task::AbortOnDropHandle;
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
    let mut cuts = ScanCuts::new(
        &ClickHouseConfig {
            mode: ClickHouseMode::Single,
            cluster: None,
            dsn: format!("http://{address}"),
            database: "capture_test".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        },
        "job",
        "owner",
    )
    .unwrap();
    cuts.client = cuts.client.with_validation(false).with_compression(Compression::None);
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
