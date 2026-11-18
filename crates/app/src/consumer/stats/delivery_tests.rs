#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::super::callback_state::CallbackState;
use super::super::callback_state::errors;
use super::super::{FullStats, ProgressBar, StatsKind};
use super::*;
use crate::config::JobType;
use crate::report::ProgressReport;
use clickhouse::Client as DatabaseClient;
use db::ClickHouseConfig;
use std::{env, thread};
use task_model::diagnostics::{CallbackAck, DiagnosticBatch};
use task_model::{ErrorEvent, StorageEntryMessage};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Builder,
    task::JoinHandle,
};
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

#[tokio::test]
async fn http_2xx_clears_only_current_details_and_keeps_cumulative_errors_without_ack_body() {
    let server = server(vec![Reply::NoContent, Reply::Accepted, Reply::WrongAck]).await;
    let consumer = consumer(&server.endpoint);
    {
        let mut state = consumer.lock().await;
        let batch = state.callback.outbox.prepare_batch().unwrap();
        state.callback.outbox.ack(&batch.batch_id);
    }
    let client = StatisticConsumer::build_callback_client(&server.endpoint).unwrap();
    for index in 1..=3 {
        consumer.lock().await.update_statistics(&StorageEntryMessage::Error {
            event: ErrorEvent::Copy,
            path: format!("file-{index}").into(),
            entry: None,
            reason: format!("failure-{index}"),
        });
        assert!(
            StatisticConsumer::post_callback(&consumer, &client, &server.endpoint, false)
                .await
                .is_some()
        );
        let mut state = consumer.lock().await;
        assert!(state.callback.outbox.prepare_batch().is_none());
        assert_eq!(errors(&state.stats).total(), index);
    }
    let bodies = server.bodies.lock().await;
    assert_eq!(bodies.len(), 3);
    for (index, raw) in bodies.iter().enumerate() {
        let report: ProgressReport = serde_json::from_str(raw).unwrap();
        let events = report.diagnostics.unwrap().events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, format!("failure-{}", index + 1));
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Reply {
    Disconnect,
    Refuse,
    WrongAck,
    Redirect,
    NoContent,
    Accepted,
    Ack,
    Stall,
}

pub(crate) struct Server {
    pub(crate) endpoint: CallbackEndpoint,
    pub(crate) bodies: Arc<Mutex<Vec<String>>>,
    handle: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn body(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = stream.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return String::from_utf8(bytes[end + 4..end + 4 + length].to_vec()).unwrap();
            }
        }
    }
}

pub(crate) async fn server(mut replies: Vec<Reply>) -> Server {
    if replies
        .last()
        .is_some_and(|reply| matches!(reply, Reply::Ack | Reply::WrongAck | Reply::NoContent | Reply::Accepted))
    {
        replies.extend([Reply::Ack; 2]);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = CallbackEndpoint::parse(&format!(
        "http://user-canary:secret-canary@{}/private?token=token-canary",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let recorded = bodies.clone();
    let handle = tokio::spawn(async move {
        // 每个连接独立处理，静默连接不能阻塞后续重试的 accept。
        let mut connections = Vec::new();
        for reply in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let raw = body(&mut stream).await;
            let report: ProgressReport = serde_json::from_str(&raw).unwrap();
            recorded.lock().await.push(raw);
            if matches!(reply, Reply::Disconnect) {
                continue;
            }
            connections.push(tokio::spawn(async move {
                if matches!(reply, Reply::Stall) {
                    let mut buffer = [0; 1];
                    let _ = stream.read(&mut buffer).await;
                    return;
                }
                let batch = report.diagnostics.map(|batch| batch.batch_id);
                let batch_id = if matches!(reply, Reply::WrongAck) { Some("wrong".into()) } else { batch };
                let raw = match reply {
                    Reply::NoContent => String::new(),
                    Reply::Accepted => "not-json".into(),
                    _ => serde_json::to_string(&CallbackAck { batch_id }).unwrap(),
                };
                let status = match reply { Reply::Refuse => "500 Internal Server Error", Reply::Redirect => "302 Found", Reply::NoContent => "204 No Content", Reply::Accepted => "202 Accepted", _ => "200 OK" };
                let location = if matches!(reply, Reply::Redirect) { "Location: /redirected\r\n" } else { "" };
                let response = format!("HTTP/1.1 {status}\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}", raw.len());
                let _ = stream.write_all(response.as_bytes()).await;
            }));
        }
        for connection in connections {
            connection.await.unwrap();
        }
    });
    Server {
        endpoint,
        bodies,
        handle,
    }
}

pub(crate) fn consumer(endpoint: &CallbackEndpoint) -> Arc<Mutex<StatisticConsumer>> {
    let mut callback = CallbackState::default();
    callback.outbox.enqueue(
        "copy",
        "copy_version_failed",
        "version=v7 HTTP=403 code=AccessDenied",
        Some("payload"),
    );
    Arc::new(Mutex::new(StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Copy,
            "callback-final".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Copy),
        job_dir: String::new(),
        callback_url: Some(endpoint.clone()),
        generation_committed: None,
        migration: None,
        scan_snapshot: None,
        callback,
        pb_handle: None,
    }))
}

#[tokio::test]
async fn final_retries_same_bytes_across_disconnect_refusal_redirect_then_success() {
    let server = server(vec![
        Reply::Disconnect,
        Reply::Refuse,
        Reply::Redirect,
        Reply::NoContent,
    ])
    .await;
    let consumer = consumer(&server.endpoint);
    let original = consumer.lock().await.callback.outbox.prepare_batch().unwrap();
    let client = StatisticConsumer::build_callback_client(&server.endpoint).unwrap();
    send_final(
        &consumer,
        Some(&client),
        Some(&server.endpoint),
        None,
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .await;
    let bodies = server.bodies.lock().await;
    assert_eq!(bodies.len(), 5);
    assert!(bodies[..4].iter().all(|body| body == &bodies[0]));
    let summary: ProgressReport = serde_json::from_str(&bodies[4]).unwrap();
    assert!(summary.is_final && summary.diagnostics_complete && summary.diagnostics.is_none());
    let report: ProgressReport = serde_json::from_str(&bodies[0]).unwrap();
    assert!(report.is_final);
    assert_eq!(report.diagnostics, Some(original));
    assert!(consumer.lock().await.callback.outbox.prepare_batch().is_none());
    assert_eq!(consumer.lock().await.callback.identity.sequence, 2);
}

#[tokio::test]
async fn stalled_requests_have_four_bounded_attempts_and_leave_errors_unacknowledged() {
    let server = server(vec![Reply::Stall; 4]).await;
    let consumer = consumer(&server.endpoint);
    let client = StatisticConsumer::build_callback_client(&server.endpoint).unwrap();
    let started = Instant::now();
    send_final(
        &consumer,
        Some(&client),
        Some(&server.endpoint),
        None,
        Duration::from_millis(500),
        Duration::from_millis(500),
    )
    .await;
    assert!(started.elapsed() < Duration::from_secs(1));
    let bodies = server.bodies.lock().await;
    assert_eq!(bodies.len(), 4);
    assert!(bodies.iter().all(|body| body == &bodies[0]));
    assert!(consumer.lock().await.callback.outbox.prepare_batch().is_some());
}

pub(crate) struct LiveDatabase {
    pub(crate) config: ClickHouseConfig,
}
impl Drop for LiveDatabase {
    fn drop(&mut self) {
        let config = self.config.clone();
        thread::spawn(move || {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut client = DatabaseClient::default()
                        .with_url(&config.dsn)
                        .with_user(&config.username);
                    if let Some(password) = &config.password {
                        client = client.with_password(password);
                    }
                    let cluster = config
                        .cluster_name()
                        .unwrap()
                        .map(|name| format!(" ON CLUSTER `{name}`"))
                        .unwrap_or_default();
                    timeout(
                        Duration::from_secs(5),
                        client
                            .query(&format!("DROP DATABASE {}{cluster} SYNC", config.database))
                            .execute(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                });
        })
        .join()
        .unwrap();
    }
}

pub(crate) async fn live_database() -> LiveDatabase {
    let cluster = env::var("LAB_NOTIFICATION_CLICKHOUSE_CLUSTER").ok();
    let mut config = ClickHouseConfig {
        dsn: env::var("LAB_NOTIFICATION_CLICKHOUSE_DSN").expect("explicit isolated service required"),
        mode: if cluster.is_some() {
            ClickHouseMode::Cluster
        } else {
            ClickHouseMode::Single
        },
        cluster,
        database: format!("ts_callback_{}", Uuid::new_v4().simple()),
        username: env::var("LAB_NOTIFICATION_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: env::var("LAB_NOTIFICATION_CLICKHOUSE_PASSWORD").ok(),
        dial_timeout: 1,
        read_timeout: 1,
    };
    let mut client = DatabaseClient::default()
        .with_url(&config.dsn)
        .with_user(&config.username);
    if let Some(password) = &config.password {
        client = client.with_password(password);
    }
    let cluster = config
        .cluster_name()
        .unwrap()
        .map(|name| format!(" ON CLUSTER `{name}`"))
        .unwrap_or_default();
    client
        .query(&format!("CREATE DATABASE {}{cluster}", config.database))
        .execute()
        .await
        .unwrap();
    // 仅归档写入走受控代理，最终事实查询绕过代理核验真实结果。
    config.dsn = format!("http://{}", env::var("LAB_NOTIFICATION_PROXY_ADDR").unwrap());
    LiveDatabase { config }
}

pub(crate) async fn fault(mode: &str) {
    let address = env::var("LAB_NOTIFICATION_PROXY_ADDR").unwrap();
    let mut stream = TcpStream::connect(&address).await.unwrap();
    stream
        .write_all(format!("GET /control?mode={mode} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    assert!(raw.starts_with(b"HTTP/1.0 200"));
}

#[tokio::test]
#[ignore = "explicit isolated ClickHouse single/cluster service and fault proxy required"]
async fn real_final_delivery_archives_exact_payload_without_delivery_error_or_duplicate_counting() {
    let live = live_database().await;
    let database = ClickHouseDatabase::new(&live.config, "callback-final").unwrap();
    for (mode, replies, should_archive, delivered) in [
        (
            "normal",
            vec![Reply::Disconnect, Reply::Refuse, Reply::Redirect, Reply::NoContent],
            true,
            true,
        ),
        ("normal", vec![Reply::Refuse; 4], true, false),
        ("lost_ack", vec![Reply::Disconnect; 4], true, false),
        ("normal", vec![Reply::Stall; 4], true, false),
        ("disconnect", vec![Reply::Refuse; 4], true, false),
        ("stall", vec![Reply::Refuse; 4], true, false),
        ("disconnect", vec![Reply::Ack], true, true),
        ("stall", vec![Reply::Ack], true, true),
        ("ack_lost_reply", vec![Reply::Ack], true, true),
        ("normal", vec![], true, false),
    ] {
        let callback_configured = !replies.is_empty();
        let initial_attempts = replies.len();
        let expected_attempts = initial_attempts + usize::from(delivered) * (1 + usize::from(mode == "stall"));
        fault(mode).await;
        let server = server(replies).await;
        let consumer = consumer(&server.endpoint);
        if !callback_configured {
            consumer.lock().await.callback_url = None;
        }
        let identity = consumer.lock().await.callback.identity.clone();
        let round = identity.round_id.clone();
        let original: DiagnosticBatch = consumer.lock().await.callback.outbox.prepare_batch().unwrap();
        let started = Instant::now();
        // 与任务取消收尾相同的预算；取消的数据库作用域不阻止独立归档。
        let cancel = CancellationToken::new();
        cancel.cancel();
        with_scan_cancellation(
            cancel,
            None,
            StatisticConsumer::end_with_database(consumer.clone(), None, Some(&database), true),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(2), "{mode}: {elapsed:?}");
        fault("normal").await;
        let saved_record = database.final_report(&round, &identity.attempt_id, 1).await.unwrap();
        assert_eq!(saved_record.is_some(), should_archive, "{mode}");
        let pending = database
            .pending_notifications(&round, NotificationKind::FinalReport)
            .await
            .unwrap();
        assert_eq!(pending.len(), usize::from(should_archive && !delivered), "{mode}");
        let bodies = server.bodies.lock().await;
        assert_eq!(bodies.len(), expected_attempts);
        assert!(bodies[..initial_attempts].iter().all(|body| body == &bodies[0]));
        if should_archive {
            let record = saved_record.as_ref().unwrap();
            assert_eq!(record.is_delivered(), delivered);
            let raw = record.payload_json();
            let saved: FinalCallbackReport = serde_json::from_str(raw).unwrap();
            if callback_configured {
                assert_eq!(saved.payload_json, bodies[0]);
            }
            let report: ProgressReport = serde_json::from_str(&saved.payload_json).unwrap();
            assert!(report.is_final);
            assert_eq!(saved.job_id, "callback-final");
            let mut first_identity = identity.clone();
            first_identity.sequence = 1;
            assert_eq!(saved.identity, first_identity);
            let envelope: serde_json::Value = serde_json::from_str(raw).unwrap();
            assert!(envelope.get("failure").is_none());
            assert!(envelope.get("error").is_none());
            // 原始迁移错误保留；callback 认证和发送失败原因不入库。
            assert!(raw.contains("AccessDenied"));
            for canary in [
                "user-canary",
                "secret-canary",
                "token-canary",
                "Internal Server Error",
                "callback HTTP request failed",
            ] {
                assert!(!raw.contains(canary), "{canary}");
            }
            let pending = consumer.lock().await.callback.outbox.prepare_batch();
            let archived = database
                .pending_notifications(&round, NotificationKind::Diagnostic)
                .await
                .unwrap();
            if delivered {
                assert!(pending.is_none() && archived.is_empty());
            } else {
                assert!(pending.is_some() || !archived.is_empty());
            }
            let report: ProgressReport = serde_json::from_str(&saved.payload_json).unwrap();
            let events = report.diagnostics.unwrap().events;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.code != "journal_unavailable")
                    .cloned()
                    .collect::<Vec<_>>(),
                original.events
            );
            if mode == "stall" {
                assert_eq!(report.task_error_count, 1);
                assert!(!report.diagnostics_complete);
                let sent: Vec<ProgressReport> = bodies.iter().map(|body| serde_json::from_str(body).unwrap()).collect();
                // 首批已冻结；收尾超时的新原因在后续成功批次发送，或仍在待发尾部。
                let retained = consumer.lock().await.callback.outbox.archive_prefix();
                assert!(
                    sent.iter()
                        .filter_map(|report| report.diagnostics.as_ref())
                        .flat_map(|batch| &batch.events)
                        .chain(&retained)
                        .any(|event| { event.code == "journal_unavailable" && event.message.contains("timed out") })
                );
            }
        }
        assert_eq!(
            consumer.lock().await.callback.identity.sequence,
            if delivered { 2 + u64::from(mode == "stall") } else { 1 }
        );
        println!(
            "callback_final mode={mode} configured={callback_configured} archived={} attempts={expected_attempts} elapsed_ms={} payload_changed=0 delivery_error_stored=0",
            usize::from(saved_record.is_some()),
            elapsed.as_millis()
        );
    }
}
