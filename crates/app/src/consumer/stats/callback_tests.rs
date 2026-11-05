//! 真实 HTTP 失败重试时批次不变，ACK 不清并发新事件和累计数字。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::{
    config::JobType,
    consumer::stats::{FullStats, ProgressBar},
};
use storage_adapter::ErrorEvent;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn failed_http_retries_same_batch_and_ack_preserves_new_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/progress", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut batches = Vec::new();
        for index in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let body_start;
            loop {
                let mut chunk = [0u8; 4096];
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
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        body_start = end + 4;
                        break;
                    }
                }
            }
            let report: ProgressReport = serde_json::from_slice(&bytes[body_start..]).unwrap();
            let batch = report.diagnostics.unwrap();
            let body = serde_json::to_string(&CallbackAck {
                batch_id: Some(batch.batch_id.clone()),
            })
            .unwrap();
            batches.push(batch);
            let status = if index == 0 {
                "500 Internal Server Error"
            } else {
                "200 OK"
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        assert_eq!(batches[0], batches[1]);
        assert_ne!(batches[1].events[0].event_id, batches[2].events[0].event_id);
        assert_eq!(batches[2].events[0].message, "second");
    });
    let consumer = Arc::new(Mutex::new(StatisticConsumer {
        stats: StatsKind::Full(FullStats::new(
            JobType::Scan,
            "http-fixture".into(),
            String::new(),
            String::new(),
        )),
        progress_bar: ProgressBar::new(JobType::Scan),
        job_dir: String::new(),
        callback_url: Some(url.clone()),
        generation_committed: None,
        scan_snapshot: None,
        callback: CallbackState::default(),
        pb_handle: None,
    }));
    let error = |reason: &str| StorageEntryMessage::Error {
        event: ErrorEvent::Scan,
        path: "fixture".into(),
        entry: None,
        reason: reason.into(),
    };
    consumer.lock().await.update_statistics(&error("first"));
    let client = StatisticConsumer::build_callback_client(&url).unwrap();
    assert!(!StatisticConsumer::post_callback(&consumer, &client, &url, false).await);
    consumer.lock().await.update_statistics(&error("second"));
    assert!(StatisticConsumer::post_callback(&consumer, &client, &url, false).await);
    {
        let mut state = consumer.lock().await;
        assert_eq!(errors(&state.stats).total(), 2);
        assert_eq!(state.callback.runtime.current_error.as_deref(), Some("second"));
        assert_eq!(
            state.callback.outbox.prepare_batch().unwrap().events[0].message,
            "second"
        );
    }
    assert!(StatisticConsumer::post_callback(&consumer, &client, &url, true).await);
    assert!(consumer.lock().await.callback.outbox.prepare_batch().is_none());
    assert_eq!(errors(&consumer.lock().await.stats).total(), 2);
    server.await.unwrap();
}
