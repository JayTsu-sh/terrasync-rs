#![allow(clippy::unwrap_used)]

use std::env;
use std::thread;
use std::time::{Duration, Instant};

use clickhouse::test::{Mock, handlers};
use clickhouse::{Client, Compression};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::*;
use crate::ClickHouseConfig;
use crate::execution::base_client;

fn record(sequence: u64) -> Result<NotificationRecord> {
    NotificationRecord::new(
        "round".into(),
        "attempt".into(),
        sequence,
        NotificationKind::Diagnostic,
        format!("{{\"version\":\"v{sequence}\",\"cause\":\"AccessDenied\"}}"),
    )
}

fn mocked_database(mock: &Mock) -> Result<ClickHouseDatabase> {
    let config = ClickHouseConfig {
        mode: ClickHouseMode::Single,
        cluster: None,
        dsn: mock.url().into(),
        database: "default".into(),
        username: "default".into(),
        password: None,
        dial_timeout: 1,
        read_timeout: 1,
    };
    ClickHouseDatabase::with_client(
        &config,
        "notification-test",
        Client::default()
            .with_url(mock.url())
            .with_validation(false)
            .with_compression(Compression::None),
    )
}

#[test]
fn content_identity_covers_original_scope_and_exact_json_and_rejects_corruption() -> Result<()> {
    let original = record(1)?;
    assert_eq!(original.record_id(), record(1)?.record_id());
    assert_ne!(original.record_id(), record(2)?.record_id());
    let mut changed = original.clone();
    changed.payload_json.push(' ');
    assert!(matches!(
        changed.validate(),
        Err(DatabaseError::NotificationRecordInvalid {
            field: "content_digest"
        })
    ));
    assert!(
        NotificationRecord::new(
            String::new(),
            "attempt".into(),
            1,
            NotificationKind::Diagnostic,
            "{}".into()
        )
        .is_err()
    );
    assert!(
        NotificationRecord::new(
            "round".into(),
            "attempt".into(),
            0,
            NotificationKind::Diagnostic,
            "{}".into()
        )
        .is_err()
    );
    assert!(
        NotificationRecord::new(
            "round".into(),
            "attempt".into(),
            1,
            NotificationKind::Diagnostic,
            "not JSON".into()
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn oversize_archive_batches_fail_before_any_request_and_empty_batches_do_not_create_tables() -> Result<()> {
    let mock = Mock::new();
    let database = mocked_database(&mock)?;
    let records = vec![record(1)?; MAX_NOTIFICATION_BATCH_RECORDS + 1];
    assert!(matches!(
        database.archive_notifications(&records).await,
        Err(DatabaseError::NotificationBatchTooLarge { .. })
    ));
    let large = format!("\"{}\"", "x".repeat(MAX_NOTIFICATION_RECORD_BYTES - 2));
    let records = (1..=5)
        .map(|sequence| {
            NotificationRecord::new(
                "round".into(),
                "attempt".into(),
                sequence,
                NotificationKind::Diagnostic,
                large.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(matches!(database.archive_notifications(&records).await,
        Err(DatabaseError::NotificationBatchTooLarge { bytes, .. }) if bytes == 5 * MAX_NOTIFICATION_RECORD_BYTES));
    database.archive_notifications(&[]).await?;
    database.acknowledge_notifications(&[]).await?;
    assert!(matches!(
        database
            .pending_notifications_after("other-round", NotificationKind::Diagnostic, Some(&record(1)?.cursor()))
            .await,
        Err(DatabaseError::NotificationRecordInvalid { field: "cursor_scope" })
    ));
    assert!(database.notifications_initialized.get().is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit isolated ClickHouse service; verifies byte-bounded pending reads"]
async fn real_archive_pending_batches_respect_payload_bytes_without_truncation() -> Result<()> {
    let (config, _guard) = isolated_database().await?;
    let database = ClickHouseDatabase::new(&config, "large-errors")?;
    let raw = format!("{{\"cause\":\"{}\"}}", "x".repeat(800_000));
    let records = (1..=7)
        .map(|sequence| {
            NotificationRecord::new(
                "round".into(),
                "attempt".into(),
                sequence,
                NotificationKind::Diagnostic,
                raw.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    for batch in records.chunks(4) {
        database.archive_notifications(batch).await?;
    }
    let mut counts = Vec::new();
    loop {
        let batch = database
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?;
        if batch.is_empty() {
            break;
        }
        assert!(batch.iter().map(|record| record.payload_json().len()).sum::<usize>() <= MAX_NOTIFICATION_BATCH_BYTES);
        assert!(batch.iter().all(|record| record.payload_json() == raw));
        counts.push(batch.len());
        database.acknowledge_notifications(&batch).await?;
    }
    assert_eq!(counts, [5, 2]);
    println!("raw_payload_bytes={} batch_counts=5,2 truncation=0", raw.len());
    Ok(())
}

#[tokio::test]
async fn archive_and_ack_write_exactly_the_same_identity_and_payload_with_monotonic_revision() -> Result<()> {
    let mock = Mock::new();
    let database = mocked_database(&mock)?;
    let ddl = mock.add(handlers::record_ddl());
    let archived = mock.add(handlers::record::<NotificationRecord>());
    let expected = vec![record(1)?, record(2)?];
    database.archive_notifications(&expected).await?;
    let ddl = ddl.query().await;
    assert!(ddl.contains("ReplacingMergeTree(`delivery_revision`)"), "{ddl}");
    let rows: Vec<NotificationRecord> = archived.collect().await;
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip(&expected) {
        assert_eq!(row.record_id(), expected.record_id());
        assert_eq!(row.payload_json(), expected.payload_json());
        assert_eq!(row.delivery_revision, 0);
    }
    let acked = mock.add(handlers::record::<NotificationRecord>());
    database.acknowledge_notifications(&expected).await?;
    let rows: Vec<NotificationRecord> = acked.collect().await;
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip(&expected) {
        assert_eq!(row.record_id(), expected.record_id());
        assert_eq!(row.payload_json(), expected.payload_json());
        assert_eq!(row.delivery_revision, 1);
    }
    Ok(())
}

struct DatabaseGuard {
    config: ClickHouseConfig,
    database: String,
}

impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        let config = self.config.clone();
        let database = self.database.clone();
        let _ = thread::spawn(move || {
            if let Ok(runtime) = Builder::new_current_thread().enable_all().build() {
                let _ = runtime.block_on(async {
                    let admin = base_client(&config)?;
                    let cluster = config
                        .cluster_name()?
                        .map(|name| format!(" ON CLUSTER `{name}`"))
                        .unwrap_or_default();
                    admin
                        .query(&format!("DROP DATABASE IF EXISTS {database}{cluster} SYNC"))
                        .execute()
                        .await?;
                    Ok::<_, DatabaseError>(())
                });
            }
        })
        .join();
    }
}

async fn isolated_database() -> Result<(ClickHouseConfig, DatabaseGuard)> {
    let dsn = env::var("LAB_NOTIFICATION_CLICKHOUSE_DSN").map_err(|_| {
        DatabaseError::ConfigError("LAB_NOTIFICATION_CLICKHOUSE_DSN must explicitly select a test service".into())
    })?;
    let cluster = env::var("LAB_NOTIFICATION_CLICKHOUSE_CLUSTER").ok();
    let mut config = ClickHouseConfig {
        mode: if cluster.is_some() {
            ClickHouseMode::Cluster
        } else {
            ClickHouseMode::Single
        },
        cluster,
        dsn,
        database: "default".into(),
        username: env::var("LAB_NOTIFICATION_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: env::var("LAB_NOTIFICATION_CLICKHOUSE_PASSWORD").ok(),
        dial_timeout: 1,
        read_timeout: 2,
    };
    let admin = base_client(&config)?;
    let name = format!("ts_notifications_{}", Uuid::new_v4().simple());
    let cluster = config
        .cluster_name()?
        .map(|name| format!(" ON CLUSTER `{name}`"))
        .unwrap_or_default();
    admin
        .query(&format!("CREATE DATABASE {name}{cluster}"))
        .execute()
        .await?;
    let guard = DatabaseGuard {
        config: config.clone(),
        database: name.clone(),
    };
    config.database = name;
    Ok((config, guard))
}

async fn control_proxy(mode: &str) -> Result<()> {
    let address = env::var("LAB_NOTIFICATION_PROXY_ADDR").map_err(|_| {
        DatabaseError::ConfigError("LAB_NOTIFICATION_PROXY_ADDR is required for real fault injection".into())
    })?;
    let mut stream = TcpStream::connect(&address).await?;
    stream
        .write_all(format!("GET /control?mode={mode} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    if !reply.starts_with(b"HTTP/1.0 200") {
        return Err(DatabaseError::ConfigError(
            "notification fault proxy control failed".into(),
        ));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated ClickHouse and controlled network proxy"]
async fn real_archive_faults_keep_unknown_outcomes_and_replay_without_losing_events() -> Result<()> {
    let (config, _guard) = isolated_database().await?;
    let database = ClickHouseDatabase::new(&config, "archive-faults")?;
    database.ensure_notification_archive().await?;
    let mut proxy_config = config.clone();
    proxy_config.dsn = format!(
        "http://{}",
        env::var("LAB_NOTIFICATION_PROXY_ADDR")
            .map_err(|_| DatabaseError::ConfigError("fault proxy address missing".into()))?
    );
    proxy_config.read_timeout = 1;
    let proxy = ClickHouseDatabase::new(&proxy_config, "archive-faults")?;
    proxy.ensure_notification_archive().await?;
    for (index, mode) in ["disconnect", "lost_ack", "stall"].into_iter().enumerate() {
        let event = record(10_000 + index as u64)?;
        control_proxy(mode).await?;
        let started = Instant::now();
        let failure = proxy.archive_notifications(std::slice::from_ref(&event)).await;
        assert!(
            matches!(&failure, Err(DatabaseError::RequestOutcomeUnconfirmed { .. })),
            "{mode}: {failure:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        control_proxy("normal").await?;
        let before = database
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?;
        assert_eq!(
            before.iter().any(|found| found.record_id() == event.record_id()),
            mode == "lost_ack"
        );
        proxy.archive_notifications(std::slice::from_ref(&event)).await?;
        let pending = database
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?;
        assert_eq!(
            pending
                .iter()
                .filter(|found| found.record_id() == event.record_id())
                .count(),
            1
        );
        control_proxy("lost_ack").await?;
        let ack = proxy.acknowledge_notifications(&pending).await;
        assert!(
            matches!(ack, Err(DatabaseError::RequestOutcomeUnconfirmed { .. })),
            "{ack:?}"
        );
        control_proxy("normal").await?;
        assert!(
            database
                .pending_notifications("round", NotificationKind::Diagnostic)
                .await?
                .is_empty()
        );
        proxy.acknowledge_notifications(&pending).await?;
        println!(
            "archive_fault={mode} archive_unknown=confirmed ack_unknown=confirmed replay_duplicates=0 elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }
    let mut readonly = ClickHouseDatabase::new(&config, "archive-faults")?;
    readonly.notifications_initialized = database.notifications_initialized.clone();
    readonly.sync_client = readonly.sync_client.with_option("readonly", "1");
    let refusal = readonly.archive_notifications(&[record(20_000)?]).await;
    assert!(
        matches!(&refusal, Err(DatabaseError::ClickHouseError(_))),
        "{refusal:?}"
    );
    assert!(
        database
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?
            .is_empty()
    );
    println!("readonly_archive=confirmed_refusal network_classification=false");
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit isolated ClickHouse service; creates and removes its own database"]
async fn real_archive_reopens_beyond_memory_limit_and_late_inserts_cannot_revoke_acks() -> Result<()> {
    let (config, _guard) = isolated_database().await?;
    let database = ClickHouseDatabase::new(&config, "history-errors")?;
    let records: Vec<_> = (1..=1024).map(record).collect::<Result<_>>()?;
    for batch in records.chunks(MAX_NOTIFICATION_BATCH_RECORDS) {
        database.archive_notifications(batch).await?;
    }
    // 模拟确认丢失后的整批重放，不依赖物理 block 去重窗口来保证逻辑去重。
    database
        .archive_notifications(&records[..MAX_NOTIFICATION_BATCH_RECORDS])
        .await?;
    drop(database);
    let reopened = ClickHouseDatabase::new(&config, "history-errors")?;
    let mut delivered = Vec::new();
    let mut cursor = None;
    loop {
        let pending = reopened
            .pending_notifications_after("round", NotificationKind::Diagnostic, cursor.as_ref())
            .await?;
        if pending.is_empty() {
            break;
        }
        assert!(pending.len() <= MAX_NOTIFICATION_BATCH_RECORDS);
        for record in &pending {
            assert_eq!(
                record.payload_json(),
                records[(record.sequence() - 1) as usize].payload_json()
            );
            delivered.push(record.sequence());
        }
        reopened.acknowledge_notifications(&pending).await?;
        cursor = pending.last().map(NotificationRecord::cursor);
        // 迟到的旧请求只允许重复保存原值，不能让已 ACK 的事件回到待投递状态。
        // 使用不同批次身份，强制产生迟到写入，不能只靠插入去重跳过此断言。
        reopened.archive_notifications(&pending[..1]).await?;
    }
    assert_eq!(delivered, (1..=1024).collect::<Vec<_>>());
    // 未确认旧请求可能稍后才到达，空尾页不能替代整轮的最终待投递检查。
    let late = NotificationRecord::new(
        "round".into(),
        "0-delayed-attempt".into(),
        1,
        NotificationKind::Diagnostic,
        "{\"cause\":\"late original request\"}".into(),
    )?;
    reopened.archive_notifications(std::slice::from_ref(&late)).await?;
    assert!(
        reopened
            .pending_notifications_after("round", NotificationKind::Diagnostic, cursor.as_ref())
            .await?
            .is_empty()
    );
    let late_pending = reopened
        .pending_notifications("round", NotificationKind::Diagnostic)
        .await?;
    assert_eq!(late_pending.len(), 1);
    assert_eq!(late_pending[0].record_id(), late.record_id());
    reopened.acknowledge_notifications(&late_pending).await?;
    assert!(
        reopened
            .pending_notifications("other-round", NotificationKind::Diagnostic)
            .await?
            .is_empty()
    );
    let isolated = ClickHouseDatabase::new(&config, "another-job")?;
    assert!(
        isolated
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?
            .is_empty()
    );
    let exact = " {\"is_final\": true, \"error\": \"callback unreachable\"} \n";
    let final_failure = NotificationRecord::new(
        "round".into(),
        "attempt".into(),
        1025,
        NotificationKind::FinalDeliveryFailure,
        exact.into(),
    )?;
    reopened.archive_notifications(&[final_failure]).await?;
    assert!(
        reopened
            .pending_notifications("round", NotificationKind::Diagnostic)
            .await?
            .is_empty()
    );
    let pending = reopened
        .pending_notifications("round", NotificationKind::FinalDeliveryFailure)
        .await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].payload_json(), exact);
    let client = base_client(&config)?;
    let table = reopened.notification_table();
    let (events, acked): (u64, u64) = client
        .query(&format!(
            "SELECT count(), countIf(delivery_revision=1) FROM {table} FINAL WHERE kind=1"
        ))
        .fetch_one()
        .await?;
    assert_eq!((events, acked), (1025, 1025));
    let raw: String = client
        .query(&format!("SELECT payload_json FROM {table} FINAL WHERE kind=2"))
        .fetch_one()
        .await?;
    assert_eq!(raw, exact);
    println!("durable_errors=1024 replay_duplicates=0 late_insert_ack_regressions=0 exact_final_payload=verified");
    Ok(())
}
