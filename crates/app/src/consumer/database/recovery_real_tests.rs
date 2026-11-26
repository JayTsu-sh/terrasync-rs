//! 实际消费者的固定批次恢复、失败停收及独立终态归档。
use std::{
    error::Error as StdError,
    sync::Arc,
    time::{Duration, Instant},
};

use clickhouse::Client;
use data_mover::model::{BackendKind, EntryKind};
use db::{
    ClickHouseConfig, ClickHouseDatabase, DatabaseError, INCREMENTAL_SCAN_TABLE_BASE_NAME, NotificationKind,
    NotificationRecord,
};
use serde::Deserialize;
use task_model::migration::CopyGenerationPhase;
use task_model::{CopyCompletion, CopyReadiness, EntryChange, SourceCleanupPolicy, StorageEntryMessage, TransferMode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use utils::app_config::{AppConfig, ClickHouseMode, DatabaseConfig};
use uuid::Uuid;

use super::DatabaseConsumer;
use crate::{callback::CallbackEndpoint, orchestrator::SyncOrchestrator};
use crate::{
    config::{ConsoleConfig, ConsumerConfig, JobType, SyncJobConfig},
    consumer::traits::Consumer,
    error::{AppError, Result},
    migration_fixtures::request,
    observed_fixtures::observation,
};

struct Fixture {
    config: ClickHouseConfig,
    admin: Client,
    control: String,
}

impl Fixture {
    async fn new(cluster: bool) -> Result<Self> {
        let required =
            |name| std::env::var(name).map_err(|_| AppError::ConfigError(format!("missing owned fixture {name}")));
        let mut config = ClickHouseConfig {
            mode: if cluster {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.then(|| "notification_cluster".into()),
            dsn: required("LAB_COPY_GENERATION_DSN")?,
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let admin = Client::default()
            .with_url(&config.dsn)
            .with_database("default")
            .with_user("default");
        config.database = format!("ts_copy_consumer_{}", Uuid::new_v4().simple());
        let on_cluster = if cluster {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        admin
            .query(&format!("CREATE DATABASE {}{on_cluster}", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from)?;
        Ok(Self {
            config,
            admin,
            control: required("LAB_COPY_GENERATION_CONTROL")?,
        })
    }

    async fn control(&self, action: &str) -> Result<Vec<u8>> {
        let mut stream = TcpStream::connect(&self.control).await?;
        stream
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: {}\r\n\r\n", self.control).as_bytes())
            .await?;
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await?;
        assert!(reply.starts_with(b"HTTP/1.0 200"));
        let offset = reply
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .ok_or_else(|| AppError::ConfigError("missing proxy headers".into()))?;
        Ok(reply[offset + 4..].to_vec())
    }

    async fn cleanup(&self) -> Result<()> {
        self.control("normal").await?;
        let on_cluster = if self.config.cluster.is_some() {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        self.admin
            .query(&format!("DROP DATABASE {}{on_cluster} SYNC", self.config.database))
            .execute()
            .await
            .map_err(DatabaseError::from)?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct Stats {
    writes: u64,
    tokens: u64,
    lost: u64,
}

fn message(index: u8) -> StorageEntryMessage {
    let item = Arc::new(observation(
        BackendKind::Local,
        format!("payload-{index}").into(),
        EntryKind::File,
        1,
        [i64::from(index); 3],
        None,
        None,
        None,
        None,
    ))
    .into();
    StorageEntryMessage::CopyItemCompleted {
        completion: CopyCompletion {
            item,
            target_version: None,
            readiness: CopyReadiness::Ready,
        },
        change: EntryChange::New,
    }
}

async fn case(fixture: &Fixture, incremental: bool, mode: &str) -> Result<()> {
    let job = format!("consumer_{incremental}_{mode}");
    let flush = mode.starts_with("final_");
    let database = ClickHouseDatabase::connect(&fixture.config, &job).await?;
    database.initialize().await?;
    database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
    database.begin_copy_generation(&request("consumer", true)).await?;
    let config = ConsumerConfig {
        job_id: job.clone(),
        job_type: if incremental {
            JobType::IncrementalCopy
        } else {
            JobType::Copy
        },
        job_dir: String::new(),
        db_config: DatabaseConfig {
            enabled: true,
            batch_size: if flush { 8 } else { 1 },
            clickhouse: fixture.config.clone(),
        },
        console_config: ConsoleConfig {
            raw_command_line: String::new(),
        },
        progress_callback_url: None,
    };
    let cancel = CancellationToken::new();
    let mut consumer = DatabaseConsumer::with_database(&config, database.clone())
        .await?
        .with_cancellation(cancel.clone());
    let (sender, receiver) = mpsc::channel(4);
    let idle = mode.starts_with("idle_");
    let fault = match mode {
        "cancel" => "batch_stall",
        "idle_disconnect" | "idle_flash" => "disconnect",
        "idle_stall" => "stall",
        "final_disconnect" => "batch_disconnect",
        "final_stall" => "batch_stall",
        "final_reject" => "batch_reject",
        "final_recover_stall" => "batch_recover_stall",
        _ => mode,
    };
    fixture.control(fault).await?;
    let start = Instant::now();
    let handle = consumer.start(receiver).await?;
    if !idle {
        sender
            .send(message(1))
            .await
            .map_err(|_| AppError::ConfigError("fixture first enqueue failed".into()))?;
        sender
            .send(message(2))
            .await
            .map_err(|_| AppError::ConfigError("fixture second enqueue failed".into()))?;
    }
    if mode == "cancel" {
        sleep(Duration::from_millis(250)).await;
        cancel.cancel();
    }
    if mode == "idle_flash" {
        sleep(Duration::from_millis(1250)).await;
        fixture.control("normal").await?;
        // 允许下一轮探测确认恢复后，通道正常关闭。
        sleep(Duration::from_millis(1100)).await;
    }
    let recovered = matches!(
        mode,
        "normal" | "batch_flash" | "batch_lost" | "batch_recover_stall" | "idle_flash" | "final_recover_stall"
    );
    if recovered || flush {
        drop(sender);
    } else {
        timeout(Duration::from_secs(7), sender.closed())
            .await
            .map_err(|_| AppError::ConfigError("consumer did not close required queue".into()))?;
        drop(sender);
    }
    let result = timeout(Duration::from_secs(7), handle)
        .await
        .map_err(|_| AppError::ConfigError("consumer failed to drain".into()))??;
    let elapsed = start.elapsed();
    let stats: Stats = serde_json::from_slice(&fixture.control("batch_stats").await?)?;
    fixture.control("normal").await?;
    if recovered {
        result?;
    } else {
        let error = result
            .err()
            .ok_or_else(|| AppError::ConfigError("fault incorrectly reported success".into()))?;
        assert!(has_database_cause(&error), "{error}");
        if matches!(mode, "batch_reject" | "final_reject") {
            assert!(error.to_string().contains("owned batch rejection"), "{error}");
        }
    }
    let count = fixture
        .admin
        .clone()
        .with_database(&fixture.config.database)
        .query(&format!("SELECT count() FROM base_{job}"))
        .fetch_one::<u64>()
        .await
        .map_err(DatabaseError::from)?;
    assert_eq!(count, if recovered && !idle { 2 } else { 0 });
    if !idle {
        assert_eq!(stats.tokens, if recovered && !flush { 2 } else { 1 });
        if mode == "batch_lost" {
            assert_eq!(stats.lost, 1);
        }
        if matches!(mode, "batch_reject" | "final_reject") {
            assert_eq!(stats.writes, 1);
            assert!(elapsed < Duration::from_secs(1));
        }
        if matches!(
            mode,
            "batch_disconnect" | "batch_stall" | "final_disconnect" | "final_stall"
        ) {
            assert!((Duration::from_millis(2900)..Duration::from_millis(4800)).contains(&elapsed));
        }
        if mode == "cancel" {
            assert!(elapsed < Duration::from_secs(1));
        }
    } else if !recovered {
        assert!(elapsed < Duration::from_secs(6));
    }
    // 原目录未被事实传输取消；恢复连接后仍可保存失败代次及最终报告。
    if recovered {
        database.commit_scan_generation().await?;
    }
    database
        .finish_copy_generation(
            if recovered {
                CopyGenerationPhase::Completed
            } else {
                CopyGenerationPhase::Failed
            },
            None,
        )
        .await?;
    let report = NotificationRecord::new(
        "consumer-round".into(),
        "consumer-attempt".into(),
        1,
        NotificationKind::FinalReport,
        "{\"fixture\":true}".into(),
    )?;
    database.archive_notifications(&[report]).await?;
    let archived = database
        .pending_notifications("consumer-round", NotificationKind::FinalReport)
        .await?;
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].payload_json(), "{\"fixture\":true}");
    eprintln!(
        "copy-consumer cluster={} incremental={incremental} mode={mode} recovered={recovered} elapsed_ms={} base_rows={count} tokens={} queue_closed=true independent_final_archive=true",
        fixture.config.cluster.is_some(),
        elapsed.as_millis(),
        stats.tokens
    );
    Ok(())
}

fn has_database_cause(error: &(dyn StdError + 'static)) -> bool {
    error.downcast_ref::<DatabaseError>().is_some() || error.source().is_some_and(has_database_cause)
}

async fn pipeline_case(fixture: &Fixture, incremental: bool) -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let directory = tempfile::tempdir()?;
    let callback = std::env::var("LAB_COPY_CONSUMER_CALLBACK")
        .map_err(|_| AppError::ConfigError("owned consumer callback missing".into()))?;
    let command = SyncJobConfig {
        job_id: format!("consumer_pipeline_{incremental}"),
        job_dir: directory.path().to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: source.path().to_string_lossy().into_owned(),
        dest_path: target.path().to_string_lossy().into_owned(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: "owned consumer fault test".into(),
        progress_callback_url: Some(CallbackEndpoint::parse(&callback)?),
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: Some(2),
    };
    AppConfig::override_with(|global| {
        global.database.enabled = true;
        global.database.batch_size = 1;
        global.database.clickhouse = fixture.config.clone();
    })?;
    if incremental {
        std::fs::write(source.path().join("previous"), b"retained previous data")?;
        SyncOrchestrator::new_local(command.clone()).run().await?;
    }
    for index in 0..64 {
        std::fs::write(source.path().join(format!("payload-{index}")), b"owned data")?;
    }
    fixture.control("batch_reject").await?;
    let result = SyncOrchestrator::new_local(command).run().await;
    fixture.control("normal").await?;
    let error = result
        .err()
        .ok_or_else(|| AppError::ConfigError("migration concealed consumer rejection".into()))?;
    assert!(has_database_cause(&error), "{error}");
    assert!(error.to_string().contains("owned batch rejection"), "{error}");
    for index in 0..64 {
        assert!(source.path().join(format!("payload-{index}")).exists());
    }
    eprintln!(
        "copy-consumer-pipeline cluster={} incremental={incremental} exact_database_cause=true source_retained=true",
        fixture.config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires --consumer-recovery owned ClickHouse pair and proxy"]
async fn real_copy_consumers_recover_and_stop_after_final_failure() -> Result<()> {
    AppConfig::init(
        Some(include_str!("../../../../cli/src/resources/default_config.toml")),
        None,
    )?;
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            for incremental in [false, true] {
                for mode in [
                    "normal",
                    "batch_flash",
                    "batch_lost",
                    "batch_recover_stall",
                    "batch_disconnect",
                    "batch_stall",
                    "batch_reject",
                    "cancel",
                    "idle_disconnect",
                    "idle_stall",
                    "idle_flash",
                    "final_disconnect",
                    "final_stall",
                    "final_reject",
                    "final_recover_stall",
                ] {
                    case(&fixture, incremental, mode).await?;
                }
                pipeline_case(&fixture, incremental).await?;
            }
            Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[path = "execution_real_tests.rs"]
mod execution_tests;

#[path = "data_cancellation_real_tests.rs"]
mod data_cancellation_tests;
