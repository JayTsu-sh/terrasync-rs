//! 真实 CH 与 Local：漏扫、缺失、未知后代及硬链接的目标收尾。
use crate::migration_fixtures::request;
use std::{path::Path, sync::Arc, time::Duration};
use task_model::migration::CopyGenerationPhase;

use async_trait::async_trait;
use clickhouse::Client;
use data_mover::model::{BackendKind, EntryKind, ObservedEntry, SourceIdentity, StoragePath};
use data_mover::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageBuilder, StorageRoleFailure};
use db::catalogue::ComparisonMode;
use db::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, INCREMENTAL_SCAN_TABLE_BASE_NAME};
use task_model::{SourcePresence, StorageEntryMessage, TransferMode};
use utils::app_config::{ClickHouseMode, DatabaseConfig};
use uuid::Uuid;

use super::SyncOrchestrator;
use crate::broadcast::BroadcastForwarder;
use crate::config::{ConsoleConfig, ConsumerConfig, JobType};
use crate::consumer::{DatabaseConsumer, traits::Consumer};
use crate::error::{AppError, Result, StorageError};
use crate::observed_fixtures::observation;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};
use crate::target_absence::SourceAbsenceCheck;

fn consumer_config(job: &str, config: &ClickHouseConfig) -> ConsumerConfig {
    ConsumerConfig {
        job_id: job.into(),
        job_type: JobType::IncrementalCopy,
        job_dir: String::new(),
        db_config: DatabaseConfig {
            enabled: true,
            batch_size: 8,
            clickhouse: config.clone(),
        },
        console_config: ConsoleConfig {
            raw_command_line: String::new(),
        },
        progress_callback_url: None,
    }
}

async fn deletion_case(config: &ClickHouseConfig, present: bool, directory: bool, known_child: bool) -> Result<()> {
    let job = format!("absence_{present}_{directory}_{known_child}");
    let database = ClickHouseDatabase::connect(config, &job).await?;
    database.initialize().await?;
    database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
    database.begin_copy_generation(&request("same", true)).await?;
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    for root in [source_root.path(), target_root.path()] {
        if directory {
            std::fs::create_dir(root.join("entry"))?;
        } else {
            std::fs::write(root.join("entry"), b"payload")?;
        }
    }
    if known_child {
        for root in [source_root.path(), target_root.path()] {
            std::fs::write(root.join("entry/known"), b"known child")?;
        }
    } else if directory {
        std::fs::write(target_root.path().join("entry/unknown"), b"unowned child")?;
    }
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let mut old = vec![Arc::new(observe_path(source.storage(), Path::new("entry")).await?)];
    if known_child {
        old.push(Arc::new(
            observe_path(source.storage(), Path::new("entry/known")).await?,
        ));
    }
    database.batch_insert_base_record(&old).await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&request("same", true)).await?;
    if !present {
        if known_child {
            std::fs::remove_file(source_root.path().join("entry/known"))?;
        }
        if directory {
            std::fs::remove_dir(source_root.path().join("entry"))?;
        } else {
            std::fs::remove_file(source_root.path().join("entry"))?;
        }
    }
    // 本轮未给 compare_catalogue_batch 交付 entry，模拟过滤或未命中的旧确认事实。
    let mut events = BroadcastForwarder::new(8);
    let mut received = events.subscribe();
    let mut consumer = DatabaseConsumer::with_database(&consumer_config(&job, config), database.clone()).await?;
    let writer = consumer.start(events.subscribe()).await?;
    let result = SyncOrchestrator::apply_deletions_and_renames(
        &database,
        source,
        target,
        true,
        &events,
        false,
        None,
        None,
        &job,
        "",
        TransferMode::Direct,
        SourceAbsenceCheck::new(Duration::from_secs(1), Duration::from_secs(3)),
    )
    .await;
    assert_eq!(result.is_ok(), present || !directory || known_child);
    let messages: Vec<_> = std::iter::from_fn(|| received.try_recv().ok()).collect();
    if present {
        assert!(
            matches!(messages.as_slice(), [StorageEntryMessage::TargetPreserved { retention, .. }] if retention.source_presence == SourcePresence::Present)
        );
    } else if known_child {
        assert!(
            matches!(messages.as_slice(), [StorageEntryMessage::Deleted(child), StorageEntryMessage::Deleted(parent)] if child.path().as_str() == "entry/known" && parent.path().as_str() == "entry")
        );
    } else if directory {
        assert!(messages.iter().any(|message| matches!(message, StorageEntryMessage::TargetPreserved { retention, .. } if retention.source_presence == SourcePresence::Absent)));
        assert!(
            !messages
                .iter()
                .any(|message| matches!(message, StorageEntryMessage::Deleted(_)))
        );
    } else {
        assert!(matches!(messages.as_slice(), [StorageEntryMessage::Deleted(_)]));
    }
    drop(events);
    writer.await??;
    let retained = present || (directory && !known_child);
    assert_eq!(database.get_count("base").await?, u64::from(retained));
    assert_eq!(target_root.path().join("entry").exists(), retained);
    if directory && !known_child {
        assert_eq!(
            std::fs::read(target_root.path().join("entry/unknown"))?,
            b"unowned child"
        );
    }
    eprintln!(
        "source-absence cluster={} present={present} directory={directory} known_child={known_child} target_retained={retained} recursive_delete=false",
        config.cluster.is_some()
    );
    Ok(())
}

// 适配器仅在测试中提供跨轮稳定身份；读取仍交由真实 Local role 校验并执行。
struct StableSource {
    inner: Arc<dyn ReadSource>,
    identity: SourceIdentity,
}

#[async_trait]
impl ReadSource for StableSource {
    async fn describe(&self, path: &StoragePath) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        let mut descriptor = self.inner.describe(path).await?;
        descriptor.source_identity = self.identity.clone();
        Ok(descriptor)
    }
    async fn read(&self, mut request: ReadRequest) -> std::result::Result<ByteStream, StorageRoleFailure> {
        request.expected_source = Some(self.inner.describe(&request.path).await?.source_identity);
        self.inner.read(request).await
    }
}

async fn stable_source(root: &Path) -> Result<StorageContext> {
    let original = create_storage(&root.to_string_lossy(), CreateStorageOptions::default()).await?;
    let identity = observation(
        BackendKind::Nfs,
        "old".into(),
        EntryKind::File,
        7,
        [0; 3],
        None,
        Some(b"same-inode".to_vec()),
        None,
        None,
    )
    .source_identity()
    .clone();
    let roles = StorageBuilder::new(identity.backend().clone(), original.roles.capabilities().clone())
        .read_source(Arc::new(StableSource {
            inner: original.roles.read_source()?,
            identity,
        }))
        .staged_destination(original.roles.staged_destination()?)
        .namespace(original.roles.namespace()?)
        .metadata(original.roles.metadata()?)
        .build()
        .map_err(|error| AppError::ConfigError(error.to_string()))?;
    Ok(StorageContext {
        roles,
        block_size: original.block_size,
        reconnect_config: None,
    })
}

async fn stable_entry(source: &StorageContext, path: &str) -> Result<Arc<ObservedEntry>> {
    let actual = observe_path(source.storage(), Path::new(path)).await?;
    let descriptor = source.roles.read_source()?.describe(actual.path()).await?;
    Ok(Arc::new(
        ObservedEntry::new(
            actual.path().clone(),
            actual.kind(),
            actual.size(),
            actual.modified(),
            descriptor.source_identity,
        )
        .map_err(StorageError::from)?
        .with_metadata(actual.metadata().clone()),
    ))
}

async fn hardlink_case(config: &ClickHouseConfig) -> Result<()> {
    let database = ClickHouseDatabase::connect(config, "hardlink").await?;
    database.initialize().await?;
    database.create_table(INCREMENTAL_SCAN_TABLE_BASE_NAME).await?;
    database.begin_copy_generation(&request("same", true)).await?;
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("old"), b"payload")?;
    std::fs::hard_link(source_root.path().join("old"), source_root.path().join("new"))?;
    std::fs::write(target_root.path().join("old"), b"payload")?;
    let source = Arc::new(stable_source(source_root.path()).await?);
    database
        .batch_insert_base_record(&[stable_entry(&source, "old").await?])
        .await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&request("same", true)).await?;
    assert!(
        database
            .compare_catalogue_batch(
                &[stable_entry(&source, "new").await?],
                ComparisonMode::AwaitCopyAcknowledgement
            )
            .await?
            .is_empty()
    );
    let target =
        Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let mut events = BroadcastForwarder::new(8);
    let mut received = events.subscribe();
    let mut consumer = DatabaseConsumer::with_database(&consumer_config("hardlink", config), database.clone()).await?;
    let writer = consumer.start(events.subscribe()).await?;
    SyncOrchestrator::apply_deletions_and_renames(
        &database,
        source,
        target,
        true,
        &events,
        false,
        None,
        None,
        "hardlink",
        "",
        TransferMode::Direct,
        SourceAbsenceCheck::new(Duration::from_secs(1), Duration::from_secs(3)),
    )
    .await?;
    let messages: Vec<_> = std::iter::from_fn(|| received.try_recv().ok()).collect();
    assert!(messages.iter().any(|message| matches!(message, StorageEntryMessage::TargetPreserved { retention, .. } if retention.source_presence == SourcePresence::Present)));
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, StorageEntryMessage::New(_)))
    );
    assert!(!messages.iter().any(|message| matches!(
        message,
        StorageEntryMessage::Renamed(_) | StorageEntryMessage::Deleted(_)
    )));
    drop(events);
    writer.await??;
    assert_eq!(database.get_count("base").await?, 2);
    for path in ["old", "new"] {
        assert_eq!(std::fs::read(target_root.path().join(path))?, b"payload");
    }
    eprintln!(
        "source-absence cluster={} hardlink=true copied_new=true old_target_and_fact_retained=true",
        config.cluster.is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair from run-notification-archive-local.py --source-absence"]
async fn real_source_absence_guards_target_mutations() -> Result<()> {
    let dsn = std::env::var("LAB_COPY_GENERATION_DSN")
        .map_err(|_| AppError::ConfigError("isolated ClickHouse URL required".into()))?;
    let admin = Client::default().with_url(&dsn).with_user("default");
    for cluster in [false, true] {
        let config = ClickHouseConfig {
            mode: if cluster {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.then(|| "notification_cluster".into()),
            dsn: dsn.clone(),
            database: format!("ts_source_absence_{}", Uuid::new_v4().simple()),
            username: "default".into(),
            password: None,
            dial_timeout: 10,
            read_timeout: 60,
        };
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
        let result: Result<()> = async {
            for present in [false, true] {
                for directory in [false, true] {
                    deletion_case(&config, present, directory, false).await?;
                }
            }
            deletion_case(&config, false, true, true).await?;
            hardlink_case(&config).await
        }
        .await;
        let cleanup = admin
            .query(&format!("DROP DATABASE {}{on_cluster} SYNC", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from);
        result?;
        cleanup?;
    }
    Ok(())
}
