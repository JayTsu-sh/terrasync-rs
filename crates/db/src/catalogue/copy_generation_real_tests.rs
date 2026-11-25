//! 真实 Single／Cluster 上验证策略切换、失败代次与记录缺失。
use crate::migration_fixtures::request;
use task_model::migration::CopyGenerationPhase;

use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Client;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::CopyGenerationStatus;
use crate::DeletionStatus;
use crate::catalogue::{ComparisonMode, CopyCatalogueEntry};
use crate::execution::base_client;
use crate::execution::queries::with_scan_cancellation;
use crate::observed_fixtures::observation;
use crate::schema::identity::encoded_file_handle;
use crate::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, DatabaseExecutionMonitor, Result};
use data_mover::model::{BackendKind, EntryKind, HistoryOrder, ObjectHistory, ObservedEntry, VersionRecord};
use task_model::observation::ObservedItem;
use task_model::{ChangeKind, CopyCompletion, CopyReadiness, EntryChange, StorageEntryMessage};

#[derive(Deserialize, clickhouse::Row)]
struct VersionSlotRow {
    version_id: String,
    source_version_id: String,
    entry_snapshot: Vec<u8>,
    copy_path_confirmed: bool,
}

struct Fixture {
    config: ClickHouseConfig,
    admin: Client,
    control: String,
}

impl Fixture {
    async fn new(cluster: bool) -> Result<Self> {
        let dsn = std::env::var("LAB_COPY_GENERATION_DSN")
            .map_err(|_| DatabaseError::ConfigError("LAB_COPY_GENERATION_DSN must select an isolated server".into()))?;
        let control = std::env::var("LAB_COPY_GENERATION_CONTROL").map_err(|_| {
            DatabaseError::ConfigError("LAB_COPY_GENERATION_CONTROL must select the owned proxy".into())
        })?;
        let mut config = ClickHouseConfig {
            mode: if cluster {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.then(|| "notification_cluster".into()),
            dsn,
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let admin = base_client(&config)?;
        config.database = format!("ts_copy_generations_{}", Uuid::new_v4().simple());
        let on_cluster = if cluster {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        admin
            .query(&format!("CREATE DATABASE {}{on_cluster}", config.database))
            .execute()
            .await?;
        Ok(Self { config, admin, control })
    }

    async fn open(&self, job: &str) -> Result<ClickHouseDatabase> {
        ClickHouseDatabase::connect(&self.config, job).await
    }

    async fn cleanup(&self) -> Result<()> {
        let on_cluster = if self.config.cluster.is_some() {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        self.admin
            .query(&format!("DROP DATABASE {}{on_cluster} SYNC", self.config.database))
            .execute()
            .await?;
        Ok(())
    }

    async fn fault(&self, fault: &str) -> Result<()> {
        timeout(Duration::from_secs(1), async {
            let mut stream = TcpStream::connect(&self.control).await?;
            stream
                .write_all(format!("GET /{fault} HTTP/1.0\r\nHost: {}\r\n\r\n", self.control).as_bytes())
                .await?;
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await?;
            if !reply.starts_with(b"HTTP/1.0 200") {
                return Err(DatabaseError::ConfigError("owned fault control failed".into()));
            }
            Ok(())
        })
        .await
        .map_err(|_| DatabaseError::TransactionError("owned fault control timed out".into()))?
    }
}

async fn transition(
    fixture: &Fixture, previous_reserved: bool, current_reserved: bool, incomplete: bool, empty: bool,
) -> Result<()> {
    let job = format!("policy_{previous_reserved}_{current_reserved}_{incomplete}_{empty}");
    let first = fixture.open(&job).await?;
    first.initialize().await?;
    assert_eq!(first.copy_catalogue_source_reserved().await?, None);
    first
        .begin_copy_generation(&request("same-endpoints", previous_reserved))
        .await?;
    if !empty {
        let entry = observation(
            BackendKind::Local,
            "payload".into(),
            EntryKind::File,
            7,
            [0, 11, 0],
            None,
            None,
            None,
            None,
        );
        first.batch_insert_base_record(&[entry.into()]).await?;
    }
    if !incomplete {
        first.commit_scan_generation().await?;
        first
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await?;
    }
    drop(first);
    let next = fixture.open(&job).await?;
    assert_eq!(next.copy_catalogue_source_reserved().await?, Some(previous_reserved));
    let status = next
        .begin_copy_generation(&request("same-endpoints", current_reserved))
        .await?;
    assert_eq!(
        status,
        if incomplete && previous_reserved {
            CopyGenerationStatus::Recovering
        } else {
            CopyGenerationStatus::Fresh
        }
    );
    let expected = u64::from(previous_reserved && !empty);
    assert_eq!(next.get_count("base").await?, expected);
    assert_eq!(next.copy_catalogue_source_reserved().await?, Some(current_reserved));
    next.commit_scan_generation().await?;
    next.finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    drop(next);
    let third = fixture.open(&job).await?;
    assert_eq!(third.copy_catalogue_source_reserved().await?, Some(current_reserved));
    assert_eq!(
        third.begin_copy_generation(&request("same-endpoints", false)).await?,
        CopyGenerationStatus::Fresh
    );
    assert_eq!(
        third.get_count("base").await?,
        if current_reserved { expected } else { 0 }
    );
    eprintln!(
        "copy-generation cluster={} previous_reserved={previous_reserved} current_reserved={current_reserved} incomplete={incomplete} missing_rows={empty} retained={expected}",
        fixture.config.cluster.is_some()
    );
    Ok(())
}

async fn read_faults(fixture: &Fixture) -> Result<()> {
    let database = fixture.open("faults").await?;
    database.initialize().await?;
    database
        .begin_copy_generation(&request("same-endpoints", false))
        .await?;
    for fault in ["disconnect", "stall"] {
        fixture.fault(fault).await?;
        let started = Instant::now();
        let result = database.copy_catalogue_source_reserved().await;
        let elapsed = started.elapsed();
        fixture.fault("normal").await?;
        assert!(
            matches!(
                result,
                Err(DatabaseError::ClickHouseError(_) | DatabaseError::RequestOutcomeUnconfirmed { .. })
            ),
            "{result:?}"
        );
        assert!(elapsed < Duration::from_secs(6), "{fault}: {elapsed:?}");
        assert_eq!(database.copy_catalogue_source_reserved().await?, Some(false));
        eprintln!(
            "copy-generation cluster={} fault={fault} elapsed={elapsed:?} typed_error=true policy_preserved=true",
            fixture.config.cluster.is_some()
        );
    }
    Ok(())
}

async fn versioned_state_read_faults(fixture: &Fixture, copying: bool) -> Result<()> {
    let database = fixture.open(&format!("versioned_state_{copying}")).await?;
    database
        .begin_copy_generation(&request("versioned-state", true))
        .await?;
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let previous = database.current_copy_generation().await?;
    for (fault, cancelling) in [
        ("state_read_disconnect", false),
        ("state_read_stall", false),
        ("state_read_stall", true),
        ("state_read_flash", false),
    ] {
        let (scoped, queries) = database.with_query_cancellation()?;
        let execution = queries.execution();
        let cancellation = CancellationToken::new();
        let monitor = DatabaseExecutionMonitor::start(queries, cancellation.clone())?;
        fixture.fault(fault).await?;
        let stop = cancellation.clone();
        let trigger = tokio::spawn(async move {
            if cancelling {
                sleep(Duration::from_millis(250)).await;
                stop.cancel();
            }
        });
        let started = Instant::now();
        let operation = async {
            if copying {
                assert!(scoped.copy_baseline_available(&previous).await?);
            } else {
                scoped.insert_scan_state(0).await?;
            }
            Ok::<(), DatabaseError>(())
        };
        let result = timeout(
            Duration::from_secs(6),
            with_scan_cancellation(cancellation, Some(execution.clone()), operation),
        )
        .await;
        let elapsed = started.elapsed();
        trigger.await?;
        monitor.finish().await?;
        fixture.fault("state_read_hit").await?;
        fixture.fault("normal").await?;
        let result = result.map_err(|_| DatabaseError::ConfigError("versioned state read exceeded deadline".into()))?;
        let recovered = fault == "state_read_flash";
        if !recovered {
            // 代理静默时，关闭在途连接没有服务端的终止确认。
            assert!(
                matches!(&result, Err(error) if error.is_confirmed_scan_cancellation()
                    || error.is_transient_connectivity_failure()
                    || matches!(error, DatabaseError::ScanTerminationUnconfirmed { .. })),
                "{result:?}"
            );
        }
        if recovered || cancelling {
            assert!(execution.failure().is_none());
        } else {
            assert!(
                execution
                    .failure()
                    .is_some_and(|source| source.is_transient_connectivity_failure())
            );
            assert_eq!(execution.recovery_window(), Duration::from_secs(3));
            let detection = if fault == "state_read_stall" {
                Duration::from_secs(u64::from(fixture.config.read_timeout))
            } else {
                Duration::ZERO
            };
            assert!(
                elapsed < detection + execution.recovery_window() + Duration::from_millis(750),
                "{elapsed:?}"
            );
        }
        if cancelling {
            assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
        }
        if recovered {
            result?;
        }
        assert_eq!(
            database.copy_baseline_available(&previous).await?,
            copying || !recovered
        );
        eprintln!(
            "versioned-state cluster={} copying={copying} fault={fault} cancelled={cancelling} elapsed={elapsed:?} recovered={recovered}",
            fixture.config.cluster.is_some()
        );
    }
    Ok(())
}

async fn version_slots(fixture: &Fixture, history_target: bool) -> Result<()> {
    let job = format!("version_slots_{history_target}");
    let descriptor = || {
        let mut request = request("version-slots", true);
        request.binding.target_history = history_target;
        request
    };
    let versions = |ids: &[&str]| -> Result<Vec<Arc<ObservedEntry>>> {
        let mut entries: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                observation(
                    BackendKind::S3,
                    "payload".into(),
                    EntryKind::File,
                    7,
                    [11; 3],
                    None,
                    None,
                    Some(vec![("raw".into(), id.to_string())]),
                    Some((Some(id.to_string()), index + 1 == ids.len(), false)),
                )
            })
            .collect();
        let records = entries
            .iter()
            .flat_map(|entry| {
                entry
                    .history()
                    .into_iter()
                    .flat_map(|history| history.records().iter().cloned())
            })
            .collect();
        let entry = entries.pop().ok_or(DatabaseError::CopyExecutionUnavailable)?;
        let history = ObjectHistory::whole(records, HistoryOrder::Server)?;
        Ok(entry
            .with_history(Some(Box::new(history)))?
            .into_segments(1)
            .into_iter()
            .map(Arc::new)
            .collect())
    };
    let initial = versions(&["old", "current"])?;
    let database = fixture.open(&job).await?;
    database.begin_copy_generation(&descriptor()).await?;
    let base = database.catalogue_storage_table(&format!("base_{job}"))?;
    database
        .execute(
            &format!("ALTER TABLE {base} MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool=1"),
            &[],
        )
        .await?;
    database.batch_insert_base_record(&initial).await?;
    assert_eq!(database.get_count("base").await?, if history_target { 2 } else { 1 });
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let updated = versions(&["old", "current", "new"])?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&descriptor()).await?;
    let changes = database
        .compare_catalogue_batch(&updated, ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert_eq!(
        changes.len(),
        1,
        "旧历史不能为最新目标重复生成工作；历史目标只新增未确认版本"
    );
    assert_eq!(changes[0].item.view().version_id(), "new");
    let expected = if history_target {
        EntryChange::New
    } else {
        EntryChange::Changed(ChangeKind::Both)
    };
    assert_eq!(changes[0].kind, expected, "相同大小和 mtime 不得掩盖源版本变化");
    let client = base_client(&fixture.config)?;
    let rows = || {
        client.query(&format!(
            "SELECT version_id,source_version_id,entry_snapshot,copy_path_confirmed FROM base_{job} FINAL ORDER BY version_id"
        ))
    };
    let before = rows().fetch_all::<VersionSlotRow>().await?;
    assert!(
        before.iter().all(|row| row.source_version_id != "new"),
        "未经 ACK 保留上个目标事实"
    );
    if !history_target {
        assert!(!before[0].copy_path_confirmed, "复制尚未确认，旧原值不能授权复用或删源");
    }
    fail_after_comparison(fixture, &database).await?;
    let mut database = database.new_execution_scope();
    assert_eq!(
        database.begin_copy_generation(&descriptor()).await?,
        CopyGenerationStatus::Recovering
    );
    if !history_target {
        let physical = client
            .query(&format!("SELECT countIf(copy_path_confirmed) FROM base_{job}"))
            .fetch_one::<u64>()
            .await?;
        assert!(physical > 0, "回归必须保留同键更早的已确认物理行");
        for _ in 0..3 {
            let reverted = database
                .compare_catalogue_batch(&initial, ComparisonMode::AwaitCopyAcknowledgement)
                .await?;
            assert_eq!(reverted.len(), 1, "失败后源恢复原版本也不能把已覆盖的目标当成就绪");
            assert_eq!(reverted[0].kind, EntryChange::New);
            database = database.new_execution_scope();
            assert_eq!(
                database.begin_copy_generation(&descriptor()).await?,
                CopyGenerationStatus::Recovering
            );
        }
    }
    let changes = database
        .compare_catalogue_batch(&updated, ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, EntryChange::New);
    database
        .batch_insert_completed_records(
            &changes
                .into_iter()
                .map(|change| change.item)
                .collect::<Vec<ObservedItem>>(),
        )
        .await?;
    assert!(
        database
            .compare_catalogue_batch(&updated, ComparisonMode::AwaitCopyAcknowledgement)
            .await?
            .is_empty()
    );
    let after = rows().fetch_all::<VersionSlotRow>().await?;
    assert_eq!(after.len(), if history_target { 3 } else { 1 });
    let latest = after
        .iter()
        .find(|row| row.source_version_id == "new")
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(latest.version_id, if history_target { "new" } else { "" });
    let entry = ObservedEntry::decode_snapshot(&latest.entry_snapshot, updated[2].source_identity().backend())?;
    assert_eq!(
        entry.history().map(|history| history.records()[0].version_id()),
        Some(Some("new"))
    );
    assert_eq!(
        entry
            .history()
            .and_then(|history| history.records()[0].metadata())
            .and_then(|metadata| metadata.tags().value())
            .map(|tags| tags[0].value()),
        Some("new")
    );
    eprintln!(
        "version-slots cluster={} history_target={history_target} before_ack_preserved=true retry_work=1 same_size_and_mtime=true base_rows={} raw_source_version_and_tags_preserved=true",
        fixture.config.cluster.is_some(),
        after.len()
    );
    Ok(())
}

async fn delayed_copy_ack_cannot_restore_metadata_readiness(fixture: &Fixture) -> Result<()> {
    let job = "delayed_metadata_ack";
    let first = fixture.open(job).await?;
    first.begin_copy_generation(&request("delayed-metadata", true)).await?;
    let item: ObservedItem = Arc::new(observation(
        BackendKind::Local,
        "payload".into(),
        EntryKind::File,
        7,
        [0, 11, 0],
        None,
        None,
        None,
        None,
    ))
    .into();
    first
        .batch_insert_copy_completions(&[CopyCompletion {
            item: item.clone(),
            target_version: Some("actual-target-version".into()),
            readiness: CopyReadiness::DataOnly,
        }])
        .await?;
    first.finish_copy_generation(CopyGenerationPhase::Failed, None).await?;
    let next = first.new_execution_scope();
    next.begin_copy_generation(&request("delayed-metadata", true)).await?;
    let compare = || {
        next.compare_catalogue_batch(
            std::slice::from_ref(&item.entry),
            ComparisonMode::AwaitCopyAcknowledgement,
        )
    };
    let changes = compare().await?;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, EntryChange::Changed(ChangeKind::MetadataOnly));
    // 模拟上轮响应丢失后延迟到达的完成写入，以及独立 Scan 的 seq=0 写入。
    first
        .batch_insert_copy_completions(&[CopyCompletion {
            item: item.clone(),
            target_version: Some("stale-target-version".into()),
            readiness: CopyReadiness::Ready,
        }])
        .await?;
    let scan = fixture.open(job).await?;
    scan.batch_insert_base_record(std::slice::from_ref(&item.entry)).await?;
    let client = base_client(&fixture.config)?;
    let row = client
        .query(&format!(
            "SELECT copy_sequence,copy_data_confirmed,copy_path_confirmed,copy_target_version FROM base_{job} FINAL"
        ))
        .fetch_one::<(u64, bool, bool, Option<String>)>()
        .await?;
    assert_eq!(row, (2, true, false, Some("actual-target-version".into())));
    let changes = compare().await?;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, EntryChange::Changed(ChangeKind::MetadataOnly));
    eprintln!(
        "metadata-fence cluster={} older_ack_and_scan_cannot_restore_readiness=true",
        fixture.config.cluster.is_some()
    );
    Ok(())
}

async fn read_current_completions(database: &ClickHouseDatabase) -> Result<Vec<CopyCompletion>> {
    let (sender, mut receiver) = mpsc::channel::<CopyCatalogueEntry>(1);
    let collect = async {
        let mut rows = Vec::new();
        while let Some(row) = receiver.recv().await {
            rows.push(row.completion);
        }
        rows
    };
    let (result, rows) = tokio::join!(database.query_current_copy_entries(None, None, None, sender), collect);
    result?;
    Ok(rows)
}

async fn copy_binding_read_faults(fixture: &Fixture, database: &ClickHouseDatabase) -> Result<()> {
    for fault in ["disconnect", "stall", "flash", "cancel"] {
        fixture
            .fault(if fault == "flash" {
                "disconnect"
            } else if fault == "cancel" {
                "stall"
            } else {
                fault
            })
            .await?;
        let started = Instant::now();
        if fault == "cancel" {
            assert!(
                timeout(Duration::from_millis(250), read_current_completions(database))
                    .await
                    .is_err()
            );
        } else {
            let read = read_current_completions(database);
            let result = if fault == "flash" {
                let restore = async {
                    sleep(Duration::from_millis(100)).await;
                    fixture.fault("normal").await
                };
                let (result, restored) = tokio::join!(read, restore);
                restored?;
                result
            } else {
                read.await
            };
            assert!(
                matches!(
                    result,
                    Err(DatabaseError::ClickHouseError(_) | DatabaseError::RequestOutcomeUnconfirmed { .. })
                ),
                "{result:?}"
            );
        }
        let elapsed = started.elapsed();
        fixture.fault("normal").await?;
        assert!(elapsed < Duration::from_secs(6), "{fault}: {elapsed:?}");
        assert_eq!(read_current_completions(database).await?.len(), 3);
        eprintln!(
            "copy-binding-read cluster={} fault={fault} elapsed={elapsed:?} explicit_retry_recovers=true no_read_authority_inferred=true",
            fixture.config.cluster.is_some()
        );
    }
    Ok(())
}

async fn selected_copy_bindings_roundtrip(fixture: &Fixture, history_target: bool) -> Result<()> {
    let job = format!("read_copy_bindings_{history_target}");
    let descriptor = || {
        let mut descriptor = request("copy-binding-readback", true);
        descriptor.binding.target_history = history_target;
        descriptor
    };
    let versions: Vec<_> = [
        (Some("old"), false, false),
        (None, false, true),
        (Some("new"), true, false),
    ]
    .into_iter()
    .map(|(id, latest, marker)| {
        observation(
            BackendKind::S3,
            "payload".into(),
            EntryKind::File,
            7,
            [0, 11, 0],
            None,
            None,
            (!marker).then(|| vec![("raw".into(), id.unwrap_or("null").into())]),
            Some((id.map(str::to_owned), latest, marker)),
        )
    })
    .collect();
    let records = versions
        .iter()
        .flat_map(|entry| {
            entry
                .history()
                .into_iter()
                .flat_map(|history| history.records().iter().cloned())
        })
        .collect();
    let entries: Vec<_> = versions[2]
        .clone()
        .with_history(Some(Box::new(ObjectHistory::whole(records, HistoryOrder::Server)?)))?
        .into_segments(1)
        .into_iter()
        .map(Arc::new)
        .collect();
    let selected: Vec<_> = entries
        .iter()
        .flat_map(ObservedItem::all)
        .filter(|item| history_target || item.view().version().is_some_and(VersionRecord::is_latest))
        .collect();
    let database = fixture.open(&job).await?;
    database.begin_copy_generation(&descriptor()).await?;
    let mut completions: Vec<_> = selected
        .into_iter()
        .map(|item| CopyCompletion {
            target_version: Some(format!("target-{}", item.view().version_id())),
            item,
            readiness: CopyReadiness::Ready,
        })
        .collect();
    completions[0].readiness = CopyReadiness::DataOnly;
    database.batch_insert_copy_completions(&completions).await?;
    let ready = read_current_completions(&database).await?;
    assert_eq!(ready.len(), completions.len() - 1, "数据完成不代表元数据就绪");
    completions[0].readiness = CopyReadiness::Ready;
    completions[0].target_version = None;
    database.batch_insert_copy_completions(&completions[..1]).await?;
    let missing = read_current_completions(&database).await?;
    assert_eq!(missing.len(), completions.len());
    assert_eq!(missing[0].target_version, None, "缺少绑定不能从源版本推断");
    completions[0].target_version = Some(format!("target-{}", completions[0].item.view().version_id()));
    database.batch_insert_copy_completions(&completions[..1]).await?;
    let check = |ready: &[CopyCompletion]| {
        assert_eq!(ready.len(), completions.len());
        for (actual, expected) in ready.iter().zip(&completions) {
            assert_eq!(actual.readiness, CopyReadiness::Ready);
            assert_eq!(actual.item.view().version_id(), expected.item.view().version_id());
            assert_eq!(actual.item.view().metadata(), expected.item.view().metadata());
            assert_eq!(actual.target_version, expected.target_version);
            assert!(
                actual
                    .item
                    .entry
                    .history()
                    .is_some_and(|history| history.records().len() == 1)
            );
        }
    };
    check(&read_current_completions(&database).await?);
    let (sender, mut receiver) = mpsc::channel::<CopyCatalogueEntry>(1);
    let positions = async {
        let mut index = 0;
        while let Some(row) = receiver.recv().await {
            assert_eq!(row.history_segment, if history_target { index } else { 2 });
            assert_eq!(row.history_index, 0);
            assert_eq!(row.history_records, 1);
            assert_eq!(row.history_complete, row.history_segment == 2);
            assert!(
                row.completion
                    .item
                    .entry
                    .history()
                    .is_some_and(|history| history.records().len() == 1)
            );
            index += 1;
        }
        assert_eq!(index, if history_target { 3 } else { 1 });
    };
    let (read, ()) = tokio::join!(database.query_current_copy_entries(None, None, None, sender), positions);
    read?;
    if history_target {
        copy_binding_read_faults(fixture, &database).await?;
    }
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let next = database.new_execution_scope();
    next.begin_copy_generation(&descriptor()).await?;
    assert!(
        read_current_completions(&next).await?.is_empty(),
        "上轮事实没有本次选中授权"
    );
    assert!(
        next.compare_catalogue_batch(&entries, ComparisonMode::AwaitCopyAcknowledgement)
            .await?
            .is_empty()
    );
    check(&read_current_completions(&next).await?);
    next.commit_scan_generation().await?;
    next.finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    eprintln!(
        "copy-binding-readback cluster={} history_target={history_target} selected_versions={} source_order=true raw_metadata_and_actual_bindings=true data_only_excluded=true missing_binding_not_inferred=true current_execution_required=true",
        fixture.config.cluster.is_some(),
        completions.len()
    );
    Ok(())
}

async fn archived_incremental_changes_are_scoped_and_paginated(fixture: &Fixture) -> Result<()> {
    let database = fixture.open("archived_changes").await?;
    let first = request("archived-changes", true);
    database.begin_copy_generation(&first).await?;
    database.create_incremental_scan_table().await?;
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    assert!(
        database
            .incremental_copy_changes(&first.execution_id, 0, 2)
            .await?
            .is_none()
    );

    let database = database.new_execution_scope();
    let run = request("archived-changes", true);
    database.begin_copy_generation(&run).await?;
    let file = |path| rename_entry(path, false);
    let new = StorageEntryMessage::New(file("added").into());
    database
        .batch_insert_incremental_record(&[
            new.clone(),
            new,
            StorageEntryMessage::Changed {
                entry: file("content").into(),
                kind: ChangeKind::DataOnly,
            },
            StorageEntryMessage::Changed {
                entry: file("metadata").into(),
                kind: ChangeKind::MetadataOnly,
            },
            StorageEntryMessage::Changed {
                entry: file("both").into(),
                kind: ChangeKind::Both,
            },
            StorageEntryMessage::Renamed((file("old"), file("renamed"))),
        ])
        .await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Failed, None)
        .await?;

    let other = database.new_execution_scope();
    let next_run = request("archived-changes", true);
    other.begin_copy_generation(&next_run).await?;
    other
        .batch_insert_incremental_record(&[StorageEntryMessage::New(file("another-run").into())])
        .await?;

    let first_page = other
        .incremental_copy_changes(&run.execution_id, 0, 2)
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(first_page.phase, CopyGenerationPhase::Failed);
    assert_eq!(first_page.counts.len(), 5);
    assert!(first_page.counts.iter().all(|count| count.count == 1));
    assert_eq!(first_page.next_offset, Some(2));
    assert_eq!(
        first_page
            .changes
            .iter()
            .map(|row| row.path.as_str())
            .collect::<Vec<_>>(),
        ["added", "both"]
    );
    let last = other
        .incremental_copy_changes(&run.execution_id, 2, 3)
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(last.next_offset, None);
    assert_eq!(
        last.changes.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>(),
        ["data_changed", "metadata_changed", "rename"]
    );
    assert_eq!(last.changes[2].previous_path.as_deref(), Some("old"));
    assert_eq!(last.changes[2].path, "renamed");
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated ClickHouse pair and fault proxy from run-notification-archive-local.py --copy-generations"]
async fn real_copy_generation_policies() -> Result<()> {
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            for previous_reserved in [false, true] {
                for current_reserved in [false, true] {
                    for incomplete in [false, true] {
                        for empty in [false, true] {
                            transition(&fixture, previous_reserved, current_reserved, incomplete, empty).await?;
                        }
                    }
                }
            }
            delayed_copy_ack_cannot_restore_metadata_readiness(&fixture).await?;
            selected_copy_bindings_roundtrip(&fixture, false).await?;
            selected_copy_bindings_roundtrip(&fixture, true).await?;
            version_slots(&fixture, false).await?;
            version_slots(&fixture, true).await?;
            read_faults(&fixture).await
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated ClickHouse state-read fault proxy"]
async fn real_versioned_copy_state_reads_are_bounded() -> Result<()> {
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result = async {
            versioned_state_read_faults(&fixture, false).await?;
            versioned_state_read_faults(&fixture, true).await
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

fn rename_entry(path: &str, directory: bool) -> Arc<ObservedEntry> {
    observation(
        BackendKind::Nfs,
        path.into(),
        if directory {
            EntryKind::Directory
        } else {
            EntryKind::File
        },
        7,
        [0, 11, 0],
        None,
        Some(b"same-stable-object".to_vec()),
        None,
        None,
    )
    .into()
}

async fn fail_after_comparison(fixture: &Fixture, database: &ClickHouseDatabase) -> Result<()> {
    fixture.fault("disconnect").await?;
    let failure = database.copy_catalogue_source_reserved().await;
    fixture.fault("normal").await?;
    assert!(
        failure.is_err(),
        "the generation must stop after a real database failure"
    );
    Ok(())
}

async fn source_cleanup_never_suppresses_new_paths_with_stable_identity(fixture: &Fixture) -> Result<()> {
    for directory in [false, true] {
        let job = format!("preserve_rename_{directory}");
        let database = fixture.open(&job).await?;
        database.begin_copy_generation(&request("same-endpoints", true)).await?;
        database
            .batch_insert_base_record(&[rename_entry("old", directory)])
            .await?;
        database.commit_scan_generation().await?;
        database
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await?;
        let database = database.new_execution_scope();
        database
            .begin_copy_generation(&request("same-endpoints", false))
            .await?;
        let changes = database
            .compare_catalogue_batch(
                &[rename_entry("new", directory)],
                ComparisonMode::AwaitCopyAcknowledgement,
            )
            .await?;
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].kind, EntryChange::New));
        assert_eq!(changes[0].item.entry.path().as_str(), "new");
        eprintln!(
            "preserve-rename cluster={} directory={directory} stable_identity=true new_path_requires_copy=true",
            fixture.config.cluster.is_some()
        );
    }
    Ok(())
}

async fn rename_after_changed_state(fixture: &Fixture, directory: bool, interruptions: usize) -> Result<()> {
    let job = format!("rename_{directory}_{interruptions}");
    let first = fixture.open(&job).await?;
    first.initialize().await?;
    first.begin_copy_generation(&request("same-endpoints", true)).await?;
    first
        .batch_insert_base_record(&[rename_entry("old", directory)])
        .await?;
    first.commit_scan_generation().await?;
    first
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let committed = first.query_scan_state().await?;
    drop(first);
    for round in 0..interruptions {
        let database = fixture.open(&job).await?;
        database.begin_copy_generation(&request("same-endpoints", true)).await?;
        assert_eq!(
            database.query_scan_state().await?,
            committed,
            "failed generations must not advance the committed state"
        );
        assert_eq!(
            database.get_cached_scan_state().await?,
            1 - committed.unwrap_or_default()
        );
        let path = if round == 0 {
            "old".into()
        } else {
            format!("renamed_{round}")
        };
        let changes = database
            .compare_catalogue_batch(
                &[rename_entry(&path, directory)],
                ComparisonMode::AwaitCopyAcknowledgement,
            )
            .await?;
        assert!(changes.is_empty());
        let state: u8 = base_client(&fixture.config)?
            .query(&format!(
                "SELECT current_state FROM base_{job} FINAL WHERE relative_path=?"
            ))
            .bind(&path)
            .fetch_one()
            .await?;
        assert_eq!(
            state,
            1 - committed.unwrap_or_default(),
            "retry uses the same working state"
        );
        if round == 0 {
            assert_ne!(
                Some(state),
                committed,
                "the same item must have persisted a changed state before failure"
            );
        }
        eprintln!(
            "rename-repro directory={directory} round={round} path={path} committed={committed:?} compared_state={state} before_failure=true"
        );
        fail_after_comparison(fixture, &database).await?;
        drop(database);
    }
    let database = fixture.open(&job).await?;
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    let latest = format!("renamed_{interruptions}");
    assert!(
        database
            .compare_catalogue_batch(
                &[rename_entry(&latest, directory)],
                ComparisonMode::AwaitCopyAcknowledgement
            )
            .await?
            .is_empty()
    );
    let changes: Vec<_> = database.detect_deleted_items().await?.collect();
    assert!(
        changes.iter().any(|change| matches!(change,
        DeletionStatus::Renamed(from, to) if from.path().as_str()=="old" && to.path().as_str()==latest)),
        "rename after confirmed comparison and failure must not disappear: {changes:?}"
    );
    Ok(())
}

async fn reused_paths_are_copied_before_replacing_confirmed_target_facts(fixture: &Fixture) -> Result<()> {
    let database = fixture.open("reused_paths").await?;
    database.initialize().await?;
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    let entry = |path: &str, identity: &[u8]| {
        Arc::new(observation(
            BackendKind::Nfs,
            path.into(),
            EntryKind::File,
            7,
            [0, 11, 0],
            None,
            Some(identity.to_vec()),
            None,
            None,
        ))
    };
    let original = [entry("a", b"identity-a"), entry("b", b"identity-b")];
    database.batch_insert_base_record(&original).await?;
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    let observed = [entry("a", b"identity-b"), entry("b", b"identity-a")];
    let changes = database
        .compare_catalogue_batch(&observed, ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert_eq!(
        changes.len(),
        2,
        "occupied paths must produce copy work rather than erase old confirmed facts"
    );
    assert!(changes.iter().all(|change| change.kind == EntryChange::New));
    let (identity, confirmed): (Option<String>, bool) = base_client(&fixture.config)?
        .query("SELECT file_handle,copy_path_confirmed FROM base_reused_paths FINAL WHERE relative_path='a'")
        .fetch_one()
        .await?;
    assert_eq!(identity, encoded_file_handle(&original[0]));
    assert!(!confirmed, "待覆盖路径的旧原值保留，但就绪证明必须撤销");
    let completed: Vec<_> = changes.into_iter().map(|change| change.item).collect();
    database.batch_insert_completed_records(&completed).await?;
    assert_eq!(database.detect_deleted_items().await?.count(), 0);
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let database = fixture.open("occupied_rename").await?;
    database.initialize().await?;
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    database.batch_insert_base_record(&original).await?;
    database.commit_scan_generation().await?;
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    let database = database.new_execution_scope();
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    let current = [entry("b", b"identity-a")];
    let changes = database
        .compare_catalogue_batch(&current, ComparisonMode::AwaitCopyAcknowledgement)
        .await?;
    assert_eq!(changes.len(), 1);
    let completed: Vec<_> = changes.into_iter().map(|change| change.item).collect();
    database.batch_insert_completed_records(&completed).await?;
    let remaining: Vec<_> = database.detect_deleted_items().await?.collect();
    assert!(
        matches!(remaining.as_slice(), [DeletionStatus::Deleted(old)] if old.path().as_str()=="a"),
        "an already copied current target must not be overwritten by an older rename: {remaining:?}"
    );
    Ok(())
}

async fn successful_rounds_advance_once_while_reusing_bindings(fixture: &Fixture) -> Result<()> {
    for reserved in [false, true] {
        let job = format!("state_cycles_{reserved}");
        let first = fixture.open(&job).await?;
        first.initialize().await?;
        drop(first);
        for round in 0..6 {
            let database = fixture.open(&job).await?;
            let committed = database.query_scan_state().await?.unwrap_or_default();
            database
                .begin_copy_generation(&request("same-endpoints", reserved))
                .await?;
            assert_eq!(database.query_scan_state().await?, Some(committed));
            assert_eq!(
                database
                    .latest_copy_generation()
                    .await?
                    .map(|generation| generation.phase),
                Some(CopyGenerationPhase::Prepared)
            );
            assert_eq!(database.get_cached_scan_state().await?, 1 - committed);
            database.commit_scan_generation().await?;
            database
                .finish_copy_generation(CopyGenerationPhase::Completed, None)
                .await?;
            assert_eq!(database.query_scan_state().await?, Some(1 - committed));
            eprintln!(
                "state-cycle cluster={} source_reserved={reserved} round={round} committed_before={committed} committed_after={} marker_confirmed=true",
                fixture.config.cluster.is_some(),
                1 - committed
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned ClickHouse pair and fault proxy from run-notification-archive-local.py --rename-generations"]
async fn real_rename_after_failed_comparison_of_the_same_item() -> Result<()> {
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            archived_incremental_changes_are_scoped_and_paginated(&fixture).await?;
            for directory in [false, true] {
                for interruptions in [1, 2, 5] {
                    rename_after_changed_state(&fixture, directory, interruptions).await?;
                }
            }
            reused_paths_are_copied_before_replacing_confirmed_target_facts(&fixture).await?;
            successful_rounds_advance_once_while_reusing_bindings(&fixture).await?;
            source_cleanup_never_suppresses_new_paths_with_stable_identity(&fixture).await?;
            Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[path = "copy_batch_recovery_real_tests.rs"]
mod batch_recovery;

#[path = "copy_retirement_real_tests.rs"]
mod retirement;
