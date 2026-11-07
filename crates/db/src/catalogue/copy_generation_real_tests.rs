//! 真实 Single／Cluster 上验证策略切换、失败代次与记录缺失。
use crate::migration_fixtures::request;
use task_model::migration::CopyGenerationPhase;

use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::CopyGenerationStatus;
use crate::DeletionStatus;
use crate::catalogue::ComparisonMode;
use crate::execution::base_client;
use crate::observed_fixtures::observation;
use crate::schema::identity::encoded_file_handle;
use crate::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, Result};
use data_mover::model::{BackendKind, EntryKind, ObservedEntry};
use task_model::EntryChange;

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
        first.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
    next.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
            read_faults(&fixture).await
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
        database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
    first.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
    assert!(confirmed);
    let completed: Vec<_> = changes.into_iter().map(|change| change.item).collect();
    database.batch_insert_completed_records(&completed).await?;
    assert_eq!(database.detect_deleted_items().await?.count(), 0);
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
    let database = fixture.open("occupied_rename").await?;
    database.initialize().await?;
    database.begin_copy_generation(&request("same-endpoints", true)).await?;
    database.batch_insert_base_record(&original).await?;
    database.commit_scan_generation().await?;
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
            database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
