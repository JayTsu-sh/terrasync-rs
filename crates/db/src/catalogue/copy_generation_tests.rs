use std::{sync::Arc, time::Duration};

use data_mover::model::{
    BackendKind, EntryKind, ExtendedAttribute, MetadataObservation, MetadataObservations, MetadataProvenance,
    ObservedEntry, OwnershipMode,
};
use tokio::time::timeout;

use clickhouse::{
    Client, Compression,
    test::{
        Mock,
        handlers::{self, RecordDdlControl},
    },
};
use hyper::StatusCode;
use task_model::TaskModelError;
use task_model::migration::{CopyGeneration, CopyGenerationPhase, CopyGenerationPlan, MigrationMode};

use super::{CopyGenerationStatus, CopyState};
use crate::catalogue::copy_runs::CopyRunRow;
use crate::migration_fixtures::request;
use crate::observed_fixtures::observation;
use crate::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, Result};

fn database(mock: &Mock) -> Result<ClickHouseDatabase> {
    let config = ClickHouseConfig {
        mode: Default::default(),
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
        "migration_state",
        Client::default()
            .with_url(mock.url())
            .with_validation(false)
            .with_compression(Compression::None),
    )
}

fn prior(binding: &str, reserved: bool, phase: CopyGenerationPhase, prepared: bool) -> CopyGeneration {
    let request = request(binding, reserved);
    CopyGeneration {
        root_snapshot: None,
        sequence: 4,
        revision: 3,
        plan: CopyGenerationPlan::decide(&request, None, false).unwrap_or_else(|error| panic!("fixture: {error}")),
        request,
        phase,
        baseline_prepared: prepared,
    }
}

fn expect_previous(mock: &Mock, previous: Option<&CopyGeneration>, committed: u8) {
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide([1u64]));
    let rows = previous.map(|generation| CopyRunRow {
        sequence: generation.sequence,
        execution_id: generation.request.execution_id.clone(),
        revision: generation.revision,
        root_snapshot: hex::encode(generation.root_snapshot.as_deref().unwrap_or_default()),
        descriptor: serde_json::to_string(generation).unwrap_or_else(|error| panic!("fixture: {error}")),
    });
    mock.add(handlers::provide(rows));
    if let Some(previous) = previous {
        if previous.baseline_prepared {
            mock.add(handlers::provide([1u64]));
            mock.add(handlers::provide([1u64]));
            mock.add(handlers::provide([CopyState {
                scan_state: committed,
                copy_execution_id: previous.request.execution_id.clone(),
            }]));
        }
    } else {
        mock.add(handlers::provide([1u64]));
        mock.add(handlers::provide([CopyState {
            scan_state: committed,
            copy_execution_id: String::new(),
        }]));
    }
}

fn expect_accepted_and_initialized(mock: &Mock, committed: u8, old_execution: &str) -> RecordDdlControl {
    let accepted = mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(["relative_path,version_id".to_owned()]));
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide([committed]));
    mock.add(handlers::provide([CopyState {
        scan_state: committed,
        copy_execution_id: old_execution.into(),
    }]));
    accepted
}

#[tokio::test]
async fn accepted_policy_precedes_clear_and_copy_commit_keeps_its_execution_stamp() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    expect_previous(&mock, None, 0);
    let accepted = expect_accepted_and_initialized(&mock, 0, "");
    let clear = mock.add(handlers::record_ddl());
    let stamp = mock.add(handlers::record_ddl());
    let prepared = mock.add(handlers::record_ddl());
    let command = request("bound", true);
    assert_eq!(
        database.begin_copy_generation(&command).await?,
        CopyGenerationStatus::Fresh
    );
    assert!(accepted.query().await.contains("accepted"));
    assert!(clear.query().await.contains("DELETE WHERE copy_sequence < 1"));
    assert!(stamp.query().await.contains(&command.execution_id));
    assert!(prepared.query().await.contains("prepared"));
    let commit = mock.add(handlers::record_ddl());
    database.commit_scan_generation().await?;
    assert!(commit.query().await.contains(&command.execution_id));
    let finish = mock.add(handlers::record_ddl());
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, None)
        .await?;
    assert!(finish.query().await.contains("completed"));
    assert_eq!(
        database.current_copy_generation().await?.phase,
        CopyGenerationPhase::Completed
    );
    Ok(())
}

#[tokio::test]
async fn failed_initial_clear_retains_accepted_policy_but_never_authorizes_copy() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    expect_previous(&mock, None, 0);
    let accepted = expect_accepted_and_initialized(&mock, 0, "");
    mock.add(handlers::failure(StatusCode::FORBIDDEN));
    assert!(database.begin_copy_generation(&request("bound", false)).await.is_err());
    assert!(accepted.query().await.contains("delete_selected_ready"));
    let generation = database.current_copy_generation().await?;
    assert_eq!(generation.phase, CopyGenerationPhase::Accepted);
    assert!(!generation.baseline_prepared);
    assert!(database.generation_lifecycle.lock().await.working.is_none());
    Ok(())
}

#[tokio::test]
async fn failed_retained_copy_rebases_without_removing_unconfirmed_tombstones() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    let previous = prior("bound", true, CopyGenerationPhase::Failed, true);
    expect_previous(&mock, Some(&previous), 0);
    expect_accepted_and_initialized(&mock, 0, &previous.request.execution_id);
    let rebase = mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    assert_eq!(
        database.begin_copy_generation(&request("bound", true)).await?,
        CopyGenerationStatus::Recovering
    );
    assert!(rebase.query().await.contains("current_state!=0 AND copy_sequence < 5"));
    assert_eq!(
        database.current_copy_generation().await?.plan.mode,
        MigrationMode::Incremental
    );
    Ok(())
}

#[tokio::test]
async fn foreign_binding_is_rejected_without_acceptance_or_baseline_mutation() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    let previous = prior("original", true, CopyGenerationPhase::Failed, true);
    expect_previous(&mock, Some(&previous), 0);
    assert!(matches!(
        database.begin_copy_generation(&request("foreign", true)).await,
        Err(DatabaseError::MigrationPolicy(TaskModelError::CopyBindingChanged))
    ));
    assert!(matches!(
        database.current_copy_generation().await,
        Err(DatabaseError::CopyExecutionUnavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn unprepared_attempt_and_previous_source_deletion_both_force_a_new_full() -> Result<()> {
    for (reserved, prepared) in [(false, true), (true, false)] {
        let mock = Mock::new();
        let database = database(&mock)?;
        let previous = prior("bound", reserved, CopyGenerationPhase::Failed, prepared);
        expect_previous(&mock, Some(&previous), 1);
        expect_accepted_and_initialized(&mock, 1, &previous.request.execution_id);
        let clear = mock.add(handlers::record_ddl());
        mock.add(handlers::record_ddl());
        mock.add(handlers::record_ddl());
        assert_eq!(
            database.begin_copy_generation(&request("bound", true)).await?,
            CopyGenerationStatus::Fresh
        );
        assert!(clear.query().await.contains("copy_sequence < 5"));
        assert_eq!(database.current_copy_generation().await?.plan.mode, MigrationMode::Full);
    }
    Ok(())
}

fn provide_generation(mock: &Mock, generation: &CopyGeneration) {
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::provide([CopyRunRow {
        sequence: generation.sequence,
        execution_id: generation.request.execution_id.clone(),
        revision: generation.revision,
        root_snapshot: hex::encode(generation.root_snapshot.as_deref().unwrap_or_default()),
        descriptor: serde_json::to_string(generation).unwrap_or_else(|error| panic!("fixture: {error}")),
    }]));
}

async fn retirement_fixture(mock: &Mock) -> Result<(ClickHouseDatabase, CopyGeneration)> {
    let database = database(mock)?;
    let generation = prior("retirement", false, CopyGenerationPhase::Prepared, true);
    let mut lifecycle = database.generation_lifecycle.lock().await;
    lifecycle.working = Some(1);
    lifecycle.committed = true;
    lifecycle.copy_execution_id = Some(generation.request.execution_id.clone());
    lifecycle.copy_generation = Some(generation.clone());
    drop(lifecycle);
    Ok((database, generation))
}

fn expect_retirement_owner(mock: &Mock, generation: &CopyGeneration) {
    provide_generation(mock, generation);
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::provide([CopyState {
        scan_state: 1,
        copy_execution_id: generation.request.execution_id.clone(),
    }]));
}

#[tokio::test]
async fn source_deletion_completion_waits_for_writers_then_closes_all_base_entry_points() -> Result<()> {
    let mock = Mock::new();
    let (database, generation) = retirement_fixture(&mock).await?;
    expect_retirement_owner(&mock, &generation);
    let deleted = mock.add(handlers::record_ddl());
    mock.add(handlers::provide([0u64]));
    let terminal = mock.add(handlers::record_ddl());
    let writer = database.base_write_guard().await?;
    let cloned = database.clone();
    let mut finishing = tokio::spawn(async move {
        cloned
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await
    });
    assert!(timeout(Duration::from_millis(50), &mut finishing).await.is_err());
    drop(writer);
    finishing
        .await
        .map_err(|source| DatabaseError::ConfigError(format!("fixture task: {source}")))??;
    let sql = deleted.query().await;
    assert!(sql.contains("copy_sequence>0 AND copy_sequence<=4"));
    assert!(sql.contains("mutations_sync=2"));
    assert!(terminal.query().await.contains("completed"));
    let entry = Arc::new(observation(
        BackendKind::Local,
        "late".into(),
        EntryKind::File,
        7,
        [0; 3],
        None,
        None,
        None,
        None,
    ));
    for result in [
        database.clone().batch_insert_base_record(&[entry.clone()]).await,
        database.insert_temp_to_base_table(&[]).await,
        database.merge_native().await,
        database.write_native(&[entry], false).await,
    ] {
        assert!(matches!(
            result,
            Err(DatabaseError::CopyGenerationInvalid {
                field: "base_writes_closed"
            })
        ));
    }
    assert!(database.new_execution_scope().base_write_guard().await.is_ok());
    Ok(())
}

#[tokio::test]
async fn failed_or_cancelled_source_deletion_still_retires_the_base() -> Result<()> {
    for phase in [CopyGenerationPhase::Failed, CopyGenerationPhase::Cancelled] {
        let mock = Mock::new();
        let (database, generation) = retirement_fixture(&mock).await?;
        expect_retirement_owner(&mock, &generation);
        mock.add(handlers::record_ddl());
        mock.add(handlers::provide([0u64]));
        mock.add(handlers::record_ddl());
        database.finish_copy_generation(phase, None).await?;
        assert_eq!(database.current_copy_generation().await?.phase, phase);
        assert!(database.base_write_guard().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn cleanup_refuses_a_newer_execution_before_mutating_any_base_rows() -> Result<()> {
    let mock = Mock::new();
    let (database, mut generation) = retirement_fixture(&mock).await?;
    generation.sequence += 1;
    provide_generation(&mock, &generation);
    let terminal = mock.add(handlers::record_ddl());
    assert!(matches!(
        database.finish_copy_generation(CopyGenerationPhase::Completed, None).await,
        Err(DatabaseError::CopyBaseRetirementFailed { source, .. }) if matches!(*source, DatabaseError::CopyGenerationInvalid { field: "stale_base_retirement" })
    ));
    assert!(terminal.query().await.contains("failed"));
    assert_eq!(
        database.current_copy_generation().await?.phase,
        CopyGenerationPhase::Failed
    );
    Ok(())
}

#[tokio::test]
async fn rows_outside_the_owned_copy_range_are_retained_and_reported_as_unconfirmed() -> Result<()> {
    let mock = Mock::new();
    let (database, generation) = retirement_fixture(&mock).await?;
    expect_retirement_owner(&mock, &generation);
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide([2u64]));
    mock.add(handlers::record_ddl());
    assert!(matches!(
        database.finish_copy_generation(CopyGenerationPhase::Completed, None).await,
        Err(DatabaseError::CopyBaseRetirementFailed { source, .. }) if matches!(*source, DatabaseError::CopyBaseRetirementIncomplete { remaining: 2, sequence: 4 })
    ));
    assert_eq!(
        database.current_copy_generation().await?.phase,
        CopyGenerationPhase::Failed
    );
    Ok(())
}

#[tokio::test]
async fn cleanup_and_terminal_record_failures_keep_both_typed_causes() -> Result<()> {
    let mock = Mock::new();
    let (database, generation) = retirement_fixture(&mock).await?;
    expect_retirement_owner(&mock, &generation);
    mock.add(handlers::failure(StatusCode::FORBIDDEN));
    mock.add(handlers::failure(StatusCode::FORBIDDEN));
    assert!(matches!(
        database
            .finish_copy_generation(CopyGenerationPhase::Completed, None)
            .await,
        Err(DatabaseError::CopyBaseRetirementReportingFailed { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn root_raw_column_roundtrips_large_values_without_consuming_descriptor_budget() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    let mut generation = prior("root-raw", true, CopyGenerationPhase::Prepared, true);
    generation.request.binding.source.namespace = "terrasync-fixture".into();
    let root = observation(
        BackendKind::Local,
        "".into(),
        EntryKind::Directory,
        0,
        [0, 1_700_000_000_123_456_789, 0],
        Some(OwnershipMode {
            uid: 1000,
            gid: 1001,
            mode: 0o750,
        }),
        Some(vec![1]),
        None,
        None,
    );
    let original = root.metadata();
    let metadata = MetadataObservations::new(
        original.acl().clone(),
        MetadataObservation::Value {
            value: vec![ExtendedAttribute::new(b"user.large-raw".to_vec(), vec![254; 400_000])?],
            provenance: MetadataProvenance::AdditionalCall,
        },
        original.tags().clone(),
        original.ownership_mode().clone(),
        original.timestamps().clone(),
    )?;
    let root = root.with_metadata(metadata);
    let snapshot = root.try_encode_snapshot()?.as_bytes().to_vec();
    {
        let mut lifecycle = database.generation_lifecycle.lock().await;
        lifecycle.committed = true;
        lifecycle.copy_generation = Some(generation.clone());
    }
    let saved = mock.add(handlers::record_ddl());
    database
        .finish_copy_generation(CopyGenerationPhase::Completed, Some(snapshot.clone()))
        .await?;
    let sql = saved.query().await;
    assert!(sql.contains("root_snapshot"));
    assert!(!sql.contains("root_snapshot\":null"));
    generation = database.current_copy_generation().await?;
    assert_eq!(generation.revision, 4);
    assert_eq!(generation.root_snapshot.as_deref(), Some(snapshot.as_slice()));
    let descriptor =
        serde_json::to_string(&generation).map_err(|source| DatabaseError::CopyGenerationJson { source })?;
    assert!(descriptor.len() < 10_000);
    assert!(!descriptor.contains("root_snapshot"));
    for supplied in [Some(snapshot.clone()), None] {
        mock.add(handlers::record_ddl());
        database
            .finish_copy_generation(CopyGenerationPhase::Completed, supplied)
            .await?;
        assert_eq!(database.current_copy_generation().await?, generation);
    }
    provide_generation(&mock, &generation);
    let stored = database
        .latest_copy_generation()
        .await?
        .ok_or(DatabaseError::CopyExecutionUnavailable)?;
    assert_eq!(stored, generation);
    let restored = ObservedEntry::decode_snapshot(
        stored.root_snapshot.as_deref().unwrap_or_default(),
        root.source_identity().backend(),
    )?;
    assert_eq!(restored, root);
    assert!(
        !serde_json::to_string(&stored.report())
            .map_err(|source| DatabaseError::CopyGenerationJson { source })?
            .contains("root_snapshot")
    );
    Ok(())
}

#[tokio::test]
async fn invalid_root_snapshot_does_not_change_terminal_phase_or_revision() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    let mut generation = prior("root-invalid", true, CopyGenerationPhase::Prepared, true);
    generation.request.binding.source.namespace = "terrasync-fixture".into();
    {
        let mut lifecycle = database.generation_lifecycle.lock().await;
        lifecycle.committed = true;
        lifecycle.copy_generation = Some(generation.clone());
    }
    assert!(
        database
            .finish_copy_generation(CopyGenerationPhase::Completed, Some(vec![1, 2, 3]))
            .await
            .is_err()
    );
    for (path, kind) in [("child", EntryKind::Directory), ("", EntryKind::File)] {
        let entry = observation(
            BackendKind::Local,
            path.into(),
            kind,
            0,
            [0; 3],
            None,
            Some(vec![1]),
            None,
            None,
        );
        let raw = entry.try_encode_snapshot()?.as_bytes().to_vec();
        assert!(matches!(
            database
                .finish_copy_generation(CopyGenerationPhase::Completed, Some(raw))
                .await,
            Err(DatabaseError::CopyGenerationInvalid { field: "root_snapshot" })
        ));
    }
    assert_eq!(database.current_copy_generation().await?, generation);
    mock.add(handlers::record_ddl());
    database
        .finish_copy_generation(CopyGenerationPhase::Failed, None)
        .await?;
    assert_eq!(
        database.current_copy_generation().await?.phase,
        CopyGenerationPhase::Failed
    );
    Ok(())
}

#[tokio::test]
async fn invalid_root_input_cannot_close_or_retire_a_source_deleting_baseline() -> Result<()> {
    let mut inputs = vec![(vec![1, 2, 3], false)];
    for (path, kind) in [("child", EntryKind::Directory), ("", EntryKind::File)] {
        let entry = observation(
            BackendKind::Local,
            path.into(),
            kind,
            0,
            [0; 3],
            None,
            Some(vec![1]),
            None,
            None,
        );
        inputs.push((entry.try_encode_snapshot()?.as_bytes().to_vec(), true));
    }
    for phase in [
        CopyGenerationPhase::Completed,
        CopyGenerationPhase::Failed,
        CopyGenerationPhase::Cancelled,
    ] {
        for (snapshot, encoded) in &inputs {
            let mock = Mock::new();
            let (database, mut generation) = retirement_fixture(&mock).await?;
            generation.request.binding.source.namespace = "terrasync-fixture".into();
            database.generation_lifecycle.lock().await.copy_generation = Some(generation.clone());
            // 不提供任何 SQL 响应：输入验证必须早于 base 关闭、查询或删除。
            let result = database.finish_copy_generation(phase, Some(snapshot.clone())).await;
            if *encoded {
                assert!(
                    matches!(
                        result,
                        Err(DatabaseError::CopyGenerationInvalid { field: "root_snapshot" })
                    ),
                    "{phase:?}: {result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(DatabaseError::SnapshotDecode(_))),
                    "{phase:?}: {result:?}"
                );
            }
            assert_eq!(database.current_copy_generation().await?, generation);
            assert!(database.base_write_guard().await.is_ok());
        }
    }
    Ok(())
}
