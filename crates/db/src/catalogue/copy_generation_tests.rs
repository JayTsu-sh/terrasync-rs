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
    database.finish_copy_generation(CopyGenerationPhase::Completed).await?;
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
