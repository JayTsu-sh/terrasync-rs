use clickhouse::{
    Client, Compression,
    test::{Mock, handlers},
};
use hyper::StatusCode;

use super::{CopyGenerationStatus, CopyState};
use crate::{ClickHouseConfig, ClickHouseDatabase, Result};

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

#[tokio::test]
async fn initial_copy_clears_scan_rows_and_committed_copy_keeps_its_binding() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    mock.add(handlers::provide([CopyState {
        scan_state: 0,
        copy_in_progress: false,
        copy_request_key: String::new(),
        copy_source_reserved: true,
    }]));
    let clear = mock.add(handlers::record_ddl());
    let begin = mock.add(handlers::record_ddl());
    assert_eq!(
        database.begin_copy_generation("bound-plan", true).await?,
        CopyGenerationStatus::Fresh
    );
    assert!(clear.query().await.starts_with("TRUNCATE TABLE"));
    assert!(begin.query().await.contains("bound-plan"));
    let commit = mock.add(handlers::record_ddl());
    database.commit_scan_generation().await?;
    let commit = commit.query().await;
    assert!(commit.contains("true,false,'bound-plan',true"));
    Ok(())
}

#[tokio::test]
async fn failed_initial_clear_cannot_start_a_copy_generation() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    mock.add(handlers::provide([CopyState {
        scan_state: 0,
        copy_in_progress: false,
        copy_request_key: String::new(),
        copy_source_reserved: true,
    }]));
    mock.add(handlers::failure(StatusCode::FORBIDDEN));
    assert!(database.begin_copy_generation("bound-plan", true).await.is_err());
    assert!(database.generation_lifecycle.lock().await.working.is_none());
    Ok(())
}

#[tokio::test]
async fn failed_copy_rebases_confirmed_rows_without_clearing_or_forcing_replay() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    mock.add(handlers::provide([CopyState {
        scan_state: 0,
        copy_in_progress: true,
        copy_request_key: "bound-plan".into(),
        copy_source_reserved: true,
    }]));
    let rebase = mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    assert_eq!(
        database.begin_copy_generation("bound-plan", true).await?,
        CopyGenerationStatus::Recovering
    );
    assert!(rebase.query().await.starts_with("ALTER TABLE"));
    assert!(!database.is_copy_recovery().await);
    Ok(())
}

#[tokio::test]
async fn different_endpoint_or_plan_is_refused_before_mutating_confirmed_facts() -> Result<()> {
    let mock = Mock::new();
    let database = database(&mock)?;
    mock.add(handlers::provide([CopyState {
        scan_state: 0,
        copy_in_progress: true,
        copy_request_key: "original-plan".into(),
        copy_source_reserved: true,
    }]));
    assert!(database.begin_copy_generation("foreign-plan", true).await.is_err());
    Ok(())
}

#[tokio::test]
async fn source_policy_controls_baseline_reuse_even_after_an_incomplete_generation() -> Result<()> {
    for previous_reserved in [false, true] {
        for source_reserved in [false, true] {
            for incomplete in [false, true] {
                let mock = Mock::new();
                let database = database(&mock)?;
                mock.add(handlers::provide([CopyState {
                    scan_state: 1,
                    copy_in_progress: incomplete,
                    copy_request_key: "same-endpoints".into(),
                    copy_source_reserved: previous_reserved,
                }]));
                let reset = !previous_reserved && !source_reserved;
                let mutation = if reset || incomplete {
                    Some(mock.add(handlers::record_ddl()))
                } else {
                    None
                };
                let begin = mock.add(handlers::record_ddl());
                let actual = database
                    .begin_copy_generation("same-endpoints", source_reserved)
                    .await?;
                let expected = if incomplete && !reset {
                    CopyGenerationStatus::Recovering
                } else {
                    CopyGenerationStatus::Fresh
                };
                assert_eq!(actual, expected);
                if let Some(mutation) = mutation {
                    let sql = mutation.query().await;
                    assert!(
                        sql.starts_with(if reset { "TRUNCATE TABLE" } else { "ALTER TABLE" }),
                        "{sql}"
                    );
                }
                assert!(
                    begin
                        .query()
                        .await
                        .ends_with(&format!("'same-endpoints',{source_reserved})"))
                );
                let commit = mock.add(handlers::record_ddl());
                database.commit_scan_generation().await?;
                assert!(
                    commit
                        .query()
                        .await
                        .ends_with(&format!("'same-endpoints',{source_reserved})"))
                );
            }
        }
    }
    Ok(())
}
