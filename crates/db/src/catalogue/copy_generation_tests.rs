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
    }]));
    let clear = mock.add(handlers::record_ddl());
    let begin = mock.add(handlers::record_ddl());
    assert_eq!(
        database.begin_copy_generation("bound-plan").await?,
        CopyGenerationStatus::Fresh
    );
    assert!(clear.query().await.starts_with("TRUNCATE TABLE"));
    assert!(begin.query().await.contains("bound-plan"));
    let commit = mock.add(handlers::record_ddl());
    database.commit_scan_generation().await?;
    let commit = commit.query().await;
    assert!(commit.contains("true,false,'bound-plan'"));
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
    }]));
    mock.add(handlers::failure(StatusCode::FORBIDDEN));
    assert!(database.begin_copy_generation("bound-plan").await.is_err());
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
    }]));
    let rebase = mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    assert_eq!(
        database.begin_copy_generation("bound-plan").await?,
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
    }]));
    assert!(database.begin_copy_generation("foreign-plan").await.is_err());
    Ok(())
}
