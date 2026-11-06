//! SQL failure boundaries are exercised through the actual catalogue session and HTTP client.

use std::sync::Arc;

use clickhouse::error::Error as ClickHouseError;
use clickhouse::test::{Mock, handlers};
use clickhouse::{Client, Compression};
use data_mover::model::{BackendKind, EntryKind, ObservedEntry};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};

use super::{ClickHouseDatabase, ComparisonMode};
use crate::catalogue::lifecycle::ScanGenerationLifecycle;
use crate::{ClickHouseConfig, DatabaseError, Result};

async fn catalogue(mock: &Mock) -> Result<ClickHouseDatabase> {
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
    let client = Client::default()
        .with_url(mock.url())
        .with_validation(false)
        .with_compression(Compression::None);
    let database = ClickHouseDatabase::with_client(&config, "batch_fault", client)?;
    // An initialized recovery generation classifies in one query, without schema discovery.
    *database.generation_lifecycle.lock().await = ScanGenerationLifecycle {
        working: Some(1),
        copy_recovery: true,
        ..Default::default()
    };
    Ok(database)
}

#[derive(Debug, Deserialize, PartialEq, Eq, clickhouse::Row)]
struct ExclusionReply {
    relative_path: String,
    version_id: String,
}

#[tokio::test]
async fn borrowed_exclusion_keys_preserve_the_rowbinary_wire_shape() -> Result<()> {
    let mock = Mock::new();
    let database = catalogue(&mock).await?;
    let recorded = mock.add(handlers::record::<ExclusionReply>());
    let keys = vec![
        ("目录/文件".to_owned(), String::new()),
        ("other".to_owned(), "v1".to_owned()),
    ];
    database.write_catalogue_exclusions("owned_exclusions", &keys).await?;
    let actual: Vec<ExclusionReply> = recorded.collect().await;
    let expected: Vec<_> = keys
        .into_iter()
        .map(|(relative_path, version_id)| ExclusionReply {
            relative_path,
            version_id,
        })
        .collect();
    assert_eq!(actual, expected);
    Ok(())
}

fn entry() -> Arc<ObservedEntry> {
    Arc::new(crate::observed_fixtures::observation(
        BackendKind::Nfs,
        "present".into(),
        EntryKind::File,
        1,
        [0; 3],
        None,
        None,
        None,
        None,
    ))
}

#[tokio::test]
async fn failed_insert_cleans_staging_and_a_later_batch_can_finish() -> Result<()> {
    let mock = Mock::new();
    let database = catalogue(&mock).await?;
    mock.add(handlers::record_ddl());
    mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
    let failed_cleanup = mock.add(handlers::record_ddl());
    let first = database
        .compare_catalogue_batch(&[entry()], ComparisonMode::PublishObserved)
        .await;
    assert!(matches!(
        first,
        Err(DatabaseError::ClickHouseError(ClickHouseError::BadResponse(_)))
    ));
    assert!(failed_cleanup.query().await.starts_with("DROP TABLE"));

    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<u8>::new()));
    let merge = mock.add(handlers::record_ddl());
    let cleanup = mock.add(handlers::record_ddl());
    assert!(
        database
            .compare_catalogue_batch(&[], ComparisonMode::PublishObserved)
            .await?
            .is_empty()
    );
    assert!(merge.query().await.starts_with("INSERT INTO"));
    assert!(cleanup.query().await.starts_with("DROP TABLE"));
    Ok(())
}

#[tokio::test]
async fn failed_classification_cleans_staging_without_publishing() -> Result<()> {
    let mock = Mock::new();
    let database = catalogue(&mock).await?;
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<u8>::new()));
    mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
    let cleanup = mock.add(handlers::record_ddl());
    let result = database
        .compare_catalogue_batch(&[entry()], ComparisonMode::PublishObserved)
        .await;
    assert!(matches!(
        result,
        Err(DatabaseError::ClickHouseError(ClickHouseError::BadResponse(_)))
    ));
    assert!(cleanup.query().await.starts_with("DROP TABLE"));
    Ok(())
}

#[tokio::test]
async fn classification_and_cleanup_failures_are_both_retained() -> Result<()> {
    let mock = Mock::new();
    let database = catalogue(&mock).await?;
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<u8>::new()));
    mock.add(handlers::failure(StatusCode::BAD_GATEWAY));
    mock.add(handlers::failure(StatusCode::SERVICE_UNAVAILABLE));
    let result = database
        .compare_catalogue_batch(&[entry()], ComparisonMode::PublishObserved)
        .await;
    match result {
        Err(DatabaseError::CatalogueBatchCleanupFailed { source, cleanup }) => {
            assert!(
                matches!(*source, DatabaseError::ClickHouseError(ClickHouseError::BadResponse(ref message)) if message.contains("Bad Gateway"))
            );
            assert!(
                matches!(*cleanup, DatabaseError::ClickHouseError(ClickHouseError::BadResponse(ref message)) if message.contains("Service Unavailable"))
            );
        }
        _ => panic!("both the operation and cleanup failures must survive"),
    }
    Ok(())
}

#[derive(Serialize, clickhouse::Row)]
struct DifferenceReply<'a> {
    relative_path: &'a str,
    version_id: &'a str,
    change_code: u8,
}

#[tokio::test]
async fn an_invalid_classification_cannot_publish_a_partial_plan() -> Result<()> {
    let mock = Mock::new();
    let database = catalogue(&mock).await?;
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<u8>::new()));
    mock.add(handlers::provide(["present", "absent"].map(|relative_path| {
        DifferenceReply {
            relative_path,
            version_id: "",
            change_code: 0,
        }
    })));
    let cleanup = mock.add(handlers::record_ddl());
    let result = database
        .compare_catalogue_batch(&[entry()], ComparisonMode::PublishObserved)
        .await;
    assert!(matches!(
        result,
        Err(DatabaseError::ObservationProjectionMismatch {
            field: "catalogue classification key"
        })
    ));
    assert!(cleanup.query().await.starts_with("DROP TABLE"));
    Ok(())
}
