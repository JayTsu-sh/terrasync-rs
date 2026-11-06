//! 首次基线由代次提交发布，不能从建表推断扫描完成。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::ClickHouseConfig;
use crate::execution::base_client;
use clickhouse::test::{Mock, handlers};
use std::time::{SystemTime, UNIX_EPOCH};

fn mocked_database(mock: &Mock, job: &str) -> ClickHouseDatabase {
    let mut database = ClickHouseDatabase::new(
        &ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: mock.url().into(),
            database: "default".into(),
            username: "default".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        },
        job,
    )
    .unwrap();
    database.sync_client = database.sync_client.with_validation(false);
    database
}

#[tokio::test]
async fn fresh_initialization_keeps_the_baseline_uncommitted() -> Result<()> {
    let mock = Mock::new();
    let database = mocked_database(&mock, "initial_baseline");
    let _base = mock.add(handlers::record_ddl());
    mock.add(handlers::provide(["relative_path, version_id".to_string()]));
    let schema = mock.add(handlers::record_ddl());
    let migration = mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<u8>::new()));
    let initial = mock.add(handlers::record_ddl());
    database.initialize().await?;
    assert!(schema.query().await.contains("baseline_committed Bool DEFAULT true"));
    assert!(migration.query().await.contains("ADD COLUMN IF NOT EXISTS"));
    let insert = initial.query().await;
    assert!(insert.contains("(id, scan_state, baseline_committed)"));
    assert!(insert.ends_with("VALUES (1, 0, false)"), "{insert}");

    // 重连不能把已经建好的表当成已完成基线。
    let reopened = mocked_database(&mock, "initial_baseline");
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide([false]));
    assert!(!reopened.has_committed_baseline().await?);
    Ok(())
}

#[tokio::test]
async fn missing_or_empty_state_has_no_committed_baseline() -> Result<()> {
    let mock = Mock::new();
    let database = mocked_database(&mock, "missing_baseline");
    mock.add(handlers::provide([0u64]));
    assert!(!database.has_committed_baseline().await?);
    mock.add(handlers::provide([1u64]));
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(Vec::<bool>::new()));
    assert!(!database.has_committed_baseline().await?);
    Ok(())
}

#[tokio::test]
async fn generation_commit_publishes_generation_and_baseline_in_one_row() -> Result<()> {
    let mock = Mock::new();
    let database = mocked_database(&mock, "committed_baseline");
    database.generation_lifecycle.lock().await.working = Some(1);
    mock.add(handlers::record_ddl());
    let committed = mock.add(handlers::record_ddl());
    database.commit_scan_generation().await?;
    let insert = committed.query().await;
    assert!(insert.contains("(id, scan_state, baseline_committed)"));
    assert!(insert.ends_with("VALUES (1, 1, true)"), "{insert}");
    // 重新初始化必须保留已提交的状态行。
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide(["relative_path, version_id".to_string()]));
    mock.add(handlers::record_ddl());
    mock.add(handlers::record_ddl());
    mock.add(handlers::provide([1u8]));
    database.initialize().await?;
    Ok(())
}

struct IsolatedDatabase {
    config: ClickHouseConfig,
    name: String,
}
impl Drop for IsolatedDatabase {
    fn drop(&mut self) {
        let config = self.config.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                let _ = runtime.block_on(async {
                    base_client(&config)
                        .unwrap()
                        .query(&format!("DROP DATABASE IF EXISTS {name} SYNC"))
                        .execute()
                        .await
                });
            }
        })
        .join();
    }
}

async fn isolated_database() -> Result<(ClickHouseConfig, IsolatedDatabase)> {
    let dsn = std::env::var("LAB_CLICKHOUSE_DSN")
        .map_err(|_| DatabaseError::ConfigError("LAB_CLICKHOUSE_DSN must explicitly select a test server".into()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| DatabaseError::ConfigError(error.to_string()))?
        .as_nanos();
    let mut config = ClickHouseConfig {
        mode: Default::default(),
        cluster: None,
        dsn,
        database: "default".into(),
        username: std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("LAB_CLICKHOUSE_PASSWORD").ok(),
        dial_timeout: 10,
        read_timeout: 30,
    };
    let admin = base_client(&config).unwrap();
    let name = format!("ts_baseline_fixture_{timestamp}");
    admin.query(&format!("CREATE DATABASE {name}")).execute().await?;
    let guard = IsolatedDatabase {
        config: config.clone(),
        name: name.clone(),
    };
    config.database.clone_from(&name);
    Ok((config, guard))
}

#[tokio::test]
#[ignore = "requires explicit LAB_CLICKHOUSE_DSN; creates and removes an isolated database"]
async fn real_reopen_before_first_resume_state_remains_full_until_commit() -> Result<()> {
    let (config, _guard) = isolated_database().await?;
    let database = ClickHouseDatabase::new(&config, "first_full").unwrap();
    database.initialize().await?;
    drop(database);
    let reopened = ClickHouseDatabase::new(&config, "first_full").unwrap();
    assert!(!reopened.check_table_exists("scan_resume_first_full").await?);
    assert!(!reopened.has_committed_baseline().await?);
    assert!(!reopened.resume_execution_is_incremental().await?);
    assert_eq!(reopened.begin_scan_generation().await?, 1);
    assert!(!reopened.has_committed_baseline().await?);
    reopened.commit_scan_generation().await?;
    drop(reopened);
    let committed = ClickHouseDatabase::new(&config, "first_full").unwrap();
    assert!(committed.has_committed_baseline().await?);
    assert!(committed.resume_execution_is_incremental().await?);
    committed.initialize().await?;
    assert!(committed.has_committed_baseline().await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit LAB_CLICKHOUSE_DSN; creates and removes an isolated database"]
async fn real_legacy_state_row_remains_a_committed_baseline() -> Result<()> {
    let (config, _guard) = isolated_database().await?;
    let database = ClickHouseDatabase::new(&config, "legacy").unwrap();
    database.create_scan_base_table().await?;
    database
        .execute(
            "CREATE TABLE state_legacy (id UInt8,scan_state UInt8) ENGINE=ReplacingMergeTree ORDER BY id",
            &[],
        )
        .await?;
    database.execute("INSERT INTO state_legacy VALUES (1,1)", &[]).await?;
    assert!(database.has_committed_baseline().await?);
    assert!(database.resume_execution_is_incremental().await?);
    assert_eq!(database.query_scan_state().await?, Some(1));
    database.initialize().await?;
    assert!(database.has_committed_baseline().await?);
    Ok(())
}
