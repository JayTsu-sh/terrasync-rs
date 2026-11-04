//! 独立临时数据库中的目录贡献历史、恢复截断及作业隔离回归。
use clickhouse::Client;
use db::scan_store::{ScanDirectoryStatisticsRow, ScanStore};
use db::{ClickHouseConfig, Result};
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture {
    admin: Client,
    database: String,
    config: ClickHouseConfig,
}

impl Fixture {
    async fn new() -> Result<(Self, ClickHouseConfig)> {
        let database = format!(
            "directory_ledger_{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let dsn = std::env::var("LAB_CLICKHOUSE_DSN").unwrap_or_else(|_| "http://10.131.9.11:8123".into());
        let username = std::env::var("LAB_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
        let password = std::env::var("LAB_CLICKHOUSE_PASSWORD")
            .ok()
            .filter(|value| !value.is_empty());
        let mut admin = Client::default().with_url(&dsn).with_user(&username);
        if let Some(password) = &password {
            admin = admin.with_password(password);
        }
        admin.query(&format!("CREATE DATABASE `{database}`")).execute().await?;
        let config = ClickHouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn,
            database: database.clone(),
            username,
            password,
            dial_timeout: 5,
            read_timeout: 10,
        };
        Ok((
            Self {
                admin,
                database,
                config: config.clone(),
            },
            config,
        ))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // 析构线程使用新连接池，不能等待已被当前线程 join 暂停的原 runtime。
        let mut client = Client::default()
            .with_url(&self.config.dsn)
            .with_user(&self.config.username);
        if let Some(password) = &self.config.password {
            client = client.with_password(password);
        }
        let database = self.database.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                let _ = runtime.block_on(
                    client
                        .query(&format!("DROP DATABASE IF EXISTS `{database}` SYNC"))
                        .execute(),
                );
            }
        })
        .join();
    }
}

fn row(directory: &str, payload: &str) -> ScanDirectoryStatisticsRow {
    ScanDirectoryStatisticsRow {
        directory: directory.into(),
        payload: payload.into(),
    }
}

fn paths() -> Vec<String> {
    ["", "a", "pending", "missing"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[tokio::test]
#[ignore = "requires an isolated ClickHouse single-server lab"]
async fn directory_statistics_keep_committed_cuts_through_merge_and_recovery() -> Result<()> {
    let (fixture, config) = Fixture::new().await?;
    let store = ScanStore::new(&config, "scan")?;
    store
        .write_directory_statistics(41, 1, &[row("", "3"), row("a", "1")])
        .await?;
    store.write_directory_statistics(41, 2, &[row("", "4")]).await?;
    store
        .write_directory_statistics(41, 3, &[row("", "99"), row("pending", "100")])
        .await?;
    fixture
        .admin
        .query(&format!(
            "OPTIMIZE TABLE `{}`.scan_directory_statistics_scan FINAL",
            fixture.database
        ))
        .execute()
        .await?;
    assert_eq!(
        store.load_directory_statistics(41, 1, &paths()).await?,
        vec![row("", "3"), row("a", "1")]
    );
    assert_eq!(
        store.load_directory_statistics(41, 2, &paths()).await?,
        vec![row("", "4"), row("a", "1")]
    );
    let resumed = ScanStore::new(&config, "scan")?;
    resumed.truncate_directory_statistics(41, 2).await?;
    assert_eq!(
        resumed.load_directory_statistics(41, 3, &paths()).await?,
        vec![row("", "4"), row("a", "1")]
    );
    resumed.write_directory_statistics(41, 3, &[row("", "5")]).await?;
    resumed.write_directory_statistics(41, 3, &[row("", "6")]).await?;
    assert_eq!(
        resumed.load_directory_statistics(41, 3, &paths()).await?,
        vec![row("", "6"), row("a", "1")]
    );
    assert_eq!(
        resumed.load_directory_statistics(41, 2, &paths()).await?,
        vec![row("", "4"), row("a", "1")]
    );
    store.clear_job().await?;
    let exists = fixture
        .admin
        .query(&format!(
            "EXISTS TABLE `{}`.scan_directory_statistics_scan",
            fixture.database
        ))
        .fetch_one::<u8>()
        .await?;
    assert_eq!(exists, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated ClickHouse single-server lab"]
async fn directory_statistics_truncation_and_reset_preserve_other_jobs_and_rounds() -> Result<()> {
    let (_fixture, config) = Fixture::new().await?;
    let first = ScanStore::new(&config, "first")?;
    let other = ScanStore::new(&config, "other")?;
    first.write_directory_statistics(1, 1, &[row("a", "first")]).await?;
    first
        .write_directory_statistics(1, 2, &[row("pending", "pending")])
        .await?;
    first
        .write_directory_statistics(2, 9, &[row("a", "other-round")])
        .await?;
    other.write_directory_statistics(1, 2, &[row("a", "other-job")]).await?;
    first.truncate_directory_statistics(1, 1).await?;
    assert_eq!(
        first.load_directory_statistics(1, 2, &paths()).await?,
        vec![row("a", "first")]
    );
    assert_eq!(
        first.load_directory_statistics(2, 9, &paths()).await?,
        vec![row("a", "other-round")]
    );
    first.clear_job().await?;
    assert_eq!(
        other.load_directory_statistics(1, 2, &paths()).await?,
        vec![row("a", "other-job")]
    );
    assert!(first.load_directory_statistics(1, 9, &paths()).await?.is_empty());
    Ok(())
}
