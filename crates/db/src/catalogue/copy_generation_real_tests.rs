//! 真实 Single／Cluster 上验证策略切换、失败代次与记录缺失。

use std::time::{Duration, Instant};

use clickhouse::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use utils::app_config::ClickHouseMode;
use uuid::Uuid;

use super::CopyGenerationStatus;
use crate::execution::base_client;
use crate::observed_fixtures::observation;
use crate::{ClickHouseConfig, ClickHouseDatabase, DatabaseError, Result};
use data_mover::model::{BackendKind, EntryKind};

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

async fn transition(fixture: &Fixture, previous_reserved: bool, incomplete: bool, empty: bool) -> Result<()> {
    let job = format!("policy_{previous_reserved}_{incomplete}_{empty}");
    let first = fixture.open(&job).await?;
    first.initialize().await?;
    assert_eq!(first.copy_catalogue_source_reserved().await?, None);
    first.begin_copy_generation("same-endpoints", previous_reserved).await?;
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
    }
    drop(first);
    let next = fixture.open(&job).await?;
    assert_eq!(next.copy_catalogue_source_reserved().await?, Some(previous_reserved));
    let status = next.begin_copy_generation("same-endpoints", false).await?;
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
    assert_eq!(next.copy_catalogue_source_reserved().await?, Some(false));
    next.commit_scan_generation().await?;
    drop(next);
    let third = fixture.open(&job).await?;
    assert_eq!(third.copy_catalogue_source_reserved().await?, Some(false));
    assert_eq!(
        third.begin_copy_generation("same-endpoints", false).await?,
        CopyGenerationStatus::Fresh
    );
    assert_eq!(third.get_count("base").await?, 0);
    eprintln!(
        "copy-generation cluster={} previous_reserved={previous_reserved} incomplete={incomplete} missing_rows={empty} retained={expected} next_full_empty=true",
        fixture.config.cluster.is_some()
    );
    Ok(())
}

async fn read_faults(fixture: &Fixture) -> Result<()> {
    let database = fixture.open("faults").await?;
    database.initialize().await?;
    database.begin_copy_generation("same-endpoints", false).await?;
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
        let result = async {
            for previous_reserved in [false, true] {
                for incomplete in [false, true] {
                    for empty in [false, true] {
                        transition(&fixture, previous_reserved, incomplete, empty).await?;
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
