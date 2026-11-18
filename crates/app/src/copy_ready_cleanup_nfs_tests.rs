//! NFS 源／目标的真实就绪窗口，以及数据库失败与在途删除结果未知同时保留的报告。
#![allow(clippy::unwrap_used)]
use std::{env, time::Duration};

use clickhouse::Client;
use data_mover::model::{FailureClass, Transience};
use data_mover::storage::StorageRoleFailure;
use db::{ClickHouseConfig, DatabaseError};
use task_model::SourceCleanupPolicy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use utils::app_config::{AppConfig, ClickHouseMode};
use uuid::Uuid;

use super::{ready_cleanup_tests::check_report, tests::job};
use crate::callback::CallbackEndpoint;
use crate::error::{AppError, Result, StorageError};
use crate::orchestrator::SyncOrchestrator;

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn control(action: &str) -> Result<()> {
    let address = environment("LAB_READY_NFS_CONTROL")?;
    timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(&address).await?;
        stream
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(response.starts_with(b"HTTP/1.0 204"), "owned NFS control failed");
        Ok::<_, AppError>(())
    })
    .await
    .map_err(|_| AppError::ConfigError("owned NFS control timed out".into()))?
}

async fn case(config: &ClickHouseConfig, deployment: &str, dialect: &str, name: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let run = environment("LAB_READY_NFS_RUN")?;
    let dual = name == "dual";
    let (version, export) = if dialect == "v3" { ("3", "v3") } else { ("4.1", "v4") };
    let prefix = format!("/srv/nfs/{export}:/ci/{run}/{deployment}/{dialect}_{name}");
    let options = format!("version={version}&noresvport=true&uid=0&gid=0");
    let mut command = job(directory.path());
    command.job_id = format!("nfready_{deployment}_{dialect}_{name}");
    command.source_cleanup = SourceCleanupPolicy::DeleteSelectedReady;
    command.concurrency = Some(2);
    command.enable_integrity_check = name == "full";
    command.src_path = if dual {
        format!(
            "nfs://127.0.0.1{prefix}/source?{options}&nfsport={}",
            environment("LAB_READY_NFS_PROXY_PORT")?
        )
    } else {
        format!("nfs://10.10.1.13{prefix}/source?{options}")
    };
    command.dest_path = format!("nfs://10.10.1.13{prefix}/target?{options}");
    command.progress_callback_url = Some(CallbackEndpoint::parse(&environment("LAB_READY_NFS_CALLBACK")?)?);
    control(&format!("arm/{deployment}/{dialect}/{name}")).await?;
    let started = tokio::time::Instant::now();
    let result = timeout(
        Duration::from_secs(45),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await
    .map_err(|_| AppError::ConfigError("owned NFS ready job timed out".into()))?;
    if dual {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("owned NFS fact rejection"), "{error}");
        let (path, source) = error.source_cleanup_entry().unwrap();
        assert!(matches!(path.to_str(), Some("a" | "b")));
        match source.storage_error() {
            Some(StorageError::SourcePurgeRefused { source, .. }) => {
                let (class, transience) = match source.source.as_ref() {
                    StorageRoleFailure::Entry(failure) => (failure.class(), failure.transience()),
                    StorageRoleFailure::Session(failure) => (failure.class(), failure.transience()),
                };
                assert_eq!(class, FailureClass::Cancelled);
                assert_eq!(transience, Transience::Unknown);
            }
            other => panic!("in-flight purge evidence missing: {other:?}"),
        }
    } else {
        result?;
    }
    check_report(config, &command, dual).await?;
    control(&format!("finish/{deployment}/{dialect}/{name}")).await?;
    println!(
        "NFS ready-window deployment={deployment} dialect={dialect} case={name} elapsed_ms={} final_report_saved=true",
        started.elapsed().as_millis()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-source-ready-nfs.py owned NFS prefixes, private relay and isolated ClickHouse"]
async fn real_nfs_full_move_ready_window_and_combined_failure() -> Result<()> {
    AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
    let dsn = environment("LAB_READY_NFS_DSN")?;
    let admin = Client::default().with_url(&dsn).with_user("default");
    for cluster in [false, true] {
        let config = ClickHouseConfig {
            mode: if cluster {
                ClickHouseMode::Cluster
            } else {
                ClickHouseMode::Single
            },
            cluster: cluster.then(|| "notification_cluster".into()),
            dsn: dsn.clone(),
            database: format!("ts_nfready_{}", Uuid::new_v4().simple()),
            username: "default".into(),
            password: None,
            dial_timeout: 10,
            read_timeout: 60,
        };
        let suffix = if cluster {
            " ON CLUSTER notification_cluster"
        } else {
            ""
        };
        admin
            .query(&format!("CREATE DATABASE {}{suffix}", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from)?;
        AppConfig::override_with(|global| {
            global.database.enabled = true;
            global.database.clickhouse = config.clone();
        })?;
        let result: Result<()> = async {
            for dialect in ["v3", "v4"] {
                for name in ["quick", "full"] {
                    case(&config, if cluster { "cluster" } else { "single" }, dialect, name).await?;
                }
            }
            case(&config, if cluster { "cluster" } else { "single" }, "v4", "dual").await?;
            Ok(())
        }
        .await;
        let cleanup = admin
            .query(&format!("DROP DATABASE {}{suffix} SYNC", config.database))
            .execute()
            .await
            .map_err(DatabaseError::from);
        result?;
        cleanup?;
    }
    Ok(())
}
