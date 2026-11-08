//! 真实增量比较和事实消费者共用一次执行的恢复预算及最初原因。
use std::time::{Duration, Instant};

use db::ClickHouseDatabase;
use serde::Deserialize;
use task_model::{SourceCleanupPolicy, TransferMode};
use tokio::time::timeout;
use utils::app_config::AppConfig;

use super::{Fixture, has_database_cause};
use crate::callback::CallbackEndpoint;
use crate::config::SyncJobConfig;
use crate::error::{AppError, Result};
use crate::orchestrator::SyncOrchestrator;

#[derive(Deserialize)]
struct FaultStats {
    reads: u64,
    elapsed_ms: Option<u64>,
}

async fn case(fixture: &Fixture, mode: &str) -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let directory = tempfile::tempdir()?;
    let callback = std::env::var("LAB_COPY_EXECUTION_CALLBACK")
        .map_err(|_| AppError::ConfigError("owned task execution callback missing".into()))?;
    let command = SyncJobConfig {
        job_id: format!("task_execution_{mode}"),
        job_dir: directory.path().to_string_lossy().into_owned(),
        job_dir_pre_existing: false,
        src_path: source.path().to_string_lossy().into_owned(),
        dest_path: target.path().to_string_lossy().into_owned(),
        source_cleanup: SourceCleanupPolicy::Retain,
        enable_integrity_check: false,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: 1.0,
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: "owned task database execution test".into(),
        progress_callback_url: Some(CallbackEndpoint::parse(&callback)?),
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: Some(2),
    };
    AppConfig::override_with(|global| {
        global.database.enabled = true;
        global.database.batch_size = 2;
        global.database.clickhouse = fixture.config.clone();
    })?;
    std::fs::write(source.path().join("previous"), b"unchanged previous payload")?;
    SyncOrchestrator::new_local(command.clone()).run().await?;
    for index in 0..4 {
        std::fs::write(source.path().join(format!("payload-{index}")), b"new payload")?;
    }
    fixture.control(mode).await?;
    let start = Instant::now();
    let result = timeout(
        Duration::from_secs(8),
        SyncOrchestrator::new_local(command.clone()).run(),
    )
    .await;
    let elapsed = start.elapsed();
    let observed: FaultStats = serde_json::from_slice(&fixture.control("compare_stats").await?)?;
    let fault_ms = observed
        .elapsed_ms
        .ok_or_else(|| AppError::ConfigError("comparison fault was never exercised".into()))?;
    assert!(observed.reads > 0);
    fixture.control("normal").await?;
    let result = result.map_err(|_| AppError::ConfigError("comparison task exceeded owned deadline".into()))?;
    let recovered = matches!(mode, "compare_flash" | "compare_lost" | "compare_recover_stall");
    if recovered {
        result?;
    } else {
        let error = result
            .err()
            .ok_or_else(|| AppError::ConfigError("comparison failure reported success".into()))?;
        assert!(has_database_cause(&error), "{error}");
        if mode == "compare_reject" {
            assert!(error.to_string().contains("owned comparison rejection"), "{error}");
        }
    }
    let database = ClickHouseDatabase::connect(&fixture.config, &command.job_id).await?;
    assert_eq!(database.get_count("base").await?, if recovered { 5 } else { 1 });
    for index in 0..4 {
        assert_eq!(
            std::fs::read(source.path().join(format!("payload-{index}")))?,
            b"new payload"
        );
        if recovered {
            assert_eq!(
                std::fs::read(target.path().join(format!("payload-{index}")))?,
                b"new payload"
            );
        }
    }
    assert_eq!(
        std::fs::read(target.path().join("previous"))?,
        b"unchanged previous payload"
    );
    match mode {
        "compare_disconnect" => assert!((2900..5000).contains(&fault_ms)),
        "compare_stall" => assert!((3900..6000).contains(&fault_ms)),
        "compare_reject" => {
            assert!(fault_ms < 2000);
            assert!(observed.reads <= 2, "permission refusals cannot be replayed");
        }
        _ => {}
    }
    eprintln!(
        "copy-task-comparison cluster={} mode={mode} recovered={recovered} elapsed_ms={} fault_to_report_ms={fault_ms} source_retained=true previous_target_preserved=true",
        fixture.config.cluster.is_some(),
        elapsed.as_millis()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires --copy-execution owned ClickHouse pair, comparison fault proxy and callback"]
async fn real_copy_comparison_uses_task_database_execution() -> Result<()> {
    AppConfig::init(
        Some(include_str!("../../../../cli/src/resources/default_config.toml")),
        None,
    )?;
    for cluster in [false, true] {
        let fixture = Fixture::new(cluster).await?;
        let result: Result<()> = async {
            for mode in [
                "compare_flash",
                "compare_lost",
                "compare_recover_stall",
                "compare_disconnect",
                "compare_stall",
                "compare_reject",
            ] {
                case(&fixture, mode).await?;
            }
            Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}
