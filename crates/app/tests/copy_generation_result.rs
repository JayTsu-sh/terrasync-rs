//! 真实回调验收通过与 CLI 共用的单进程迁移入口。
use std::{env, path::Path};

use app::{callback::CallbackEndpoint, config::SyncJobConfig, sync::sync};
use task_model::{SourceCleanupPolicy, TransferMode};
use utils::app_config::AppConfig;

#[tokio::test]
#[ignore = "requires the isolated copy generation lab"]
async fn copy_generation_callback_entrypoint() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = env::var("LAB_COPY_RESULT_CONFIG")?;
    AppConfig::init(
        Some(include_str!("../../cli/src/resources/default_config.toml")),
        Some(Path::new(&config)),
    )?;
    let job_dir = env::var("LAB_COPY_RESULT_JOB_DIR")?;
    let config = AppConfig::fetch()?;
    let request = SyncJobConfig {
        job_id: "mig_result".into(),
        job_dir_pre_existing: Path::new(&job_dir).exists(),
        job_dir,
        src_path: env::var("LAB_COPY_RESULT_SOURCE")?,
        dest_path: env::var("LAB_COPY_RESULT_TARGET")?,
        source_cleanup: SourceCleanupPolicy::from_reserved(config.sync.is_source_reserved),
        enable_integrity_check: true,
        enable_acl: false,
        r#match: None,
        exclude: None,
        qos: None,
        peak_qos_rate: config.sync.peak_qos_rate.unwrap_or(2.0),
        block_size: None,
        delta_size_threshold: None,
        file_list: None,
        iops: None,
        packaged: false,
        package_depth: 0,
        raw_command_line: "copy generation callback lab".into(),
        progress_callback_url: Some(CallbackEndpoint::parse(&env::var("LAB_COPY_RESULT_CALLBACK")?)?),
        transfer_mode: TransferMode::Direct,
        delete_target: false,
        concurrency: None,
    };
    sync(request).await?;
    Ok(())
}
