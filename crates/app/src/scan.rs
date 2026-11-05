//! 扫描统一入口：落库使用持久化检查点，不落库使用轻量流式统计。
use crate::error::{AppError, Result};
use crate::scan_resume::run_persisted;
#[cfg(feature = "license")]
use licensing::{get_global_license, verify::quick_verify};
use tokio_util::sync::CancellationToken;
use utils::app_config::AppConfig;

pub(crate) mod cancellation;
pub(crate) mod progress;
pub(crate) mod statistics;
mod stream;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScanType {
    Full,
    Incremental,
}

impl std::fmt::Display for ScanType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
        })
    }
}

/// CLI 与 GUI 共用的扫描请求；持久化及全量／增量选择由扫描模块负责。
pub struct ScanRequest<'a> {
    pub resume: bool,
    pub job_id: &'a str,
    pub job_dir: &'a str,
    pub path: &'a str,
    pub depth: u32,
    pub include: &'a Option<String>,
    pub exclude: &'a Option<String>,
    pub command: String,
    pub cancel: CancellationToken,
    pub callback_url: Option<String>,
    pub concurrency: Option<usize>,
}

pub async fn scan(request: ScanRequest<'_>) -> Result<()> {
    #[cfg(feature = "license")]
    if let Ok(license) = get_global_license() {
        quick_verify(license)?;
    }
    let mut config = AppConfig::fetch()?;
    if request.resume && !config.database.enabled {
        return Err(AppError::ConfigError("扫描 --resume 需要启用 ClickHouse".into()));
    }
    if let Some(concurrency) = request.concurrency {
        if concurrency == 0 {
            return Err(AppError::ConfigError("扫描并发数必须大于 0".into()));
        }
        config.scan.concurrency = concurrency;
    }
    if config.database.enabled {
        run_persisted(request, config).await
    } else {
        stream::run(request, config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::Duration};
    use tokio::time::timeout;

    #[tokio::test]
    #[serial_test::serial]
    async fn public_gateway_repeats_streaming_scans_and_never_falls_back_from_a_database_error() -> Result<()> {
        AppConfig::init(Some(include_str!("../../cli/src/resources/default_config.toml")), None)?;
        AppConfig::override_with(|config| config.database.enabled = false)?;
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let job = directory.path().join("job");
        fs::create_dir_all(&source)?;
        fs::create_dir_all(&job)?;
        fs::write(source.join("fixture.txt"), "fixture")?;
        let source = source.to_string_lossy();
        let job = job.to_string_lossy();
        for _ in 0..2 {
            scan(ScanRequest {
                resume: false,
                job_id: "gateway",
                job_dir: &job,
                path: &source,
                depth: 0,
                include: &None,
                exclude: &None,
                command: "fixture".into(),
                cancel: CancellationToken::new(),
                callback_url: None,
                concurrency: Some(2),
            })
            .await?;
        }
        assert!(!directory.path().join("job/job.lock").exists());
        AppConfig::override_with(|config| {
            config.database.enabled = true;
            config.database.clickhouse.dsn = "http://127.0.0.1:1".into();
            config.database.clickhouse.dial_timeout = 1;
            config.database.clickhouse.read_timeout = 1;
        })?;
        let persisted = timeout(
            Duration::from_secs(5),
            scan(ScanRequest {
                resume: false,
                job_id: "gateway",
                job_dir: &job,
                path: &source,
                depth: 0,
                include: &None,
                exclude: &None,
                command: "fixture".into(),
                cancel: CancellationToken::new(),
                callback_url: None,
                concurrency: None,
            }),
        )
        .await;
        assert!(matches!(persisted, Ok(Err(AppError::DatabaseError(_)))));
        assert!(directory.path().join("job/job.lock").exists());
        Ok(())
    }
}
