//! 复制执行的互斥与恢复授权；不把轮次策略下沉到 backend。
use crate::job_execution::JobLease;
use crate::{
    config::SyncJobConfig,
    error::{AppError, Result},
    storage_factory::{StorageRole, create_storage_for_role},
};
use blake3::Hasher;
use db::{CopyGenerationStatus, Database};
use std::fs::File;
use tokio::task::spawn_blocking;
use tracing::info;
use utils::app_config::ClickhouseConfig;

pub(crate) struct CopyLease {
    _guard: JobLease,
}
impl CopyLease {
    pub(crate) fn acquire(connection: &ClickhouseConfig, job: &SyncJobConfig) -> Result<Self> {
        Ok(Self {
            _guard: JobLease::acquire(connection, &job.job_id, &job.job_dir)?,
        })
    }
}

pub(crate) async fn begin_copy_catalogue(
    database: &dyn Database, job: &SyncJobConfig, block_size: Option<usize>, source_reserved: bool,
) -> Result<()> {
    // 身份不包含凭据；恢复计划变化必须在任何 payload 或回标前被拒绝。
    let source = create_storage_for_role(
        &job.src_path,
        block_size.map(|value| value as u64),
        false,
        StorageRole::Source,
    )
    .await?;
    let target = create_storage_for_role(
        &job.dest_path,
        block_size.map(|value| value as u64),
        true,
        StorageRole::Destination,
    )
    .await?;
    let source = source.storage().identity();
    let target_identity = target.storage().identity();
    let history = target.is_bucket_versioned().await?;
    let list_digest = if let Some(path) = job.file_list.clone() {
        Some(
            spawn_blocking(move || -> Result<String> {
                let mut digest = Hasher::new();
                digest.update_reader(File::open(path)?)?;
                Ok(digest.finalize().to_hex().to_string())
            })
            .await??,
        )
    } else {
        None
    };
    let request_key = blake3::hash(&serde_json::to_vec(&(
        source.kind().as_str(),
        source.stable_id(),
        target_identity.kind().as_str(),
        target_identity.stable_id(),
        source_reserved,
        history,
        &job.r#match,
        &job.exclude,
        &job.file_list,
        job.packaged,
        job.package_depth,
        job.enable_acl,
        job.enable_integrity_check,
        job.transfer_mode,
        list_digest,
    ))?)
    .to_hex()
    .to_string();
    let status = database.begin_copy_generation(&request_key).await?;
    require_safe_copy_recovery(status, source_reserved, history)?;
    if status == CopyGenerationStatus::Recovering && job.file_list.is_some() {
        return Err(AppError::ConfigError(
            "文件清单复制未完成，不支持改用完整目录自动续作；请核对未完成条目".into(),
        ));
    }
    if status == CopyGenerationStatus::Recovering {
        info!("Recovering incomplete copy by replaying the full source and reconciling target paths");
    }
    Ok(())
}

fn require_safe_copy_recovery(status: CopyGenerationStatus, source_reserved: bool, target_history: bool) -> Result<()> {
    if status == CopyGenerationStatus::Recovering {
        if !source_reserved {
            return Err(AppError::ConfigError(
                "上次移动任务未完成，不能按源缺失项自动清理目标；请核对已搬运数据后恢复任务".into(),
            ));
        }
        if target_history {
            return Err(AppError::ConfigError(
                "上次复制未完成，版本化目标无法确认未应答的写入；请先核对目标版本，禁止自动重放".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::path::Path;
    use task_model::TransferMode;

    fn job(directory: &Path) -> SyncJobConfig {
        SyncJobConfig {
            job_id: directory.to_string_lossy().into_owned(),
            job_dir: directory.to_string_lossy().into_owned(),
            job_dir_pre_existing: false,
            src_path: String::new(),
            dest_path: String::new(),
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
            raw_command_line: String::new(),
            progress_callback_url: None,
            transfer_mode: TransferMode::Direct,
            delete_target: false,
            concurrency: None,
        }
    }

    fn connection() -> ClickhouseConfig {
        ClickhouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: "http://lease-fixture".into(),
            database: "fixture".into(),
            username: "fixture".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        }
    }

    #[test]
    fn same_database_job_cannot_be_recovered_while_running_in_another_directory() -> Result<()> {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        let mut job = job(first.path());
        let lease = CopyLease::acquire(&connection(), &job)?;
        job.job_dir = second.path().to_string_lossy().into_owned();
        assert!(CopyLease::acquire(&connection(), &job).is_err());
        let mut other_user = connection();
        other_user.username = "another-database-user".into();
        assert!(CopyLease::acquire(&other_user, &job).is_err());
        drop(lease);
        assert!(CopyLease::acquire(&connection(), &job).is_ok());
        Ok(())
    }

    #[test]
    fn file_lock_failure_releases_the_process_registration() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let job = job(directory.path());
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.path().join("job.lock"))?;
        file.try_lock().map_err(|source| AppError::CopyJobLock {
            job_id: job.job_id.clone(),
            source,
        })?;
        assert!(matches!(
            CopyLease::acquire(&connection(), &job),
            Err(AppError::JobExecutionLock { .. })
        ));
        drop(file);
        assert!(CopyLease::acquire(&connection(), &job).is_ok());
        Ok(())
    }

    #[test]
    fn only_preserved_sources_and_overwritable_targets_allow_automatic_recovery() {
        assert!(require_safe_copy_recovery(CopyGenerationStatus::Fresh, false, true).is_ok());
        assert!(require_safe_copy_recovery(CopyGenerationStatus::Recovering, true, false).is_ok());
        assert!(require_safe_copy_recovery(CopyGenerationStatus::Recovering, false, false).is_err());
        assert!(require_safe_copy_recovery(CopyGenerationStatus::Recovering, true, true).is_err());
    }
}
