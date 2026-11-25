//! 复制执行的互斥与恢复授权；不把轮次策略下沉到 backend。

use std::fs::File;

use crate::migration_plan::MigrationPlan;
use crate::storage_factory::StorageContext;
use blake3::Hasher;
use data_mover::{
    DslTraversalFilter,
    filter::{FilterExpression, parse_filter_expression},
};
use db::ClickHouseDatabase;
use db::CopyGenerationStatus;
use task_model::migration::{
    CopyBinding, CopyContract, CopyRequestDescriptor, CopyRuntimeSettings, CopySelection, FileSelection,
    NamespaceBinding, PathMapping, VerificationLevel,
};
use tokio::task::spawn_blocking;
use tracing::info;
use utils::app_config::{AppConfig, ClickhouseConfig};

use crate::job_execution::JobLease;
use crate::{
    config::SyncJobConfig,
    error::{AppError, Result},
};

mod database;
pub(crate) use database::CopyDatabaseExecution;

pub(crate) struct CopyLease {
    _guard: JobLease,
}

#[cfg(all(test, unix))]
#[path = "copy_completion_cancel_real_tests.rs"]
mod directory_cancel_tests;
#[cfg(all(test, unix))]
#[path = "copy_completion_real_tests.rs"]
mod directory_completion_tests;
#[cfg(test)]
#[path = "copy_mutable_object_tests.rs"]
mod mutable_tests;
#[cfg(all(test, unix))]
#[path = "copy_object_cleanup_tests.rs"]
mod object_cleanup_tests;
#[cfg(test)]
#[path = "copy_plan_tests.rs"]
mod plan_tests;
#[cfg(all(test, unix))]
#[path = "copy_ready_cleanup_nfs_tests.rs"]
mod ready_cleanup_nfs_tests;
#[cfg(all(test, unix))]
#[path = "copy_ready_cleanup_tests.rs"]
mod ready_cleanup_tests;
#[cfg(test)]
#[path = "copy_request_tests.rs"]
mod request_tests;
#[cfg(all(test, unix))]
#[path = "copy_root_real_tests.rs"]
mod root_metadata_tests;
#[cfg(all(test, unix))]
#[path = "copy_source_content_tests.rs"]
mod source_content_tests;
#[cfg(test)]
#[path = "copy_version_metadata_tests.rs"]
mod version_metadata_tests;
#[cfg(test)]
#[path = "copy_version_plan_tests.rs"]
mod version_tests;
impl CopyLease {
    pub(crate) fn acquire(connection: &ClickhouseConfig, job: &SyncJobConfig) -> Result<Self> {
        Ok(Self {
            _guard: JobLease::acquire(connection, &job.job_id, &job.job_dir)?,
        })
    }
}

pub(crate) async fn compile_copy_request(
    job: &SyncJobConfig, source: &StorageContext, target: &StorageContext, metadata: &MigrationPlan,
    config: &AppConfig, execution_id: String,
) -> Result<CopyRequestDescriptor> {
    let include = job.r#match.as_deref().map(parse_filter_expression).transpose()?;
    let exclude = job.exclude.as_deref().map(parse_filter_expression).transpose()?;
    let relative_time = include
        .iter()
        .chain(exclude.iter())
        .any(FilterExpression::has_relative_time_condition);
    if job.file_list.is_some()
        && (include.is_some() || exclude.is_some() || job.packaged || !metadata.scope.latest_version_only)
    {
        return Err(AppError::ConfigError(
            "文件清单暂不支持同时使用过滤、打包或历史目标；已拒绝，避免扩大选择或遗漏历史".into(),
        ));
    }
    let native_filter = DslTraversalFilter::new(include, exclude);
    let file_list = if let Some(path) = job.file_list.clone() {
        Some(
            spawn_blocking(move || -> Result<FileSelection> {
                let mut digest = Hasher::new();
                digest.update_reader(File::open(&path)?)?;
                Ok(FileSelection {
                    path,
                    digest: digest.finalize().to_hex().to_string(),
                })
            })
            .await??,
        )
    } else {
        None
    };
    let mut definition = Hasher::new();
    definition.update(b"terrasync/copy-selection/v1\0");
    definition.update(native_filter.selection_key());
    definition.update(&[u8::from(file_list.is_some())]);
    if let Some(list) = &file_list {
        definition.update(list.digest.as_bytes());
    }
    let namespace = |storage: &StorageContext| {
        let identity = storage.storage().identity();
        NamespaceBinding {
            backend_kind: identity.kind().as_str().into(),
            namespace: identity.stable_id().into(),
        }
    };
    Ok(CopyRequestDescriptor {
        execution_id,
        binding: CopyBinding {
            source: namespace(source),
            target: namespace(target),
            target_history: !metadata.scope.latest_version_only,
            mapping: if job.packaged {
                PathMapping::Packaged {
                    depth: job.package_depth,
                }
            } else {
                PathMapping::Relative
            },
        },
        source_cleanup: job.source_cleanup,
        selection: CopySelection {
            include: job.r#match.clone(),
            exclude: job.exclude.clone(),
            file_list,
            definition_key: definition.finalize().to_hex().to_string(),
            relative_time,
        },
        contract: CopyContract {
            acl: job.enable_acl,
            verification: if job.enable_integrity_check {
                VerificationLevel::Full
            } else {
                VerificationLevel::Quick
            },
        },
        runtime: CopyRuntimeSettings {
            transfer_mode: job.transfer_mode,
            scan_concurrency: job.concurrency.unwrap_or(config.scan.concurrency),
            copy_concurrency: job.concurrency.unwrap_or(config.sync.concurrency),
            block_size: job.block_size.clone(),
            delta_size_threshold: job.delta_size_threshold.clone(),
            qos: job.qos.clone(),
            peak_qos_rate: job.peak_qos_rate,
            iops: job.iops,
        },
        filter_evaluated_at: metadata.filter_time_anchor,
        delete_target: job.delete_target,
    })
}

pub(crate) async fn begin_copy_catalogue(database: &ClickHouseDatabase, request: &CopyRequestDescriptor) -> Result<()> {
    let status = database.begin_copy_generation(request).await?;
    if status == CopyGenerationStatus::Recovering {
        info!("Continuing failed migration with a fresh incremental scan over confirmed copy facts");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::path::Path;
    use task_model::{SourceCleanupPolicy, TransferMode};

    pub(super) fn job(directory: &Path) -> SyncJobConfig {
        SyncJobConfig {
            job_id: directory.to_string_lossy().into_owned(),
            job_dir: directory.to_string_lossy().into_owned(),
            job_dir_pre_existing: false,
            src_path: String::new(),
            dest_path: String::new(),
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
    fn directory_lock_failure_releases_the_job_identity_lock() -> Result<()> {
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
            Err(AppError::JobExecutionBusy { .. })
        ));
        drop(file);
        assert!(CopyLease::acquire(&connection(), &job).is_ok());
        Ok(())
    }
}
