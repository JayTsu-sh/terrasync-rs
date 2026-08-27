use std::sync::Arc;

use data_mover::hdfs::HdfsResumeMode;
use data_mover::{CommitCallback, CopyOptions, EntryEnum, HdfsRecoverableCopyOptions, StorageEnum};

use crate::error::{AppError, Result};

/// Caller-owned recovery inputs for one HDFS destination copy.
#[derive(Clone)]
pub(crate) struct HdfsCopyContext {
    transfer_identity: String,
    resume_mode: HdfsResumeMode,
    on_committed: CommitCallback,
}

impl HdfsCopyContext {
    pub(crate) fn from_job(job_id: &str, no_resume: bool) -> Result<Self> {
        let resume_mode = if no_resume {
            HdfsResumeMode::Restart
        } else {
            HdfsResumeMode::Auto
        };
        Self::with_mode(job_id, resume_mode)
    }

    pub(crate) fn with_mode(job_id: &str, resume_mode: HdfsResumeMode) -> Result<Self> {
        if job_id.trim().is_empty() {
            return Err(AppError::ConfigError(
                "HDFS recoverable copy requires a stable job identity".to_string(),
            ));
        }
        Ok(Self {
            transfer_identity: job_id.to_string(),
            resume_mode,
            on_committed: Arc::new(|_, _| {}),
        })
    }

    pub(crate) fn with_commit_callback(mut self, on_committed: CommitCallback) -> Self {
        self.on_committed = on_committed;
        self
    }

    pub(crate) fn transfer_identity(&self) -> &str {
        &self.transfer_identity
    }

    pub(crate) const fn resume_mode(&self) -> HdfsResumeMode {
        self.resume_mode
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataApplication {
    Caller,
    DataMover,
}

pub(crate) async fn copy_file(
    from: &StorageEnum, to: &StorageEnum, entry: &EntryEnum, options: CopyOptions, hdfs: Option<&HdfsCopyContext>,
) -> Result<MetadataApplication> {
    if matches!(to, StorageEnum::HDFS(_)) {
        let context = hdfs
            .ok_or_else(|| AppError::ConfigError("HDFS recoverable copy requires a stable job identity".to_string()))?;
        let recovery = HdfsRecoverableCopyOptions::new(context.transfer_identity(), context.on_committed.clone())
            .with_resume_mode(context.resume_mode());
        StorageEnum::copy_file_hdfs_recoverable(from, to, entry, options, recovery).await?;
        return Ok(MetadataApplication::DataMover);
    }
    StorageEnum::copy_file(from, to, entry, options).await?;
    Ok(MetadataApplication::Caller)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use data_mover::hdfs::HdfsResumeMode;
    use data_mover::{CreateStorageOptions, StorageEnum, create_storage};

    use super::HdfsCopyContext;

    #[test]
    fn hdfs_context_defaults_to_auto_and_keeps_stable_job_identity() {
        let first = HdfsCopyContext::from_job("persistent-job", false)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));
        let restarted = HdfsCopyContext::from_job("persistent-job", false)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));

        assert_eq!(first.transfer_identity(), "persistent-job");
        assert_eq!(first.transfer_identity(), restarted.transfer_identity());
        assert_eq!(first.resume_mode(), HdfsResumeMode::Auto);
    }

    #[test]
    fn legacy_no_resume_maps_to_hdfs_restart() {
        let context = HdfsCopyContext::from_job("persistent-job", true)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));

        assert_eq!(context.resume_mode(), HdfsResumeMode::Restart);
    }

    #[test]
    fn hdfs_context_rejects_missing_stable_job_identity() {
        assert!(HdfsCopyContext::from_job("", false).is_err());
        assert!(HdfsCopyContext::from_job("   ", false).is_err());
    }

    #[test]
    fn hdfs_context_can_express_require_mode() {
        let context = HdfsCopyContext::with_mode("persistent-job", HdfsResumeMode::Require)
            .unwrap_or_else(|error| panic!("valid Require context rejected: {error}"));

        assert_eq!(context.resume_mode(), HdfsResumeMode::Require);
    }

    #[tokio::test]
    async fn non_hdfs_destination_keeps_the_legacy_copy_route() {
        let source_root = tempfile::tempdir().unwrap_or_else(|error| panic!("source tempdir: {error}"));
        let destination_root = tempfile::tempdir().unwrap_or_else(|error| panic!("destination tempdir: {error}"));
        tokio::fs::write(source_root.path().join("payload.bin"), b"legacy route")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let source = create_storage(
            source_root
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("source path is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create source storage: {error}"));
        let destination = create_storage(
            destination_root
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("destination path is not UTF-8")),
            CreateStorageOptions::new(None, false),
        )
        .await
        .unwrap_or_else(|error| panic!("create destination storage: {error}"));
        let entry = source
            .get_metadata(Path::new("payload.bin"))
            .await
            .unwrap_or_else(|error| panic!("source metadata: {error}"));

        let metadata = super::copy_file(&source, &destination, &entry, data_mover::CopyOptions::default(), None)
            .await
            .unwrap_or_else(|error| panic!("legacy local copy: {error}"));

        assert_eq!(metadata, super::MetadataApplication::Caller);
        assert!(matches!(destination, StorageEnum::Local(_)));
        assert_eq!(
            tokio::fs::read(destination_root.path().join("payload.bin"))
                .await
                .unwrap_or_else(|error| panic!("read destination: {error}")),
            b"legacy route"
        );
    }
}
