use crate::*;
use crate::{connected::path, streaming::source_qos};
use data_mover::{
    model::SourceVersion,
    transfer::{self, ReadBackVerification, TransferIdentity, TransferRequest},
};
use std::sync::atomic::Ordering;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsResumeMode {
    Auto,
    Restart,
    Require,
    Direct,
}
pub struct HdfsRecoverableCopyOptions {
    identity: String,
    callback: CommitCallback,
    mode: HdfsResumeMode,
}
impl HdfsRecoverableCopyOptions {
    pub fn new(identity: impl Into<String>, callback: CommitCallback) -> Self {
        Self {
            identity: identity.into(),
            callback,
            mode: HdfsResumeMode::Auto,
        }
    }
    pub fn with_resume_mode(mut self, mode: HdfsResumeMode) -> Self {
        self.mode = mode;
        self
    }
}
impl StorageEnum {
    pub async fn copy_file_hdfs_recoverable(
        source: &Self, destination: &Self, entry: &EntryEnum, options: CopyOptions,
        recovery: HdfsRecoverableCopyOptions,
    ) -> Result<()> {
        if recovery.mode == HdfsResumeMode::Require {
            return Err(StorageError::ConfigError(
                "the current transfer API does not support require-resume; use automatic destination recovery".into(),
            ));
        }
        let mode = match recovery.mode {
            HdfsResumeMode::Auto => TransferMode::Checkpoint,
            HdfsResumeMode::Restart => TransferMode::Atomic,
            HdfsResumeMode::Direct => TransferMode::Direct,
            HdfsResumeMode::Require => unreachable!("require-resume was rejected"),
        };
        destination.validate_transfer_mode(mode)?;
        let mut request = TransferRequest::new(
            source.roles.clone(),
            path(entry.get_relative_path())?,
            destination.roles.clone(),
            options.cancel.unwrap_or_default(),
        )
        .with_identity_override(TransferIdentity::from_label(recovery.identity).map_err(StorageError::from)?)
        .with_transfer_policy(mode.policy());
        if let Some(version) = entry.get_version_id() {
            request = request.with_source_version(SourceVersion::Id(version.into()));
        }
        if let Some(qos) = options.qos {
            request = request.with_source_qos(source_qos(&qos, source.block_size)?);
        }
        request = request.with_read_back_verification(if options.enable_integrity_check {
            ReadBackVerification::Enabled
        } else {
            ReadBackVerification::Disabled
        });
        let result = if options.is_source_reserved {
            transfer::transfer(request).await?
        } else {
            transfer::move_file(request).await?
        };
        if let Some(counter) = options.bytes_counter {
            counter.fetch_add(result.transferred_bytes, Ordering::Relaxed);
        }
        (recovery.callback)(0, entry.get_size());
        Ok(())
    }
}
