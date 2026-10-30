use crate::{error::operation, *};
use data_mover::transfer::{TransferIdentity, TransferPolicy, TransferRequest};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsResumeMode {
    Auto,
    Restart,
    Require,
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
        let mut request = TransferRequest::new(
            source.roles.clone(),
            crate::connected::path(entry.get_relative_path())?,
            destination.roles.clone(),
            options.cancel.unwrap_or_default(),
        )
        .with_identity_override(TransferIdentity::from_label(recovery.identity).map_err(operation)?)
        .with_transfer_policy(if recovery.mode == HdfsResumeMode::Restart {
            TransferPolicy::AtomicReplace
        } else {
            TransferPolicy::Checkpointed
        });
        if let Some(qos) = options.qos {
            request = request.with_source_qos(crate::streaming::source_qos(&qos, source.block_size)?);
        }
        request = request.with_read_back_verification(if options.enable_integrity_check {
            data_mover::transfer::ReadBackVerification::Enabled
        } else {
            data_mover::transfer::ReadBackVerification::Disabled
        });
        let result = if options.is_source_reserved {
            data_mover::transfer::transfer(request).await?
        } else {
            data_mover::transfer::move_file(request).await?
        };
        if let Some(counter) = options.bytes_counter {
            counter.fetch_add(result.transferred_bytes, std::sync::atomic::Ordering::Relaxed);
        }
        (recovery.callback)(0, entry.get_size());
        Ok(())
    }
}
