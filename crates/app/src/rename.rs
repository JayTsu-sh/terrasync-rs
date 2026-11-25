//! 目标 rename 的执行与缺失旧路径恢复；复制复用同一任务策略，不信任未确认发布。
use crate::copy_bytes::CopyBytes;
use std::sync::Arc;

use data_mover::model::ObservedEntry;
use data_mover::qos::QosManager;
use task_model::StorageEntryMessage;
use tracing::warn;

use crate::broadcast::BroadcastForwarder;
use crate::error::{Result, StorageError};
use crate::storage_factory::StorageContext;
use crate::sync::{ResumeOpts, process_entry};

pub(crate) struct RenameOptions<'a> {
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub resume: ResumeOpts,
    pub qos: Option<QosManager>,
    pub bytes: Option<Arc<CopyBytes>>,
    pub events: &'a BroadcastForwarder<StorageEntryMessage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenameOutcome {
    Moved,
    Copied,
}

pub(crate) async fn rename_or_copy(
    from: Arc<ObservedEntry>, to: Arc<ObservedEntry>, source: Arc<StorageContext>, target: Arc<StorageContext>,
    options: RenameOptions<'_>,
) -> Result<RenameOutcome> {
    match target
        .roles
        .namespace()?
        .rename(from.path(), to.path())
        .await
        .map_err(StorageError::from)
    {
        Ok(()) => return Ok(RenameOutcome::Moved),
        Err(error) if error.is_not_found() => {
            // 前轮可能完成了物理移动但未记账；从当前源复制，避免假定新目标内容可靠。
            warn!(from = %from.path(), to = %to.path(), %error, "Old target path missing; recovering rename by copying current source");
        }
        Err(error) => return Err(error.into()),
    }
    process_entry(
        &to,
        source,
        target,
        options.enable_integrity_check,
        options.enable_acl,
        true,
        options.qos,
        options.bytes,
        options.events,
        options.resume,
    )
    .await?;
    Ok(RenameOutcome::Copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::observe_path;
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use std::path::Path;
    use task_model::TransferMode;
    use tempfile::tempdir;

    #[tokio::test]
    async fn already_moved_but_unrecorded_target_is_recovered_from_current_source() -> Result<()> {
        let source_root = tempdir()?;
        let target_root = tempdir()?;
        std::fs::write(source_root.path().join("current"), b"current source payload")?;
        std::fs::write(target_root.path().join("old"), b"previous payload")?;
        let source =
            Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let target =
            Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let from = Arc::new(observe_path(target.storage(), Path::new("old")).await?);
        let to = Arc::new(observe_path(source.storage(), Path::new("current")).await?);
        std::fs::rename(target_root.path().join("old"), target_root.path().join("intermediate"))?;
        std::fs::write(target_root.path().join("current"), b"unconfirmed or partial data")?;
        let events = BroadcastForwarder::new(4);
        let bytes = Arc::new(CopyBytes::default());
        let result = rename_or_copy(
            from,
            to,
            source,
            target,
            RenameOptions {
                enable_integrity_check: true,
                enable_acl: false,
                resume: ResumeOpts {
                    transfer_mode: TransferMode::Direct,
                    ..Default::default()
                },
                qos: None,
                bytes: Some(Arc::clone(&bytes)),
                events: &events,
            },
        )
        .await?;
        assert_eq!(result, RenameOutcome::Copied);
        assert_eq!(
            std::fs::read(target_root.path().join("current"))?,
            b"current source payload"
        );
        assert_eq!(
            std::fs::read(source_root.path().join("current"))?,
            b"current source payload"
        );
        assert_eq!(
            std::fs::read(target_root.path().join("intermediate"))?,
            b"previous payload"
        );
        assert_eq!(bytes.published(), 22);
        Ok(())
    }

    #[tokio::test]
    async fn a_confirmed_old_target_is_moved_without_copying_data() -> Result<()> {
        let source_root = tempdir()?;
        let target_root = tempdir()?;
        std::fs::write(source_root.path().join("new"), b"payload")?;
        std::fs::write(target_root.path().join("old"), b"payload")?;
        let source =
            Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let target =
            Arc::new(create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
        let from = Arc::new(observe_path(target.storage(), Path::new("old")).await?);
        let to = Arc::new(observe_path(source.storage(), Path::new("new")).await?);
        let events = BroadcastForwarder::new(4);
        let bytes = Arc::new(CopyBytes::default());
        let result = rename_or_copy(
            from,
            to,
            source,
            target,
            RenameOptions {
                enable_integrity_check: false,
                enable_acl: false,
                resume: ResumeOpts::default(),
                qos: None,
                bytes: Some(Arc::clone(&bytes)),
                events: &events,
            },
        )
        .await?;
        assert_eq!(result, RenameOutcome::Moved);
        assert!(!target_root.path().join("old").exists());
        assert_eq!(std::fs::read(target_root.path().join("new"))?, b"payload");
        assert!(source_root.path().join("new").exists());
        assert_eq!(bytes.published(), 0);
        Ok(())
    }
}
