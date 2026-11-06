use crate::broadcast::BroadcastForwarder;
use crate::file_copy::namespace as copy_namespace;
use data_mover::model::BackendKind;
use data_mover::model::EntryKind;
use data_mover::qos::QosManager;
use data_mover::storage::CopiedValueTarget;
use data_mover::transfer::TransferPolicy;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use task_model::observation::ObservedItem;
use task_model::{EntryChange, StorageEntryMessage};
use task_model::{ObservedEntryExt, TransferMode};
use tokio_util::sync::CancellationToken;

use crate::byte_resume::CommitCallback;
use crate::storage_factory::StorageContext;
use data_mover::model::ObservedEntry;

use crate::error::{AppError, Result};

pub(crate) mod metadata;
pub(crate) mod namespace;
pub(crate) mod transfer;

#[derive(Clone, Default)]
pub struct CopyOptions {
    pub transfer_mode: TransferMode,
    pub qos: Option<QosManager>,
    pub enable_integrity_check: bool,
    pub is_source_reserved: bool,
    pub bytes_counter: Option<Arc<AtomicU64>>,
    pub cancel: Option<CancellationToken>,
}

pub(crate) const fn transfer_policy(mode: TransferMode) -> TransferPolicy {
    match mode {
        TransferMode::Checkpoint => TransferPolicy::Checkpointed,
        TransferMode::Atomic => TransferPolicy::AtomicReplace,
        TransferMode::Direct => TransferPolicy::Direct,
    }
}

/// 一次 HDFS 目标复制所需的调用方恢复参数。
#[derive(Clone)]
pub(crate) struct HdfsCopyContext {
    transfer_identity: String,
    mode: TransferMode,
    on_committed: CommitCallback,
}

impl HdfsCopyContext {
    pub(crate) fn from_job(job_id: &str, mode: TransferMode) -> Result<Self> {
        if job_id.trim().is_empty() {
            return Err(AppError::ConfigError(
                "HDFS recoverable copy requires a stable job identity".to_string(),
            ));
        }
        Ok(Self {
            transfer_identity: job_id.to_string(),
            mode,
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

    pub(crate) const fn transfer_mode(&self) -> TransferMode {
        self.mode
    }
}

/// 复制契约取决于已连接目标实例的能力，不受扫描展示开关影响。
pub(crate) fn copied_tags_required(to: &StorageContext) -> crate::error::storage::Result<bool> {
    Ok(to
        .storage()
        .staged_destination()?
        .copied_metadata_target()
        .is_some_and(|target| target.tags == CopiedValueTarget::Supported))
}

/// 真实目录由 namespace 创建；虚拟目录无需落地，目录标记交给普通文件传输。
pub(crate) async fn prepare_directory(entry: &ObservedEntry, to: &StorageContext) -> Result<bool> {
    if entry.kind() != EntryKind::Directory {
        return Ok(false);
    }
    if to.storage().holds_directory_objects() {
        return Ok(entry.directory_marker_object().is_none());
    }
    copy_namespace::create_dir_all(to.storage(), entry).await?;
    Ok(true)
}

pub(crate) async fn copy_file(
    from: &StorageContext, to: &StorageContext, entry: &Arc<ObservedEntry>, mut options: CopyOptions,
    hdfs: Option<&HdfsCopyContext>,
) -> Result<()> {
    if to.roles.kind() == BackendKind::Hdfs {
        let context = hdfs
            .ok_or_else(|| AppError::ConfigError("HDFS recoverable copy requires a stable job identity".to_string()))?;
        options.transfer_mode = context.transfer_mode();
        transfer::copy_current(from, to, entry, &options, Some(context.transfer_identity())).await?;
        (context.on_committed)(0, entry.get_size());
    } else {
        transfer::copy_entry(from, to, entry, options).await?;
    }
    Ok(())
}

/// 全量与增量共用源端确认的逐版本顺序；每个目标版本提交后才发送对应的完成事件。
pub(crate) async fn copy_versions<I: Iterator<Item = ObservedItem> + Send>(
    items: I, from: &StorageContext, to: &StorageContext, options: CopyOptions, job_id: &str,
    broadcaster: &BroadcastForwarder<StorageEntryMessage>, change: EntryChange,
) -> Result<()> {
    let keep_history = to.storage().staged_destination()?.keeps_history().await?;
    let hdfs = if to.storage().kind() == BackendKind::Hdfs {
        Some(HdfsCopyContext::from_job(job_id, options.transfer_mode)?)
    } else {
        None
    };
    for item in items.filter(|item| retain_version(item, keep_history)) {
        if !to.storage().holds_directory_objects() && item.entry.kind() == EntryKind::Directory {
            copy_namespace::create_dir_all(to.storage(), &item.entry).await?;
        } else {
            transfer::copy_item(
                from,
                to,
                &item,
                &options,
                hdfs.as_ref().map(HdfsCopyContext::transfer_identity),
            )
            .await?;
        }
        broadcaster.broadcast(change.message(item)).await;
    }
    Ok(())
}

/// 借用原生历史选择目标实际保留的版本，不分配或复制版本列表。
#[cfg(test)]
pub(crate) fn selected_copy_items(
    entry: &Arc<ObservedEntry>, keep_history: bool,
) -> impl Iterator<Item = ObservedItem> + '_ {
    ObservedItem::all(entry).filter(move |item| retain_version(item, keep_history))
}
fn retain_version(item: &ObservedItem, keep_history: bool) -> bool {
    item.view()
        .version()
        .is_none_or(|record| keep_history || (record.is_latest() && !record.is_delete_marker()))
}

#[cfg(test)]
mod tests {
    use crate::scan::observe_path;
    use data_mover::model::BackendKind;
    use tokio::fs;

    use std::{path::Path, sync::Arc};

    use crate::storage_factory::{CreateStorageOptions, create_storage};

    use super::{CopyOptions, HdfsCopyContext, TransferMode};

    #[test]
    fn checkpoint_mode_keeps_stable_job_identity_and_uses_auto_recovery() {
        let first = HdfsCopyContext::from_job("persistent-job", TransferMode::Checkpoint)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));
        let restarted = HdfsCopyContext::from_job("persistent-job", TransferMode::Checkpoint)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));

        assert_eq!(first.transfer_identity(), "persistent-job");
        assert_eq!(first.transfer_identity(), restarted.transfer_identity());
        assert_eq!(first.transfer_mode(), TransferMode::Checkpoint);
    }

    #[test]
    fn atomic_mode_maps_to_hdfs_restart() {
        let context = HdfsCopyContext::from_job("persistent-job", TransferMode::Atomic)
            .unwrap_or_else(|error| panic!("valid job identity rejected: {error}"));

        assert_eq!(context.transfer_mode(), TransferMode::Atomic);
    }

    #[test]
    fn direct_mode_does_not_use_hdfs_recovery() {
        let context = HdfsCopyContext::from_job("persistent-job", TransferMode::Direct)
            .unwrap_or_else(|error| panic!("valid Direct context rejected: {error}"));
        assert_eq!(context.transfer_mode(), TransferMode::Direct);
    }

    #[test]
    fn hdfs_context_rejects_missing_stable_job_identity() {
        assert!(HdfsCopyContext::from_job("", TransferMode::Checkpoint).is_err());
        assert!(HdfsCopyContext::from_job("   ", TransferMode::Checkpoint).is_err());
    }

    #[tokio::test]
    async fn ordinary_copy_applies_metadata_inside_data_mover() {
        let source_root = tempfile::tempdir().unwrap_or_else(|error| panic!("source tempdir: {error}"));
        let destination_root = tempfile::tempdir().unwrap_or_else(|error| panic!("destination tempdir: {error}"));
        fs::write(source_root.path().join("payload.bin"), b"legacy route")
            .await
            .unwrap_or_else(|error| panic!("write source: {error}"));
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_001);
        std::fs::File::open(source_root.path().join("payload.bin"))
            .and_then(|file| file.set_modified(modified))
            .unwrap_or_else(|error| panic!("set source mtime: {error}"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                source_root.path().join("payload.bin"),
                std::fs::Permissions::from_mode(0o640),
            )
            .unwrap_or_else(|error| panic!("set source permissions: {error}"));
        }
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
        let entry = observe_path(source.storage(), Path::new("payload.bin"))
            .await
            .unwrap_or_else(|error| panic!("source metadata: {error}"));
        let entry = Arc::new(entry);

        super::copy_file(&source, &destination, &entry, CopyOptions::default(), None)
            .await
            .unwrap_or_else(|error| panic!("legacy local copy: {error}"));

        let metadata = std::fs::metadata(destination_root.path().join("payload.bin"))
            .unwrap_or_else(|error| panic!("read destination metadata: {error}"));
        assert_eq!(
            metadata.modified().unwrap_or_else(|error| panic!("mtime: {error}")),
            modified
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o640);
        }
        assert_eq!(destination.storage().kind(), BackendKind::Local);
        assert_eq!(
            fs::read(destination_root.path().join("payload.bin"))
                .await
                .unwrap_or_else(|error| panic!("read destination: {error}")),
            b"legacy route"
        );
    }
}
