//! 只消费本次重新选中且目标已确认落库的事实；不清理范围外的旧源项。

use std::sync::Arc;

use data_mover::model::{EntryKind, ObservedEntry};
use db::ClickHouseDatabase;
use task_model::observation::ObservedView;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::error::{AppError, Result, StorageError};
use crate::file_copy::metadata::copied_metadata_plan;
use crate::file_copy::selection::unrepresentable_object;
use crate::storage_factory::StorageContext;

/// 调用方必须先排空复制和数据库消费者，并完成目标目录元数据与代次提交。
pub(crate) async fn remove_confirmed_sources(
    database: ClickHouseDatabase, source: Arc<StorageContext>, destination: Arc<StorageContext>,
) -> Result<()> {
    let (sender, mut receiver) = mpsc::channel(32);
    let query = AbortOnDropHandle::new(tokio::spawn(async move {
        database.query_current_copy_entries(None, None, None, sender).await
    }));
    while let Some(entry) = receiver.recv().await {
        // 一旦清理失败就停止发出新删除，避免断连后继续扩大未知副作用。
        remove_entry(&source, &destination, &entry).await?;
    }
    query.await.map_err(AppError::from)??;
    Ok(())
}

async fn remove_entry(source: &StorageContext, destination: &StorageContext, entry: &ObservedEntry) -> Result<()> {
    // 存储根和虚拟目录不是本次传输拥有的可删除实体。
    if entry.path().as_str().is_empty() || unrepresentable_object(entry, destination).is_some() {
        return Ok(());
    }
    if entry.history().is_some() || source.storage().holds_directory_objects() {
        return Err(StorageError::PermanentVersionCleanupUnavailable {
            path: entry.path().clone(),
        }
        .into());
    }
    // 只核验目标与落库的原始事实；删源改变的目录 mtime 和已经消失的源项不回写目标。
    let target = copied_metadata_plan(
        destination.storage(),
        ObservedView {
            entry,
            version_index: None,
        },
    )?
    .verify_published(destination.storage(), entry.path(), None, CancellationToken::new())
    .await
    .map_err(StorageError::from)?;
    if target.kind != entry.kind() || (entry.kind() == EntryKind::File && target.size != entry.size()) {
        return Err(StorageError::TargetPublicationChanged {
            path: entry.path().clone(),
        }
        .into());
    }
    if entry.kind() == EntryKind::Symlink
        && Some(
            &destination
                .storage()
                .namespace()?
                .read_link(entry.path())
                .await
                .map_err(StorageError::from)?,
        ) != entry.symlink_target()
    {
        return Err(StorageError::TargetPublicationChanged {
            path: entry.path().clone(),
        }
        .into());
    }
    // 仅删除文件、链接本身或空目录；过滤掉或并发新增的子项会阻止目录删除。
    source
        .storage()
        .namespace()?
        .purge_observed(entry, CancellationToken::new())
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::file_copy::{CopyOptions, copy_file, metadata::set_entry_metadata};
    use crate::observed_fixtures::observation;
    use crate::scan::observe_path;
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use data_mover::model::BackendKind;
    use std::path::Path;

    #[tokio::test]
    async fn skipped_slash_object_never_reaches_source_purge() -> Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("a"), b"retained")?;
        let storage = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let entry = observation(
            BackendKind::S3,
            "a/".into(),
            EntryKind::File,
            8,
            [0; 3],
            None,
            None,
            None,
            None,
        );
        remove_entry(&storage, &storage, &entry).await?;
        assert_eq!(std::fs::read(root.path().join("a"))?, b"retained");
        Ok(())
    }

    #[tokio::test]
    async fn changed_source_is_retained_and_cleanup_never_recurses() -> Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("tree"))?;
        std::fs::write(root.path().join("tree/file"), b"before")?;
        let storage = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let file = observe_path(storage.storage(), Path::new("tree/file")).await?;
        let directory = observe_path(storage.storage(), Path::new("tree")).await?;
        std::fs::write(root.path().join("tree/file"), b"changed and longer")?;
        assert!(remove_entry(&storage, &storage, &file).await.is_err());
        assert!(remove_entry(&storage, &storage, &directory).await.is_err());
        assert_eq!(std::fs::read(root.path().join("tree/file"))?, b"changed and longer");
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_entry_cleanup_is_idempotent() -> Result<()> {
        let root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"payload")?;
        let storage = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let entry = observe_path(storage.storage(), Path::new("file")).await?;
        let target = create_storage(
            destination_root.path().to_str().unwrap(),
            CreateStorageOptions::default(),
        )
        .await?;
        copy_file(
            &storage,
            &target,
            &Arc::new(entry.clone()),
            CopyOptions::default(),
            None,
        )
        .await?;
        remove_entry(&storage, &target, &entry).await?;
        remove_entry(&storage, &target, &entry).await?;
        assert!(!root.path().join("file").exists());
        assert_eq!(std::fs::read(destination_root.path().join("file"))?, b"payload");
        Ok(())
    }

    #[tokio::test]
    async fn child_removal_does_not_replace_the_original_directory_mtime() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::create_dir(source_root.path().join("tree"))?;
        std::fs::write(source_root.path().join("tree/file"), b"payload")?;
        let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let directory = observe_path(source.storage(), Path::new("tree")).await?;
        let file = Arc::new(observe_path(source.storage(), Path::new("tree/file")).await?);
        copy_file(&source, &target, &file, CopyOptions::default(), None).await?;
        set_entry_metadata(target.storage(), &directory).await?;
        let expected = std::fs::metadata(target_root.path().join("tree"))?.modified()?;
        remove_entry(&source, &target, &file).await?;
        remove_entry(&source, &target, &directory).await?;
        assert!(!source_root.path().join("tree").exists());
        assert_eq!(
            std::fs::metadata(target_root.path().join("tree"))?.modified()?,
            expected
        );
        assert_eq!(std::fs::read(target_root.path().join("tree/file"))?, b"payload");
        Ok(())
    }
}
