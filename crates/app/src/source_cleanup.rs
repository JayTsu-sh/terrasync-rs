//! 只消费任务已经确认落库的复制事实；永不递归删除未列入目录的源项。

use std::sync::Arc;

use data_mover::model::{EntryKind, ObservedEntry};
use db::ClickHouseDatabase;
use task_model::observation::timestamp_nanos;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use crate::error::{AppError, Result, StorageError};
use crate::integrity_check::{IntegrityCheckMode, IntegrityCheckOptions, compare_entry};
use crate::storage_factory::StorageContext;

/// 调用方必须先排空复制和数据库消费者，并完成目标目录元数据与代次提交。
pub(crate) async fn remove_confirmed_sources(
    database: ClickHouseDatabase, source: Arc<StorageContext>, destination: Arc<StorageContext>,
) -> Result<()> {
    let (sender, mut receiver) = mpsc::channel(32);
    let query = AbortOnDropHandle::new(tokio::spawn(async move {
        database.query_storage_entry(None, None, None, sender).await
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
    if entry.path().as_str().is_empty() {
        return Ok(());
    }
    if entry.history().is_some() || source.storage().holds_directory_objects() {
        return Err(StorageError::PermanentVersionCleanupUnavailable {
            path: entry.path().clone(),
        }
        .into());
    }
    let namespace = source.storage().namespace()?;
    let current = match namespace.stat(entry.path()).await {
        Ok(current) => current,
        Err(error) => {
            let error = StorageError::from(error);
            // 上次清理成功但响应丢失时，目标事实依旧保留，缺失源项无需重删。
            if error.is_not_found() {
                return Ok(());
            }
            return Err(error.into());
        }
    };
    let modified = entry.metadata().timestamps().value().and_then(|times| times.modified);
    let current_modified = current.inline_timestamps.and_then(|times| times.modified);
    if current.kind != entry.kind()
        || current.source_identity != *entry.source_identity()
        || (entry.kind() != EntryKind::Directory
            && (current.size != entry.size() || timestamp_nanos(current_modified) != timestamp_nanos(modified)))
    {
        return Err(StorageError::SourceChangedBeforeDelete {
            path: entry.path().clone(),
        }
        .into());
    }
    compare_entry(
        source,
        destination,
        entry,
        IntegrityCheckOptions::new(IntegrityCheckMode::Quick),
        None,
    )
    .await?;
    // 仅删除文件、链接本身或空目录；过滤掉或并发新增的子项会阻止目录删除。
    namespace.delete(entry.path()).await.map_err(StorageError::from)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::scan::observe_path;
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use std::path::Path;

    #[tokio::test]
    async fn changed_source_is_retained_and_cleanup_never_recurses() -> Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("tree"))?;
        std::fs::write(root.path().join("tree/file"), b"before")?;
        let storage = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let file = observe_path(storage.storage(), Path::new("tree/file")).await?;
        let directory = observe_path(storage.storage(), Path::new("tree")).await?;
        std::fs::write(root.path().join("tree/file"), b"changed and longer")?;
        assert!(matches!(
            remove_entry(&storage, &storage, &file).await,
            Err(AppError::StorageError(StorageError::SourceChangedBeforeDelete { .. }))
        ));
        assert!(remove_entry(&storage, &storage, &directory).await.is_err());
        assert_eq!(std::fs::read(root.path().join("tree/file"))?, b"changed and longer");
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_entry_cleanup_is_idempotent() -> Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"payload")?;
        let storage = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let entry = observe_path(storage.storage(), Path::new("file")).await?;
        remove_entry(&storage, &storage, &entry).await?;
        remove_entry(&storage, &storage, &entry).await?;
        assert!(!root.path().join("file").exists());
        Ok(())
    }
}
