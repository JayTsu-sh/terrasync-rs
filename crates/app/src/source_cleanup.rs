//! 清理本次选中且目标就绪的源项：普通对象可逐项先于入库，其他路线保留窗口及数据库收尾。

use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use data_mover::model::{EntryIdentityKey, EntryKind, ObservedEntry, SourceVersion, StoragePath, VersionRecord};
use data_mover::storage::NamespaceReadPolicy;
use db::ClickHouseDatabase;
use task_model::observation::ObservedView;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::consumer::ConsumerManager;
use crate::error::CopyStep;
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::selection::unrepresentable_object;
use crate::file_copy::{CopiedPublication, metadata::captured_metadata_plan};
use crate::storage_factory::{StorageContext, path};
use task_model::{SourcePresence, TargetRetention, TargetRetentionReason};

mod verification;
use verification::verify_source_metadata;
mod content;
mod history;
mod ready_files;
pub(crate) use ready_files::{ReadyFileCleanup, can_stream_files};
mod ready_window;
pub(crate) use ready_window::{FullCleanupStage, finish_with_ready_cleanup};

type CompletedChecks = HashMap<StoragePath, EntryIdentityKey>;

#[cfg(all(test, unix))]
#[path = "source_cleanup_real_tests.rs"]
mod real_tests;

#[derive(Clone)]
pub(crate) struct SourceCleanupCheck {
    read_policy: NamespaceReadPolicy,
    cancel: CancellationToken,
    verify_content: bool,
    prechecked: Option<Arc<CompletedChecks>>,
    streamed_files: bool,
}

impl SourceCleanupCheck {
    pub fn new(operation_timeout: Duration, recovery_window: Duration, cancel: CancellationToken) -> Self {
        Self {
            read_policy: NamespaceReadPolicy {
                operation_timeout,
                recovery_window,
                retry_interval: Duration::from_millis(100),
            },
            cancel,
            verify_content: false,
            prechecked: None,
            streamed_files: false,
        }
    }

    pub(crate) fn with_content_verification(mut self, enabled: bool) -> Self {
        self.verify_content = enabled;
        self
    }

    /// 仅完整成功执行可以证明全部普通文件已走逐项清理；不扩展到历史、目录或后续代次。
    pub(crate) fn with_prechecked(mut self, entries: CompletedChecks, streamed_files: bool) -> Self {
        self.streamed_files = streamed_files;
        if !entries.is_empty() {
            self.prechecked = Some(Arc::new(entries));
        }
        self
    }

    fn is_prechecked(&self, entry: &ObservedEntry) -> bool {
        (self.streamed_files && ready_files::ordinary_file(entry))
            || self
                .prechecked
                .as_ref()
                .is_some_and(|entries| entries.get(entry.path()) == Some(&entry.identity_key()))
    }
}

/// 调用方必须先排空复制和数据库消费者，并完成目标目录元数据与代次提交。
pub(crate) async fn remove_confirmed_sources(
    database: ClickHouseDatabase, source: Arc<StorageContext>, destination: Arc<StorageContext>,
    check: SourceCleanupCheck,
) -> Result<()> {
    let (sender, mut receiver) = mpsc::channel(32);
    let query = AbortOnDropHandle::new(tokio::spawn(async move {
        database.query_current_copy_entries(None, None, None, sender).await
    }));
    let target_history = destination.storage().staged_destination()?.keeps_history().await?;
    let mut histories = history::HistoryCleanup::default();
    while let Some(completion) = receiver.recv().await {
        let entry = completion.completion.item.entry.clone();
        if histories.has_other_path(entry.path()) {
            histories.finish(&source, &check).await?;
        }
        if target_history && entry.history().is_some() {
            histories
                .observe(completion, &source, &destination, &check)
                .await
                .map_err(|source| {
                    AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(entry.path().as_str()), source)
                })?;
            continue;
        }
        if check.is_prechecked(&entry) {
            continue;
        }
        // 一旦清理失败就停止发出新删除，避免断连后继续扩大未知副作用。
        remove_entry(&source, &destination, &entry, &check, None)
            .await
            .map_err(|source| {
                AppError::copy_entry_failure(CopyStep::SourceCleanup, Path::new(entry.path().as_str()), source)
            })?;
    }
    query
        .await
        .map_err(AppError::from)
        .and_then(|result| result.map_err(AppError::from))
        .map_err(|error| histories.query_failed(error))?;
    histories.finish(&source, &check).await?;
    Ok(())
}

/// 终态保留事件包含已识别路径；未观察到实际存在性的故障仍明确为 Unknown。
pub(crate) async fn report_failure(result: Result<()>, reports: &ConsumerManager) -> Result<()> {
    let Err(source) = result else {
        return result;
    };
    let Some((entry_path, failure)) = source.source_cleanup_entry() else {
        return Err(source);
    };
    let report = async {
        let storage = failure.storage_error().map(|error| match error {
            StorageError::HistoryCleanupVerification { source, .. } => source.as_ref(),
            other => other,
        });
        let reason = match storage {
            Some(StorageError::NamespaceRead(_)) => TargetRetentionReason::SourceQueryFailed,
            Some(StorageError::TargetPersistenceUnconfirmed { .. }) => {
                TargetRetentionReason::TargetPersistenceUnconfirmed
            }
            Some(StorageError::SourcePurgeRefused { .. } | StorageError::SourceHistoryCurrentUnconfirmed { .. }) => {
                TargetRetentionReason::SourceDeletionUnconfirmed
            }
            _ => TargetRetentionReason::StaleEvidence,
        };
        let mut retention = TargetRetention::entry(&path(entry_path)?, reason);
        if matches!(storage, Some(StorageError::SourceMetadataChanged { .. })) {
            retention.source_presence = SourcePresence::Present;
        }
        reports.record_target_retention(retention, source.to_string()).await
    }
    .await;
    match report {
        Ok(()) => Err(source),
        Err(reporting) => Err(AppError::MigrationReportingFailed {
            source: Box::new(source),
            reporting: Box::new(reporting),
        }),
    }
}

async fn remove_entry(
    source: &StorageContext, destination: &StorageContext, entry: &ObservedEntry, check: &SourceCleanupCheck,
    published: Option<CopiedPublication>,
) -> Result<()> {
    // 存储根和虚拟目录不是本次传输拥有的可删除实体。
    if entry.path().as_str().is_empty() || unrepresentable_object(entry, destination).is_some() {
        return Ok(());
    }
    let version_index = match entry.history() {
        Some(history)
            if entry.kind() == EntryKind::File
                && history.records().len() == 1
                && history.current().is_some_and(VersionRecord::is_latest)
                && !destination.storage().staged_destination()?.keeps_history().await? =>
        {
            Some(0)
        }
        Some(_) => {
            // 历史目标仍须逐个绑定核验全部所需版本，不能凭一行就绪事实授权整 key 清理。
            return Err(StorageError::PermanentVersionCleanupUnavailable {
                path: entry.path().clone(),
            }
            .into());
        }
        None => None,
    };
    if source.storage().holds_directory_objects() && entry.kind() == EntryKind::Directory {
        if entry.directory_marker_object().is_some() {
            return Err(StorageError::DirectoryMarkerCleanupUnavailable {
                path: entry.path().clone(),
            }
            .into());
        }
        // 纯前缀没有可删除实体；子对象各自核验，绝不把前缀递归删除。
        return Ok(());
    }
    // 只核验目标与复制时捕获的原始事实；删源改变的目录 mtime 和已经消失的源项不回写目标。
    let view = ObservedView { entry, version_index };
    let target = match published {
        Some(published) => {
            if !ready_files::ordinary_file(entry) || !can_stream_files(source, destination) {
                return Err(StorageError::TargetPublicationChanged {
                    path: entry.path().clone(),
                    source: None,
                }
                .into());
            }
            published.into_target(entry)?
        }
        None => captured_metadata_plan(destination.storage(), view)?
            .verify_published(
                destination.storage(),
                entry.path(),
                &SourceVersion::Current,
                None,
                None,
                check.cancel.clone(),
            )
            .await
            .map_err(StorageError::from)?,
    };
    if target.kind != entry.kind() || (entry.kind() == EntryKind::File && target.size != entry.size()) {
        return Err(StorageError::TargetPublicationChanged {
            path: entry.path().clone(),
            source: None,
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
            source: None,
        }
        .into());
    }
    // 可读且校验一致不代表已落盘；包括仅更新属性、未重新复制的数据和目录。
    destination
        .storage()
        .namespace()?
        .persist_observed(&target, check.cancel.clone())
        .await
        .map_err(|source| StorageError::TargetPersistenceUnconfirmed {
            path: entry.path().clone(),
            source,
        })?;
    // 历史容器的属性不代表最新版本；按扫描时捕获的该版本原值复核当前源。
    let expected = version_index.map(|_| entry.clone().with_metadata(view.metadata().clone()));
    if !verify_source_metadata(source, expected.as_ref().unwrap_or(entry), check).await? {
        if version_index.is_some() {
            // 当前不可见不证明全部历史已清空，也不能据此忽略刚出现的删除标记。
            return Err(StorageError::SourceHistoryCurrentUnconfirmed {
                path: entry.path().clone(),
            }
            .into());
        }
        return Ok(());
    }
    // 增量跳过／仅修改属性的条目没有本轮数据传输回读，也必须执行用户选择的全量校验。
    if check.verify_content && entry.kind() == EntryKind::File {
        content::verify(source, destination, entry, &target, check, false).await?;
    }
    // 仅删除文件、链接本身或空目录；过滤掉或并发新增的子项会阻止目录删除。
    source
        .storage()
        .namespace()?
        .purge_observed(entry, check.cancel.clone())
        .await
        .map_err(|source| StorageError::SourcePurgeRefused {
            path: entry.path().clone(),
            source,
        })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    async fn remove_entry(source: &StorageContext, destination: &StorageContext, entry: &ObservedEntry) -> Result<()> {
        super::remove_entry(
            source,
            destination,
            entry,
            &SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new()),
            None,
        )
        .await
    }
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
        set_entry_metadata(target.storage(), &directory, None).await?;
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

    #[cfg(unix)]
    #[tokio::test]
    async fn ownership_changes_after_copy_preserve_the_source_file_and_directory() -> Result<()> {
        use data_mover::model::MetadataFamily;
        use std::os::unix::fs::PermissionsExt;

        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        for directory in [false, true] {
            let name = if directory { "directory" } else { "file" };
            let path = source_root.path().join(name);
            if directory {
                std::fs::create_dir(&path)?;
            } else {
                std::fs::write(&path, b"payload")?;
            }
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
            let entry = observe_path(source.storage(), Path::new(name)).await?;
            if directory {
                std::fs::create_dir(target_root.path().join(name))?;
                set_entry_metadata(target.storage(), &entry, None).await?;
            } else {
                copy_file(&source, &target, &Arc::new(entry.clone()), CopyOptions::default(), None).await?;
            }
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750))?;
            assert!(matches!(remove_entry(&source, &target, &entry).await,
                Err(AppError::StorageError(StorageError::SourceMetadataChanged { families, .. }))
                if families == [MetadataFamily::OwnershipMode]));
            assert!(path.exists());
            assert!(target_root.path().join(name).exists());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn xattr_changes_after_copy_keep_both_original_target_and_changed_source() -> Result<()> {
        use crate::scan::observe_path_with_plan;
        use data_mover::model::{ExtendedAttribute, MetadataFamily, ObservationMode, ObservationPlan, StoragePath};
        use data_mover::storage::MetadataMutation;

        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"payload")?;
        let source = create_storage(source_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let path = StoragePath::new("file").map_err(StorageError::from)?;
        let attributes =
            |value: &[u8]| vec![ExtendedAttribute::new(b"user.cleanup-proof".to_vec(), value.to_vec()).unwrap()];
        source
            .storage()
            .metadata()?
            .apply(
                &path,
                MetadataMutation::Xattrs(attributes(b"before")),
                CancellationToken::new(),
            )
            .await?;
        let plan = ObservationPlan::default()
            .with_xattrs(ObservationMode::Required)
            .with_timestamps(ObservationMode::InlineOnly)
            .with_ownership_mode(ObservationMode::InlineOnly);
        let entry = observe_path_with_plan(source.storage(), Path::new("file"), plan).await?;
        assert_eq!(entry.metadata().xattrs().value(), Some(&attributes(b"before")));
        copy_file(&source, &target, &Arc::new(entry.clone()), CopyOptions::default(), None).await?;
        source
            .storage()
            .metadata()?
            .apply(
                &path,
                MetadataMutation::Xattrs(attributes(b"after")),
                CancellationToken::new(),
            )
            .await?;
        assert!(matches!(remove_entry(&source, &target, &entry).await,
            Err(AppError::StorageError(StorageError::SourceMetadataChanged { families, .. }))
            if families == [MetadataFamily::Xattrs]));
        let actual_source = observe_path_with_plan(source.storage(), Path::new("file"), plan).await?;
        let actual_target = observe_path_with_plan(target.storage(), Path::new("file"), plan).await?;
        assert_eq!(actual_source.metadata().xattrs().value(), Some(&attributes(b"after")));
        assert_eq!(actual_target.metadata().xattrs().value(), Some(&attributes(b"before")));
        assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
        assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "source_cleanup/persistence_tests.rs"]
#[allow(clippy::unwrap_used)]
mod persistence_tests;

#[cfg(all(test, unix))]
#[path = "source_cleanup/purge_tests.rs"]
#[allow(clippy::unwrap_used)]
mod purge_tests;

#[cfg(all(test, unix))]
#[path = "source_cleanup/content_tests.rs"]
#[allow(clippy::unwrap_used)]
mod content_tests;
