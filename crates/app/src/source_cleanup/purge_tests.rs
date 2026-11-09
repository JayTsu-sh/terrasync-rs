//! 应用删源路径必须保留扫描之后改写的内容及未迁移的隐藏子项。
use std::{
    fs::{File, FileTimes},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::Arc,
    time::Duration,
};

use data_mover::model::{FailureClass, ObservedEntry};
use data_mover::storage::StorageRoleFailure;
use tokio_util::sync::CancellationToken;

use super::{SourceCleanupCheck, remove_entry};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file, metadata::set_entry_metadata};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

fn check() -> SourceCleanupCheck {
    SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new())
}

fn assert_refusal(error: AppError, diagnostic: &str) {
    assert!(error.to_string().contains(diagnostic), "{error}");
    assert!(matches!(error,
        AppError::StorageError(StorageError::SourcePurgeRefused { source, .. })
        if matches!(source.source.as_ref(), StorageRoleFailure::Entry(failure)
            if failure.class() == FailureClass::Conflict && failure.diagnostic().contains(diagnostic))));
}

#[tokio::test]
async fn equal_length_source_rewrite_with_restored_mtime_is_retained_after_copy() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let file = source_root.path().join("file");
    std::fs::write(&file, b"copied")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = observe_path(source.storage(), Path::new("file")).await?;
    let snapshot = entry.encode_snapshot();
    let entry = Arc::new(ObservedEntry::decode_snapshot(snapshot.as_bytes(), source.storage().identity()).unwrap());
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    let before = std::fs::metadata(&file)?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    std::fs::write(&file, b"newest")?;
    File::open(&file)?.set_times(FileTimes::new().set_modified(before.modified()?))?;
    let after = std::fs::metadata(&file)?;
    assert_eq!(before.ino(), after.ino());
    assert_eq!(before.len(), after.len());
    assert_eq!(before.modified()?, after.modified()?);
    assert_refusal(
        remove_entry(&source, &target, &entry, &check()).await.unwrap_err(),
        "ctime",
    );
    assert_eq!(std::fs::read(&file)?, b"newest");
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"copied");
    Ok(())
}

#[tokio::test]
async fn source_directory_with_unmigrated_hidden_data_is_retained() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let hidden = source_root.path().join("tree/.data-mover-other-task/private");
    std::fs::create_dir_all(hidden.parent().unwrap())?;
    std::fs::write(&hidden, b"unowned source data")?;
    std::fs::create_dir(target_root.path().join("tree"))?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = observe_path(source.storage(), Path::new("tree")).await?;
    set_entry_metadata(target.storage(), &entry).await?;
    assert_refusal(
        remove_entry(&source, &target, &entry, &check()).await.unwrap_err(),
        "not empty",
    );
    assert_eq!(std::fs::read(&hidden)?, b"unowned source data");
    assert!(target_root.path().join("tree").is_dir());
    Ok(())
}
