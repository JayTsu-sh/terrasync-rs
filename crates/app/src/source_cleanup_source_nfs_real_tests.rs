//! 在独占 NFS 源／目标前缀中验证产品删源原语；服务端独立核对最终内容。
use std::{os::unix::fs::symlink, path::Path, sync::Arc, time::Duration};

use data_mover::model::{FailureClass, ObservedEntry};
use data_mover::storage::{MetadataMutation, StorageRoleFailure};
use task_model::TransferMode;
use tokio_util::sync::CancellationToken;

use super::environment;
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file, metadata::set_entry_metadata, namespace, prepare_directory};
use crate::scan::observe_path;
use crate::source_cleanup::{SourceCleanupCheck, remove_entry};
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage, path};

fn options(full: bool) -> CopyOptions {
    CopyOptions {
        transfer_mode: TransferMode::Direct,
        enable_integrity_check: full,
        ..CopyOptions::default()
    }
}

async fn seed(local: &StorageContext, source: &StorageContext, root: &Path, name: &str, data: &[u8]) -> Result<()> {
    let file = root.join(name);
    std::fs::create_dir_all(
        file.parent()
            .ok_or_else(|| AppError::ConfigError("fixture parent".into()))?,
    )?;
    std::fs::write(&file, data)?;
    let entry = Arc::new(observe_path(local.storage(), Path::new(name)).await?);
    copy_file(local, source, &entry, options(false), None).await?;
    Ok(())
}

async fn copy(source: &StorageContext, target: &StorageContext, name: &str, full: bool) -> Result<ObservedEntry> {
    let entry = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
    copy_file(source, target, &entry, options(full), None).await?;
    Ok((*entry).clone())
}

async fn absent(source: &StorageContext, name: &str) -> Result<()> {
    assert!(
        matches!(source.storage().namespace()?.stat(&path(Path::new(name))?).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
    );
    Ok(())
}

async fn rewritten(
    local: &StorageContext, source: &StorageContext, target: &StorageContext, root: &Path, check: &SourceCleanupCheck,
) -> Result<()> {
    let name = "app-source-rewritten";
    seed(local, source, root, name, b"original").await?;
    let entry = copy(source, target, name, false).await?;
    seed(local, source, root, name, b"changed!").await?;
    let mut times = *entry
        .metadata()
        .timestamps()
        .value()
        .ok_or_else(|| AppError::ConfigError("original source timestamps required".into()))?;
    times.created = None;
    source
        .storage()
        .metadata()?
        .apply(
            entry.path(),
            MetadataMutation::Timestamps(times),
            CancellationToken::new(),
        )
        .await?;
    assert!(matches!(
        remove_entry(source, target, &entry, check, None).await,
        Err(AppError::StorageError(StorageError::SourcePurgeRefused { .. }))
    ));
    Ok(())
}

async fn changed_mode(
    local: &StorageContext, source: &StorageContext, target: &StorageContext, root: &Path, check: &SourceCleanupCheck,
    change_source: bool,
) -> Result<()> {
    let name = if change_source {
        "app-source-mode"
    } else {
        "app-target-mode"
    };
    seed(local, source, root, name, b"mode payload").await?;
    let entry = copy(source, target, name, false).await?;
    let mode = entry
        .metadata()
        .ownership_mode()
        .value()
        .ok_or_else(|| AppError::ConfigError("original source mode required".into()))?
        .mode
        ^ 0o100;
    let changed = if change_source { source } else { target };
    changed
        .storage()
        .metadata()?
        .apply(entry.path(), MetadataMutation::Mode(mode), CancellationToken::new())
        .await?;
    let result = remove_entry(source, target, &entry, check, None).await;
    if change_source {
        assert!(matches!(
            result,
            Err(AppError::StorageError(StorageError::SourceMetadataChanged { .. }))
        ));
    } else {
        assert!(matches!(
            result,
            Err(AppError::StorageError(StorageError::MetadataVerification(_)))
        ));
    }
    source.storage().namespace()?.stat(entry.path()).await?;
    Ok(())
}

async fn directory(
    local: &StorageContext, source: &StorageContext, target: &StorageContext, root: &Path, check: &SourceCleanupCheck,
) -> Result<()> {
    let name = "app-source-directory";
    std::fs::create_dir(root.join(name))?;
    let local_entry = observe_path(local.storage(), Path::new(name)).await?;
    assert!(prepare_directory(&local_entry, source).await?);
    seed(local, source, root, "app-source-directory/child", b"child payload").await?;
    let entry = observe_path(source.storage(), Path::new(name)).await?;
    assert!(prepare_directory(&entry, target).await?);
    let child = copy(source, target, "app-source-directory/child", false).await?;
    set_entry_metadata(target.storage(), &entry).await?;
    assert!(matches!(
        remove_entry(source, target, &entry, check, None).await,
        Err(AppError::StorageError(StorageError::SourcePurgeRefused { .. }))
    ));
    source.storage().namespace()?.stat(child.path()).await?;
    remove_entry(source, target, &child, check, None).await?;
    remove_entry(source, target, &entry, check, None).await?;
    absent(source, name).await?;
    Ok(())
}

async fn link(
    local: &StorageContext, source: &StorageContext, target: &StorageContext, root: &Path, check: &SourceCleanupCheck,
) -> Result<()> {
    let name = "app-source-link";
    let raw = Path::new("../opaque/missing");
    symlink(raw, root.join(name))?;
    let seed = observe_path(local.storage(), Path::new(name)).await?;
    namespace::create_symlink(source.storage(), &seed, raw).await?;
    set_entry_metadata(source.storage(), &seed).await?;
    let entry = observe_path(source.storage(), Path::new(name)).await?;
    namespace::create_symlink(target.storage(), &entry, raw).await?;
    set_entry_metadata(target.storage(), &entry).await?;
    remove_entry(source, target, &entry, check, None).await?;
    absent(source, name).await?;
    assert_eq!(
        target.storage().namespace()?.read_link(entry.path()).await?,
        *entry.symlink_target().unwrap_or_else(|| panic!("fixture link"))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires separate prepared owned NFS source and target prefixes"]
async fn real_source_nfs_purge_uses_target_ready_and_original_source_facts() -> Result<()> {
    let source = create_storage(
        &environment("LAB_SOURCE_PURGE_NFS_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let target = create_storage(
        &environment("LAB_SOURCE_PURGE_TARGET_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let root = tempfile::tempdir()?;
    let local = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new());
    for full in [false, true] {
        let name = if full { "app-source-full" } else { "app-source-quick" };
        seed(&local, &source, root.path(), name, b"guarded payload").await?;
        let entry = copy(&source, &target, name, full).await?;
        remove_entry(&source, &target, &entry, &check, None).await?;
        remove_entry(&source, &target, &entry, &check, None).await?;
        absent(&source, name).await?;
    }
    rewritten(&local, &source, &target, root.path(), &check).await?;
    for change_source in [true, false] {
        changed_mode(&local, &source, &target, root.path(), &check, change_source).await?;
    }
    directory(&local, &source, &target, root.path(), &check).await?;
    link(&local, &source, &target, root.path(), &check).await?;
    println!(
        "real application NFS source: quick/full and repeated cleanup, equal-length rewrite with restored mtime, source/target metadata change, child-first empty directory, broken symlink"
    );
    Ok(())
}
