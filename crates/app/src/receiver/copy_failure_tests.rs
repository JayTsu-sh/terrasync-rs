//! Receiver 本机复制错误必须保留可辨识的原生原因。
use std::{path::Path, sync::Arc};

use data_mover::model::{FailureClass, ObservedEntry};
use data_mover::storage::StorageRoleFailure;
use task_model::TransferMode;

use super::{ReceiverConfig, process_entry_on_receiver};
use crate::error::{AppError, CopyStep, Result, StorageError};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

async fn storage(root: &Path) -> Result<Arc<StorageContext>> {
    Ok(Arc::new(
        create_storage(&root.to_string_lossy(), CreateStorageOptions::default()).await?,
    ))
}

fn config() -> ReceiverConfig {
    ReceiverConfig {
        job_id: "typed-copy-failure".into(),
        job_dir: String::new(),
        enable_integrity_check: true,
        enable_acl: false,
        is_source_reserved: true,
        transfer_mode: TransferMode::Atomic,
    }
}

async fn copy(
    entry: &Arc<ObservedEntry>, source: &Arc<StorageContext>, destination: &Arc<StorageContext>,
) -> Result<()> {
    process_entry_on_receiver(entry, source, destination, &config(), None, None).await
}

#[tokio::test]
async fn vanished_regular_source_keeps_native_transfer_failure_and_the_old_target() -> Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    std::fs::write(src.path().join("file"), b"source")?;
    std::fs::write(dst.path().join("file"), b"old target")?;
    let source = storage(src.path()).await?;
    let destination = storage(dst.path()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    std::fs::remove_file(src.path().join("file"))?;
    let error = match copy(&entry, &source, &destination).await {
        Err(error) => error,
        Ok(()) => panic!("消失的源文件不能被确认成功"),
    };
    assert!(
        matches!(&error, AppError::CopyEntryFailed { step: CopyStep::Transfer, path, .. } if path == Path::new("file"))
    );
    assert!(
        matches!(error.storage_error(), Some(StorageError::Transfer(_))),
        "{error}"
    );
    assert_eq!(std::fs::read(dst.path().join("file"))?, b"old target");
    assert_eq!(std::fs::read_dir(dst.path())?.count(), 1, "失败暂存必须清理");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn vanished_symlink_keeps_the_native_not_found_class() -> Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    std::os::unix::fs::symlink("missing-target", src.path().join("link"))?;
    let source = storage(src.path()).await?;
    let destination = storage(dst.path()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("link")).await?);
    std::fs::remove_file(src.path().join("link"))?;
    let error = match copy(&entry, &source, &destination).await {
        Err(error) => error,
        Ok(()) => panic!("消失的链接不能被确认成功"),
    };
    assert!(
        matches!(&error, AppError::CopyEntryFailed { step: CopyStep::ReadSourceLink, path, .. } if path == Path::new("link"))
    );
    assert!(error.storage_error().is_some_and(StorageError::is_not_found), "{error}");
    assert_eq!(std::fs::read_dir(dst.path())?.count(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn refused_symlink_replacement_keeps_its_typed_cause_and_existing_children() -> Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    std::os::unix::fs::symlink("missing-target", src.path().join("link"))?;
    std::fs::create_dir(dst.path().join("link"))?;
    std::fs::write(dst.path().join("link/child"), b"keep")?;
    let source = storage(src.path()).await?;
    let destination = storage(dst.path()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("link")).await?);
    let error = match copy(&entry, &source, &destination).await {
        Err(error) => error,
        Ok(()) => panic!("非空目录不能被链接替换"),
    };
    assert!(
        matches!(&error, AppError::CopyEntryFailed { step: CopyStep::CreateDestinationLink, path, .. } if path == Path::new("link"))
    );
    assert!(
        matches!(error.storage_error(), Some(StorageError::Role(StorageRoleFailure::Entry(cause))) if cause.class() == FailureClass::Conflict),
        "{error}"
    );
    assert_eq!(std::fs::read(dst.path().join("link/child"))?, b"keep");
    Ok(())
}
