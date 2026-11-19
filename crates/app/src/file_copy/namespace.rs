use crate::error::storage::{Result, StorageError};
use crate::storage_factory::{path, timestamp};
use data_mover::model::*;
use std::path::{Path, PathBuf};
use task_model::ObservedEntryExt;
use tokio_util::sync::CancellationToken;

use data_mover::storage::{self, CreateDirectoryAllFailure, Namespace, SourceDescriptor, Storage, StorageRoleFailure};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};
pub(crate) async fn create_dir_all(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    storage::create_directory_all(storage, entry.path())
        .await
        .map_err(StorageError::from)
}

pub(crate) async fn prepare_local_directory(
    storage: &Storage, path: &StoragePath, cancel: Option<&CancellationToken>,
) -> Result<SourceDescriptor> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        return Err(StorageError::Cancelled);
    }
    let namespace = storage
        .mutable_namespace(|caps| caps.create_directory)
        .map_err(CreateDirectoryAllFailure::from)?;
    let refusal = match namespace.create_directory(path).await {
        Ok(()) => None,
        Err(failure) if matches!(&failure, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::NotFound) =>
        {
            // Linux mkdir 成功后设置权限也可能报 NotFound。先证明当前父路径缺失，
            // 仅对此情形重建路径；其余失败不重新提交叶子写入。
            let Some((parent, _)) = path.as_str().rsplit_once('/') else {
                return Err(CreateDirectoryAllFailure::from(failure).into());
            };
            let parent = StoragePath::new(parent)?;
            match stat_fresh(namespace.as_ref(), &parent, cancel).await {
                Err(error) if error.is_not_found() => {
                    if cancel.is_some_and(CancellationToken::is_cancelled) {
                        return Err(StorageError::Cancelled);
                    }
                    storage::create_directory_all(storage, path).await?;
                    None
                }
                Err(error) if failed_beyond_entry(&error) => return Err(error),
                _ => return Err(CreateDirectoryAllFailure::from(failure).into()),
            }
        }
        Err(failure) if matches!(&failure, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::Conflict) => {
            Some(failure)
        }
        Err(failure) => return Err(CreateDirectoryAllFailure::from(failure).into()),
    };
    let current = stat_fresh(namespace.as_ref(), path, cancel).await;
    let Some(refusal) = refusal else {
        return current;
    };
    // 仅复用已存在的真实目录。权限错误可能发生在创建后的 chmod 或同步阶段。
    match &current {
        Ok(descriptor) if descriptor.kind == EntryKind::Directory => current,
        Err(error) if failed_beyond_entry(error) => current,
        _ => Err(CreateDirectoryAllFailure::from(refusal).into()),
    }
}

fn failed_beyond_entry(error: &StorageError) -> bool {
    match error {
        StorageError::Cancelled | StorageError::Role(StorageRoleFailure::Session(_)) => true,
        StorageError::Role(StorageRoleFailure::Entry(entry)) => matches!(
            entry.class(),
            FailureClass::Cancelled
                | FailureClass::Connectivity
                | FailureClass::Authentication
                | FailureClass::Throttled
        ),
        _ => false,
    }
}

pub(crate) async fn stat_fresh(
    namespace: &dyn Namespace, path: &StoragePath, cancel: Option<&CancellationToken>,
) -> Result<SourceDescriptor> {
    let reading = namespace.stat(path, true);
    match cancel {
        Some(cancel) => tokio::select! {
            biased;
            () = cancel.cancelled() => Err(StorageError::Cancelled),
            result = reading => result.map_err(StorageError::from),
        },
        None => reading.await.map_err(StorageError::from),
    }
}

/// 只移除目录本身的零字节对象，保留同前缀下所有后代及历史对象。
#[cfg(any(feature = "remote", test))]
pub(crate) async fn remove_directory_marker(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    if !storage.holds_directory_objects() {
        return Ok(());
    }
    let marker = StoragePath::new(format!("{}/", entry.path().as_str())).map_err(StorageError::from)?;
    let namespace = storage.namespace().map_err(StorageError::from)?;
    let object = match namespace.stat(&marker, false).await.map_err(StorageError::from) {
        Ok(object) => object,
        Err(error) if error.is_not_found() => return Ok(()),
        Err(error) => return Err(error),
    };
    if object.kind == EntryKind::File && object.size == Some(0) {
        namespace.delete(&marker).await.map_err(StorageError::from)?;
    }
    Ok(())
}

pub(crate) async fn delete_file(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    Ok(storage.namespace()?.delete(entry.path()).await?)
}

pub(crate) async fn read_symlink(storage: &Storage, entry: &ObservedEntry) -> Result<PathBuf> {
    let target = storage.namespace()?.read_link(entry.path()).await?;
    #[cfg(unix)]
    {
        Ok(std::ffi::OsString::from_vec(target.as_bytes().to_vec()).into())
    }
    #[cfg(windows)]
    {
        if target.encoding() == SymlinkTargetEncoding::WindowsWide {
            let wide: Vec<_> = target
                .as_bytes()
                .chunks_exact(2)
                .map(|v| u16::from_le_bytes([v[0], v[1]]))
                .collect();
            Ok(std::ffi::OsString::from_wide(&wide).into())
        } else {
            Ok(String::from_utf8(target.as_bytes().to_vec())
                .map_err(StorageError::from)?
                .into())
        }
    }
}

pub(crate) async fn create_symlink(storage: &Storage, entry: &ObservedEntry, target: &Path) -> Result<()> {
    let name = path(entry.get_relative_path())?;
    if let Some(parent) = entry.get_relative_path().parent() {
        storage::create_directory_all(storage, &path(parent)?)
            .await
            .map_err(StorageError::from)?;
    }
    // 旧产品支持用链接替换已有目录项；角色接口显式要求调用方删除。
    match storage.namespace()?.stat(&name, false).await {
        Ok(_) => storage.namespace()?.delete(&name).await?,
        Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    #[cfg(unix)]
    let bytes = { target.as_os_str().as_bytes().to_vec() };
    #[cfg(windows)]
    let bytes = {
        target
            .as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>()
    };
    let encoding = if cfg!(windows) {
        SymlinkTargetEncoding::WindowsWide
    } else {
        SymlinkTargetEncoding::UnixBytes
    };
    let target = SymlinkTarget::new(encoding, bytes)?;
    Ok(storage
        .namespace()?
        .create_symlink(storage::CreateSymlinkRequest {
            path: name,
            target,
            uid: entry.get_uid(),
            gid: entry.get_gid(),
            accessed: Some(timestamp(entry.get_atime())),
            modified: Some(timestamp(entry.get_mtime())),
        })
        .await?)
}
