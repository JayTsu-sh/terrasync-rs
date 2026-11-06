use crate::error::storage::{Result, StorageError};
use crate::storage_factory::{path, timestamp};
use data_mover::model::*;
use data_mover::storage::Storage;
use std::path::{Path, PathBuf};
use task_model::ObservedEntryExt;

use data_mover::storage::{self, StorageRoleFailure};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};
pub(crate) async fn create_dir_all(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    storage::create_directory_all(storage, entry.path())
        .await
        .map_err(StorageError::from)
}

/// 只移除目录本身的零字节对象，保留同前缀下所有后代及历史对象。
pub(crate) async fn remove_directory_marker(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    if !storage.holds_directory_objects() {
        return Ok(());
    }
    let marker = StoragePath::new(format!("{}/", entry.path().as_str())).map_err(StorageError::from)?;
    let namespace = storage.namespace().map_err(StorageError::from)?;
    let object = match namespace.stat(&marker).await.map_err(StorageError::from) {
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
    match storage.namespace()?.stat(&name).await {
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
