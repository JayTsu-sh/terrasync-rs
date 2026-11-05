// 标准库
use data_mover::model::StoragePath;
use data_mover::tar_pack::{self, TarRequest};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use storage_adapter::WalkOptions;
use task_model::ObservedEntryExt;

// 内部模块
use storage_adapter::qos::QosManager;
use storage_adapter::{StorageContext, StorageError, source_qos};
use {
    storage_adapter::{ObservedEntry, WalkDirAsyncIterator},
    task_model::StorageEntryMessage,
};
// 外部crate
use tracing::{debug, info};

use crate::error::{AppError, Result};

/// 将源端目录打包为 .tar 文件写入目标端
///
/// 两阶段流程：
/// 1. 子 walkdir 收集目录下所有条目 + 计算 tar 总大小
/// 2. 使用原生归档引擎流式打包；清单与后续通知共享同一不可变切片
///
/// # 返回值
/// - `tar_entry`: 已发布目标 .tar 文件的原生观察
/// - `manifest_list`: tar 内所有条目的列表（供后续写入 manifest 表）
pub async fn pack_directory(
    src_storage: &StorageContext, dest_storage: &StorageContext, dir_entry: &ObservedEntry, is_source_reserved: bool,
    qos: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
) -> Result<(ObservedEntry, Arc<[Arc<ObservedEntry>]>)> {
    let dir_path = dir_entry.get_relative_path();

    info!("Pack directory: {:?}", dir_path);

    // ── 阶段 1：收集子条目 + 计算 tar 大小 ──
    let iterator = src_storage
        .walkdir(Some(dir_entry.get_relative_path()), WalkOptions::default())
        .await?;
    let manifest_list: Arc<[Arc<ObservedEntry>]> = collect_entries(iterator).await?.into();

    debug!("Collected {} entries for packing {:?}", manifest_list.len(), dir_path);

    let tar_relative_path =
        StoragePath::new(format!("{}.tar", dir_entry.path().as_str())).map_err(StorageError::from)?;
    let request = TarRequest {
        source: src_storage.storage().clone(),
        destination: dest_storage.storage().clone(),
        entries: Arc::clone(&manifest_list),
        final_path: tar_relative_path.clone(),
        cancel: Default::default(),
        source_qos: qos
            .as_ref()
            .map(|qos| source_qos(qos, src_storage.block_size()))
            .transpose()?,
    };
    let outcome = tar_pack::pack(request).await.map_err(StorageError::from)?;
    let tar_size = outcome.transferred_bytes;
    if let Some(counter) = bytes_counter {
        counter.fetch_add(tar_size, Ordering::Relaxed);
    }

    info!(
        "Pack complete: {:?} -> {:?} ({} bytes, {} entries)",
        dir_path,
        Path::new(tar_relative_path.as_str()),
        tar_size,
        manifest_list.len()
    );

    // 归档是目标端已经发布的真实文件，读取其原生观察，不能借用源目录身份拼造条目。
    let tar_entry = dest_storage.get_metadata(Path::new(tar_relative_path.as_str())).await?;

    // 删除源端目录（如果不保留）
    if !is_source_reserved {
        debug!("Deleting source directory: {:?}", dir_path);
        src_storage.delete_dir_all(dir_entry).await?;
    }

    Ok((tar_entry, manifest_list))
}

/// 从 walkdir 迭代器收集所有条目
async fn collect_entries(iterator: WalkDirAsyncIterator) -> Result<Vec<Arc<ObservedEntry>>> {
    let mut entries = Vec::new();
    let mut enumeration_error = None;
    while let Some(message) = iterator.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => {
                entries.push(entry);
            }
            StorageEntryMessage::Error {
                event, path, reason, ..
            } => {
                enumeration_error.get_or_insert_with(|| {
                    AppError::ScanError(format!(
                        "Subdirectory enumeration failed ({event}) at {}: {reason}",
                        path.display()
                    ))
                });
            }
            _ => {}
        }
    }
    if let Some(error) = iterator.take_error() {
        return Err(error.into());
    }
    if let Some(error) = enumeration_error {
        return Err(error);
    }
    Ok(entries)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use storage_adapter::{AsyncReceiver, CreateStorageOptions, create_storage};
    use task_model::ErrorEvent;

    #[tokio::test]
    async fn an_enumeration_error_cannot_become_a_successful_tar_manifest() {
        let (sender, receiver) = async_channel::bounded(1);
        sender
            .send(StorageEntryMessage::Error {
                event: ErrorEvent::Scan,
                path: "unreadable".into(),
                entry: None,
                reason: "listing interrupted".into(),
            })
            .await
            .unwrap();
        drop(sender);
        assert!(collect_entries(AsyncReceiver::new(receiver)).await.is_err());
    }

    #[tokio::test]
    async fn packaged_directory_keeps_native_manifest_and_checked_pax_content() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::create_dir(source_root.path().join("tree"))?;
        let name = format!("{}.bin", "long".repeat(35));
        let source_path = source_root.path().join("tree").join(&name);
        std::fs::write(&source_path, b"archive")?;
        let source = create_storage(
            source_root.path().to_str().unwrap(),
            CreateStorageOptions::new(Some(1024), false),
        )
        .await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let directory = source.get_metadata(Path::new("tree")).await?;
        let counter = Arc::new(AtomicU64::new(0));
        let (tar_entry, manifest) =
            pack_directory(&source, &target, &directory, true, None, Some(Arc::clone(&counter))).await?;
        let archive = std::fs::read(target_root.path().join("tree.tar"))?;
        assert_eq!(tar_entry.path().as_str(), "tree.tar");
        assert_eq!(tar_entry.get_size(), archive.len() as u64);
        assert_eq!(counter.load(Ordering::Relaxed), archive.len() as u64);
        assert!(archive.len() > 2048);
        assert!(archive.windows(name.len()).any(|bytes| bytes == name.as_bytes()));
        assert!(archive.windows(7).any(|bytes| bytes == b"archive"));
        assert!(
            manifest
                .iter()
                .any(|entry| entry.path().as_str() == format!("tree/{name}"))
        );
        assert_eq!(std::fs::read(source_path)?, b"archive");
        Ok(())
    }
}
