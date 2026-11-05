// 标准库
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use storage_adapter::WalkOptions;
use task_model::ObservedEntryExt;

// 内部模块
use storage_adapter::qos::QosManager;
use storage_adapter::storage_enum::StorageEnum;
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
/// 2. 调用 `StorageEnum::pack_files_to_tar()` 流式打包写入
///
/// # 返回值
/// - `tar_entry`: 合成的条目，表示目标端的 .tar 文件（变体与输入 `dir_entry` 一致）
/// - `manifest_list`: tar 内所有条目的列表（供后续写入 manifest 表）
pub async fn pack_directory(
    src_storage: &StorageEnum, dest_storage: &StorageEnum, dir_entry: &ObservedEntry, is_source_reserved: bool,
    qos: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
) -> Result<(ObservedEntry, Vec<Arc<ObservedEntry>>)> {
    let dir_path = dir_entry.get_relative_path();
    let dir_mtime = dir_entry.get_mtime();

    info!("Pack directory: {:?}", dir_path);

    // ── 阶段 1：收集子条目 + 计算 tar 大小 ──
    let iterator = src_storage
        .walkdir(Some(dir_entry.get_relative_path()), WalkOptions::default())
        .await?;
    let manifest_list = collect_entries(iterator).await?;

    debug!("Collected {} entries for packing {:?}", manifest_list.len(), dir_path);

    // 构造 .tar 文件路径
    let tar_relative_path = PathBuf::from(format!("{}.tar", dir_path.to_string_lossy()));

    // ── 阶段 2：流式打包写入 ──
    let tar_size = StorageEnum::pack_files_to_tar(
        src_storage,
        dest_storage,
        &manifest_list,
        &tar_relative_path,
        dir_mtime,
        storage_adapter::TarPackOptions { qos, bytes_counter },
    )
    .await?;

    info!(
        "Pack complete: {:?} -> {:?} ({} bytes, {} entries)",
        dir_path,
        tar_relative_path,
        tar_size,
        manifest_list.len()
    );

    // 归档是目标端已经发布的真实文件，读取其原生观察，不能借用源目录身份拼造条目。
    let tar_entry = dest_storage.get_metadata(&tar_relative_path).await?;

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
    use {storage_adapter::AsyncReceiver, task_model::ErrorEvent};

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
}
