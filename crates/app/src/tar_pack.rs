// 标准库
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// 内部模块
use storage_v2::qos::QosManager;
use storage_v2::storage_enum::StorageEnum;
use storage_v2::{EntryEnum, NASEntry, S3Entry, StorageEntryMessage, WalkDirAsyncIterator, calculate_tar_size};
// 外部crate
use tracing::{debug, info};

use crate::error::Result;

/// 将源端目录打包为 .tar 文件写入目标端
///
/// 两阶段流程：
/// 1. 子 walkdir 收集目录下所有条目 + 计算 tar 总大小
/// 2. 调用 `StorageEnum::pack_files_to_tar()` 流式打包写入
///
/// # 返回值
/// - `tar_entry`: 合成的条目，表示目标端的 .tar 文件（变体与输入 dir_entry 一致）
/// - `manifest_list`: tar 内所有条目的列表（供后续写入 manifest 表）
pub async fn pack_directory(
    src_storage: &StorageEnum, dest_storage: &StorageEnum, dir_entry: &EntryEnum, is_source_reserved: bool,
    qos: Option<QosManager>, bytes_counter: Option<Arc<AtomicU64>>,
) -> Result<(EntryEnum, Vec<Arc<EntryEnum>>)> {
    let dir_path = dir_entry.get_relative_path();
    let dir_mtime = dir_entry.get_mtime();

    info!("Pack directory: {:?}", dir_path);

    // ── 阶段 1：收集子条目 + 计算 tar 大小 ──
    let iterator = src_storage
        .walkdir(
            Some(dir_entry.get_relative_path()),
            None,
            None,
            None,
            1,
            false,
            false,
            0,
        )
        .await?;
    let manifest_list = collect_entries(iterator).await;

    debug!("Collected {} entries for packing {:?}", manifest_list.len(), dir_path);

    let tar_size = calculate_tar_size(&manifest_list);

    // 构造 .tar 文件路径
    let tar_relative_path = PathBuf::from(format!("{}.tar", dir_path.to_string_lossy()));

    // ── 阶段 2：流式打包写入 ──
    StorageEnum::pack_files_to_tar(
        src_storage,
        dest_storage,
        &manifest_list,
        &tar_relative_path,
        tar_size,
        dir_mtime,
        qos,
        bytes_counter,
    )
    .await?;

    info!(
        "Pack complete: {:?} -> {:?} ({} bytes, {} entries)",
        dir_path,
        tar_relative_path,
        tar_size,
        manifest_list.len()
    );

    // 根据输入条目类型构造对应变体的 tar_entry
    let tar_name = tar_relative_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let tar_entry = match dir_entry {
        EntryEnum::NAS(_) => EntryEnum::NAS(NASEntry {
            name: tar_name,
            relative_path: tar_relative_path,
            extension: Some("tar".to_string()),
            is_dir: false,
            size: tar_size,
            atime: dir_mtime,
            ctime: dir_mtime,
            mtime: dir_mtime,
            mode: dir_entry.get_mode().unwrap_or(0),
            is_symlink: false,
            hard_links: None,
            uid: dir_entry.get_uid(),
            gid: dir_entry.get_gid(),
            ino: None,
            file_handle: None,
            acl: None,
            owner: None,
            owner_group: None,
            xattrs: None,
        }),
        EntryEnum::S3(_) => EntryEnum::S3(S3Entry {
            name: tar_name,
            relative_path: tar_relative_path.to_string_lossy().replace('\\', "/"),
            extension: Some("tar".to_string()),
            is_dir: false,
            size: tar_size,
            mtime: dir_mtime,
            tags: None,
            version_id: None,
            is_latest: false,
            is_delete_marker: false,
            version_count: None,
        }),
    };

    // 删除源端目录（如果不保留）
    if !is_source_reserved {
        debug!("Deleting source directory: {:?}", dir_path);
        src_storage.delete_dir_all(dir_entry).await?;
    }

    Ok((tar_entry, manifest_list))
}

/// 从 walkdir 迭代器收集所有条目
async fn collect_entries(iterator: WalkDirAsyncIterator) -> Vec<Arc<EntryEnum>> {
    let mut entries = Vec::new();
    while let Some(message) = iterator.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => {
                entries.push(entry);
            }
            StorageEntryMessage::Error { event, path, reason } => {
                tracing::warn!("Error during sub-walkdir ({}): {:?} - {}", event, path, reason);
            }
            _ => {}
        }
    }
    entries
}
