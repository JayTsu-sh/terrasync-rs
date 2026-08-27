// 标准库
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use data_mover::storage_enum::{StorageEnum, create_storage};
use data_mover::{ErrorEvent, StorageEntryMessage, WalkDirAsyncIterator};
// 外部crate
use tracing::{Instrument, error, info, info_span};

// 内部模块
use crate::config::ScanConfig;
use crate::error::{AppError, Result};

/// 目录遍历器 - 提供通用的目录遍历功能
pub struct DirectoryWalker {
    config: ScanConfig,
}

impl DirectoryWalker {
    /// 创建一个新的目录遍历器
    pub fn new(config: ScanConfig) -> Self {
        Self { config }
    }

    /// 开始遍历目录
    pub async fn walk(&self) -> Result<WalkDirAsyncIterator> {
        // 检查是否有file_list参数
        if let Some(file_list_path) = &self.config.file_list {
            info!("Using file list from: {}", file_list_path);

            // 创建存储实例
            let storage = create_storage(&self.config.path, data_mover::CreateStorageOptions::new(None, false)).await?;

            // 从文件列表创建迭代器
            walkdir_from_file_list(storage, file_list_path)
        } else {
            // 使用现有的walkdir扫描方式
            let depth = if self.config.depth > 0 {
                Some(self.config.depth as usize)
            } else {
                None
            };

            // 先实例化存储，再调用实例方法遍历目录
            info!(
                "Attempting to create storage for directory walk with concurrency: {}, path: {}",
                self.config.concurrency, self.config.path
            );
            let storage = create_storage(&self.config.path, data_mover::CreateStorageOptions::new(None, false))
                .await
                .map_err(|e| AppError::ScanError(format!("Failed to create storage: {e}")))?;
            let walkdir_iter = storage
                .walkdir(
                    None,
                    data_mover::WalkOptions {
                        depth,
                        match_expressions: self.config.match_expressions.clone(),
                        exclude_expressions: self.config.exclude_expressions.clone(),
                        concurrency: self.config.concurrency,
                        include_tags: self.config.include_tags,
                        packaged: self.config.packaged,
                        package_depth: self.config.package_depth,
                    },
                )
                .await
                .map_err(|e| AppError::ScanError(format!("Failed to start walkdir: {e}")))?;

            Ok(walkdir_iter)
        }
    }
}

/// 从文件列表创建 `WalkDirAsyncIterator`
///
/// 该函数从文件列表中读取文件路径，然后为每个文件创建 `EntryEnum`，
/// 并通过 `WalkDirAsyncIterator` 返回。
///
/// # 参数
/// - `storage`: `data_mover` 存储实例
/// - `file_list_path`: 文件列表路径
///
/// # 返回值
/// - 成功时返回 `WalkDirAsyncIterator`
/// - 失败时返回包含错误信息的 `Err`
fn walkdir_from_file_list(storage: StorageEnum, file_list_path: &str) -> Result<WalkDirAsyncIterator> {
    // 从文件中读取文件路径
    let file = File::open(file_list_path).map_err(|e| AppError::ScanError(format!("Failed to open file list: {e}")))?;
    let reader = BufReader::new(file);

    // 创建一个迭代器，逐行读取文件并转换为PathBuf
    let file_paths_iter = reader.lines().filter_map(|line_result| {
        match line_result {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    None // 跳过空行和注释行
                } else {
                    let path = PathBuf::from(line);
                    // 安全校验：拒绝绝对路径和包含 ".." 的路径，防止路径穿越攻击
                    if path.is_absolute() {
                        error!("file_list 中包含绝对路径，已跳过: {:?}", path);
                        return None;
                    }
                    if path.components().any(|c| c == std::path::Component::ParentDir) {
                        error!("file_list 中包含 '..' 路径组件，已跳过: {:?}", path);
                        return None;
                    }
                    Some(Ok(path))
                }
            }
            Err(e) => Some(Err(e)),
        }
    });

    let (tx, rx) = async_channel::bounded(1000);

    // 启动一个异步任务，遍历文件列表并发送StorageEntryMessage
    let span = info_span!("dir_walker");
    tokio::spawn(
        async move {
            for relative_path in file_paths_iter {
                match relative_path {
                    Ok(file_path) => {
                        // 尝试获取文件的EntryEnum
                        match storage.get_metadata(&file_path).await {
                            Ok(metadata) => {
                                // 发送Scanned消息到通道
                                let msg = StorageEntryMessage::Scanned(std::sync::Arc::new(metadata));
                                if let Err(e) = tx.send(msg).await {
                                    error!("Failed to send storage entry: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to get metadata for file {:?}: {}", file_path, e);
                                // 发送错误消息到通道
                                let msg = StorageEntryMessage::Error {
                                    event: ErrorEvent::Scan,
                                    path: file_path,
                                    entry: None,
                                    reason: e.to_string(),
                                };
                                if let Err(e) = tx.send(msg).await {
                                    error!("Failed to send error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read file path: {}", e);
                        // 发送错误消息到通道
                        let msg = StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: PathBuf::new(),
                            entry: None,
                            reason: e.to_string(),
                        };
                        if let Err(e) = tx.send(msg).await {
                            error!("Failed to send error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
        .instrument(span),
    );

    Ok(data_mover::WalkDirAsyncIterator::new(rx))
}

/// 通用的目录遍历函数 - 作为便捷的包装器
pub async fn walkdir(config: ScanConfig) -> Result<data_mover::WalkDirAsyncIterator> {
    let walker = DirectoryWalker::new(config);
    walker.walk().await
}
