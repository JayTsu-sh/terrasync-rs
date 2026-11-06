use crate::scan::observe_path;
// 标准库
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::StorageError;
use crate::scan::source::WalkDirAsyncIterator;
use crate::storage_factory::StorageContext;
use task_model::{ErrorEvent, StorageEntryMessage};
// 外部crate
use tracing::{Instrument, error, info, info_span};

// 内部模块
use crate::config::ScanConfig;
use crate::error::{AppError, Result};
use crate::storage_factory::{StorageRole, create_storage_for_role};

/// 目录遍历器 - 提供通用的目录遍历功能
pub struct DirectoryWalker {
    config: ScanConfig,
    operation_timeout: Option<std::time::Duration>,
    recovery_window: Option<std::time::Duration>,
}

impl DirectoryWalker {
    /// 创建一个新的目录遍历器
    pub fn new(config: ScanConfig) -> Self {
        Self {
            config,
            operation_timeout: None,
            recovery_window: None,
        }
    }

    async fn source_storage(&self) -> Result<StorageContext> {
        create_storage_for_role(&self.config.path, None, false, StorageRole::Source).await
    }

    /// 开始遍历目录
    pub async fn walk(&self) -> Result<WalkDirAsyncIterator> {
        // 检查是否有file_list参数
        if let Some(file_list_path) = &self.config.file_list {
            if self.operation_timeout.is_some() || self.recovery_window.is_some() {
                return Err(AppError::ConfigError(
                    "file_list metadata enumeration does not support directory traversal timeout/recovery options"
                        .into(),
                ));
            }
            info!("Using file list from: {}", file_list_path);

            // 创建存储实例
            let storage = self.source_storage().await?;

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
            let storage = self
                .source_storage()
                .await
                .map_err(|e| AppError::ScanError(format!("Failed to create storage: {e}")))?;
            let walkdir_iter = storage
                .walkdir(
                    None,
                    crate::scan::source::WalkOptions {
                        operation_timeout: self.operation_timeout,
                        recovery_window: self.recovery_window,
                        depth,
                        match_expressions: self.config.match_expressions.clone(),
                        exclude_expressions: self.config.exclude_expressions.clone(),
                        concurrency: self.config.concurrency,
                        include_tags: self.config.include_tags,
                        packaged: self.config.packaged,
                        package_depth: self.config.package_depth,
                    },
                )
                .await?;

            Ok(walkdir_iter)
        }
    }
}

/// 从文件列表创建 `WalkDirAsyncIterator`
///
/// 该函数从文件列表中读取文件路径，然后为每个文件创建 `ObservedEntry`，
/// 并通过 `WalkDirAsyncIterator` 返回。
///
/// # 参数
/// - `storage`: `data_mover` 存储实例
/// - `file_list_path`: 文件列表路径
///
/// # 返回值
/// - 成功时返回 `WalkDirAsyncIterator`
/// - 失败时返回包含错误信息的 `Err`
fn walkdir_from_file_list(storage: StorageContext, file_list_path: &str) -> Result<WalkDirAsyncIterator> {
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
    let terminal_error = Arc::new(Mutex::new(None));
    let stored_error = terminal_error.clone();

    // 启动一个异步任务，遍历文件列表并发送StorageEntryMessage
    let span = info_span!("dir_walker");
    tokio::spawn(
        async move {
            for relative_path in file_paths_iter {
                match relative_path {
                    Ok(file_path) => {
                        // 尝试获取文件的ObservedEntry
                        match observe_path(storage.storage(), &file_path).await {
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
                                let reason = e.to_string();
                                stored_error
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .get_or_insert(e);
                                // 发送错误消息到通道
                                let msg = StorageEntryMessage::Error {
                                    event: ErrorEvent::Scan,
                                    path: file_path,
                                    entry: None,
                                    reason,
                                };
                                if let Err(e) = tx.send(msg).await {
                                    error!("Failed to send error: {}", e);
                                }
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read file path: {}", e);
                        let reason = e.to_string();
                        stored_error
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_or_insert(StorageError::from(e));
                        // 发送错误消息到通道
                        let msg = StorageEntryMessage::Error {
                            event: ErrorEvent::Scan,
                            path: PathBuf::new(),
                            entry: None,
                            reason,
                        };
                        if let Err(e) = tx.send(msg).await {
                            error!("Failed to send error: {}", e);
                        }
                        break;
                    }
                }
            }
        }
        .instrument(span),
    );

    Ok(WalkDirAsyncIterator::new(rx).with_error(terminal_error))
}

/// 通用的目录遍历函数 - 作为便捷的包装器
pub async fn walkdir(config: ScanConfig) -> Result<crate::scan::source::WalkDirAsyncIterator> {
    let walker = DirectoryWalker::new(config);
    walker.walk().await
}

/// 扫描显式选择遍历恢复预算；普通遍历由其调用方选择策略。
pub async fn scan_walkdir(
    config: ScanConfig, operation_timeout: std::time::Duration, recovery_window: std::time::Duration,
) -> Result<WalkDirAsyncIterator> {
    DirectoryWalker {
        config,
        operation_timeout: Some(operation_timeout),
        recovery_window: Some(recovery_window),
    }
    .walk()
    .await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        config::initialize_scan_config,
        scan::ScanType,
        storage_factory::{StorageRole, create_storage_for_role},
    };
    use std::time::Duration;
    use tokio::fs;

    #[tokio::test]
    async fn file_list_cannot_silently_ignore_directory_recovery_options() -> Result<()> {
        let config = initialize_scan_config(
            "file-list-options",
            "unused",
            0,
            ScanType::Full,
            &None,
            &None,
            1,
            false,
            &Some("unused-list".into()),
            false,
            0,
        )?;
        assert!(
            matches!(scan_walkdir(config, Duration::from_secs(1), Duration::from_secs(3)).await,
            Err(AppError::ConfigError(reason)) if reason.contains("file_list"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn file_list_metadata_failure_is_not_a_successful_end_of_enumeration() -> Result<()> {
        let root = tempfile::tempdir()?;
        let paths = root.path().join("files.txt");
        fs::write(&paths, b"missing-file\n").await?;
        let storage = create_storage_for_role(root.path().to_str().unwrap(), None, false, StorageRole::Source).await?;
        let iterator = walkdir_from_file_list(storage, paths.to_str().unwrap())?;
        while iterator.next().await.is_some() {}
        assert!(iterator.take_error().is_some_and(|error| error.is_not_found()));
        Ok(())
    }
}
