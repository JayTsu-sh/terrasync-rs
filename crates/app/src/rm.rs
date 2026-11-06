//! 文件删除模块
//!
//! 该模块提供了删除指定路径下所有文件和目录的功能，支持递归删除并实时显示进度。

use crate::rm::progress as deletion;
pub(crate) mod progress;
// 标准库
use std::time::Duration;

// 外部crate
use data_mover::redact_storage_url;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::{debug, info};
use utils::prelude::AppConfig;

use crate::storage_factory::{StorageRole, create_storage_for_role};

// 内部模块
use crate::error::Result;

/// 删除指定路径下的所有文件和目录，并实时显示进度
pub async fn rm(path: &str) -> Result<()> {
    let path = path.trim();

    info!("开始删除路径: {}", redact_storage_url(path));

    let storage = create_storage_for_role(path, None, false, StorageRole::Destination).await?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} 删除中: 已删除 {msg} | 耗时 {elapsed_precise}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    pb.enable_steady_tick(Duration::from_secs(2));

    let concurrency = AppConfig::fetch()?.delete.concurrency;
    let iter = deletion::delete_dir_all_with_progress(storage.storage(), None, concurrency).await?;

    let mut file_count: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut failure = None;

    while let Some(event) = iter.next().await {
        let (relative_path, is_dir) = match event.into_deleted() {
            Ok(deleted) => deleted,
            Err(error) => {
                // 继续消费到原生删除会话收尾，避免任务返回后仍有在途删除。
                failure.get_or_insert(error);
                continue;
            }
        };
        debug!(path = %relative_path.display(), "已确认删除条目");
        if is_dir {
            dir_count += 1;
        } else {
            file_count += 1;
        }
        pb.set_message(format!("{file_count} 文件, {dir_count} 目录"));
    }

    if let Some(error) = failure {
        pb.abandon_with_message("删除失败");
        return Err(error.into());
    }

    pb.finish_with_message(format!("完成! 共删除 {file_count} 文件, {dir_count} 目录"));

    info!("成功删除路径: {}", redact_storage_url(path));
    Ok(())
}
