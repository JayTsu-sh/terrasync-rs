//! 文件删除模块
//!
//! 该模块提供了删除指定路径下所有文件和目录的功能，支持递归删除并实时显示进度。

// 标准库
use std::time::Duration;

// 外部crate
use data_mover::create_storage;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::info;
use utils::prelude::AppConfig;

// 内部模块
use crate::error::Result;

/// 删除指定路径下的所有文件和目录，并实时显示进度
pub async fn rm(path: &str) -> Result<()> {
    let path = path.trim();

    info!("开始删除路径: {}", path);

    let storage = create_storage(path, data_mover::CreateStorageOptions::new(None, false)).await?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} 删除中: 已删除 {msg} | 耗时 {elapsed_precise}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    pb.enable_steady_tick(Duration::from_secs(2));

    let concurrency = AppConfig::fetch()?.delete.concurrency;
    let iter = storage.delete_dir_all_with_progress(None, concurrency)?;

    let mut file_count: u64 = 0;
    let mut dir_count: u64 = 0;

    while let Some(event) = iter.next().await {
        if event.is_dir {
            dir_count += 1;
        } else {
            file_count += 1;
        }
        pb.set_message(format!("{file_count} 文件, {dir_count} 目录"));
    }

    pb.finish_with_message(format!("完成! 共删除 {file_count} 文件, {dir_count} 目录"));

    info!("成功删除路径: {}", path);
    Ok(())
}
