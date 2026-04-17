use std::path::{Path, PathBuf};

use storage_v2::{NFSStorage, S3BucketInfo, S3Storage, StorageEntryMessage, create_storage};

use crate::error::Result;

#[derive(Debug)]
pub struct DirEntryItem {
    pub name: String,
    pub full_path: String,
}

#[derive(Debug)]
pub struct ListDirsResult {
    pub parent: String,
    pub entries: Vec<DirEntryItem>,
    pub sep: String,
}

#[derive(Debug)]
pub struct NfsExportEntry {
    pub path: String,
    pub groups: Vec<String>,
}

/// 存储浏览服务，封装目录浏览、`NFS` 导出查询、`S3` 桶列表等操作
#[derive(Default)]
pub struct StorageBrowserService;

impl StorageBrowserService {
    pub fn new() -> Self {
        Self
    }

    /// 列出指定路径下的子目录
    /// - `storage_url` 为 `Some` 时通过 `storage_v2` 连接远程存储
    /// - `storage_url` 为 None 时列出本地文件系统
    pub async fn list_dirs(&self, path: &str, storage_url: Option<&str>) -> Result<ListDirsResult> {
        if let Some(url) = storage_url {
            self.list_remote_dirs(url, path).await
        } else {
            self.list_local_dirs(path).await
        }
    }

    /// 列出 NFS 服务器的导出路径
    pub async fn list_nfs_exports(&self, server: &str, port: Option<u16>) -> Result<Vec<NfsExportEntry>> {
        let host = match port {
            Some(p) if p != 2049 => format!("nfs://{server}:{p}/."),
            _ => server.to_string(),
        };

        let exports = NFSStorage::list_exports(&host).await?;
        Ok(exports
            .into_iter()
            .map(|e| NfsExportEntry {
                path: e.path,
                groups: e.groups,
            })
            .collect())
    }

    /// 列出 S3 兼容存储的桶列表
    pub async fn list_s3_buckets(
        &self, server: &str, access_key: &str, secret_key: &str, use_https: bool,
    ) -> Result<Vec<S3BucketInfo>> {
        let scheme = if use_https { "s3+https" } else { "s3" };
        let url = format!("{scheme}://{access_key}:{secret_key}@{server}");
        let buckets = S3Storage::list_buckets(&url).await?;
        Ok(buckets)
    }

    async fn list_remote_dirs(&self, url: &str, path: &str) -> Result<ListDirsResult> {
        let storage = create_storage(url, None).await?;
        let path_str = if path.is_empty() {
            "/".to_string()
        } else {
            path.to_string()
        };
        let (parent_dir, prefix) = split_path_for_browse(&path_str);

        let sub_path =
            if parent_dir.as_os_str().is_empty() || parent_dir == Path::new("/") || parent_dir == Path::new("") {
                None
            } else {
                Some(parent_dir.as_path())
            };

        let iter = storage
            .walkdir(sub_path, Some(1), None, None, 1, false, false, 0)
            .await?;

        let mut entries = Vec::new();
        while let Some(msg) = iter.next().await {
            if let StorageEntryMessage::Scanned(entry) = msg {
                if !entry.get_is_dir() {
                    continue;
                }
                let name = entry.get_name().to_string();
                if !prefix.is_empty() && !name.to_lowercase().starts_with(&prefix.to_lowercase()) {
                    continue;
                }
                let full_path = entry.get_relative_path().to_string_lossy().to_string();
                entries.push(DirEntryItem { name, full_path });
            }
        }

        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let parent_str = parent_dir.to_string_lossy().to_string();
        Ok(ListDirsResult {
            parent: parent_str,
            entries,
            sep: "/".to_string(),
        })
    }

    async fn list_local_dirs(&self, path: &str) -> Result<ListDirsResult> {
        let sep = std::path::MAIN_SEPARATOR;

        // Windows 盘符处理：`C:` → `C:\`
        let path_str = if cfg!(windows)
            && path.len() == 2
            && path.as_bytes()[0].is_ascii_alphabetic()
            && path.as_bytes()[1] == b':'
        {
            format!("{path}\\")
        } else {
            path.to_string()
        };

        let (parent_dir, prefix) = split_path_for_browse(&path_str);
        let parent_str = parent_dir.to_string_lossy().to_string();
        let mut entries = Vec::new();

        if let Ok(mut read_dir) = tokio::fs::read_dir(&parent_dir).await {
            while let Ok(Some(entry)) = read_dir.next_entry().await {
                let Ok(ft) = entry.file_type().await else {
                    continue;
                };
                if !ft.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if !prefix.is_empty() && !name.to_lowercase().starts_with(&prefix.to_lowercase()) {
                    continue;
                }
                let full_path = entry.path().to_string_lossy().to_string();
                entries.push(DirEntryItem { name, full_path });
            }
        }

        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(ListDirsResult {
            parent: parent_str,
            entries,
            sep: sep.to_string(),
        })
    }
}

fn split_path_for_browse(path_str: &str) -> (PathBuf, String) {
    let input_path = Path::new(path_str);
    if path_str.ends_with('/') || path_str.ends_with('\\') {
        (input_path.to_path_buf(), String::new())
    } else {
        let default_root = if cfg!(windows) {
            Path::new("C:\\")
        } else {
            Path::new("/")
        };
        let parent = input_path.parent().unwrap_or(default_root);
        let file_name = input_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        (parent.to_path_buf(), file_name)
    }
}
