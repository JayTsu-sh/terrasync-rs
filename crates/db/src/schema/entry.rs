//! 用于比较、统计和最终事件的轻量查询事实；不伪造完整观察。
use serde::{Deserialize, Serialize};
use storage_adapter::observed_scan::EntryStatistics;

#[derive(Default, Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct EntrySummary {
    /// 文件或目录的名称
    pub name: String,
    /// 相对路径(主键)
    pub relative_path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// 文件扩展名（可选）
    pub ext: Option<String>,
    /// 创建时间（时间戳）
    pub ctime: i64,
    /// 修改时间（时间戳）
    pub mtime: i64,
    /// 访问时间（时间戳）
    pub atime: i64,
    /// Unix权限模式（可选，如0o755）
    pub mode: Option<u32>,
    /// 存储类型 ("nas" 或 "s3")
    pub storage_type: String,
    /// 是否为符号链接
    pub is_symlink: bool,
    /// 是否为目录
    pub is_dir: bool,
    /// 是否为普通文件
    pub is_regular_file: bool,
    /// 硬链接数量
    pub hard_links: u32,
    /// 当前扫描状态
    pub current_state: u8,
    /// 用户ID
    pub uid: Option<u32>,
    /// 组ID
    pub gid: Option<u32>,
    /// 索引节点号
    pub ino: Option<u64>,
    /// 文件句柄（NFS file handle / CIFS file ID 等）
    pub file_handle: Option<String>,
    /// 对象版本ID（S3多版本使用）
    pub version_id: String,
    /// 文件标签
    pub tags: Option<String>,
    /// 版本计数
    pub version_count: Option<u32>,
    pub marker_state: Option<String>,
}

impl EntryStatistics for EntrySummary {
    fn get_size(&self) -> u64 {
        self.size
    }
    fn get_name(&self) -> &str {
        &self.name
    }
    fn get_relative_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.relative_path)
    }
    fn get_extension(&self) -> Option<&str> {
        self.ext.as_deref()
    }
    fn get_is_dir(&self) -> bool {
        self.is_dir
    }
    fn get_is_symlink(&self) -> bool {
        self.is_symlink
    }
    fn get_is_regular_file(&self) -> bool {
        self.is_regular_file
    }
    fn get_mtime(&self) -> i64 {
        self.mtime
    }
}
