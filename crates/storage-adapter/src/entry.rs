//! TerraSync 数据库及 wire 使用的数据投影；不承担存储 IO。
// 数据结构源自 data-mover d76ff5c（Apache-2.0）。
use serde::{Deserialize, Serialize};
#[must_use]
pub fn datetime_to_string(time: i64) -> String {
    let magnitude = std::time::Duration::from_nanos(time.unsigned_abs());
    let system_time = if time.is_negative() {
        std::time::SystemTime::UNIX_EPOCH.checked_sub(magnitude)
    } else {
        std::time::SystemTime::UNIX_EPOCH.checked_add(magnitude)
    }
    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    let datetime: chrono::DateTime<chrono::Utc> = system_time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
}

/// 字节级断点续传的进度回调：参数 `(offset, len)` 表示 `[offset, offset+len)`
/// 已由目标端确认写入；不代表 crash durability。回调内不得阻塞写管道。
pub type CommitCallback = std::sync::Arc<dyn Fn(u64, u64) + Send + Sync>;

/// 旧产品进度记录。新引擎忽略这里的路径和区间，独立发现目标端恢复状态。
pub struct ResumeContext {
    /// 兼容保留的旧 `.part` 路径，不用于驱动新存储写入。
    pub part_relative_path: std::path::PathBuf,
    /// 兼容保留的本地缺失区间，不作为目标端恢复权威。
    pub missing_intervals: Vec<(u64, u64)>,
    /// 每个 chunk 确认落盘后触发的进度回调
    pub on_committed: CommitCallback,
}

/// 文件标签结构，用于存储文件的元数据标签
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// 标签键
    pub key: String,
    /// 标签值
    pub value: String,
}
