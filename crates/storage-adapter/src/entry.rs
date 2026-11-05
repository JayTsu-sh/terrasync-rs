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
