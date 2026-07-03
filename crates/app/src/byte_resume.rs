//! 字节级断点续传状态（单文件 offset Resume）
//!
//! 为「多块大文件」记录已确认落盘的 offset 区间集合，持久化在
//! `jobs/<job_id>/byte_resume/<dest_rel_hash>.json` 下。任务中断后重跑时，
//! 重新计算缺失区间，只补未完成部分，避免重传整个前半段。
//!
//! 设计取舍（见方案文档）：
//! - 进度 = 已完成区间集合（多块管道乱序并发写，半截文件可能有空洞）。
//! - 半截文件写到 `<dest>.terrasync-part`，全部补齐后由 data-mover 原子 rename。
//! - 信任进度记录直接续：靠 commit 语义保证落盘 + 源 mtime/size 未变兜底，不重算 hash。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::error::{AppError, Result};

/// 状态文件子目录（位于 `job_dir` 下）
const RESUME_SUBDIR: &str = "byte_resume";
/// `.part` 临时文件后缀
const PART_SUFFIX: &str = ".terrasync-part";
/// 状态格式版本（不兼容变更时 +1，旧状态作废）
const STATE_VERSION: u32 = 1;
/// 每累计 N 个已提交 chunk 触发一次落盘（约 64MB @ 4MB chunk）
const FLUSH_EVERY_CHUNKS: usize = 16;

/// 返回 `dest_rel` 对应的 `.part` 相对路径（同目录、同名加后缀）。
pub fn part_path_for(dest_rel: &Path) -> PathBuf {
    let mut s = dest_rel.as_os_str().to_os_string();
    s.push(PART_SUFFIX);
    PathBuf::from(s)
}

/// 该路径是否为续传临时文件（`.terrasync-part` 后缀）。
/// `--delete-target` 的孤儿清理必须跳过这类文件，否则会破坏进行中的续传进度。
pub fn is_part_file(path: &Path) -> bool {
    path.as_os_str().to_string_lossy().ends_with(PART_SUFFIX)
}

// ============================================================
// IntervalSet —— 有序、不相交、半开区间 [start, end) 的集合
// ============================================================

/// 已完成 offset 区间集合。内部保持按 start 升序、互不重叠、相邻自动合并。
#[derive(Debug, Default, Clone)]
pub struct IntervalSet {
    intervals: Vec<(u64, u64)>,
}

impl IntervalSet {
    /// 从已有区间列表重建（逐个插入以规范化排序/合并）。
    fn from_vec(items: &[(u64, u64)]) -> Self {
        let mut set = Self::default();
        for &(s, e) in items {
            set.insert(s, e);
        }
        set
    }

    /// 插入 [start, end)，与重叠或相邻（end == 下一段 start）的区间自动合并。
    pub fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut result = Vec::with_capacity(self.intervals.len() + 1);
        let (mut ns, mut ne) = (start, end);
        let mut placed = false;
        for &(s, e) in &self.intervals {
            if e < ns {
                // 完全在新区间左侧（e == ns 视为相邻，落入合并分支）
                result.push((s, e));
            } else if s > ne {
                // 完全在新区间右侧（s == ne 视为相邻，落入合并分支）
                if !placed {
                    result.push((ns, ne));
                    placed = true;
                }
                result.push((s, e));
            } else {
                // 重叠或相邻 → 合并
                ns = ns.min(s);
                ne = ne.max(e);
            }
        }
        if !placed {
            result.push((ns, ne));
        }
        self.intervals = result;
    }

    /// 返回 [0, total) 范围内尚未覆盖的缺失区间。
    pub fn missing(&self, total: u64) -> Vec<(u64, u64)> {
        let mut res = Vec::new();
        let mut cur = 0u64;
        for &(s, e) in &self.intervals {
            if cur >= total {
                break;
            }
            if s > cur {
                let end = s.min(total);
                if cur < end {
                    res.push((cur, end));
                }
            }
            cur = cur.max(e);
        }
        if cur < total {
            res.push((cur, total));
        }
        res
    }

    /// [0, total) 是否已被完全覆盖。
    pub fn covers(&self, total: u64) -> bool {
        self.missing(total).is_empty()
    }

    fn to_vec(&self) -> Vec<(u64, u64)> {
        self.intervals.clone()
    }
}

// ============================================================
// 持久化状态
// ============================================================

/// 落盘的续传状态（JSON）。
#[derive(Debug, Serialize, Deserialize)]
struct ByteResumeState {
    version: u32,
    part_path: String,
    src_relative_path: String,
    src_size: u64,
    src_mtime: i64,
    block_size: u64,
    /// 已合并、有序的已落盘区间 [start, end)
    completed: Vec<(u64, u64)>,
}

struct ByteResumeInner {
    completed: IntervalSet,
    dirty: usize,
}

/// 单个大文件的续传状态存储。多个 receiver worker 通过 `Arc` 共享，
/// 内部 `Mutex` 保证 `mark_committed` 并发安全。
pub struct ByteResumeStore {
    state_path: PathBuf,
    tmp_path: PathBuf,
    part_relative_path: String,
    src_relative_path: String,
    src_size: u64,
    src_mtime: i64,
    block_size: u64,
    inner: Mutex<ByteResumeInner>,
}

impl ByteResumeStore {
    /// 打开/创建某大文件的续传状态。
    ///
    /// 续传判据：状态文件存在且 `version`/`src_size`/`src_mtime`/`block_size` 全部匹配
    /// → 加载已完成区间（续传）；否则（不存在 / 源已变 / 格式过期）删除旧状态、以空集合开始（全量）。
    pub async fn open(
        job_dir: &str, dest_rel: &Path, src_size: u64, src_mtime: i64, block_size: u64,
    ) -> Result<Arc<Self>> {
        let dir = Path::new(job_dir).join(RESUME_SUBDIR);
        tokio::fs::create_dir_all(&dir).await?;

        let file_name = format!("{}.json", blake3::hash(dest_rel.to_string_lossy().as_bytes()).to_hex());
        let state_path = dir.join(&file_name);
        let tmp_path = dir.join(format!("{file_name}.tmp"));
        let part_relative_path = part_path_for(dest_rel).to_string_lossy().to_string();
        let src_relative_path = dest_rel.to_string_lossy().to_string();

        let loaded = Self::load_if_match(&state_path, src_size, src_mtime, block_size).await;
        let completed = if let Some(intervals) = loaded {
            info!(
                "[ByteResume] {} resuming: {} completed intervals",
                src_relative_path,
                intervals.len()
            );
            IntervalSet::from_vec(&intervals)
        } else {
            // 不匹配/不存在：清掉可能残留的旧状态，从零开始。
            // 目标端可能残留旧 .part，但无需主动清理：write_data_resumable 是定位写
            // （非 append），全新状态 missing=[0,size) 会整体重写；收尾 set_file_len(size)
            // 截掉超出部分。残留脏字节不可能存活到最终文件。
            let _ = tokio::fs::remove_file(&state_path).await;
            IntervalSet::default()
        };

        Ok(Arc::new(Self {
            state_path,
            tmp_path,
            part_relative_path,
            src_relative_path,
            src_size,
            src_mtime,
            block_size,
            inner: Mutex::new(ByteResumeInner { completed, dirty: 0 }),
        }))
    }

    /// 读取并校验状态文件；任何不匹配（不存在/解析失败/版本或源不符）返回 `None`。
    async fn load_if_match(path: &Path, src_size: u64, src_mtime: i64, block_size: u64) -> Option<Vec<(u64, u64)>> {
        let Ok(data) = tokio::fs::read_to_string(path).await else {
            return None;
        };
        let state: ByteResumeState = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => {
                warn!("[ByteResume] 状态解析失败: {}, 视为全新", e);
                return None;
            }
        };
        if state.version != STATE_VERSION
            || state.src_size != src_size
            || state.src_mtime != src_mtime
            || state.block_size != block_size
        {
            warn!("[ByteResume] 源已变更或格式过期，作废旧续传状态");
            return None;
        }
        Some(state.completed)
    }

    /// `.part` 文件的目标端相对路径。
    pub fn part_relative_path(&self) -> &str {
        &self.part_relative_path
    }

    /// 当前缺失（待传输）的 offset 区间。
    pub async fn missing_intervals(&self) -> Vec<(u64, u64)> {
        self.inner.lock().await.completed.missing(self.src_size)
    }

    /// 记录一个 chunk 已确认落盘 [offset, offset+len)；达阈值自动 flush。
    pub async fn mark_committed(&self, offset: u64, len: u64) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.completed.insert(offset, offset + len);
        inner.dirty += 1;
        if inner.dirty >= FLUSH_EVERY_CHUNKS {
            self.flush_locked(&mut inner).await?;
        }
        Ok(())
    }

    /// 已完成区间是否覆盖整个文件 `[0, src_size)`。
    pub async fn is_complete(&self) -> bool {
        self.inner.lock().await.completed.covers(self.src_size)
    }

    /// 强制落盘当前区间集合。
    pub async fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        self.flush_locked(&mut inner).await
    }

    /// 收尾：删除状态文件与临时文件。
    ///
    /// `.part` → 最终文件的 rename 由 data-mover 完成：`copy_file_resumable` 仅在
    /// rename 成功后返回 `Ok(())`（rename 失败会返回 Err，走保留状态的续传分支），
    /// 因此本函数被调用时 `.part` 必已不存在，无需清理。
    pub async fn finalize_done(&self) -> Result<()> {
        let _ = tokio::fs::remove_file(&self.tmp_path).await;
        match tokio::fs::remove_file(&self.state_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AppError::IoError(e)),
        }
    }

    /// 持锁落盘：序列化 → 写临时文件 → 原子 rename。
    async fn flush_locked(&self, inner: &mut ByteResumeInner) -> Result<()> {
        let state = ByteResumeState {
            version: STATE_VERSION,
            part_path: self.part_relative_path.clone(),
            src_relative_path: self.src_relative_path.clone(),
            src_size: self.src_size,
            src_mtime: self.src_mtime,
            block_size: self.block_size,
            completed: inner.completed.to_vec(),
        };
        let json = serde_json::to_string(&state)
            .map_err(|e| AppError::CheckpointError(format!("serialize byte-resume state: {e}")))?;
        tokio::fs::write(&self.tmp_path, json.as_bytes()).await?;
        tokio::fs::rename(&self.tmp_path, &self.state_path).await?;
        inner.dirty = 0;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn interval_merge_adjacent_and_overlap() {
        let mut s = IntervalSet::default();
        s.insert(0, 4);
        s.insert(4, 8); // 相邻 → 合并为 [0,8)
        s.insert(10, 12);
        s.insert(6, 11); // 跨越 [4,8) 与 [10,12) 的重叠 → 合并为 [0,12)
        assert_eq!(s.to_vec(), vec![(0, 12)]);
    }

    #[test]
    fn interval_insert_out_of_order() {
        let mut s = IntervalSet::default();
        s.insert(8, 12);
        s.insert(0, 4);
        s.insert(4, 8);
        assert_eq!(s.to_vec(), vec![(0, 12)]);
    }

    #[test]
    fn interval_empty_and_invalid() {
        let mut s = IntervalSet::default();
        s.insert(5, 5); // 空区间忽略
        s.insert(8, 4); // 非法（start>end）忽略
        assert!(s.to_vec().is_empty());
    }

    #[test]
    fn interval_missing_basic() {
        let mut s = IntervalSet::default();
        s.insert(0, 4);
        s.insert(8, 12);
        assert_eq!(s.missing(16), vec![(4, 8), (12, 16)]);
    }

    #[test]
    fn interval_missing_full_and_empty() {
        let mut s = IntervalSet::default();
        assert_eq!(s.missing(10), vec![(0, 10)]); // 空集合 → 全缺失
        s.insert(0, 10);
        assert!(s.missing(10).is_empty()); // 全覆盖 → 无缺失
        assert!(s.covers(10));
    }

    #[test]
    fn interval_missing_clamps_to_total() {
        let mut s = IntervalSet::default();
        s.insert(0, 4);
        s.insert(20, 30); // 超出 total
        assert_eq!(s.missing(10), vec![(4, 10)]);
        assert!(!s.covers(10));
    }

    #[test]
    fn part_path_appends_suffix() {
        let p = part_path_for(Path::new("a/b/big.bin"));
        assert_eq!(p.to_string_lossy(), format!("a/b/big.bin{PART_SUFFIX}"));
    }

    #[tokio::test]
    async fn store_persists_and_resumes() {
        let dir = std::env::temp_dir().join(format!("br_test_{}", std::process::id()));
        let job_dir = dir.to_string_lossy().to_string();
        let dest = Path::new("sub/big.bin");

        let store = ByteResumeStore::open(&job_dir, dest, 1000, 42, 256).await.unwrap();
        assert_eq!(store.missing_intervals().await, vec![(0, 1000)]);
        store.mark_committed(0, 256).await.unwrap();
        store.mark_committed(256, 256).await.unwrap();
        store.flush().await.unwrap();
        assert!(!store.is_complete().await);

        // 同源参数重开 → 续传，已完成区间保留
        let store2 = ByteResumeStore::open(&job_dir, dest, 1000, 42, 256).await.unwrap();
        assert_eq!(store2.missing_intervals().await, vec![(512, 1000)]);

        store2.finalize_done().await.unwrap();
        // finalize 后重开 → 全新
        let store3 = ByteResumeStore::open(&job_dir, dest, 1000, 42, 256).await.unwrap();
        assert_eq!(store3.missing_intervals().await, vec![(0, 1000)]);
        store3.finalize_done().await.unwrap();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn store_invalidates_on_source_change() {
        let dir = std::env::temp_dir().join(format!("br_test_chg_{}", std::process::id()));
        let job_dir = dir.to_string_lossy().to_string();
        let dest = Path::new("big.bin");

        let store = ByteResumeStore::open(&job_dir, dest, 1000, 42, 256).await.unwrap();
        store.mark_committed(0, 512).await.unwrap();
        store.flush().await.unwrap();

        // mtime 变化 → 作废，全新全量
        let changed = ByteResumeStore::open(&job_dir, dest, 1000, 99, 256).await.unwrap();
        assert_eq!(changed.missing_intervals().await, vec![(0, 1000)]);

        changed.finalize_done().await.unwrap();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn store_complete_when_all_covered() {
        let dir = std::env::temp_dir().join(format!("br_test_done_{}", std::process::id()));
        let job_dir = dir.to_string_lossy().to_string();
        let dest = Path::new("big.bin");

        let store = ByteResumeStore::open(&job_dir, dest, 512, 1, 256).await.unwrap();
        store.mark_committed(256, 256).await.unwrap();
        store.mark_committed(0, 256).await.unwrap();
        assert!(store.is_complete().await);
        assert!(store.missing_intervals().await.is_empty());

        store.finalize_done().await.unwrap();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
