//! TerraSync 会话的目录页及编号，不属于存储后端。
use serde::{Deserialize, Serialize};
use task_model::observation::ObservedItem;
/// 带 NDX 编号的 entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NdxEntry {
    pub ndx: i32,
    #[serde(with = "crate::observation_wire::selected")]
    pub entry: ObservedItem,
}

/// 一整页目录内容，消费完 drop 即释放内存
#[derive(Debug, Serialize, Deserialize)]
pub struct DirPageResult {
    /// 本目录的 `relative_path`（root 为空字符串）
    pub dir_path: String,
    /// 本页 NDX 范围起始（含）
    pub ndx_start: i32,
    /// 文件 entries（已按 name 排序，NDX 从 `ndx_start` 连续递增）
    pub files: Vec<NdxEntry>,
    /// 子目录 entries（已按 name 排序，NDX 紧接 files 之后连续递增）
    pub subdirs: Vec<NdxEntry>,
    /// 本页段间隔的 gap NDX 值（-1 表示整棵树最后一页，无 gap）
    pub gap_ndx: i32,
}
