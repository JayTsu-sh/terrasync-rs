//! chunk 级增量算法（rsync 风格）
//!
//! 纯算法 crate，无 IO 依赖。提供：
//! - Rolling checksum（Adler32 变体，O(1) 滑动更新）
//! - Block 签名计算（rolling + BLAKE3 截断 16 字节）
//! - 滑动窗口匹配（Sender 侧，生成 `DeltaToken`）
//! - 文件重建（Receiver 侧，basis + tokens → 新文件）

pub mod matcher;
pub mod reconstruct;
pub mod rolling;
pub mod signature;

// 外部 crate
use bytes::Bytes;

/// Delta token — 描述源文件与目标 basis file 的差异
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaToken {
    /// 引用 basis file 的 block（Receiver 本地读取，不需要传输）
    Match { block_index: u32 },
    /// 新数据（需要通过网络传输）
    Data(Bytes),
}

/// Block 签名（Receiver 计算并发给 Sender）
#[derive(Debug, Clone)]
pub struct BlockSignature {
    /// 快速 rolling checksum（Adler32 变体，用于滑动窗口初筛）
    pub rolling: u32,
    /// 强 checksum（BLAKE3 截断为 16 字节，确认匹配）
    pub strong: [u8; 16],
}

/// 计算 block size（参考 rsync：`sqrt(file_size)`，bounded \[700, 128KB\]）
pub fn calculate_block_size(file_size: u64) -> u32 {
    let raw = (file_size as f64).sqrt() as u32;
    raw.clamp(700, 128 * 1024)
}
