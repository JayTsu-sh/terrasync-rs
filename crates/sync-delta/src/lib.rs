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

/// 强 hash（BLAKE3 截断）长度，字节数。
///
/// 当前唯一支持的算法是 BLAKE3-128（对应 `transport::message::HashAlgorithm::Blake3`
/// 占位枚举）；多算法协商推迟到后续增量，本次仅将截断长度固化为显式可测的常量。
pub const STRONG_HASH_LEN: usize = 16;

/// 计算 BLAKE3 并截断为 `STRONG_HASH_LEN` 字节，`signature`/`matcher` 共用，避免重复实现
pub(crate) fn blake3_truncated(data: &[u8]) -> [u8; STRONG_HASH_LEN] {
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes();
    let mut result = [0u8; STRONG_HASH_LEN];
    result.copy_from_slice(&bytes[..STRONG_HASH_LEN]);
    result
}

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
    /// 强 checksum（BLAKE3 截断为 `STRONG_HASH_LEN` 字节，确认匹配）
    pub strong: [u8; STRONG_HASH_LEN],
}

/// 计算 block size（参考 rsync：`sqrt(file_size)`，bounded \[700, 128KB\]）
pub fn calculate_block_size(file_size: u64) -> u32 {
    let raw = (file_size as f64).sqrt() as u32;
    raw.clamp(700, 128 * 1024)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn strong_hash_len_is_16() {
        assert_eq!(STRONG_HASH_LEN, 16);
    }

    #[test]
    fn blake3_truncated_always_returns_strong_hash_len_bytes() {
        for data in [&b""[..], b"a", b"hello world", &vec![0u8; 10_000]] {
            assert_eq!(blake3_truncated(data).len(), STRONG_HASH_LEN);
        }
    }

    #[test]
    fn blake3_truncated_is_deterministic_and_sensitive_to_input() {
        let a = blake3_truncated(b"same input");
        let b = blake3_truncated(b"same input");
        let c = blake3_truncated(b"different input");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
