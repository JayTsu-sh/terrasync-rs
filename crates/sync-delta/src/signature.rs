//! Block 签名计算
//!
//! 对 basis file（目标端已有文件）按 `block_size` 切分，
//! 每个 block 计算 (`rolling_checksum`, `BLAKE3_truncated_16`) 签名对。

use crate::BlockSignature;
use crate::rolling::RollingChecksum;

/// 计算 BLAKE3 并截断为 16 字节
fn blake3_truncated_16(data: &[u8]) -> [u8; 16] {
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes();
    let mut result = [0u8; 16];
    result.copy_from_slice(&bytes[..16]);
    result
}

/// 对 basis file 数据计算所有 block 的签名
///
/// 返回 `Vec<BlockSignature>`，每个元素对应一个 block。
/// 最后一个 block 可能不足 `block_size`（尾部 block）。
pub fn compute_block_signatures(data: &[u8], block_size: u32) -> Vec<BlockSignature> {
    let bs = block_size as usize;
    if data.is_empty() || bs == 0 {
        return Vec::new();
    }

    let mut signatures = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        let end = (offset + bs).min(data.len());
        let block = &data[offset..end];

        let rolling = RollingChecksum::checksum(block);
        let strong = blake3_truncated_16(block);

        signatures.push(BlockSignature { rolling, strong });
        offset = end;
    }

    signatures
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_data() {
        let sigs = compute_block_signatures(&[], 1024);
        assert!(sigs.is_empty());
    }

    #[test]
    fn test_single_block() {
        let data = b"hello world, this is a test block";
        let sigs = compute_block_signatures(data, 1024);
        assert_eq!(sigs.len(), 1);
        assert_ne!(sigs[0].rolling, 0);
        assert_ne!(sigs[0].strong, [0u8; 16]);
    }

    #[test]
    fn test_multiple_blocks() {
        let data = vec![0u8; 3000];
        let sigs = compute_block_signatures(&data, 1000);
        assert_eq!(sigs.len(), 3);
    }

    #[test]
    fn test_partial_last_block() {
        let data = vec![0u8; 2500];
        let sigs = compute_block_signatures(&data, 1000);
        assert_eq!(sigs.len(), 3); // 1000 + 1000 + 500
    }

    #[test]
    fn test_deterministic() {
        let data = b"deterministic test data here";
        let s1 = compute_block_signatures(data, 10);
        let s2 = compute_block_signatures(data, 10);
        assert_eq!(s1.len(), s2.len());
        for (a, b) in s1.iter().zip(s2.iter()) {
            assert_eq!(a.rolling, b.rolling);
            assert_eq!(a.strong, b.strong);
        }
    }
}
