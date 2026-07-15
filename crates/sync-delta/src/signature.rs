//! Block 签名计算
//!
//! 对 basis file（目标端已有文件）按 `block_size` 切分，
//! 每个 block 计算 (`rolling_checksum`, `BLAKE3_truncated_16`) 签名对。

use crate::rolling::RollingChecksum;
use crate::{BlockSignature, blake3_truncated};

/// 增量签名状态机：逐块喂入数据，内部只保留 partial-block staging buffer
///
/// 与 `compute_block_signatures(&[u8], _)` 语义等价，但不要求调用方持有整文件切片——
/// 数据可以任意大小的 chunk 分批 `push`，staging buffer 容量恒为 `block_size`（不随
/// `push` 调用次数增长），供 Receiver 端流式驱动 basis file 签名生成使用。
pub struct SignatureCalculator {
    block_size: usize,
    /// partial-block staging buffer，长度恒 <= block_size，攒满一个 block 即 flush
    buffer: Vec<u8>,
    signatures: Vec<BlockSignature>,
}

impl SignatureCalculator {
    /// 创建状态机；`block_size == 0` 视为非法输入，`push` 直接丢弃数据、`finish` 恒
    /// 返回空签名列表（与原整块函数 `bs == 0 → Vec::new()` 语义一致）。
    pub fn new(block_size: u32) -> Self {
        let bs = block_size as usize;
        Self {
            block_size: bs,
            buffer: Vec::with_capacity(bs),
            signatures: Vec::new(),
        }
    }

    /// 逐块喂入数据；staging buffer 攒满 `block_size` 立即计算签名并清空缓冲，
    /// 一次 `push` 可以跨越任意数量的 block 边界。
    pub fn push(&mut self, mut bytes: &[u8]) {
        if self.block_size == 0 {
            return;
        }
        while !bytes.is_empty() {
            let need = self.block_size - self.buffer.len();
            let take = need.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.block_size {
                self.flush_block();
            }
        }
    }

    /// 收尾：若还有未攒满 block_size 的尾部数据（partial last block），计算其签名后
    /// 一并返回全部签名。
    pub fn finish(&mut self) -> Vec<BlockSignature> {
        if !self.buffer.is_empty() {
            self.flush_block();
        }
        std::mem::take(&mut self.signatures)
    }

    fn flush_block(&mut self) {
        let rolling = RollingChecksum::checksum(&self.buffer);
        let strong = blake3_truncated(&self.buffer);
        self.signatures.push(BlockSignature { rolling, strong });
        self.buffer.clear();
    }
}

/// 对 basis file 数据计算所有 block 的签名
///
/// 返回 `Vec<BlockSignature>`，每个元素对应一个 block。
/// 最后一个 block 可能不足 `block_size`（尾部 block）。
///
/// 薄封装：内部 `SignatureCalculator::new` + 单次 `push` + `finish`，保留原 API 供
/// 一次性持有整文件切片的调用方使用。
pub fn compute_block_signatures(data: &[u8], block_size: u32) -> Vec<BlockSignature> {
    let mut calc = SignatureCalculator::new(block_size);
    calc.push(data);
    calc.finish()
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

    /// 逐字段比对两组签名，`BlockSignature` 未 derive `PartialEq`，测试内手工比较
    fn assert_signatures_eq(streamed: &[BlockSignature], whole: &[BlockSignature]) {
        assert_eq!(streamed.len(), whole.len());
        for (a, b) in streamed.iter().zip(whole.iter()) {
            assert_eq!(a.rolling, b.rolling);
            assert_eq!(a.strong, b.strong);
        }
    }

    // ── 跨 chunk 边界等价性测试：push/finish 增量状态机 vs compute_block_signatures
    //    整块函数，任意分片方式下输出必须逐字节等价 ──

    #[test]
    fn test_push_finish_matches_whole_when_chunk_boundary_aligns_with_block_boundary() {
        let data: Vec<u8> = (0..40u8).collect();
        let block_size = 8; // chunk 大小与 block_size 相同，边界完全对齐
        let mut calc = SignatureCalculator::new(block_size);
        for chunk in data.chunks(8) {
            calc.push(chunk);
        }
        assert_signatures_eq(&calc.finish(), &compute_block_signatures(&data, block_size));
    }

    #[test]
    fn test_push_finish_matches_whole_when_chunk_spans_multiple_blocks() {
        let data: Vec<u8> = (0..37u8).collect();
        let block_size = 5; // chunk 大小 12，不与 block_size 对齐，跨越多个 block 边界
        let mut calc = SignatureCalculator::new(block_size);
        for chunk in data.chunks(12) {
            calc.push(chunk);
        }
        assert_signatures_eq(&calc.finish(), &compute_block_signatures(&data, block_size));
    }

    #[test]
    fn test_push_finish_matches_whole_when_chunk_smaller_than_block() {
        let data: Vec<u8> = (0..23u8).collect();
        let block_size = 7; // chunk 大小 1，逐字节喂入，远小于 block_size
        let mut calc = SignatureCalculator::new(block_size);
        for byte in &data {
            calc.push(std::slice::from_ref(byte));
        }
        assert_signatures_eq(&calc.finish(), &compute_block_signatures(&data, block_size));
    }

    #[test]
    fn test_push_finish_many_small_pushes() {
        // 多次小块 push（1000 次 3 字节）累计跨越远超单个 block 的总长度
        let data: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let block_size = 64;
        let mut calc = SignatureCalculator::new(block_size);
        for chunk in data.chunks(3) {
            calc.push(chunk);
        }
        assert_signatures_eq(&calc.finish(), &compute_block_signatures(&data, block_size));
    }

    #[test]
    fn test_push_finish_empty_file() {
        let mut calc = SignatureCalculator::new(1024);
        assert!(calc.finish().is_empty());
    }

    #[test]
    fn test_push_finish_single_byte() {
        let data = [0x42u8];
        let block_size = 10;
        let mut calc = SignatureCalculator::new(block_size);
        calc.push(&data);
        let streamed = calc.finish();
        assert_eq!(streamed.len(), 1);
        assert_signatures_eq(&streamed, &compute_block_signatures(&data, block_size));
    }

    /// 构造性证明：staging buffer 容量恒为 O(block_size)，不随 push 调用次数增长
    #[test]
    fn test_staging_buffer_capacity_bounded_by_block_size_regardless_of_push_count() {
        let block_size: u32 = 16;
        let mut calc = SignatureCalculator::new(block_size);
        for _ in 0..1000 {
            calc.push(&[1u8, 2, 3]);
            assert!(
                calc.buffer.capacity() <= block_size as usize,
                "staging buffer capacity {} exceeded block_size {}",
                calc.buffer.capacity(),
                block_size
            );
        }
        let _ = calc.finish();
    }
}
