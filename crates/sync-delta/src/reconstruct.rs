//! 文件重建（Receiver 侧）
//!
//! 从 basis file 数据 + DeltaToken 序列拼接出完整的新文件。
//! Match token → 从 basis file 读取对应 block
//! Data token → 使用 Sender 传来的新数据

use crate::DeltaToken;

/// 从 basis file + delta tokens 重建完整文件
///
/// # 参数
/// - `basis_data`: 目标端已有文件的完整内容（basis file）
/// - `tokens`: Sender 生成的 delta token 序列
/// - `block_size`: block 大小（用于定位 basis file 中的 block）
///
/// # 返回
/// 重建后的完整文件内容
pub fn reconstruct(basis_data: &[u8], tokens: &[DeltaToken], block_size: u32) -> Vec<u8> {
    let bs = block_size as usize;
    let mut result = Vec::new();

    for token in tokens {
        match token {
            DeltaToken::Match { block_index } => {
                let offset = *block_index as usize * bs;
                let end = (offset + bs).min(basis_data.len());
                if offset < basis_data.len() {
                    result.extend_from_slice(&basis_data[offset..end]);
                }
            }
            DeltaToken::Data(data) => {
                result.extend_from_slice(data);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::matcher::delta_match;
    use crate::signature::compute_block_signatures;

    /// 辅助函数：完整 delta roundtrip 测试
    fn delta_roundtrip(basis: &[u8], source: &[u8], block_size: u32) -> Vec<u8> {
        let sigs = compute_block_signatures(basis, block_size);
        let tokens = delta_match(source, &sigs, block_size);
        reconstruct(basis, &tokens, block_size)
    }

    #[test]
    fn test_identical_roundtrip() {
        let data = b"hello world, this is a test of delta sync";
        let result = delta_roundtrip(data, data, 10);
        assert_eq!(result, data);
    }

    #[test]
    fn test_append_roundtrip() {
        let basis = b"AAAAABBBBB";
        let source = b"AAAAABBBBBCCCCC";
        let result = delta_roundtrip(basis, source, 5);
        assert_eq!(result, source);
    }

    #[test]
    fn test_insert_middle_roundtrip() {
        let basis = b"AAAAABBBBB";
        let mut source = Vec::from(&b"AAAAA"[..]);
        source.extend_from_slice(b"XXXXX");
        source.extend_from_slice(b"BBBBB");

        let result = delta_roundtrip(basis, &source, 5);
        assert_eq!(result, source);
    }

    #[test]
    fn test_completely_new_roundtrip() {
        let basis = b"old content here";
        let source = b"totally new content";
        let result = delta_roundtrip(basis, source, 5);
        assert_eq!(result, source);
    }

    #[test]
    fn test_empty_source_roundtrip() {
        let basis = b"some data";
        let result = delta_roundtrip(basis, &[], 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_basis_roundtrip() {
        let source = b"brand new file";
        let result = delta_roundtrip(&[], source, 5);
        assert_eq!(result, source);
    }

    #[test]
    fn test_large_file_partial_change() {
        // 模拟大文件中间 2KB 变更
        let mut basis = vec![0xAA; 10000]; // 10KB, all 0xAA
        let mut source = basis.clone();
        // 修改中间 2KB
        for byte in &mut source[4000..6000] {
            *byte = 0xBB;
        }

        let result = delta_roundtrip(&basis, &source, 1000);
        assert_eq!(result, source);

        // 验证 delta 效率：大部分 block 应该 match
        let sigs = compute_block_signatures(&basis, 1000);
        let tokens = delta_match(&source, &sigs, 1000);
        let match_count = tokens.iter().filter(|t| matches!(t, DeltaToken::Match { .. })).count();
        // 10 blocks 中至少 7 个应该 match（中间 2 个变更 + 可能的边界 block）
        assert!(match_count >= 7, "Expected >= 7 matches, got {}", match_count);
    }
}
