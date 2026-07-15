//! 滑动窗口匹配（Sender 侧）
//!
//! 对源文件使用滑动窗口扫描，与 Receiver 发来的 basis file block 签名比较。
//! 匹配到的 block 生成 `DeltaToken::Match`，未匹配的数据生成 `DeltaToken::Data`。
//! 结果：只传输差异部分，matched blocks 由 Receiver 从本地 basis file 读取。

use std::collections::HashMap;

use bytes::Bytes;

use crate::rolling::RollingChecksum;
use crate::{BlockSignature, DeltaToken, blake3_truncated};

/// 构建 rolling checksum → block 索引的查找表
fn build_sig_table(signatures: &[BlockSignature]) -> HashMap<u32, Vec<(u32, &BlockSignature)>> {
    let mut table: HashMap<u32, Vec<(u32, &BlockSignature)>> = HashMap::new();
    for (idx, sig) in signatures.iter().enumerate() {
        table.entry(sig.rolling).or_default().push((idx as u32, sig));
    }
    table
}

/// 对源文件执行 delta 匹配
///
/// 滑动窗口扫描 `src_data`，与 `signatures`（basis file 的 block 签名）比较。
/// 返回 `Vec<DeltaToken>`：
/// - `Match { block_index }` — 源文件这段数据 == basis file 的某个 block
/// - `Data(bytes)` — 源文件的新数据，需要传输
pub fn delta_match(src_data: &[u8], signatures: &[BlockSignature], block_size: u32) -> Vec<DeltaToken> {
    let bs = block_size as usize;

    // 特殊情况
    if src_data.is_empty() {
        return Vec::new();
    }
    if signatures.is_empty() || bs == 0 {
        // 没有 basis 签名 → 全量 Data
        return vec![DeltaToken::Data(Bytes::copy_from_slice(src_data))];
    }

    let sig_table = build_sig_table(signatures);
    let mut tokens = Vec::new();
    let mut literal_buf: Vec<u8> = Vec::new();
    let mut pos = 0;

    // 初始化 rolling checksum 窗口
    let mut rc = RollingChecksum::new();
    if src_data.len() >= bs {
        rc.init(&src_data[..bs]);
    }

    while pos + bs <= src_data.len() {
        let rolling = rc.digest();

        // 快速初筛：rolling checksum 匹配？
        let mut matched = false;
        if let Some(candidates) = sig_table.get(&rolling) {
            // 慢速确认：strong checksum (BLAKE3) 匹配？
            let strong = blake3_truncated(&src_data[pos..pos + bs]);
            for (block_idx, sig) in candidates {
                if sig.strong == strong {
                    // 匹配！先 flush 累积的 literal data
                    if !literal_buf.is_empty() {
                        tokens.push(DeltaToken::Data(Bytes::from(std::mem::take(&mut literal_buf))));
                    }
                    tokens.push(DeltaToken::Match {
                        block_index: *block_idx,
                    });
                    pos += bs;
                    matched = true;

                    // 重新初始化下一个窗口
                    if pos + bs <= src_data.len() {
                        rc = RollingChecksum::new();
                        rc.init(&src_data[pos..pos + bs]);
                    }
                    break;
                }
            }
        }

        if !matched {
            // 无匹配 → 累积当前字节为 literal
            literal_buf.push(src_data[pos]);
            pos += 1;

            // 滑动窗口更新
            if pos + bs <= src_data.len() {
                rc.update(src_data[pos - 1], src_data[pos + bs - 1], block_size);
            }
        }
    }

    // 尾部不足一个 block 的数据
    if pos < src_data.len() {
        literal_buf.extend_from_slice(&src_data[pos..]);
    }

    // flush 剩余 literal
    if !literal_buf.is_empty() {
        tokens.push(DeltaToken::Data(Bytes::from(literal_buf)));
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signature::compute_block_signatures;

    #[test]
    fn test_identical_files() {
        let data = b"hello world, this is identical data for testing";
        let sigs = compute_block_signatures(data, 10);
        let tokens = delta_match(data, &sigs, 10);

        // 完全相同 → 全部 Match，无 Data
        let match_count = tokens.iter().filter(|t| matches!(t, DeltaToken::Match { .. })).count();
        let data_count = tokens.iter().filter(|t| matches!(t, DeltaToken::Data(_))).count();

        assert!(match_count > 0, "Should have matches");
        // 可能有尾部不足 block_size 的 Data
        assert!(data_count <= 1, "At most 1 trailing Data token");
    }

    #[test]
    fn test_completely_different() {
        let basis = b"aaaaaaaaaa"; // 10 bytes
        let source = b"bbbbbbbbbb"; // 10 bytes, completely different
        let sigs = compute_block_signatures(basis, 5);
        let tokens = delta_match(source, &sigs, 5);

        // 完全不同 → 全部 Data
        for token in &tokens {
            assert!(
                matches!(token, DeltaToken::Data(_)),
                "Should be all Data, got {token:?}",
            );
        }
    }

    #[test]
    fn test_append_at_end() {
        let basis = b"AAAAABBBBB"; // 10 bytes
        let source = b"AAAAABBBBBCCCCC"; // 15 bytes, 5 appended

        let sigs = compute_block_signatures(basis, 5);
        let tokens = delta_match(source, &sigs, 5);

        // 前 10 bytes 匹配 → 2 Match，后 5 bytes 新增 → 1 Data
        let matches: Vec<_> = tokens
            .iter()
            .filter(|t| matches!(t, DeltaToken::Match { .. }))
            .collect();
        let datas: Vec<_> = tokens.iter().filter(|t| matches!(t, DeltaToken::Data(_))).collect();

        assert_eq!(matches.len(), 2, "2 blocks should match");
        assert_eq!(datas.len(), 1, "1 trailing Data");
    }

    #[test]
    fn test_insert_in_middle() {
        let basis = b"AAAAABBBBB"; // 2 blocks of 5
        let mut source = Vec::from(&b"AAAAA"[..]);
        source.extend_from_slice(b"XXXXX"); // 插入 5 bytes
        source.extend_from_slice(b"BBBBB");

        let sigs = compute_block_signatures(basis, 5);
        let tokens = delta_match(&source, &sigs, 5);

        // AAAAA → Match, XXXXX → Data, BBBBB → Match
        assert_eq!(tokens.len(), 3);
        assert!(matches!(tokens[0], DeltaToken::Match { block_index: 0 }));
        assert!(matches!(tokens[1], DeltaToken::Data(_)));
        assert!(matches!(tokens[2], DeltaToken::Match { block_index: 1 }));
    }

    #[test]
    fn test_empty_source() {
        let basis = b"some data";
        let sigs = compute_block_signatures(basis, 5);
        let tokens = delta_match(&[], &sigs, 5);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_empty_basis() {
        let source = b"new file content";
        let tokens = delta_match(source, &[], 5);
        assert_eq!(tokens.len(), 1);
        assert!(matches!(tokens[0], DeltaToken::Data(_)));
    }

    // ── DeltaMatcher 跨 push 边界等价性测试：任意切分方式下流式输出必须与整片
    //    delta_match 逐项相等（issue #54 阶段 2）──

    /// 按 `boundaries` 切分 `data` 依次 push 给 `DeltaMatcher`，断言拼接后的 token 序列
    /// 与整片 `delta_match(data, sigs, block_size)` 完全一致。
    fn assert_stream_matches_whole(data: &[u8], sigs: &[BlockSignature], block_size: u32, boundaries: &[usize]) {
        let whole = delta_match(data, sigs, block_size);

        let mut matcher = DeltaMatcher::new(sigs, block_size);
        let mut streamed = Vec::new();
        let mut start = 0;
        for &end in boundaries {
            streamed.extend(matcher.push(&data[start..end]));
            start = end;
        }
        streamed.extend(matcher.push(&data[start..]));
        streamed.extend(matcher.finish());

        assert_eq!(
            streamed, whole,
            "流式输出应与整片 delta_match 完全一致（boundaries={boundaries:?}）"
        );
    }

    /// 构造一个真实感的 basis/source 对：block0 命中、5 字节非对齐插入字面量、
    /// block1/block2 命中、尾部整块替换为字面量——覆盖 match/literal 混合场景。
    fn build_mixed_case() -> (Vec<u8>, Vec<u8>, u32) {
        let block_size = 8u32;
        let basis = [&b"AAAAAAAA"[..], &b"BBBBBBBB"[..], &b"CCCCCCCC"[..], &b"DDDDDDDD"[..]].concat();
        let source = [
            &b"AAAAAAAA"[..],
            &b"XXXXX"[..],
            &b"BBBBBBBB"[..],
            &b"CCCCCCCC"[..],
            &b"YYYYYYYY"[..],
        ]
        .concat();
        (basis, source, block_size)
    }

    #[test]
    fn test_push_finish_matches_whole_split_at_token_boundary() {
        let (basis, source, block_size) = build_mixed_case();
        let sigs = compute_block_signatures(&basis, block_size);
        // 恰好切在 Match → Data（8）与 Data → Match（13）的 token 边界上
        assert_stream_matches_whole(&source, &sigs, block_size, &[8, 13, 21, 29]);
    }

    #[test]
    fn test_push_finish_matches_whole_split_inside_window() {
        let (basis, source, block_size) = build_mixed_case();
        let sigs = compute_block_signatures(&basis, block_size);
        // 在滑动窗口/block 中间切分，不与任何 token 边界对齐
        assert_stream_matches_whole(&source, &sigs, block_size, &[3, 11, 17, 25, 33]);
    }

    #[test]
    fn test_push_finish_matches_whole_one_byte_at_a_time() {
        let (basis, source, block_size) = build_mixed_case();
        let sigs = compute_block_signatures(&basis, block_size);
        let boundaries: Vec<usize> = (1..source.len()).collect();
        assert_stream_matches_whole(&source, &sigs, block_size, &boundaries);
    }

    #[test]
    fn test_push_finish_matches_whole_insert_in_middle() {
        let basis = b"AAAAABBBBB"; // 2 blocks of 5
        let mut source = Vec::from(&b"AAAAA"[..]);
        source.extend_from_slice(b"XXXXX"); // 插入 5 bytes
        source.extend_from_slice(b"BBBBB");
        let block_size = 5u32;
        let sigs = compute_block_signatures(basis, block_size);
        assert_stream_matches_whole(&source, &sigs, block_size, &[2, 7, 12]);
    }

    #[test]
    fn test_push_finish_matches_whole_delete_in_middle() {
        // basis 3 blocks，source 删除中间 block → 平移后 Match(0) + Match(2)
        let basis = b"AAAAABBBBBCCCCC";
        let source = b"AAAAACCCCC";
        let block_size = 5u32;
        let sigs = compute_block_signatures(basis, block_size);
        assert_stream_matches_whole(source, &sigs, block_size, &[3, 7]);
    }

    #[test]
    fn test_push_finish_matches_whole_empty_source() {
        let sigs = compute_block_signatures(b"some data", 5);
        assert_stream_matches_whole(&[], &sigs, 5, &[]);
    }

    #[test]
    fn test_push_finish_matches_whole_smaller_than_one_block() {
        let sigs = compute_block_signatures(b"AAAAABBBBB", 5);
        assert_stream_matches_whole(b"AAA", &sigs, 5, &[1, 2]);
    }

    #[test]
    fn test_push_finish_matches_whole_empty_basis() {
        let source = b"new file content";
        assert_stream_matches_whole(source, &[], 5, &[3, 9]);
    }

    /// property 式随机切分对拍（固定种子手写 LCG，避免引入 rand 依赖，与阶段 1 保持同一
    /// "无外部依赖" 风格）：多组数据/block_size 组合下，任意切分方式的流式输出都必须与
    /// 整片 delta_match 一致。
    #[test]
    fn test_push_finish_matches_whole_random_splits_fixed_seed() {
        fn next_rand(state: &mut u64) -> u64 {
            *state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *state
        }

        let cases: Vec<(Vec<u8>, Vec<u8>, u32)> = vec![
            build_mixed_case(),
            (b"AAAAABBBBBCCCCC".to_vec(), b"AAAAACCCCC".to_vec(), 5),
            {
                let basis: Vec<u8> = (0..64u8).collect();
                let source: Vec<u8> = (0..64u8).rev().collect(); // 完全不同
                (basis, source, 8)
            },
        ];

        let mut seed = 0x1234_5678_9abc_def0u64;
        for (basis, source, block_size) in cases {
            let sigs = compute_block_signatures(&basis, block_size);
            let whole = delta_match(&source, &sigs, block_size);

            for _ in 0..20 {
                let mut boundaries: Vec<usize> = Vec::new();
                let mut pos = 0usize;
                while pos < source.len() {
                    let step = 1 + (next_rand(&mut seed) as usize % 6);
                    pos += step;
                    if pos < source.len() {
                        boundaries.push(pos);
                    }
                }

                let mut matcher = DeltaMatcher::new(&sigs, block_size);
                let mut streamed = Vec::new();
                let mut start = 0;
                for &end in &boundaries {
                    streamed.extend(matcher.push(&source[start..end]));
                    start = end;
                }
                streamed.extend(matcher.push(&source[start..]));
                streamed.extend(matcher.finish());

                assert_eq!(
                    streamed,
                    whole,
                    "随机切分 {boundaries:?} 后流式输出应与整片一致（basis len={}, source len={}, bs={block_size}）",
                    basis.len(),
                    source.len()
                );
            }
        }
    }
}
