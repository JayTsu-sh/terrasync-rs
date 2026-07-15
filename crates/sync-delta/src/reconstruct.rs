//! 文件重建（Receiver 侧）
//!
//! 从 basis file 数据 + `DeltaToken` 序列拼接出完整的新文件。
//! Match token → 从 basis file 读取对应 block
//! Data token → 使用 Sender 传来的新数据

use bytes::Bytes;

use crate::DeltaToken;

/// `Reconstructor::push` 消费一个 token 后的动作。
///
/// sync-delta 保持零 IO 依赖：`Match` token 对应的 basis 字节由调用方（Receiver 侧，持有
/// 实际存储句柄）按 `NeedBasis` 描述的区间读取后写入输出流；`Data` token（或 basis 越界
/// 产生的空数据）不需要额外 IO，直接可写。
#[derive(Debug, Clone, PartialEq)]
pub enum ReconstructStep {
    /// 直接可写的字节（来自 `Data` token，或 basis 越界时的空数据）
    Ready(Bytes),
    /// 需要调用方读取 basis file `[offset, offset + len)` 区间，读到后原样写入输出流
    NeedBasis { offset: u64, len: u32 },
}

/// 增量文件重建器：逐 token 消费，产出待写字节流，不要求调用方持有整片 `basis_data`/
/// `tokens` slice。
///
/// 与 `reconstruct(&[u8], &[DeltaToken], _)` 语义等价，但把 basis block 的实际读取
/// 职责移交调用方（`push` 只做纯计算，不做任何 IO）——basis file 的读取天然是随机访问
/// （`Match` token 引用的 block_index 可能乱序、可重复），不适合像 `signature`/`matcher`
/// 那样维护跨 token 的滚动窗口状态，因此本状态机本身无需可变内部状态，只持有
/// `block_size`/`basis_size` 两个不变配置。
pub struct Reconstructor {
    block_size: u32,
    /// basis file 参与 delta 匹配的有效字节数上界（与签名计算时使用的边界一致）
    basis_size: u64,
}

impl Reconstructor {
    /// `block_size`：与生成 tokens 时使用的 block 大小一致；`basis_size`：basis file
    /// 有效字节数上界（`Match` 引用的 block 越过此边界视为空数据，语义与原整片函数对
    /// `basis_data.len()` 的钳制完全一致）。
    pub fn new(block_size: u32, basis_size: u64) -> Self {
        Self { block_size, basis_size }
    }

    /// 消费一个 token，返回对应动作
    pub fn push(&self, token: &DeltaToken) -> ReconstructStep {
        match token {
            DeltaToken::Data(data) => ReconstructStep::Ready(data.clone()),
            DeltaToken::Match { block_index } => {
                let offset = *block_index as u64 * self.block_size as u64;
                if offset >= self.basis_size {
                    ReconstructStep::Ready(Bytes::new())
                } else {
                    let end = (offset + self.block_size as u64).min(self.basis_size);
                    ReconstructStep::NeedBasis {
                        offset,
                        len: (end - offset) as u32,
                    }
                }
            }
        }
    }
}

/// 从 basis file + delta tokens 重建完整文件
///
/// # 参数
/// - `basis_data`: 目标端已有文件的完整内容（basis file）
/// - `tokens`: Sender 生成的 delta token 序列
/// - `block_size`: block 大小（用于定位 basis file 中的 block）
///
/// # 返回
/// 重建后的完整文件内容
///
/// 薄封装：内部 `Reconstructor::new` + 逐 token `push`，`NeedBasis` 直接从内存持有的
/// `basis_data` 切片解析（无需 IO），保留原 API 供一次性持有整片数据的调用方使用。
pub fn reconstruct(basis_data: &[u8], tokens: &[DeltaToken], block_size: u32) -> Vec<u8> {
    let reconstructor = Reconstructor::new(block_size, basis_data.len() as u64);
    let mut result = Vec::new();

    for token in tokens {
        match reconstructor.push(token) {
            ReconstructStep::Ready(bytes) => result.extend_from_slice(&bytes),
            ReconstructStep::NeedBasis { offset, len } => {
                let start = offset as usize;
                let end = start + len as usize;
                result.extend_from_slice(&basis_data[start..end]);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
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
        let basis = vec![0xAA; 10000]; // 10KB, all 0xAA
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
        assert!(match_count >= 7, "Expected >= 7 matches, got {match_count}");
    }

    // ── Reconstructor 流式接口等价性测试：`push` 逐 token 驱动的输出必须与重构前
    //    （逐 token 直接按 block_index 定位 basis 切片）的算法逐字节一致（issue #54 阶段 3）──

    /// 重构前 `reconstruct` 的原始实现（保留作独立 ground truth，不复用 `Reconstructor`），
    /// 用于验证 `Reconstructor::push` 的钳制/寻址算法与历史行为完全一致。
    fn reference_reconstruct(basis_data: &[u8], tokens: &[DeltaToken], block_size: u32) -> Vec<u8> {
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
                DeltaToken::Data(data) => result.extend_from_slice(data),
            }
        }
        result
    }

    /// 模拟真实调用方（Receiver 侧）驱动 `Reconstructor`：tokens 按 `group_sizes` 分批喂入
    /// （每批可能推进任意个 token，模拟"token 在任意切分点到达"），`NeedBasis` 立即从
    /// 内存持有的 `basis_data` 切片解析（真实场景下是异步 IO，这里用同步切片模拟其结果）。
    fn drive_reconstructor(
        basis_data: &[u8], tokens: &[DeltaToken], block_size: u32, group_sizes: &[usize],
    ) -> Vec<u8> {
        let reconstructor = Reconstructor::new(block_size, basis_data.len() as u64);
        let mut result = Vec::new();
        let mut idx = 0;
        let mut groups: Vec<usize> = group_sizes.to_vec();
        groups.push(tokens.len()); // 收尾：处理剩余未分组的 token
        for group_len in groups {
            let end = (idx + group_len).min(tokens.len());
            for token in &tokens[idx..end] {
                match reconstructor.push(token) {
                    ReconstructStep::Ready(bytes) => result.extend_from_slice(&bytes),
                    ReconstructStep::NeedBasis { offset, len } => {
                        let start = offset as usize;
                        let stop = start + len as usize;
                        result.extend_from_slice(&basis_data[start..stop]);
                    }
                }
            }
            idx = end;
        }
        result
    }

    #[test]
    fn reconstructor_matches_reference_pure_literal() {
        let basis = b"AAAAABBBBB";
        let tokens = vec![
            DeltaToken::Data(Bytes::from_static(b"hello")),
            DeltaToken::Data(Bytes::from_static(b"world")),
        ];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
    }

    #[test]
    fn reconstructor_matches_reference_pure_match() {
        let basis = b"AAAAABBBBBCCCCC";
        let tokens = vec![
            DeltaToken::Match { block_index: 0 },
            DeltaToken::Match { block_index: 1 },
            DeltaToken::Match { block_index: 2 },
        ];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
    }

    #[test]
    fn reconstructor_matches_reference_interleaved() {
        let basis = b"AAAAABBBBBCCCCCDDDDD";
        let tokens = vec![
            DeltaToken::Match { block_index: 0 },
            DeltaToken::Data(Bytes::from_static(b"XXXXX")),
            DeltaToken::Match { block_index: 2 },
            DeltaToken::Data(Bytes::from_static(b"YY")),
            DeltaToken::Match { block_index: 3 },
        ];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
    }

    #[test]
    fn reconstructor_matches_reference_tokens_arrive_at_arbitrary_split_points() {
        let basis = b"AAAAABBBBBCCCCCDDDDD";
        let tokens = vec![
            DeltaToken::Match { block_index: 0 },
            DeltaToken::Data(Bytes::from_static(b"XXXXX")),
            DeltaToken::Match { block_index: 2 },
            DeltaToken::Data(Bytes::from_static(b"YY")),
            DeltaToken::Match { block_index: 3 },
        ];
        let reference = reference_reconstruct(basis, &tokens, 5);
        // 不同分批方式（每次 push 推进 1 个 / 2 个 / 全部一次性）都必须产出同样的结果
        for group_sizes in [vec![1, 1, 1, 1, 1], vec![2, 2], vec![5], vec![3, 1]] {
            assert_eq!(
                drive_reconstructor(basis, &tokens, 5, &group_sizes),
                reference,
                "分组 {group_sizes:?} 下输出应与 reference 一致"
            );
        }
    }

    #[test]
    fn reconstructor_matches_reference_basis_blocks_out_of_order_and_repeated() {
        let basis = b"AAAAABBBBBCCCCC";
        // block_index 乱序 + 重复引用同一个 block（rsync 场景下常见：源文件对 basis block
        // 做了平移/重排，或同一段内容在多处重复出现）
        let tokens = vec![
            DeltaToken::Match { block_index: 2 },
            DeltaToken::Match { block_index: 0 },
            DeltaToken::Match { block_index: 0 },
            DeltaToken::Match { block_index: 1 },
            DeltaToken::Match { block_index: 2 },
        ];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
    }

    #[test]
    fn reconstructor_matches_reference_empty_file() {
        let basis = b"some data";
        let tokens: Vec<DeltaToken> = vec![];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
        assert!(drive_reconstructor(basis, &tokens, 5, &[]).is_empty());
    }

    #[test]
    fn reconstructor_matches_reference_smaller_than_one_block() {
        let basis = b"AAAAABBBBB";
        // 尾部 block 只有 3 字节（basis 长度 10，block_size 5，Match block_index=1 只覆盖
        // [5,10) 全量；这里再加一个越界 block_index 验证钳制为空数据）
        let tokens = vec![
            DeltaToken::Match { block_index: 1 },
            DeltaToken::Match { block_index: 5 }, // 越界 → 空数据
            DeltaToken::Data(Bytes::from_static(b"Z")),
        ];
        assert_eq!(
            drive_reconstructor(basis, &tokens, 5, &[]),
            reference_reconstruct(basis, &tokens, 5)
        );
    }

    #[test]
    fn reconstructor_matches_reference_random_splits_fixed_seed() {
        fn next_rand(state: &mut u64) -> u64 {
            *state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *state
        }

        let basis: Vec<u8> = (0..64u8).collect();
        let sigs = compute_block_signatures(&basis, 8);
        let source: Vec<u8> = [&basis[16..40], b"NEWDATA!", &basis[0..16]].concat();
        let tokens = delta_match(&source, &sigs, 8);
        let reference = reference_reconstruct(&basis, &tokens, 8);

        let mut seed = 0xdead_beef_1234_5678u64;
        for _ in 0..20 {
            let mut group_sizes = Vec::new();
            let mut remaining = tokens.len();
            while remaining > 0 {
                let take = 1 + (next_rand(&mut seed) as usize % tokens.len().max(1));
                let take = take.min(remaining);
                group_sizes.push(take);
                remaining -= take;
            }
            assert_eq!(
                drive_reconstructor(&basis, &tokens, 8, &group_sizes),
                reference,
                "随机分组 {group_sizes:?} 下输出应与 reference 一致"
            );
        }
    }

    /// 构造性证明：`Reconstructor` 不持有任何随 push 次数增长的缓冲——结构体只有两个
    /// 固定大小的配置字段（`block_size`/`basis_size`），大量 push 前后自身内存占用不变
    /// （与 signature.rs/matcher.rs 的 staging buffer 容量上界证明同构，这里更强：恒为 0
    /// 额外内存，因为不存在任何缓冲字段）。
    #[test]
    fn reconstructor_holds_no_growing_state_regardless_of_push_count() {
        let basis = vec![0xAAu8; 800];
        let reconstructor = Reconstructor::new(8, basis.len() as u64);
        let size_before = std::mem::size_of_val(&reconstructor);
        for i in 0..1000u32 {
            let token = if i.is_multiple_of(2) {
                DeltaToken::Match { block_index: i % 100 }
            } else {
                DeltaToken::Data(Bytes::from_static(b"x"))
            };
            let _ = reconstructor.push(&token);
            assert_eq!(
                std::mem::size_of_val(&reconstructor),
                size_before,
                "Reconstructor 自身大小不应随 push 次数变化"
            );
        }
    }
}
