//! Rolling checksum（Adler32 变体）
//!
//! O(1) 滑动窗口更新：窗口每移动一个字节，只需减去离开的字节、加上进入的字节。
//! 用于 Sender 侧滑动窗口扫描源文件，快速初筛是否匹配某个 basis block。

/// Adler32 变体的 rolling checksum
///
/// 与 rsync 一致的算法：
/// - `a` = sum of bytes mod M
/// - `b` = sum of (a values) mod M
/// - digest = (b << 16) | a
pub struct RollingChecksum {
    a: u32,
    b: u32,
}

const MODULUS: u32 = 65521; // 最大素数 < 2^16

impl RollingChecksum {
    pub fn new() -> Self {
        Self { a: 0, b: 0 }
    }

    /// 对一段数据计算初始 checksum
    pub fn checksum(data: &[u8]) -> u32 {
        let (a, b) = Self::sums(data);
        (b << 16) | a
    }

    /// 初始化窗口。
    pub fn init(&mut self, data: &[u8]) {
        (self.a, self.b) = Self::sums(data);
    }

    /// 5552 个最大字节累加也不溢出 u32，按批取模保持原 checksum 不变。
    fn sums(data: &[u8]) -> (u32, u32) {
        let (mut a, mut b) = (0u32, 0u32);
        for chunk in data.chunks(5552) {
            for &byte in chunk {
                a += u32::from(byte);
                b += a;
            }
            a %= MODULUS;
            b %= MODULUS;
        }
        (a, b)
    }

    /// O(1) 滑动更新：移除 `old_byte`，加入 `new_byte`
    ///
    /// 推导（`window_size` = n）：
    /// ```text
    /// a(s)   = Σ data[s+i]          for i=0..n-1
    /// b(s)   = Σ (n-i) * data[s+i]  for i=0..n-1
    /// a(s+1) = a(s) - old + new
    /// b(s+1) = b(s) - n * old + a(s+1)
    /// ```
    pub fn update(&mut self, old_byte: u8, new_byte: u8, window_size: u32) {
        let old = u64::from(old_byte);
        let new = u64::from(new_byte);
        let m = u64::from(MODULUS);
        let n = u64::from(window_size);
        let a = u64::from(self.a);
        let b = u64::from(self.b);

        let a_new = (a + m + new - old) % m;
        let n_old = (n % m) * (old % m) % m; // n * old mod m，防溢出
        let b_new = (b + m + a_new - n_old) % m;

        self.a = a_new as u32;
        self.b = b_new as u32;
    }

    /// 获取当前 digest
    pub fn digest(&self) -> u32 {
        (self.b << 16) | self.a
    }
}

impl Default for RollingChecksum {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_basic() {
        let data = b"hello world";
        let cs = RollingChecksum::checksum(data);
        assert_ne!(cs, 0);
    }

    #[test]
    fn test_checksum_deterministic() {
        let data = b"test data for checksum";
        assert_eq!(RollingChecksum::checksum(data), RollingChecksum::checksum(data));
    }

    #[test]
    fn test_rolling_matches_full() {
        let data = b"abcdefgh";
        let full = RollingChecksum::checksum(data);

        let mut rc = RollingChecksum::new();
        rc.init(data);
        assert_eq!(rc.digest(), full);
    }

    #[test]
    fn test_sliding_window() {
        // 窗口 [a,b,c,d] → slide → [b,c,d,e]
        let data1 = b"abcd";
        let data2 = b"bcde";

        let expected = RollingChecksum::checksum(data2);

        let mut rc = RollingChecksum::new();
        rc.init(data1);
        rc.update(b'a', b'e', 4);

        assert_eq!(rc.digest(), expected);
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    #[test]
    fn batched_sums_equal_the_original_algorithm_at_overflow_boundaries() {
        for size in [0, 1, 5551, 5552, 5553, 11104, 128 * 1024 + 13] {
            for pattern in [0, 1, 127, 255] {
                let bytes = vec![pattern; size];
                let (mut a, mut b) = (0, 0);
                for &byte in &bytes {
                    a = (a + u32::from(byte)) % MODULUS;
                    b = (b + a) % MODULUS;
                }
                assert_eq!(RollingChecksum::checksum(&bytes), (b << 16) | a);
            }
        }
    }
}
