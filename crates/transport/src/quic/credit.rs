//! 应用层字节 credit 流控（issue #59，方案 b）
//!
//! ## 要解决的问题
//!
//! `quic::mux` 的 fan-in channel 是无界的（见其模块文档），且 QUIC per-stream 接收窗口
//! 只约束"未被应用读走的字节"——`mux::reader_loop` 读到帧立即转发进无界 channel，从不
//! 在窗口上阻塞，Receiver 侧真正消费（落盘）的速度与 Sender 能发多快完全解耦。Receiver
//! 消费停摆（磁盘慢/挂起）时，Data 类消息（`FileData`/`DeltaData`/`TarPacked`）会在
//! Sender→Receiver 链路上无界积压，最坏情况下堆到数百 MB~GB 级内存（原始故障描述）。
//!
//! ## 设计：字节级 credit 窗口
//!
//! [`CreditWindow`] 用 `tokio::sync::Semaphore` 实现一个全局字节配额：Sender 每发送一条
//! Data 类消息前先按其 payload 字节数 `acquire_many` 等量 permits（不足则挂起），成功后
//! 立即 `forget`（消费，不归还）；Receiver 消费完等量字节后调用 [`CreditWindow::grant`]
//! （内部 `add_permits`）补充。`Semaphore` 的阻塞语义结构性地保证 outstanding（已发未
//! 授信的字节数）不会超过窗口大小，也不可能被并发扣成负数——不需要手搓锁或原子计数器
//! 自己维护上下界。
//!
//! [`DEFAULT_CREDIT_WINDOW_BYTES`] = 64MiB：以 BDP（bandwidth-delay product）为依据，
//! 10 Gbps 链路 × ~50ms 典型 RTT ≈ 62.5MB，凑整到 64MiB，确保窗口本身足够大、不会成为
//! 健康链路下的人为限速瓶颈；同时远大于下游 `dc_tx`（容量 16）+ disk-commit 内层缓冲
//! （容量 8）的量级，保证 credit 窗口才是应用层积压的主控约束，不会被下游更小的缓冲
//! 提前卡住而让 credit 机制形同虚设。v1 不提供配置项（防止 scope creep），窗口大小通过
//! crate-internal 的 `quic::sender::connect_with_credit_window`（仅测试使用）注入。
//!
//! ## 记账不变量与死锁防护
//!
//! `outstanding ∈ [0, window]`（`outstanding` = 已被 Sender 扣减但尚未被 Receiver 授信
//! 归还的字节数）：
//! - 下界（`outstanding >= 0`，即不会被扣成负数）由 `Semaphore::acquire_many` 的阻塞
//!   语义结构性保证——permits 不足时调用方直接挂起，不存在"扣减到负值"的代码路径。
//! - 上界（`outstanding <= window`）由 Receiver 侧的授信金额直接对应其消费字节数保证：
//!   Receiver 单线程顺序处理消息（`recv_file_list_and_data_phase` 是单一消费者循环，
//!   无并发 worker 竞争消费同一份计数），每消费一条 Data 消息就累计其字节数，从不重复
//!   计数（无双计路径），因此 `grant()` 的金额恒等于真实消费量，不会超发。
//!
//! ## 重连即重置语义
//!
//! [`CreditWindow`] 是 `QuicSenderTransport` 的内部字段，随连接对象一起创建/销毁；断线
//! 重连会创建全新的 `QuicSenderTransport`（含全新的 `CreditWindow`，满窗口重新开始）。
//! **不做持久化**——这是有意为之：credit 窗口描述的是"当前这条连接上，Sender 已发但
//! Receiver 尚未确认消费的字节数"，连接断开的瞬间这个状态本身就已经失去意义（在途的
//! 数据要么从未到达 Receiver、要么到达了但 ack 没能传回来），沿用旧窗口状态毫无依据。
//! 后续实现者不要为了"优化重连体验"给这里加持久化——那是解决一个不存在的问题。
//!
//! ## 与 `data_mover::qos::QosManager` 的关系
//!
//! 两者用法形似（`acquire(cost).await` 挂起在调用点、外部补充后自动唤醒），但控制的
//! 物理量不同，不能互相替代：
//! - 控制目标：`QosManager` 管**速率**（bytes/秒）；`CreditWindow` 管**在途量**
//!   （已发但未确认消费的字节数）。
//! - 令牌补充驱动源：`QosManager` 靠**时钟**（固定速率 × 流逝时间，本地自动补充，
//!   不需要对端反馈）；`CreditWindow` 靠**对端消费反馈**（Receiver 消费后显式
//!   `grant()`）。
//! - Receiver 完全停摆时的行为：`QosManager` 照常按时补令牌、照常放行——积压 =
//!   速率 × 停摆时长，仍然无界；`CreditWindow` 授信停止 → 窗口耗尽 → Sender 停发，
//!   积压钉死在窗口值。
//!
//! `QosManager` 回答"我可以发多快"，`CreditWindow` 回答"我可以有多少字节在外面没被
//! 消化"——前者是开环限速，后者是闭环流控，只有后者能解决"Receiver 消费停摆导致
//! 积压无界"的问题（governor 的 API 本身也没有"由外部事件补充令牌"的模式，硬套上去
//! 等于把时钟补充关掉、只剩计数器语义，那就是 `Semaphore` 本身，不如直接用
//! `Semaphore`）。两者正交、继续共存：`QosManager` 管"别把网络/源端打满"（Sender 读端，
//! 不受本 issue 影响），`CreditWindow` 管"别把 Receiver 内存打爆"（Sender 发送端，本
//! 模块新增）。

// 外部 crate
use tokio::sync::Semaphore;

// 内部模块
use crate::error::{Result, TransportError};

/// 默认 credit 窗口：64 MiB（BDP 依据见模块文档）
pub const DEFAULT_CREDIT_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

/// `Semaphore::acquire_many` 单次调用的 permits 数受限于 `u32`；单条 Data 类消息的字节数
/// 理论上可能超过 `u32::MAX`（如巨大的 `TarPacked` 打包文件），超出部分拆成多次
/// `acquire_many` 顺序申请，避免 `u64 -> u32` 转换截断。
const MAX_ACQUIRE_CHUNK_BYTES: u64 = u32::MAX as u64;

/// 字节级 credit 流控窗口（issue #59 方案 b），语义与死锁防护见模块文档
pub(crate) struct CreditWindow {
    semaphore: Semaphore,
}

impl CreditWindow {
    /// 用指定窗口大小构造；生产路径固定使用 [`DEFAULT_CREDIT_WINDOW_BYTES`]
    /// （见 `quic::sender::connect`），测试注入更小的窗口以触发阻塞/授信路径。
    pub(crate) fn new(window_bytes: u64) -> Self {
        let permits = usize::try_from(window_bytes).unwrap_or(usize::MAX);
        Self {
            semaphore: Semaphore::new(permits),
        }
    }

    /// 扣减 `bytes` credit：窗口内余量不足时挂起，直到 [`Self::grant`] 补充足够的 permits
    /// 后自动被唤醒——`Semaphore` 的阻塞语义结构性地保证 outstanding 不会被扣成负数，
    /// 不需要额外的原子计数器自己维护下界。
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<()> {
        let mut remaining = bytes;
        while remaining > 0 {
            let chunk = remaining.min(MAX_ACQUIRE_CHUNK_BYTES);
            // chunk <= MAX_ACQUIRE_CHUNK_BYTES <= u32::MAX，转换不会截断
            let n = u32::try_from(chunk).unwrap_or(u32::MAX);
            let permit = self
                .semaphore
                .acquire_many(n)
                .await
                .map_err(|e| TransportError::SendFailed(format!("credit window closed: {e}")))?;
            // 消费掉这批 permits，不归还——归还只能通过 grant() 显式补充
            permit.forget();
            remaining -= chunk;
        }
        Ok(())
    }

    /// 补授 `bytes` credit（Receiver 半窗批量授信后由 `QuicSenderTransport::recv()` 调用）
    pub(crate) fn grant(&self, bytes: u64) {
        let n = usize::try_from(bytes).unwrap_or(usize::MAX);
        self.semaphore.add_permits(n);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// 窗口耗尽后 `acquire()` 应挂起，直到 `grant()` 补充足够 permits 才被唤醒——这是
    /// credit 机制"死锁防护"的直接证据：不存在能让 outstanding 突破窗口上限的代码路径。
    #[tokio::test]
    async fn acquire_blocks_when_window_exhausted_and_unblocks_after_grant() {
        let window = Arc::new(CreditWindow::new(10));

        // 耗尽全部 10 字节窗口，应立即成功（首次 acquire 不应阻塞）
        window.acquire(10).await.unwrap();

        // 窗口已空，再申请 5 字节应该挂起
        let w = window.clone();
        let handle = tokio::spawn(async move { w.acquire(5).await });

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !handle.is_finished(),
            "窗口耗尽时 acquire() 应挂起（前提不成立，测试无意义）"
        );

        // 补授 5 字节，应尽快解除阻塞
        window.grant(5);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("grant() 后 acquire() 应尽快被唤醒完成")
            .unwrap()
            .unwrap();
    }
}
