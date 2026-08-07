//! 传输无关的应用层字节 credit 流控（issue #59，方案 b）
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
//! [`SenderCreditState`] 用一个互斥账本和 `Notify` 实现全局字节配额：Sender 每发送一条 Data
//! 类消息前按 payload 字节数原子扣减（不足则挂起）；Receiver 消费后调用
//! [`SenderCreditState::grant`] 补充。acquire 与 grant 共用同一个线性化临界区，因此
//! outstanding 不会超过窗口，也不存在互相漂移的镜像账本。
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
//! - 下界（`outstanding >= 0`）由账本临界区中的先检查后扣减保证；余额不足时调用方挂起。
//! - 上界（`outstanding <= window`）由两端共同结构性保证：Receiver 只在 bounded
//!   disk-commit seam 接受 payload 后累计授信；Sender 在同一账本中把重复、恶意或竞态产生
//!   的超额 grant 截断到配置窗口。
//!
//! ## 重连即重置语义
//!
//! [`SenderCreditState`] 是 `QuicSenderTransport` 的内部字段，随连接对象一起创建/销毁；断线
//! 重连会创建全新的 `QuicSenderTransport`（含全新的 `SenderCreditState`，满窗口重新开始）。
//! **不做持久化**——这是有意为之：credit 窗口描述的是"当前这条连接上，Sender 已发但
//! Receiver 尚未确认消费的字节数"，连接断开的瞬间这个状态本身就已经失去意义（在途的
//! 数据要么从未到达 Receiver、要么到达了但 ack 没能传回来），沿用旧窗口状态毫无依据。
//! 后续实现者不要为了"优化重连体验"给这里加持久化——那是解决一个不存在的问题。
//!
//! ## 与 `data_mover::qos::QosManager` 的关系
//!
//! 两者用法形似（`acquire(cost).await` 挂起在调用点、外部补充后自动唤醒），但控制的
//! 物理量不同，不能互相替代：
//! - 控制目标：`QosManager` 管**速率**（bytes/秒）；`SenderCreditState` 管**在途量**
//!   （已发但未确认消费的字节数）。
//! - 令牌补充驱动源：`QosManager` 靠**时钟**（固定速率 × 流逝时间，本地自动补充，
//!   不需要对端反馈）；`SenderCreditState` 靠**对端消费反馈**（Receiver 消费后显式
//!   `grant()`）。
//! - Receiver 完全停摆时的行为：`QosManager` 照常按时补令牌、照常放行——积压 =
//!   速率 × 停摆时长，仍然无界；`SenderCreditState` 授信停止 → 窗口耗尽 → Sender 停发，
//!   积压钉死在窗口值。
//!
//! `QosManager` 回答"我可以发多快"，`SenderCreditState` 回答"我可以有多少字节在外面没被
//! 消化"——前者是开环限速，后者是闭环流控，只有后者能解决"Receiver 消费停摆导致
//! 积压无界"的问题（governor 的 API 本身也没有"由外部事件补充令牌"的模式，硬套上去
//! 等于把时钟补充关掉、只剩外部事件驱动的窗口账本）。两者正交、继续共存：`QosManager`
//! 管"别把网络/源端打满"（Sender 读端，
//! 不受本 issue 影响），`SenderCreditState` 管"别把 Receiver 内存打爆"（Sender 发送端，本
//! 模块新增）。
//!
//! ## 所有权与 adapter 语义
//!
//! 本模块是闭环策略的唯一所有者：`ReceiverCreditState` 把 bounded disk-commit seam 的
//! 成功接受转换为延迟 grant；`SenderCreditState` 扣减窗口、内部消费 grant、限制容量并在
//! close 时唤醒等待者。QUIC adapter 传输真实 grant；in-process adapter 依靠自身 bounded
//! channel 提供背压并过滤 wire-level grant。两种 adapter 都不会把 `CreditGrant` 暴露给
//! Remote Sender session。
//!
//! 删除本模块会迫使 cost、接受时机、批量阈值、窗口容量、grant 过滤、重连和关闭规则重新
//! 散落到 Sender/Receiver adapters 与应用 session；这正是该深模块通过 deletion test 的
//! 依据。测试应断言阻塞、恢复、消息可见性和 typed terminal outcome，而非内部计数值。

use std::sync::Mutex;

// 外部 crate
use tokio::sync::Notify;

// 内部模块
use crate::error::{Result, TransportError};
use crate::message::ReceiverMsg;
use crate::traits::ReceiverTransport;

/// 默认 credit 窗口：64 MiB（BDP 依据见模块文档）
pub const DEFAULT_CREDIT_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

const MAX_CREDIT_WINDOW_BYTES: u64 = u32::MAX as u64;

/// Receiver-side result of recording data accepted by the bounded sink.
#[derive(Debug)]
enum ReceiverCreditOutcome {
    Pending,
    Grant(ReceiverMsg),
}

/// Authoritative Receiver-side delayed-grant ledger for one connection.
#[derive(Debug)]
pub struct ReceiverCreditState {
    grant_threshold: u64,
    accepted_pending: u64,
}

impl ReceiverCreditState {
    /// Uses the existing half-window delayed-grant policy.
    pub fn new(window_bytes: u64) -> Result<Self> {
        validate_window(window_bytes)?;
        Ok(Self {
            grant_threshold: (window_bytes / 2).max(1),
            accepted_pending: 0,
        })
    }

    /// Records bytes only after the bounded disk-commit seam accepts them.
    fn record_accepted(&mut self, bytes: u64) -> Result<ReceiverCreditOutcome> {
        self.accepted_pending = self
            .accepted_pending
            .checked_add(bytes)
            .ok_or(TransportError::CreditAccountingOverflow)?;
        if self.accepted_pending < self.grant_threshold {
            return Ok(ReceiverCreditOutcome::Pending);
        }
        let bytes = std::mem::take(&mut self.accepted_pending);
        Ok(ReceiverCreditOutcome::Grant(ReceiverMsg::CreditGrant {
            bytes,
            ndx: None,
        }))
    }

    /// Records a bounded-sink acceptance and owns delivery of any resulting wire grant.
    pub async fn accepted(&mut self, transport: &(dyn ReceiverTransport + 'static), bytes: u64) -> Result<()> {
        if let ReceiverCreditOutcome::Grant(message) = self.record_accepted(bytes)? {
            transport.send(message).await?;
        }
        Ok(())
    }

    /// Drops connection-local accounting when a connection is replaced.
    pub fn reset(&mut self) {
        self.accepted_pending = 0;
    }
}

impl Default for ReceiverCreditState {
    fn default() -> Self {
        Self {
            grant_threshold: DEFAULT_CREDIT_WINDOW_BYTES / 2,
            accepted_pending: 0,
        }
    }
}

/// 字节级 credit 流控窗口（issue #59 方案 b），语义与死锁防护见模块文档
pub(crate) struct SenderCreditState {
    ledger: Mutex<SenderCreditLedger>,
    notify: Notify,
}

struct SenderCreditLedger {
    available: u64,
    capacity: u64,
    closed: bool,
}

/// Separates transport-internal grants from application-visible messages.
pub(crate) enum SenderCreditOutcome {
    Consumed(CreditGrantOutcome),
    Forward(ReceiverMsg),
}

/// Exact result of applying a grant without allowing capacity inflation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CreditGrantOutcome {
    pub(crate) applied: u64,
    pub(crate) discarded: u64,
}

impl SenderCreditState {
    /// 用指定窗口大小构造；生产路径固定使用 [`DEFAULT_CREDIT_WINDOW_BYTES`]
    /// （见 `quic::sender::connect`），测试注入更小的窗口以触发阻塞/授信路径。
    pub(crate) fn new(window_bytes: u64) -> Result<Self> {
        validate_window(window_bytes)?;
        Ok(Self {
            ledger: Mutex::new(SenderCreditLedger {
                available: window_bytes,
                capacity: window_bytes,
                closed: false,
            }),
            notify: Notify::new(),
        })
    }

    /// 原子扣减 `bytes` credit；余量不足时挂起，直到 [`Self::grant`] 补充或连接关闭。
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<()> {
        let capacity = self
            .ledger
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .capacity;
        if bytes > capacity {
            return Err(TransportError::CreditCostExceedsWindow {
                cost: bytes,
                window: capacity,
            });
        }
        loop {
            let notified = self.notify.notified();
            {
                let mut ledger = self.ledger.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if ledger.closed {
                    return Err(TransportError::SendFailed("credit window closed".to_string()));
                }
                if ledger.available >= bytes {
                    ledger.available -= bytes;
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// 补授 `bytes` credit（Receiver 半窗批量授信后由 `QuicSenderTransport::recv()` 调用）
    pub(crate) fn grant(&self, bytes: u64) -> CreditGrantOutcome {
        let mut ledger = self.ledger.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let applied = bytes.min(ledger.capacity.saturating_sub(ledger.available));
        if applied > 0 {
            ledger.available += applied;
        }
        drop(ledger);
        self.notify.notify_waiters();
        CreditGrantOutcome {
            applied,
            discarded: bytes.saturating_sub(applied),
        }
    }

    pub(crate) fn apply_incoming(&self, message: ReceiverMsg) -> SenderCreditOutcome {
        match message {
            ReceiverMsg::CreditGrant { bytes, .. } => SenderCreditOutcome::Consumed(self.grant(bytes)),
            message => SenderCreditOutcome::Forward(message),
        }
    }

    /// Wakes every pending acquire with an error during connection shutdown.
    pub(crate) fn close(&self) {
        self.ledger
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
        self.notify.notify_waiters();
    }
}

fn validate_window(window_bytes: u64) -> Result<()> {
    let runtime_limit = MAX_CREDIT_WINDOW_BYTES;
    if window_bytes == 0 || window_bytes > runtime_limit {
        return Err(TransportError::InvalidCreditConfiguration {
            reason: format!("window must be within 1..={runtime_limit} bytes, got {window_bytes}"),
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn receiver_credit_batches_at_half_window_and_discards_terminal_residual() {
        let mut credit = ReceiverCreditState::new(20).unwrap();

        assert!(matches!(
            credit.record_accepted(6).unwrap(),
            ReceiverCreditOutcome::Pending
        ));
        assert!(matches!(
            credit.record_accepted(7).unwrap(),
            ReceiverCreditOutcome::Grant(ReceiverMsg::CreditGrant { bytes: 13, ndx: None })
        ));
        assert!(matches!(
            credit.record_accepted(5).unwrap(),
            ReceiverCreditOutcome::Pending
        ));

        credit.reset();

        assert!(matches!(
            credit.record_accepted(5).unwrap(),
            ReceiverCreditOutcome::Pending
        ));
    }

    #[test]
    fn zero_bytes_never_produces_a_grant() {
        let mut receiver = ReceiverCreditState::new(2).unwrap();
        assert!(matches!(
            receiver.record_accepted(0).unwrap(),
            ReceiverCreditOutcome::Pending
        ));

        let sender = SenderCreditState::new(2).unwrap();
        assert_eq!(
            sender.grant(0),
            CreditGrantOutcome {
                applied: 0,
                discarded: 0,
            }
        );
    }

    #[test]
    fn credit_configuration_rejects_zero_capacity() {
        assert!(matches!(
            ReceiverCreditState::new(0),
            Err(TransportError::InvalidCreditConfiguration { .. })
        ));
    }

    #[test]
    fn credit_configuration_rejects_capacity_above_runtime_limit() {
        assert!(matches!(
            ReceiverCreditState::new(u64::MAX),
            Err(TransportError::InvalidCreditConfiguration { .. })
        ));
    }

    #[test]
    fn sender_credit_consumes_grants_and_forwards_application_messages() {
        let credit = SenderCreditState::new(10).unwrap();

        assert!(matches!(
            credit.apply_incoming(ReceiverMsg::CreditGrant { bytes: 4, ndx: None }),
            SenderCreditOutcome::Consumed(CreditGrantOutcome {
                applied: 0,
                discarded: 4,
            })
        ));
        assert!(matches!(
            credit.apply_incoming(ReceiverMsg::RequestsDone),
            SenderCreditOutcome::Forward(ReceiverMsg::RequestsDone)
        ));
    }

    #[tokio::test]
    async fn sender_credit_close_releases_pending_acquire_with_error() {
        let credit = Arc::new(SenderCreditState::new(1).unwrap());
        credit.acquire(1).await.unwrap();
        let waiting_credit = credit.clone();
        let waiting = tokio::spawn(async move { waiting_credit.acquire(1).await });
        tokio::task::yield_now().await;

        credit.close();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn oversized_frame_fails_without_waiting() {
        let credit = SenderCreditState::new(8).unwrap();

        assert!(matches!(
            credit.acquire(9).await,
            Err(TransportError::CreditCostExceedsWindow { cost: 9, window: 8 })
        ));
    }

    #[tokio::test]
    async fn duplicate_and_excess_grants_never_inflate_capacity() {
        let credit = SenderCreditState::new(8).unwrap();
        credit.acquire(8).await.unwrap();

        assert_eq!(
            credit.grant(12),
            CreditGrantOutcome {
                applied: 8,
                discarded: 4,
            }
        );
        assert_eq!(
            credit.grant(8),
            CreditGrantOutcome {
                applied: 0,
                discarded: 8,
            }
        );
        credit.acquire(8).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), credit.acquire(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn concurrent_grants_share_one_capacity_ledger() {
        let credit = Arc::new(SenderCreditState::new(8).unwrap());
        credit.acquire(8).await.unwrap();

        let first = credit.clone();
        let second = credit.clone();
        let (first, second) = tokio::join!(
            tokio::task::spawn_blocking(move || first.grant(8)),
            tokio::task::spawn_blocking(move || second.grant(u64::MAX)),
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.applied + second.applied, 8);
        assert_eq!(
            credit
                .ledger
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .available,
            8
        );
    }

    #[tokio::test]
    async fn concurrent_acquire_and_duplicate_grant_cannot_exceed_capacity() {
        let credit = Arc::new(SenderCreditState::new(8).unwrap());
        credit.acquire(8).await.unwrap();
        assert_eq!(credit.grant(8).applied, 8);

        let acquiring = credit.clone();
        let granting = credit.clone();
        let (acquired, duplicate) = tokio::join!(
            tokio::spawn(async move { acquiring.acquire(8).await }),
            tokio::task::spawn_blocking(move || granting.grant(8)),
        );
        acquired.unwrap().unwrap();
        let duplicate = duplicate.unwrap();
        let ledger = credit.ledger.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        assert!(ledger.available <= ledger.capacity);
        assert!(duplicate.applied == 0 || ledger.available == 8);
    }

    /// 窗口耗尽后 `acquire()` 应挂起，直到 `grant()` 补充足够 permits 才被唤醒——这是
    /// credit 机制"死锁防护"的直接证据：不存在能让 outstanding 突破窗口上限的代码路径。
    #[tokio::test]
    async fn acquire_blocks_when_window_exhausted_and_unblocks_after_grant() {
        let window = Arc::new(SenderCreditState::new(10).unwrap());

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
