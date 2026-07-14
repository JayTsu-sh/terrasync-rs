# issue #59 执行计划：QUIC 应用层字节 credit 流控（方案 b）

## spec 锚定

以 issue #59 最新一条 🤖「建议 spec（方案 b：应用层字节 credit）」评论（2026-07-14，
VERDICT: REASONABLE）为准；更早的方案 a spec 已作废，不采用。

分支基线：`origin/main` @ `50d1734`（含 PR #56，协议基线现为 v4 → 本次按动态基线规则
bump 到 v5）。

## 需求 & 目标（摘自 spec）

1. **credit 模型**：全局字节窗口，`tokio::sync::Semaphore` 实现（`acquire_many` + `forget`
   消费、`add_permits` 补授）。默认 `DEFAULT_CREDIT_WINDOW_BYTES = 64MiB`（BDP 依据）。
   v1 不暴露配置项，测试留窗口注入口（crate-internal 参数）。`CreditGrant.ndx: Option<i32>`
   预留 per-ndx 扩展，恒 `None`。
2. **wire**：`ReceiverMsg::CreditGrant{bytes, ndx}` 走既有 AckProgress 物理流；半窗口批量
   授信；计量口径 = 应用层 payload 字节。计入 `FileData`/`DeltaData`/`TarPacked`（后者
   defense-in-depth）；不计入 `DeltaMatch` 及控制/元数据类。共享 `credit_cost(msg)`。
3. **状态机**：Sender 在 `QuicSenderTransport::send()` 内部拦截扣减（`remote_sync.rs`
   零改动，靠既有 `Some(other) => {}` catch-all 天然兼容）；Receiver 在消息处理完成
   （送 dc_tx/入 delta_tokens）后累计，半窗触发 grant（`recv_file_list_and_data_phase`）。
4. **死锁防护**：不变量 `outstanding ∈ [0, window]`；重连 = 新实例 = 窗口自然重置。
5. **协议版本**：v4 → v5，硬 cutover + `negotiate_rejects_v4_peer` 测试。
6. **与 QUIC 层**：mux unbounded fan-in 保留，不叠加方案 a 的有界 channel；mux.rs 模块
   文档改写（去掉错误的"背压在 QUIC per-stream 窗口"声明）。
7. **既有 8MiB flood 测试**：核实 8MiB < 64MiB 默认窗口，加注释说明测试语义不变质。

## 验收标准

1. Data 类消息发送前必须持有等量 credit，不再无界发送。
2. Receiver 应用层积压上界 = window + dc 固定缓冲（常量），不随阻塞时长增长。
3. 记账不变量恒成立，死锁测试全绿。
4. 三类控制消息隔离不回归、credit 耗尽期间畅通。
5. 协议 bump 正确（v4→v5）+ 旧对端拒绝。
6. mux 文档与实现一致。
7. workspace 全绿，fmt/check 干净。

## 测试计划（spec 8 项 → 落点）

1. 窗口耗尽挂起/补授唤醒（死锁直接证据）→ `quic/credit.rs` 内 `#[cfg(test)]` 纯 Semaphore 测试
2. 半窗批量授信金额精确（防双计/泄漏）→ `app/src/receiver.rs` 内 `accumulate_credit` 纯函数单测
3. 真实 QUIC 超窗口 pending→grant unblock（注入小窗口）→ `quic/sender.rs` 内 `#[cfg(test)]`（用 crate-internal `connect_with_credit_window`）
4. credit 耗尽期间控制消息畅通 → 同上文件另一测试
5. v4 对端握手拒绝 → `message.rs` 内 `negotiate_rejects_v4_peer`
6. 8MiB flood 回归 → `crates/transport/tests/quic_roundtrip.rs` 既有测试 + 注释确认
7. 真实双进程超默认窗口大文件 e2e（hash 过、无死锁超时）→ `tests/remote_process_e2e.rs` 新增
8. `cargo test --workspace --no-fail-fast` 全绿 + fmt/check 干净 → 收尾步骤

## 执行步骤清单

- ✅ 步骤 1：`crates/transport/src/message.rs` —— 新增 `ReceiverMsg::CreditGrant{bytes,ndx}`、
  共享 `credit_cost(msg)`、协议 v4→v5 bump + doc、`negotiate_rejects_v4_peer` 测试、
  `credit_cost` 纯函数单测。同步在 `quic/mux.rs::receiver_stream_kind` 补上新 variant 的
  穷尽匹配分支（`AckProgress`），否则 crate 无法编译——这是新增枚举 variant 的必然联动，
  与后续「模块文档改写」的语义性改动分开提交（见步骤 3）。
- ✅ 步骤 2：新增 `crates/transport/src/quic/credit.rs` —— `CreditWindow`（Semaphore 封装）+
  `DEFAULT_CREDIT_WINDOW_BYTES` + 模块文档（记账不变量/重连重置语义/与 qos.rs 对应与差异）+
  窗口耗尽挂起/授信解阻塞单测；`quic/mod.rs` 加 `pub mod credit;`。
- 🔄 步骤 3：`crates/transport/src/quic/mux.rs` —— `receiver_stream_kind` 加 `CreditGrant` →
  `AckProgress`；模块文档改写"背压"声明。
- ⬜ 步骤 4：`crates/transport/src/quic/sender.rs` —— `QuicSenderTransport` 加 `credit` 字段；
  `connect()` 委托给 crate-internal `connect_with_credit_window(..., window_bytes)`；`send()`
  按 `credit_cost` 扣减；`recv()` 拦截 `CreditGrant` 补授后 `continue`；新增真实 QUIC 注入小
  窗口的 pending→grant unblock 测试 + 控制消息畅通测试。
- ⬜ 步骤 5：`crates/app/src/receiver.rs` —— 引入 `transport::quic::credit::DEFAULT_CREDIT_WINDOW_BYTES`
  算半窗阈值；`accumulate_credit` 纯函数 + 单测；`recv_file_list_and_data_phase` 的
  `FileData`/`DeltaData` 分支累计消费并在达阈值时发 `CreditGrant`。
- ⬜ 步骤 6：`crates/transport/tests/quic_roundtrip.rs` —— 既有 8MiB flood 测试加注释确认
  量级 < 64MiB 默认窗口，语义不变质。
- ⬜ 步骤 7：`tests/remote_process_e2e.rs` —— 新增真实双进程 e2e：源目录含 >64MiB 大文件，
  `--enable-integrity-check`，断言同步成功、dest 与 src 字节一致、无死锁超时。
- ⬜ 步骤 8：收尾 —— `cargo fmt`、`cargo test -p transport --features quic`、
  `cargo test -p app`、`cargo test -p terrasync-rs --test remote_process_e2e`（连跑 2 次）、
  `cargo test --workspace --no-fail-fast`，确认 `git status` 无越界文件，移除本计划文件。

## 关键设计决策记录（防实现漂移）

- `CreditWindow` 只存在于 `QuicSenderTransport`（Sender 侧），Receiver 侧不持有窗口对象，
  只负责累计消费字节数 + 发 `CreditGrant`。
- `connect()` 公开签名不变（`addr, server_name, server_cert`），窗口注入走
  `pub(crate) connect_with_credit_window(..., window_bytes: u64)`，只有 crate 内测试可见。
- `credit_cost()` 定义在 `transport::message`（`pub fn`），Sender 侧（`sender.rs::send()`）与
  Receiver 侧（`receiver.rs::recv_file_list_and_data_phase`）共用同一份口径，避免双边算法
  漂移导致 credit 泄漏/双计。
- Receiver 半窗批量授信发的是"实际累计消费量"（可能因消息大小不整除而略高于半窗口），
  不是恒定的半窗口常量值——保证长期账目精确相等（授信总量 == 消费总量），不产生系统性
  drift。
- `remote_sync.rs` 零改动：其 `process_requests_and_acks` 循环已有 `Some(other) => {}`
  catch-all，新增的 `ReceiverMsg::CreditGrant`（若通过 InProcess transport 泄漏到该层，如
  app crate 的单元测试场景）天然被吞掉，不需要显式处理分支。
