# issue #54 阶段 0：delta size 门槛降级 — 执行计划

## 需求 & 目标（摘自 approved spec）

维护者已拍板 delta 算法维持 rsync 滚动窗口实现，阶段 1-3（签名/match/reconstruct 流式化）
另行开发。**本次严格限定阶段 0**，不碰 `sync-delta`、不做任何流式化。

阶段 0 范围：

1. **门槛判定**：`receiver.rs` 的 `TransferDecision::DeltaTransfer` 分支——文件 size 超过阈值时
   走既有"降级全量"路径（复用 `!negotiated_features.delta` 分支的
   `TransferRequest{ndx, decision}` 模式；`decision` 仍携带 `DeltaTransfer`，保 Sender 侧统计
   口径为 Changed）。
2. **配置全链路**（照抄 `block_size` 模板）：`SyncJobConfig.delta_size_threshold: Option<String>`
   （人类可读格式如 "512MiB"，复用 `parse_size`）→ CLI `--delta-size-threshold`（doc 注明仅
   `--remote` 模式生效）→ `SessionConfig` 透传 → receiver 消费。**默认 512MiB**。
3. **协议**：`SessionConfig` 加字段变 bincode 线格式 → bump `PROTOCOL_VERSION` /
   `MIN_SUPPORTED_PROTOCOL_VERSION`（v3→v4），加握手拒绝测试（仿 `negotiate_rejects_v2_peer`）。
   web 层 `task_runner.rs` 的 `SyncJobConfig` 字面量补默认值。
4. **测试**：超阈值走全量降级（发 `TransferRequest` 非 `DeltaTransferRequest`）且统计仍计
   Changed；低于阈值走 delta 不受影响；阈值解析（合法/非法格式）；协议 v3 对端被拒。

## 验收标准

- `cargo fmt --all -- --check` 干净
- `cargo test --workspace --no-fail-fast` 全绿
- `cargo test --test remote_process_e2e` 8/8（含新增 e2e，连跑 2 次不 flake）
- 默认阈值与 override 有单测覆盖
- 不碰 `sync-delta` 三个文件（signature.rs / matcher.rs / reconstruct.rs）

## 分支基线

`origin/main` @ `f841e10`（含 #53 size 断言 + data-mover bb8b60b bump）
工作分支：`claude/issue-54-stage0`

## 执行步骤清单

- ✅ step 0: 立计划（本文件）+ 调研代码现状（receiver.rs DeltaTransfer 分支、SyncJobConfig/
  SessionConfig/block_size 全链路、PROTOCOL_VERSION 现状、既有 delta 降级测试风格）
- ✅ step 1: transport 协议层 —— `SessionConfig` 加 `delta_size_threshold: Option<String>` 字段；
  `PROTOCOL_VERSION`/`MIN_SUPPORTED_PROTOCOL_VERSION` 3→4（doc 注释说明第 4 次线格式变更）；
  加 `negotiate_rejects_v3_peer` 测试（仿 `negotiate_rejects_v2_peer`）；更新
  `crates/transport/tests/quic_roundtrip.rs` 3 处 `SessionConfig{}` 字面量补字段。
  验证：`cargo check -p transport && cargo test -p transport`
- ✅ step 2: app 层配置 —— `SyncJobConfig` 加 `delta_size_threshold: Option<String>` 字段
  （`crates/app/src/config.rs`）；`remote_sync.rs::run()` 的 `SessionConfig` 构造透传该字段；
  同文件内 4 处测试 `SessionConfig{}` 字面量补 `delta_size_threshold: None`；
  `crates/app/tests/dual_process_streaming.rs::session_cfg` 补字段。
  验证：`cargo check -p app && cargo test -p app --lib remote_sync::`
- ✅ step 3: receiver.rs 核心逻辑 —— 加 `DEFAULT_DELTA_SIZE_THRESHOLD_BYTES`（512MiB）常量 +
  `resolve_delta_size_threshold(&Option<String>) -> Result<u64>` 纯函数（复用
  `crate::sync::parse_size`，None → 默认值）；`receiver_task_remote` 解析一次并传给
  `recv_file_list_and_data_phase`；`DeltaTransfer` match 新增 size 超阈值降级分支（复用
  `!negotiated_features.delta` 分支的 `TransferRequest{ndx, decision}` 模式）+ `info!` 日志
  （供 e2e 非侵入式验证）；`mod tests` 加 `resolve_delta_size_threshold` 单测
  （默认值/合法 override/非法格式）。
  验证：`cargo check -p app && cargo test -p app --lib receiver::tests::`
- ✅ step 4: CLI 全链路 —— `commands_enum.rs` 加 `--delta-size-threshold` arg（doc 注明仅
  `--remote` 模式生效，复用 `validate_block_size` 同款校验器或新增
  `validate_delta_size_threshold`）；`lib.rs` 解构透传；`commands.rs::sync_cmd` 加形参 +
  写入 `SyncJobConfig` 字面量。
  验证：`cargo check -p cli`
- ✅ step 5: web 层收尾 —— `task_runner.rs` 的 `SyncJobConfig` 字面量补
  `delta_size_threshold: None`（同 `delete_target` 先例注释风格）。
  验证：`cargo check -p web`
- ✅ step 6: receiver.rs 集成测试（app 层）—— 仿 `recv_file_list_delta_transfer_downgrade_signal`
  / `recv_file_list_delta_transfer_negotiated_signal`，加
  `spawn_receiver_and_handshake_with_threshold` 包装（不改动既有 9 处调用签名），新增两个测试：
  超阈值 → `TransferRequest{decision:DeltaTransfer}`；阈值内 → `DeltaTransferRequest` 不受影响。
  验证：`cargo test -p app --lib remote_sync::tests::recv_file_list`
- ⬜ step 7: e2e（`tests/remote_process_e2e.rs`）—— 仿
  `test_remote_process_e2e_delta_sync_transfers_changed_content`，加
  `test_remote_process_e2e_delta_size_threshold_downgrades_to_full`：真实两进程，
  `--delta-size-threshold` 设为小于修改文件大小的值，断言 dest 内容仍正确 + Receiver 日志
  出现 size 降级 info 行（非侵入式证据，同 `assert_delta_negotiated_in_log` 风格）。
  验证：`cargo test --test remote_process_e2e -- delta_size_threshold`（连跑 2 次）
- ⬜ step 8: 收尾核验 —— `cargo fmt --all -- --check`；
  `cargo test --workspace --no-fail-fast`；`cargo test --test remote_process_e2e`（8→9 项，
  连跑 2 次）；`git status` 无越界文件；移除本 plan 文件。
