# issue-23 执行计划：实现远端 incremental sync

## 需求 & 目标（摘自批准 spec，2026-07-13 修订版）

双进程 remote sync 本就是 compare-based（`DestIndex` 实时比对），天然产生
New/Changed/MetadataOnly/Skip 四类判定 —— **不存在需要新写的"远端增量模式"**。
补齐三件缺失能力 + 解一个死分支：

1. 结构化统计报表（New/Changed/MetadataOnly/Skip/Deleted，Sender 侧汇总，复用本地
   `IncrementalStats` 口径）。
2. `--delete-target` 门控的 Deleted（orphan-delete 执行逻辑已存在，配置管线不可达）。
3. 与本地模式一致的 progress callback（payload 结构、POST 频率复用 `StatisticConsumer`）。
4. orchestrator 对 Remote 模式的 ScanType 分支塌陷：删掉死分支，`(Remote, Full)` 与
   `(Remote, Incremental)` 走同一路径。

不做：DB/`DatabaseConsumer` 集成、rename 支持、`FeatureFlags.rename` 解锁。

## 分支基线

`origin/main` @ `d0a45030102cda03d96f5afdaa97d7b2f45ca6fa`

## 验收标准（摘自 spec）

- `Remote + Incremental` 不再报 "not yet implemented"；两者走同一路径，无独立分支。
- Receiver 对 New/Changed(含 MetadataOnly)/Skip/Deleted 五类判定产生正确 wire 信号，
  执行动作与判定一致。
- `--delete-target`：CLI flag 存在，默认 false；清理结果（成功/失败）都产生可观测信号
  （`Classified{Deleted}` 或 `EntryError`），无静默 `warn!` 吞错误。
- Sender 侧结束时可获取结构化报表，New/Changed/Deleted 三维计数与实际分类事件数一致，
  Renamed 恒 0。
- 配置 `progress_callback_url` 时周期(2s)+结束各发一次回调，payload 类型与本地
  `IncrementalCopy` 完全同构。
- 协议版本握手正确拒绝旧版本对端。
- 不引入 DB/`ConsumerManager`/`DatabaseConsumer` 依赖；`FeatureFlags.rename` 保持 false。
- `cargo fmt && cargo check` 通过；`cargo test --workspace --no-fail-fast` 全绿。

## 执行步骤

- ✅ 步骤 0：读 spec + 通读现有代码（orchestrator/message/receiver/remote_sync/
  config/consumer/stats/cli/web），确认无 blocker，立本计划。
- ✅ 步骤 1：协议层 —— `crates/transport/src/message.rs`：`TransferDecision` 加
  `Serialize/Deserialize` + `Deleted` variant；`ReceiverMsg::TransferRequest` 加
  `decision` 字段；新增 `ReceiverMsg::Classified{entry, decision}`；
  `PROTOCOL_VERSION`/`MIN_SUPPORTED_PROTOCOL_VERSION` 2→3；`mux.rs` 路由新增
  `Classified` 到 `FileList` stream；message.rs 单测新增 `negotiate_rejects_v2_peer`。
  验证：`cargo check -p transport --features quic`（通过）+ `cargo test -p transport
  --features quic --lib`（3 passed，含新增 `negotiate_rejects_v2_peer`）。
- ✅ 步骤 2：orchestrator 路由塌陷 —— `crates/app/src/orchestrator.rs`：
  `match (&self.mode, scan_type)` 改为先 match `mode`，`Remote{..}` 忽略
  `scan_type` 直调 `run_sync_remote`，删除死分支。
  验证：`cargo check -p app`（orchestrator.rs 自身无新增错误；剩余 4 处
  `TransferRequest{decision}` 缺字段错误是步骤 1 协议变更的预期下游影响，
  步骤 3/5 修复）。
- ✅ 步骤 3：Receiver 分类信号 —— `crates/app/src/receiver.rs`：四个
  `TransferDecision` 分支各自补发/携带 `decision`；MetadataOnly/Skip 补发
  `Classified`；orphan-delete 循环成功发 `Classified{Deleted}`、失败发
  `EntryError`（替换纯 `warn!`）。
  验证：`cargo check -p app`（receiver.rs 自身无错误；剩余 1 处
  `TransferRequest{decision}` 缺字段错误在 `remote_sync.rs`，步骤 5 修复）。
- ⬜ 步骤 4：`--delete-target` 配置管线 —— `crates/app/src/config.rs`
  （`SyncJobConfig` 加 `delete_target`）、`crates/cli/src/commands_enum.rs`
  （CLI flag）、`crates/cli/src/commands.rs`（`sync_cmd` 透传）、
  `crates/cli/src/lib.rs`（解构透传）、
  `crates/web/src/infrastructure/task_runner.rs`（字面量补 `delete_target: false`）。
  验证：`cargo check -p cli -p web -p app`。
- ⬜ 步骤 5：Sender 侧结构化报表 + delete_target 透传 + progress callback ——
  `crates/app/src/remote_sync.rs`：`StatisticConsumer` 生命周期（begin/end）；
  `send_file_list_phase` 喂 `Scanned`；`process_requests_and_acks` 翻译
  `TransferRequest{decision}`/`DeltaTransferRequest`/`Classified` 为
  `StorageEntryMessage` 喂 `update_statistics`；`Progress` 喂 bytes tracker；
  `SessionConfig.delete_target` 从硬编码改为 `config.delete_target`；更新既有测试
  调用点适配新签名。
  验证：`cargo check -p app` + `cargo test -p app remote_sync::`。
- ⬜ 步骤 6：新增协议层序列化测试 —— `crates/transport/tests/quic_roundtrip.rs`
  新增 `TransferRequest{decision}`/`Classified`/`TransferDecision::Deleted`
  真实 QUIC 往返测试。
  验证：`cargo test -p transport --features quic --test quic_roundtrip`。
- ⬜ 步骤 7：新增 Receiver 分类信号正确性测试（`remote_sync.rs` test mod，双端真实
  `StorageEnum` + `receiver_task_remote`）：FullTransfer/DeltaTransfer(协商成功/降级)/
  MetadataOnly/Skip/Deleted(true/false)/删除失败。
  验证：`cargo test -p app remote_sync::tests`。
- ⬜ 步骤 8：新增 Sender 侧统计桥接单测 + 报表口径一致性测试（`remote_sync.rs` test
  mod）：`classification_to_stats_message` 五类输入、`scanned` 累加、
  `to_final_stats()`/`to_job_result()` 计数校验。
  验证：`cargo test -p app remote_sync::tests`。
- ⬜ 步骤 9：新增 callback payload/频率测试（`remote_sync.rs` test mod，真实 QUIC +
  本地 mock HTTP server，验证 final 回调结构；QoS 限速确保跨越周期回调间隔）。
  验证：`cargo test -p app remote_sync::tests`。
- ⬜ 步骤 10：新增 `--delete-target` 进程级 e2e —— `tests/remote_process_e2e.rs`：
  默认不删除 / 加 flag 后删除。
  验证：`cargo test -p terrasync-rs --test remote_process_e2e`。
- ⬜ 步骤 11：收尾 —— `cargo fmt --all -- --check`、`cargo test --workspace
  --no-fail-fast`、e2e 连跑 2 次、`git status` 核对无越界文件、移除本计划文件。
