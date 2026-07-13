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
- ✅ 步骤 4：`--delete-target` 配置管线 —— `crates/app/src/config.rs`
  （`SyncJobConfig` 加 `delete_target`）、`crates/cli/src/commands_enum.rs`
  （CLI flag）、`crates/cli/src/commands.rs`（`sync_cmd` 透传）、
  `crates/cli/src/lib.rs`（解构透传）、
  `crates/web/src/infrastructure/task_runner.rs`（字面量补 `delete_target: false`）。
  验证：`cargo check -p cli`（本步骤新增代码本身无错误；唯一报错仍是
  `remote_sync.rs` 的 `TransferRequest{decision}`，步骤 5 修复后一并核实
  cli/web 全绿）。
- ✅ 步骤 5：Sender 侧结构化报表 + delete_target 透传 + progress callback ——
  `crates/app/src/remote_sync.rs`：`StatisticConsumer` 生命周期（begin/end）；
  `send_file_list_phase` 喂 `Scanned`；`process_requests_and_acks` 翻译
  `TransferRequest{decision}`/`DeltaTransferRequest`/`Classified` 为
  `StorageEntryMessage` 喂 `update_statistics`；`Progress` 喂 bytes tracker；
  `SessionConfig.delete_target` 从硬编码改为 `config.delete_target`；更新既有测试
  调用点适配新签名（新增 `test_stats_consumer()` 测试 helper）。
  验证：`cargo check -p app`（干净）+ `cargo test -p app remote_sync::`
  （13 passed）+ `cargo check -p cli -p web`（干净，确认 config.rs 加字段未破坏
  下游）。
- ✅ 步骤 6：新增协议层序列化测试 —— `crates/transport/tests/quic_roundtrip.rs`
  新增 `test_quic_classification_messages_roundtrip`：`TransferRequest{decision}`/
  `Classified`/`TransferDecision::Deleted` 真实 QUIC 往返测试。
  验证：`cargo test -p transport --features quic --test quic_roundtrip`
  （10 passed，含新增测试，无回归）。
- ✅ 步骤 7：新增 Receiver 分类信号正确性测试（`remote_sync.rs` test mod，双端真实
  `StorageEnum` + `receiver_task_remote`）：FullTransfer/DeltaTransfer(协商成功/降级)/
  MetadataOnly/Skip/Deleted(true/false)/删除失败，共 8 个测试；调试过程中发现
  `progress_reporter` 首个 tick 立即触发（`tokio::time::interval` 默认行为）会与断言
  交错，补 `recv_skip_progress` helper 吸收。
  验证：`cargo test -p app remote_sync::tests::recv_file_list`（8 passed）+
  `cargo test -p app remote_sync::tests` 连跑 3 次（均 21 passed，无 flaky）。
- ✅ 步骤 8：新增 Sender 侧统计桥接单测 + 报表口径一致性测试（`remote_sync.rs` test
  mod）：`classification_to_stats_message` 五类输入各自单测、混合场景验证
  `IncrementalStats.new/changed/deleted/renamed`、`send_file_list_phase` 的
  `scanned` 累加、`to_final_stats()`/`to_job_result()` 计数与预期逐一匹配。
  验证：`cargo test -p app remote_sync::tests`（29 passed，含全部新增测试）。
- ✅ 步骤 9：新增 callback payload/频率测试（`remote_sync.rs` test mod）：直接构造
  `StatisticConsumer`（与 `remote_sync::run()` 同一套构造方式）+ 本地 mock HTTP
  server（raw TCP 手写 HTTP/1.1 帧解析，不引入新依赖），`sleep(2.2s)` 确定性跨越
  `CALLBACK_INTERVAL_SECS=2`，验证周期性(非 final)+恰好一次 final 回调、payload
  为 `ProgressReport`/`ProgressDetail::Incremental`/`FinalStats`。不经真实 QUIC——
  callback 机制活在 `StatisticConsumer` 内部与 transport 无关，`run()` 到这里的唯一
  接线是一行字段赋值，双进程整条链路已由 `tests/remote_process_e2e.rs` + 步骤 7 覆盖。
  验证：`cargo test -p app remote_sync::tests`（30 passed）连跑 2 次，无 flaky。
- ✅ 步骤 10：新增 `--delete-target` 进程级 e2e —— `tests/remote_process_e2e.rs`：
  `test_remote_process_e2e_without_delete_target_keeps_orphan`（默认不删除）+
  `test_remote_process_e2e_delete_target_removes_orphan`（加 flag 后删除，覆盖
  CLI→SyncJobConfig→SessionConfig→Receiver orphan-delete→Classified{Deleted} 全链路）。
  验证：`cargo test -p terrasync-rs --test remote_process_e2e` 连跑 2 次，均
  7 passed（5 既有 + 2 新增），无 flaky。
- ✅ 步骤 11：收尾 —— `cargo fmt --all -- --check`（干净，exit 0）、
  `cargo test --workspace --no-fail-fast`（全绿：app 62 + dual_process_streaming 6
  + cli 0 + db 22（14 ClickHouse 集成测试预期 ignored，无本地 ClickHouse）+
  duckdb 0 + licensing 0 + sync_delta 25 + terrasync bin 0 +
  remote_process_e2e 7 + transport 3 + quic_roundtrip 10 + utils 0 +
  crypto_cmd 0 + web 3 + 全部 doc-tests 0，0 failed）、e2e 单独连跑 2 次均
  7 passed、`git status` 核对无越界文件、移除本计划文件。
