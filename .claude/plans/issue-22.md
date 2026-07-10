# issue #22 执行计划：完善 delta / redo / ack 状态机

来源：https://github.com/JayTsu-sh/terrasync-rs/issues/22#issuecomment-4932730801
（2026-07-10 刷新版 spec，取代 2026-07-03 版；方案 A：串 ndx + 主 task 集中 redo 决策）

## 分支基线

`origin/main` @ `0d4a3b0cde853fd51d027e1c6d673f3e76110180`

## 需求 & 目标（摘自 spec）

补齐双进程（remote）传输的 ndx 级 redo/ack 闭环：hash 不符可靠触发 redo（每 ndx 至多重试 1 次），
第二次失败进入明确 `Error` 终态并影响进程退出码；delta redo 一律降级为全量重发；
`STRONG_HASH_LEN` 固化为常量并补测试。

## 验收标准

1. 目标端已有 basis file 时只传差异 token/data。
2. 人为制造 hash mismatch 时能触发 redo（每 ndx 至多 1 次）。
3. redo 成功后任务成功；redo 二次失败后有明确 `Error` 终态和退出码 1。
4. ndx ack 与 Entry ack 语义清晰，不重复计数。

## 范围护栏

- 不做 delta 物理合并进 disk_commit_task、不做流式 reconstruct、不做内存有界化（→ #46）。
- 不做多 hash 算法协商（`HashAlgorithm` enum 占位即可）。
- 重构（STRONG_HASH_LEN 提取）与逻辑改动（redo 状态机）分开提交。
- 无 `.unwrap()/.expect()`；错误用 thiserror 具名变种；`use` 顶部集中；注释中文。

## 关键设计（方案 A，已复核到 main 0d4a3b0 行号）

- `ndx` 串入全量数据路径：`SenderMsg::FileBegin/FileData/EndOfFile`、
  `DiskCommitMsg::FileBegin/FileChunk/FileCommit` 各加 `ndx: i32`。
- 新增内部（不上线）协议 `transport::message::{DcAck, FileOutcome}`：dc task /
  delta inline 上报 outcome（Success/HashMismatch/HardError）+ ndx 给 Receiver 主 task，
  不再自行发终态 ack。
- Receiver 主 task（`recv_file_list_and_data_phase`）维护 `attempts: HashMap<i32,u8>`，
  用统一函数 `decide_file_ack` 做 redo 决策：
  - HashMismatch·首次 → `Redo{ndx}`，不计 completed。
  - HashMismatch·二次 → `Error{ndx,"hash mismatch"}`，计 completed，`progress.error_count++`。
  - Success → `Success{ndx}`，计 completed。
  - HardError → `Error{ndx,reason}`，计 completed，不 redo，`progress.error_count++`。
- Sender（`process_requests_and_acks`）新增 `Redo/Success/Error` match 分支：
  `Redo` → 查 `ndx_table` → 一律 `handle_full_transfer`（delta redo 降级全量）；
  `Success` → `success_count++` + checkpoint；`Error` → `error_count++`。
- `run()` 收尾：`error_count>0` → `AppError::RemoteSyncFailed{errors}`（新增具名变种），
  使 `main.rs:36` 的 `exit(1)` 生效。
- `sync-delta`：`pub const STRONG_HASH_LEN: usize = 16;` + 提取共用 `blake3_truncated_16`，
  消除 `signature.rs`/`matcher.rs` 重复。

## 执行步骤

- ✅ 步骤 0：立计划、commit plan。

- ✅ 步骤 1（重构，单独提交）：`sync-delta` 提取 `STRONG_HASH_LEN` 常量 + 共用
  `blake3_truncated_16`，消除 `signature.rs`/`matcher.rs` 重复；补边界/一致性测试。
  验证：`cargo check -p sync-delta && cargo test -p sync-delta`。

- ✅ 步骤 2（协议层）：`crates/transport/src/message.rs` 给 `SenderMsg::FileBegin/
  FileData/EndOfFile`、`DiskCommitMsg::FileBegin/FileChunk/FileCommit` 加 `ndx: i32`；
  新增 `DcAck`/`FileOutcome` 内部消息类型（dc task → Receiver 主 task）。修复
  `crates/transport/tests/quic_roundtrip.rs` 中唯一的 `SenderMsg::FileData` 构造点。
  验证：`cargo check -p transport --features quic && cargo test -p transport --features quic`。

- ✅ 步骤 3（核心行为变更，单独提交）：`crates/app` 落地 redo/ack 状态机：
  - `receiver.rs`：`attempts` map + `decide_file_ack` + `handle_end_of_file` 改为返回
    `FileOutcome`（不再自行发 ack）+ 主循环按 `DcAck::Entry`/`FileOutcome` 分流。
  - `disk_commit.rs`：`ack_tx` 改为 `UnboundedSender<DcAck>`；`FileBegin` 失败、
    `finalize_file` 各失败分支改为上报 `FileOutcome`（保留 `remove_part` 清理）。
  - `remote_sync.rs`：`handle_full_transfer`/`handle_delta_transfer` 线路穿 `ndx`；
    `process_requests_and_acks` 加 `Redo/Success/Error` 分支；`run()` 收尾按
    `error_count` 走 `finalize_run_result`。
  - `error.rs`：新增 `AppError::RemoteSyncFailed { errors: u64 }`。
  - 同步修复 `crates/app/tests/dual_process_streaming.rs`（`DiskCommitMsg` 新字段 +
    断言从 `ReceiverMsg` 改为 `DcAck`/`FileOutcome`）。
  - 补 `decide_file_ack`/`finalize_run_result` 纯函数单元测试。
  验证：`cargo check -p app && cargo test -p app`（含既有
  `sender_receiver_pipeline_roundtrip_in_process` 与 `dual_process_streaming.rs` 全部通过）。

- ✅ 步骤 4（新增集成测试，spec 测试计划 b–g）：在 `remote_sync.rs` 测试模块新增
  `CorruptingTransport`（篡改 `EndOfFile.source_hash` 制造人为 hash mismatch）+：
  - (b) 全量·一次 mismatch → Redo → Success，`finalize_run_result` Ok。
  - (c) 全量·连续两次 mismatch → Error，`finalize_run_result` Err。
  - (d) delta·一次 mismatch → Redo → 降级全量 → Success。
  - (e) delta·连续两次 mismatch → Error。
  - (f) 计数不重复：混合 文件(Success{ndx}) + 符号链接(EntrySuccess) 精确断言。
  - (g) 大文件树/mux 无 ack 丢失：已由既有
    `test_remote_process_e2e_large_multi_chunk_file_mux`（真实两进程）与
    `sender_receiver_pipeline_roundtrip_in_process` 覆盖，本步骤仅确认，不新增。
  验证：`cargo test -p app`，新测试连跑 2 次确认不 flake。

- ⬜ 步骤 5（收尾）：`cargo fmt`；`cargo test --workspace --no-fail-fast`；
  `git status` 核对无越界文件；移除本 plan 文件并提交。

## e2e 说明（预判）

- 本地可执行：`crates/app` in-process 集成测试（步骤 3/4 新增，走真实
  `receiver_task_remote` + Sender 侧函数，只是用 in-process transport 代替真实 QUIC）；
  `tests/remote_process_e2e.rs`（已有，真实两进程 + 真实 loopback QUIC，覆盖 happy path
  与大文件 mux，不受本次改动破坏需回归确认）。
- 不能本地执行：**跨真实 OS 进程边界的人为 hash mismatch 注入**——需要在生产代码里加
  测试专用的“损坏钩子”才能在两个独立 `terrasync` 子进程间伪造 hash mismatch，属于
  scope creep，不做。redo/error 状态机的核心逻辑与真实两进程共享同一套
  `process_requests_and_acks`/`recv_file_list_and_data_phase`/`disk_commit_task`
  代码路径，仅传输层substitute 为 in-process channel，协议行为等价，由步骤 4 的
  in-process 测试覆盖。
