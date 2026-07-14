# issue #57 执行计划：统一单/双进程退出码为报表驱动 + 双进程报表错误统计补齐 + 全量 walkdir 统计对齐

## spec 版本锚定

以 issue #57 最新一条「spec v3」评论为准（双进程也统一为报表驱动，entry 级失败 exit 0）。
v1/v2 已被覆盖，不采用。

## 分支基线

`origin/main` @ `f841e10893c608be99dcefdae478aeebb0963f48`
工作分支：`claude/issue-57`

## 需求 & 目标（摘自 spec v3）

1. **双进程退出码统一为报表驱动**：`remote_sync.rs::finalize_run_result` 改为恒
   `Ok(())`（保留函数体与 error_count>0 时的日志）；`AppError::RemoteSyncFailed`
   variant 随之无构造点——删除。web 层 `task_runner` 因此不再因 entry 失败标记
   `TaskStatus::Failed`（PR 描述需注明这一行为变化）。
2. **双进程报表错误统计补齐（必做耦合项）**：`remote_sync.rs::process_requests_and_acks`
   收到 `ReceiverMsg::Error{ndx}` / `EntryError{entry}` 时，以及 Sender 自检失败处
   （L324-328 全量、L356-372 delta）同样补翻译为
   `StorageEntryMessage::Error{event, path, reason}` 喂 `stats_consumer`。
3. **全量 walkdir 统计对齐**：`sender.rs` walkdir `Error` 分支（L143-150）补广播（与
   增量分支 `orchestrator.rs:1075-1084` 同模式），`sender_worker` 签名传入
   `broadcaster`，`orchestrator.rs` L513 起的 sender worker spawn 处传
   `broadcaster.clone()`。

## 验收标准

1. 单/双进程 entry 级失败（部分或全部）均 exit 0，语义一致。
2. 双进程终态报表 ERROR STATISTICS 与 HTTP 回调 `error_count` 如实非零（不再恒 0）。
3. 全量 walkdir 扫描错误进 `ErrorStats.scan`（经由广播喂入 `StatisticConsumer`）。
4. 致命错误（挂载失败/握手拒绝/协议不兼容等 setup 类 Err）保持 exit 非 0，不受影响。
5. workspace 全绿，fmt/check 干净。

## 关键设计决策（记录，供恢复时复用，不必重新推导）

- `ErrorEvent` variant 统一用 `Copy`（双进程模式下所有失败来源都发生在传输/复制阶段，
  不是扫描阶段；与 orchestrator.rs 本地 Receiver worker 的 `CopyEntry` 失败广播口径一致）。
- 新增 pure helper `entry_error_stats_message(path, reason) -> StorageEntryMessage`
  统一构造，4 个调用点复用，避免重复；可直接单测（同 `classification_to_stats_message`
  已有模式）。
- `count_ndx_error` 改为返回 `bool`（是否新计数），self-check 失败与 `ReceiverMsg::Error{ndx}`
  两处按同一 dedup 结果决定是否喂 stats，避免复合失败（同 ndx 双源触发）导致 ErrorStats
  与本地 `error_count` u64 计数不一致。
- **范围边界（明确排除）**：`ReceiverMsg::Redo{ndx}` 分支（约 L392-395）调用
  `handle_full_transfer` 失败时的 `count_ndx_error` 调用点，spec 未列入 L324-328/356-372
  引用范围，本次不补喂 stats（与 spec 逐行核实结果一致，非遗漏）。PR 描述会提及此已知边界，
  不在本 issue 范围内修。
- `path` 取不到 entry（`ReceiverMsg::Error{ndx}` 且 `ndx_table` 查不到）时用合成路径
  `PathBuf::from(format!("<ndx-{ndx}>"))`。
- e2e（`tests/remote_process_e2e.rs`）新增一个真实双进程测试：chmod 0o000 使某个源文件
  内容不可读（非 root 环境下确定性触发 Sender 自检读失败，不影响 walkdir 枚举），断言
  Sender 进程 exit 0 + stdout 报表 ERROR STATISTICS total 行非零 + 其余文件正常同步。
- 全量 walkdir 广播是单测（不是 e2e）：直接调用 `sender_worker` 喂一条
  `StorageEntryMessage::Error`，断言 broadcaster 订阅者收到同一条消息。

## 执行步骤

- ✅ step 0：立计划（本文件），commit。
- ✅ step 1+2（合并执行，两者测试相互依赖，分开提交会导致中间态测试失败）：
  `error.rs` 删除 `AppError::RemoteSyncFailed`；`remote_sync.rs::finalize_run_result`
  改为恒 `Ok(())`（保留 error_count>0 时的 warn 日志）；更新受影响的既有单测断言；
  新增 `entry_error_stats_message` helper + 单测；`count_ndx_error` 改为返回 `bool`；
  4 个调用点（TransferRequest 自检失败、DeltaTransferRequest 自检失败、
  `ReceiverMsg::EntryError`、`ReceiverMsg::Error{ndx}`）补喂 `stats_consumer`；
  `ReceiverMsg::Error{ndx}` 处补 ndx_table 路径查找 + 合成路径兜底；增强既有 pipeline
  测试暴露/断言 `ErrorStats`（`run_pipeline_with_disruption` 返回值带上 `stats_consumer`）。
  验证：`cargo check -p app --tests` 通过；`cargo test -p app --lib` 69 passed 0 failed；
  `cargo fmt -p app` 无额外改动。
- ✅ step 3：`sender.rs` walkdir `Error` 分支补广播（`ref` 绑定 + `broadcaster.broadcast(msg.clone())`）；
  `sender_worker` 签名新增 `broadcaster: &BroadcastForwarder<StorageEntryMessage>` 参数；
  `orchestrator.rs` L513 起 sender worker spawn 循环内 `let bc = broadcaster.clone();` +
  传参；新增 `sender.rs` 单测验证广播。
  验证：`cargo check -p app --tests` 通过；`cargo test -p app --lib` 70 passed 0 failed；
  `cargo fmt -p app` 无额外改动。
- 🔄 step 4：`tests/remote_process_e2e.rs` 新增双进程部分失败 e2e（chmod 0o000 触发一个文件
  自检读失败，断言 exit 0 + stdout ERROR STATISTICS total 非零 + 其余文件正常同步）；
  新增 `parse_error_stats_total` stdout 解析 helper。
  验证：`cargo test -p terrasync-rs --test remote_process_e2e -- partial_failure`
  （连跑 2 次确认不 flake）。
- ⬜ step 5：收尾——`cargo fmt --all -- --check`；
  `cargo test --workspace --no-fail-fast`（排除极慢的 `--all-features`，用默认 feature 集）；
  `cargo test -p terrasync-rs --test remote_process_e2e`（全量、连跑 2 次）；
  `git status` 核验无越界文件；移除本计划文件、单独 commit。
