# issue #26 执行计划：补充 rsync-like 远端同步端到端测试矩阵

## spec 来源

issue #26 上 2026-07-13 🤖 issue-triager「重 triage — VERDICT: REASONABLE」评论中的「建议
spec」（已 `claude:approved`，以此为准；2026-07-03 的旧 UNCLEAR 判定已过时，不采用）。

## 需求 & 目标

补齐"协议已实现但缺真实 2 进程 e2e"的测试缺口，**纯测试、零功能实现**
（rename/xattr/hash 协商/checkpoint 消费均为未实现功能，不在范围）。

影响范围：主要 `tests/remote_process_e2e.rs`；`crates/transport/tests/quic_roundtrip.rs`
可选补充协议层连接异常测试。

## 验收标准（逐条对照，完成时勾选）

- ✅ 1. delta sync 真实 2 进程 e2e ≥1 个通过。
- ✅ 2. symlink 真实 2 进程 e2e 通过。
- ✅ 3. resume（进程中断重跑）e2e ≥1 个通过。
- ✅ 4. transport 异常关闭测试 ≥1 个通过（不 hang、非零退出码或明确错误）。
- ⬜ 5. uid/gid 有断言覆盖（非 root 环境限制：无法测试"跨用户 chown 成功"，只能验证
      字段经 wire 正确传播且落地值与源端一致——运行进程本身的 uid/gid）。
- ⬜ 6. packaged/tar 适用性结论写入 PR 描述：**不适用**。`crates/app/src/remote_sync.rs`
      的 Sender 侧 `walkdir_2(...)` 调用不传递 `config.packaged`/`package_depth`
      （对照 `crates/app/src/dir_walker.rs` 单进程路径会用这两个字段驱动打包逻辑），
      `SenderMsg::TarPacked` 只在单进程 `crates/app/src/sender.rs` 里被构造/发送；
      `--remote` 模式下 `--packaged` 标志被静默忽略，未接线到双进程协议。不强行造场景。
- ⬜ 7. `cargo test --workspace --no-fail-fast` 保持全绿。

## 明确排除

ACL/xattr（#24）、Renamed（未实现）、hash 算法协商（deferred）、checkpoint 死代码处置
（#25）、真实跨进程字节损坏注入（维持 #48 的 scope creep 结论）。

## 分支基线

`origin/main` @ `98e5d331ae162355c4c7ec3e14c8815dccd11b8f`（含 PR #49）。
分支：`claude/issue-26`。

## 关键实现依据（读码确认）

- Delta 触发：`DestIndex::check()`（`crates/transport/src/message.rs`）比较
  `data_check`（mtime+size）+ `metadata_check`（mode/uid/gid），data 不一致即走
  `TransferDecision::DeltaTransfer`；receiver.rs 在 `negotiated_features.delta==true`
  时发真实 `ReceiverMsg::DeltaTransferRequest`。两端同二进制，握手后 `delta` 恒为
  `true`（`FeatureFlags::default().delta = true`，协商是 AND）。非侵入式证据：
  Sender 日志 "Handshake accepted, negotiated features" 含 `delta: true`
  + Sender stdout 最终报表 `IncrementalStats::fmt` 打印的 "Changed:" 行 total ≥ 1
  （`StatisticConsumer::finalize()` 用 `println!("{}", self.stats)`，assert_cmd 可
  从 `Assert::get_output().stdout` 读到）。
- resume：双进程 `disk_commit.rs::FileBegin` 硬编码
  `StorageEnum::resume_prepare(&dest, &entry, &part_path, false)`（`resume=false`），
  即真正的字节级断点续传在双进程模式**未接线**（此为 #25 处置范围，非本 issue）；
  本 issue 的"resume e2e"验收标准是"中断重跑后最终收敛"，不要求验证字节级续传。
  中断产物是 `.terrasync-part`（`part_path_for`，见 `crates/app/src/byte_resume.rs`），
  最终文件名在完整落盘前不存在（`disk_commit.rs::finalize_file` 原子 rename 后才
  出现），可用作"确实被中断"的确定性断言，避免测试假阳性。
  用 `--qos` 限速（`crates/cli/src/commands_enum.rs` 已有 `--qos`/`--peak-qos-rate`，
  `parse_bandwidth_string` 支持到 `KiB/s`）制造足够宽的中断窗口，避免 sleep+kill 时序
  竞争。
- transport 故障注入：kill Receiver 后 Sender 各 reader task（`quic/mux.rs::reader_loop`）
  读流出错/EOF 会各自 drop 手上的 `tx` clone，待 4 条都 drop 后
  `sender.recv()`（`UnboundedReceiver::recv`）返回 `None`，不 panic。等待用已有
  `wait_for_child_exit` + 复用 `RECEIVER_EXIT_TIMEOUT`（quinn 默认 `max_idle_timeout`
  兜底 30s，故超时留足 35s）。
- uid/gid：`disk_commit.rs::finalize_file` 与 `DiskCommitMsg::CreateDir` 分支均调用
  `dest.set_entry_metadata(&entry)`，透传 `NASEntry.uid/gid`（来自源端 scan，经 wire
  传输，Receiver 不做本地 stat）。非 root 环境下无法构造"不同 uid"的源文件（无
  `chown` 权限），断言退化为"dest uid/gid == src uid/gid"（均等于运行进程 euid/egid），
  但仍验证了字段传播路径未损坏。
- packaged/tar：见验收标准第 6 条结论。

## 执行步骤

- ✅ 步骤 0：立计划文件（本文件），commit `chore(plan): issue-26 执行计划`。
- ✅ 步骤 1：`tests/remote_process_e2e.rs` 新增真实 2 进程 delta sync e2e
      （两轮同步：第一轮播种 dest，修改 src 文件中间一段字节保留首尾不变，第二轮
      断言走 `DeltaTransferRequest` 的非侵入式证据 + 最终内容一致）。
      验证：`cargo test -p terrasync-rs --test remote_process_e2e delta_sync -- --nocapture`。
- ✅ 步骤 2：新增 symlink 真实 2 进程 e2e（专用小数据集，含 target 文件 + 相对路径
      symlink，断言 dest 侧 symlink 类型 + `read_link` 目标一致 + 内容可解引用读取）。
      验证：`cargo test -p terrasync-rs --test remote_process_e2e symlink -- --nocapture`。
- ✅ 步骤 3：新增 resume e2e（`--qos` 限速 + 大文件，中途 kill Sender，断言最终文件名
      未落盘 → 相同 src/dest 重跑（不限速）→ 断言 dest 与 src 最终一致）。
      验证：`cargo test -p terrasync-rs --test remote_process_e2e resume -- --nocapture`
      （连跑 2 次确认不 flake）。
- ✅ 步骤 4：新增 transport 故障注入 e2e（`--qos` 限速 + 大文件，中途 kill Receiver，
      断言 Sender 在超时内非零退出、不 hang）。
      验证：`cargo test -p terrasync-rs --test remote_process_e2e receiver_killed -- --nocapture`
      （连跑 2 次确认不 flake）。
- ⬜ 步骤 5：新增 uid/gid 断言 e2e（全量同步后断言 dest 各文件 `MetadataExt::uid()/gid()`
      与 src 一致，注释说明非 root 环境限制）。
      验证：`cargo test -p terrasync-rs --test remote_process_e2e uid_gid -- --nocapture`。
- ⬜ 步骤 6（可选项，spec 明确标注可选）：`crates/transport/tests/quic_roundtrip.rs`
      新增连接异常测试：Receiver 侧提前 `conn.close()`，断言 Sender
      `sender.recv()` 在超时内返回 `None` 而非 panic/hang。
      验证：`cargo test -p transport --features quic -- --nocapture`。
- ⬜ 步骤 7：收尾——`cargo fmt --all -- --check`、
      `cargo test -p terrasync-rs --test remote_process_e2e -- --nocapture`（全量新老用例）、
      `cargo test -p transport --features quic`、`cargo test --workspace --no-fail-fast`、
      `git status` 确认无越界文件，验收标准逐条勾选，移除本计划文件并 commit。

## 端到端验证要求

本 issue 本身就是"补 e2e"，因此步骤 1-6 新增的进程级测试**本身就是**要交付的 e2e 覆盖，
无需额外再造一层。`crates/transport/tests/quic_roundtrip.rs` 是同进程协议层测试，不算
"跨进程 e2e"，仅作为步骤 4 的协议层补充证据（可选项）。所有新增测试均可在当前开发环境
（本地 loopback，无外部 NFS/S3/CIFS 依赖）直接运行，无不能本地执行的 e2e。
