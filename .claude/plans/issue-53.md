# issue #53 执行计划（PR-B：本仓库，size 断言防线）

## 需求 & 目标（摘自 approved spec PR-B 部分）

根修已在 data-mover（PR-A'，rev `bb8b60b`，真实 NFS 验证通过）。本仓库加一道与根因
无关的 defense-in-depth：全量/delta 两条落盘提交路径，在原子提交/写入前断言实际字节
数 == entry 声明大小，不符走既有 Redo 路径。这层拦截的是"hash 同源失明"（截断发生在
hash 计算之前，hash 自洽通过，拦不住）以及 `enable_integrity_check=false` 场景——与根
因在哪层修复无关。

## 验收标准

- `cargo fmt --all -- --check` 干净；`cargo test --workspace --no-fail-fast` 全绿
  （含现有 delta redo 状态机测试无回归）。
- 本地主路径 e2e：`cargo test --test remote_process_e2e` 全过。
- `Cargo.lock` 中 data-mover rev = `bb8b60b`，nfs-rs rev 保持 `de1e0e4` 不变。
- 真实 NFS 人工验收由编排层在交付后执行（不在本计划范围内）。

## 分支基线

`origin/main` @ `98e5d331ae162355c4c7ec3e14c8815dccd11b8f`

## 关键设计确认（编码前调研结论）

- `FileOutcome`（`transport::message`）只 `#[derive(Debug)]`，不参与 wire 序列化
  （`DcAck`/`FileOutcome` 是 disk-commit task ↔ Receiver 主 task 的进程内消息），新增
  variant 不影响协议。
- 新增 `FileOutcome::SizeMismatch`，复用现有"首次 Redo、二次 Error"计数状态机（提取
  `redo_or_error` 小 helper 消除 `HashMismatch`/`SizeMismatch` 两个 match 分支的重复
  计数逻辑，不改变 `HashMismatch` 原有行为/文案）。
- data-mover `commit_chunk_stream`（NAS 分支）内部会 `set_file_len(part_path, size)`
  强制把 `.part` 补齐/截断到声明大小再 rename——即截断的 `.part` 若不在 commit 前拦截，
  会被静默零填充后当作"大小正确"提交，产出大小对、内容错（尾部为零）的文件。因此新
  断言必须在 `commit_chunk_stream` 调用**之前**。
- 全量路径 size 断言：`disk_commit.rs::finalize_file`，位置在既有 hash 校验块之后、
  `commit_chunk_stream` 调用之前——只在 hash 校验关闭或未拦截到问题时才会触达，天然是
  独立防线。
- delta 路径 size 断言：`receiver.rs::handle_end_of_file`，位置在 hash 校验块之后、
  `write_file_from_bytes` 调用之前。

## 执行步骤

- ✅ 步骤 0：`cargo update -p data-mover`，确认 `Cargo.lock` data-mover rev = `bb8b60b`，
  nfs-rs rev 不变（`de1e0e4`）。单独 commit。
- ⬜ 步骤 1：`transport::message` 新增 `FileOutcome::SizeMismatch` variant + doc 注释；
  `app/receiver.rs::decide_file_ack` 提取 `redo_or_error` helper 并接入新 variant
  （首次 Redo、二次 Error，与 HashMismatch 语义一致）。定向验证：
  `cargo check -p transport -p app`。
- ⬜ 步骤 2：`disk_commit.rs::finalize_file` 加 size 断言（`get_metadata(&part_path)`
  实际大小 vs `entry.get_size()`，不符发 `FileOutcome::SizeMismatch` 并 `remove_part`
  后返回；`get_metadata` 本身出错走 `HardError`，与既有 hash 读回错误处理一致）。
  定向验证：`cargo check -p app`。
- ⬜ 步骤 3：`receiver.rs::handle_end_of_file` 加 size 断言（`file_bytes.len() as u64`
  vs `entry.get_size()`，不符返回 `FileOutcome::SizeMismatch`）。定向验证：
  `cargo check -p app`。
- ⬜ 步骤 4：`app/receiver.rs` 单测：`decide_file_ack` 新增 `SizeMismatch` 首次 Redo /
  二次 Error 两个纯函数单测（参照现有 HashMismatch 单测）。验证：
  `cargo test -p app decide_file_ack -- --nocapture`。
- ⬜ 步骤 5：`app/remote_sync.rs` 测试模块新增 `SizeTruncationInjector`（按目标文件相对
  路径截断首个匹配 `FileData` chunk 为一半长度并停发该 attempt 剩余 chunk，改写
  `EndOfFile.source_hash` 为对截断后转发字节重新计算的自洽 hash，复现"同源失明"）+ 两
  个 pipeline 测试：
  - `full_transfer_size_mismatch_redo_recovers_with_integrity_check`
    （`enable_integrity_check=true`，自洽 hash 通过 hash 校验，验证 size 断言独立拦截
    并首次 Redo 恢复成功）。
  - `full_transfer_size_mismatch_redo_recovers_without_integrity_check`
    （`enable_integrity_check=false`，验证 hash 校验关闭时断言同样生效）。
  验证：`cargo test -p app size_mismatch -- --nocapture`。
- ⬜ 步骤 6：`app/remote_sync.rs` 测试模块新增 `DeltaTruncationInjector`（按目标 ndx 丢弃
  该次传输首个 `DeltaMatch`/`DeltaData` token，不改写 hash）+ 1 个 pipeline 测试
  `delta_transfer_size_mismatch_redo_recovers_without_integrity_check`
  （`enable_integrity_check=false`，覆盖 `receiver.rs::handle_end_of_file` 路径，验证
  首次 Redo 降级全量重发恢复成功）。验证：`cargo test -p app size_mismatch -- --nocapture`。
- ⬜ 步骤 7：收尾核验——`cargo fmt --all -- --check`；
  `cargo test --workspace --no-fail-fast`（全绿，含现有 delta redo 状态机测试无回归）；
  `cargo test --test remote_process_e2e`（连跑 2 次不 flake）；`git status` 确认无越界
  文件；移除本计划文件并单独 commit。

## 端到端验证安排

- 本地可执行：复用 `tests/remote_process_e2e.rs`（真实进程 `serve`+`sync`，loopback
  QUIC），覆盖本次改动不影响的主路径（happy path，无截断注入）。
- 不能本地执行：真实 NFS 场景下的短读修复人工验收（`10.131.9.12/13`，>1MB 文件 + 中间
  改 4KB + `--enable-integrity-check`）——需要真实 NFS 服务器，由编排层在 PR 交付后
  执行，结果记入 PR「端到端验证」小节。size 断言本身的单测覆盖不依赖外部环境（in-process
  transport 注入）。
