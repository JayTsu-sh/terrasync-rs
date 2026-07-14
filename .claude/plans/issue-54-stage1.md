# issue #54 阶段 1 执行计划（签名生成流式化）

## 需求 & 目标（摘自 approved spec 阶段 1 部分 + 维护者决策）

delta 路径当前 4 个整文件缓冲点之一：Receiver 为 `DeltaTransferRequest` 生成 basis
签名时，先 `StorageEnum::read_file_from` 整读 basis file 到内存，再一次性
`compute_block_signatures(&[u8], block_size)` 切块算签名。本阶段消灭这一整读：

1. `sync-delta::signature` 新增增量状态机（`push(&mut self, bytes: &[u8])` 逐块喂入 +
   `finish(&mut self) -> Vec<BlockSignature>` 收尾，形态参照仓库 `HashCalculator`
   push/finish 增量模式），内部只保留 partial-block staging buffer（容量上界
   `O(block_size)`，不随 push 调用次数增长——构造性证明，写测试断言）。crate 保持纯算法
   零 IO/async 依赖。
2. 原 `compute_block_signatures(&[u8], block_size)` 保留为薄封装（内部 `new` + `push` +
   `finish`），现有 5 个单测零改动通过（重构只改结构不改语义）。
3. `crates/app/src/receiver.rs` 签名生成调用点（`TransferDecision::DeltaTransfer` 分支，
   `origin/main` 50b4f27 上在 :648-681 区域，#59 credit 流控合入后行号已漂移，编码时
   现场定位）：basis 读取从 `read_file_from`（整读）改为 `read_chunk_stream`
   （`intervals=None` 全文件流式）驱动新状态机逐块喂入，消灭该调用点的整文件缓冲。
4. 新增跨 chunk 边界单测（block 边界恰好落在 chunk 边界 / 跨 chunk / chunk 小于
   block_size / 多次小块 push / 空文件 / 单字节），新状态机输出与原整块函数输出逐字节
   等价（rolling + strong 双字段比对）。

## 不做（阶段边界，spec 明确排除）

- matcher（delta_match 滚动哈希）流式化 —— 阶段 2。
- reconstruct 流式化 + pread 化、delta 接入 disk_commit —— 阶段 3。
- 任何协议改动（`signatures` 表 wire 形态不变，无需 bump 版本）。
- `remote_sync.rs`（Sender 侧源文件流式读已在早前 issue 完成，不属本阶段）。
- delta 算法方向：维持 rsync 滚动窗口（阶段 2 的事），本阶段 API 设计不得与之冲突——
  `push`/`finish` 是纯顺序流式接口，不预设滚动窗口跨 push 状态，不冲突。

## 验收标准

- `cargo fmt --all -- --check` 干净。
- `cargo test --workspace --no-fail-fast` 全绿（含 sync-delta 现有 25 测原样通过）。
- `cargo test --test remote_process_e2e` 全过（15 个，连跑 2 次不 flake）。
- 状态机 staging buffer 容量上界断言测试（构造性证明，不随 push 次数增长）。
- 跨 chunk 边界等价性测试通过。

## 分支基线

`origin/main` @ `50b4f271e2ac74413aeedbf258de7e71ec7dacda`（含 #59 credit 流控）

## 关键设计确认（编码前调研结论）

- `sync-delta` crate 无 `error.rs`（纯算法、无 IO、无 Result 使用），维持现状，不新增。
- `receiver.rs` 现有代码对 `sync_delta::` 路径统一直接三段式书写
  （如 `sync_delta::signature::compute_block_signatures`、
  `sync_delta::matcher::delta_match`），无顶层 `use sync_delta::...`；新调用点沿用同一
  风格（`sync_delta::signature::SignatureCalculator::new(...)`），保持文件内一致性。
- basis 读取（`read_chunk_stream`）沿用 `enable_integrity_check=false`（原
  `read_file_from` 路径本就不算文件级 hash，签名本身就是完整性依据）、`qos=None`
  （现有 `read_file_from` 调用同样不传 qos），`capacity=8`（与 `remote_sync.rs` 源文件
  流式读点同量级）。
- basis 读失败的错误处理与原逻辑保持一致：`warn!` 日志 + 降级为
  `ReceiverMsg::TransferRequest` 全量传输（不中断整个接收循环）；流式读的错误来源
  （`JoinHandle` 内部 `StorageError` / `JoinError`）统一 `.to_string()` 归一后按原文案
  打日志，与 `remote_sync.rs` 源文件流式读点（:632-635）已有归一模式一致。
- `DataChunk.data: bytes::Bytes` 通过 deref coercion 传给 `push(&self, bytes: &[u8])`。

## 执行步骤

- ✅ 步骤 0：立计划，commit（本文件）。
- ✅ 步骤 1：`sync-delta/src/signature.rs` 实现 `SignatureCalculator`（`new`/`push`/
  `finish`，staging buffer 容量恒为 `block_size`），`compute_block_signatures` 改写为
  薄封装。验证：`cargo test -p sync-delta`（现有 25 测零改动全绿）。
- ⬜ 步骤 2：`sync-delta/src/signature.rs` 新增跨 chunk 边界等价性测试（对齐边界/跨
  block 的 chunk/小于 block_size 的 chunk/多次小块 push/空文件/单字节，逐字段比对
  streamed vs whole-buffer 输出）+ staging buffer 容量上界断言测试。验证：
  `cargo test -p sync-delta`。
- ⬜ 步骤 3：`crates/app/src/receiver.rs` `TransferDecision::DeltaTransfer` 分支改用
  `StorageEnum::read_chunk_stream` 驱动 `SignatureCalculator` 逐块喂入，替换
  `read_file_from` 整读；错误路径保持"降级全量传输"语义不变。验证：
  `cargo check -p app` + `cargo test -p app`（delta 相关测试，如
  `delta_transfer`/`redo` 关键字过滤）。
- ⬜ 步骤 4：收尾核验 —— `cargo fmt --all -- --check`；
  `cargo test --workspace --no-fail-fast`；`cargo test --test remote_process_e2e`
  （连跑 2 次不 flake）；`git status` 确认无越界文件；移除本计划文件并单独 commit。

## 端到端验证安排

- 本地可执行：复用 `tests/remote_process_e2e.rs`（真实进程 `serve`+`sync`，loopback
  QUIC），delta/credit 相关用例覆盖签名生成路径改动无回归（happy path，无需额外新增
  e2e 用例——本阶段是内部实现替换，wire 协议/可观测行为不变，现有 e2e 即是覆盖）。
- 不能本地执行：无（本阶段无外部存储/网络依赖新增，签名生成读取的是 Receiver 本地
  dest_storage，e2e harness 已覆盖 Local 后端）。
