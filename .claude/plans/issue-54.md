# issue #54 阶段 2：delta_match 滚动窗口流式化 — 执行计划

## 分支基线

`origin/main` = `298f38c92b1d11b2a5cd43236fd5c91efe61253e`（阶段 1 已合入 PR #63）。
分支：`claude/issue-54-stage2`。

## 需求 & 目标（摘自 approved spec）

阶段 2 范围（issue #54 `## 建议 spec` + 维护者决策"维持 rsync 滚动窗口方式"）：

1. `crates/sync-delta/src/matcher.rs`：新增增量匹配器 `DeltaMatcher::new(signatures, block_size)`
   + `push(&[u8]) -> Vec<DeltaToken>` + `finish() -> Vec<DeltaToken>`，滚动哈希窗口状态必须跨
   push 边界保持——字节流在任意位置切分，输出 token 序列与整片 `delta_match` 完全一致。原
   `delta_match(src_data, signatures, block_size)` 改为薄封装，签名与语义不变，原有测试零改动
   通过。
2. `crates/app/src/remote_sync.rs::handle_delta_transfer`：sender 侧改为经
   `StorageEnum::read_chunk_stream`（intervals=None）流式读源文件、逐 chunk 喂 `DeltaMatcher`，
   token 边产出边经现有 `DeltaMatch`/`DeltaData` 消息发送，消除整文件 `read_file_from` 缓冲。
   wire 协议零改动、不 bump 版本。
3. 内存上界：matcher 内部暂存（字面量攒批 + 窗口）必须 O(block_size) 级有界，写容量上界测试
   （构造多次 push、周期性命中 basis block 证明暂存不随输入总量增长），参照阶段 1
   signature.rs 的容量证明测试写法。

## 验收标准

- TDD：先写跨 push 边界等价性测试并看失败——覆盖块边界处切分、窗口中间切分、1 字节一 push、
  匹配块紧邻字面量处切分、插入/删除平移场景、空文件/小于一块的文件；每个用例断言流式输出与
  整片 `delta_match` 的 token 序列逐项相等。
- 原有 `matcher.rs` / `reconstruct.rs` 测试零改动通过。
- `cargo test -p sync-delta`、`cargo test -p app` 全绿。
- `tests/remote_process_e2e.rs` 全量（16 个）回归通过。
- 禁 `.unwrap()`/`.expect()`（测试块可 allow）。
- `cargo fmt` 只格式化本次改动文件；提交前 `git status` 检查、revert 越界改动
  （尤其 `web-ui/package-lock.json` 漂移）。

## 执行步骤

- ✅ 步骤 0：读 spec、盘点现有代码（matcher.rs / signature.rs 参照模板 / rolling.rs /
  remote_sync.rs handle_delta_transfer / receiver.rs EntryError 语义），确认设计方案，立本计划。
- ✅ 步骤 1：`sync-delta/src/matcher.rs` 新增跨 push 边界等价性测试（先写测试，此时
  `DeltaMatcher` 尚未实现，预期编译失败/测试失败），覆盖 spec 列出的全部场景 + property 式
  固定种子随机切分对拍。
- ✅ 步骤 2：实现 `DeltaMatcher`（滚动窗口跨 push 状态机：carry 缓冲 + 惰性 window
  init/incremental update，处理不足一个 block 的暂停/续算），`delta_match` 改薄封装；跑通
  步骤 1 全部测试 + 原有 matcher.rs 测试零改动通过（`cargo test -p sync-delta`：41 passed）。
- ✅ 步骤 3：容量上界测试（周期性命中场景下 carry/literal_buf 不随 push 次数增长，
  `cargo test -p sync-delta`：42 passed）。
- ⬜ 步骤 4：`app/src/remote_sync.rs::handle_delta_transfer` 改为 `read_chunk_stream` 驱动
  `DeltaMatcher::push`/`finish`，复用 `read_chunk_stream` 自带 hash_handle 生成 `source_hash`
  （与 `handle_full_transfer` 同构），错误路径对齐（读失败发 `EntryError`，不 hang）；
  `cargo test -p app` 定向验证 delta 相关测试全绿（含读失败/redo 用例零改动通过）。
- ⬜ 步骤 5：收尾——`cargo fmt`（仅本次改动文件）、`cargo test -p sync-delta`、
  `cargo test -p app`、`tests/remote_process_e2e.rs` 全量回归（跑 2 次防 flake）、
  `git status` 检查无越界文件、移除本计划文件。

## 设计要点（DeltaMatcher 内部状态机）

- `carry: Vec<u8>` + `pos: usize`：待处理字节缓冲，处理完的前缀周期性 drain，保证不随
  push 次数累积增长（O(block_size) + 单次 push chunk 大小）。
- `window_init: bool`：标记 rc 是否已代表 `carry[pos..pos+bs)` 的有效 checksum；push
  边界处（数据不足以判定/滑动）时置 false，暂停处理，下次有足够数据时 `rc.init` 重新起算
  （数学上与增量 `update` 结果一致，仅性能差异，不影响正确性/等价性）。
- `literal_buf: Vec<u8>`：跨 push 累积未匹配字节，语义与整片版本完全一致（只在命中 block 或
  `finish()` 时 flush 为一个 `DeltaToken::Data`）——真实场景命中 block 会周期性 flush，
  保持有界；无命中的极端情形下与整片版本同样需要缓冲全部数据（算法固有特性，非本阶段引入的
  回退）。
- `finish()`：处理 carry 中残留（不足一个 block 的尾部）→ 全部转入 literal_buf → flush，
  语义对齐原 `delta_match` 尾部处理。
