# issue #54 阶段 3：reconstruct 流式化 + delta 重建接入 disk_commit

## 需求 & 目标（摘自 approved spec）

- 基线：main 7dc1d9a（阶段 0/1/2 已合入：`SignatureCalculator`/`DeltaMatcher` 流式状态机就位）。
- 消灭最后一个隐性整文件缓冲点：delta 重建目前完全不走 `disk_commit` 三段式——receiver
  接收循环把整个文件的 token 攒满 Vec 后才一次性 `reconstruct` + 整文件写
  (`write_file_from_bytes`)。
- pread 原语已存在：`StorageEnum::read_chunk_stream(entry, intervals: Option<Vec<(u64,u64)>>, ...)`，
  basis 随机块读直接用，不需要动 data-mover。

## 范围

1. `crates/sync-delta/src/reconstruct.rs`：新增 `Reconstructor` + `ReconstructStep`，逐 token
   消费、产出待写字节流或"需要 basis 区间"描述，不再要求整片 `basis_data`/token Vec。原
   `reconstruct(basis_data, tokens, block_size)` 改薄封装，原有测试零改动通过。
2. `crates/app/src/disk_commit.rs`：`DiskCommitMsg` 已预留的 `DeltaBegin`/`DeltaData`/
   `DeltaMatch`/`DeltaCommit` 变体正式接入——复用 `ActiveFile`/`write_chunk_stream`/
   `finalize_file` 三段式，basis 块按需经 `read_chunk_stream(intervals=Some(...))` 读取。
3. `crates/app/src/receiver.rs`：delta 路径改为逐 token 转发给 disk-commit task（不再攒
   `delta_tokens: Vec`），移除 `handle_end_of_file`（inline 重建路径），EndOfFile 统一经
   `dispatch_file_outcome`/`decide_file_ack`（全量/delta 共用同一状态机，语义不变）。
4. wire 协议（`SenderMsg`/`ReceiverMsg`）零改动；`DiskCommitMsg`（进程内 mpsc，非
   `Serialize`/`Deserialize`）内部新增 `ndx` 字段不算 wire 改动。

## 关键设计决策

- `Reconstructor` 保持 sync-delta"零 IO"：`push(&self, token: &DeltaToken) -> ReconstructStep`
  纯计算（`Ready(Bytes)` 直接可写 / `NeedBasis{offset,len}` 需调用方读 basis 区间），
  basis 边界钳制语义与原整片函数逐字节一致（`offset>=basis_size` → 空；否则
  `end=min(offset+bs,basis_size)`）。
- disk_commit 侧新增 `DeltaCtx{ndx, reconstructor, write_pos}`：`write_pos` 为已写入输出流的
  字节数（driving 输出端 `DataChunk.offset`，与 basis 偏移无关）；basis 读失败按 ndx 上报
  `FileOutcome::HardError` 并中止该 ActiveFile（同 AbortFile 语义）。
- receiver 侧不再需要"整文件 Vec 攒批"，改用 `delta_active: bool`（镜像现有 `full_active`）
  + `ensure_delta_active`：首个属于该 ndx 的 delta 数据事件（token 或 EndOfFile）才触发一次
  `DeltaBegin`——避免 `FilePage` 阶段流水线化发出多个 `DeltaTransferRequest`（早于任何响应
  到达）导致 dc_tx 收到乱序 `DeltaBegin`。依据：Sender 侧 `process_requests_and_acks` 单一
  消费者循环严格串行处理每个 ndx 的数据阶段（无并发 spawn），故 Receiver 单一收消息循环里
  首个属于某 ndx 的 delta 数据事件必然对应"当前正在流的文件"。
- `entry.get_size()`（= 源端新 size）在 basis 读取路径中的角色与阶段 0/1 保持一致（bound
  block_size 计算、basis 有效区间钳制、`ActiveFile.size` 期望落盘 size 均为同一值）。

## 执行步骤

- ✅ 1. sync-delta：`Reconstructor`/`ReconstructStep`，`reconstruct()` 薄封装，等价性测试
     （纯字面量/纯匹配/交错、token 任意分组、basis 乱序/重复引用、空文件/小于一块）+
     容量上界证明测试。`cargo test -p sync-delta`（51 passed，原有测试零改动通过）。
- ⬜ 2. transport::message：`DiskCommitMsg::DeltaBegin`/`DeltaCommit` 加 `ndx: i32` 字段
     （进程内 channel，非 wire）。`cargo check -p transport`。
- ⬜ 3. app::disk_commit：`ActiveFile` 加 `delta: Option<DeltaCtx>`；接入
     `DeltaBegin`/`DeltaMatch`/`DeltaData`/`DeltaCommit` 四个分支 + `push_delta_token`/
     `read_basis_block` helper；`finalize_file` 复用不变。`cargo check -p app`。
- ⬜ 4. app::receiver：移除 `delta_tokens`/`handle_end_of_file`，接入 `delta_active` +
     `ensure_delta_active`，`DeltaMatch`/`DeltaData`/`EndOfFile` 分支改为转发 dc_tx。
     `cargo test -p app`（含全部 delta 相关 in-process 集成测试）。
- ⬜ 5. 收尾：`cargo fmt`（仅本次改动文件）、`cargo test -p sync-delta` / `-p app` 全量、
     `cargo test -p terrasync-rs --test remote_process_e2e` 连跑 2 次、`git status` 检查越界
     文件、移除本 plan 文件。

## 验证命令

- `cargo check -p sync-delta -p transport -p app`
- `cargo test -p sync-delta`
- `cargo test -p app`
- `cargo test -p terrasync-rs --test remote_process_e2e`（16 个，连跑 2 次）
- `cargo fmt`（仅改动文件）
