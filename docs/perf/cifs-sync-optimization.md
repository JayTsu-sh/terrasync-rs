# CIFS/SMB sync 性能优化计划

> 来源：分析 `target/release/logs/20260511_145649/app.log`（CIFS→CIFS sync）后形成的优化方案。
> 适用范围：terrasync-rs + data-mover-rs + smb-rs 三层。
> 持久化日期：2026-05-11。

## Context

分析对象：`target/release/logs/20260511_145649/app.log`（CIFS → CIFS sync，源 `smb://...@10.131.7.203/jay_cifs1`，目标 `smb://...@10.131.7.203/jay_cifs2`）。

**实际负载**：3 个目录 (`a`, `a/b`, `a/b/c`) + 1 个文件 (`a/b/c/d.txt`)，但总耗时约 **8 秒**（06:56:51.808 → 06:56:59.859）。

**核心问题**：连接建立时间几乎吞掉全部 wall-clock。日志统计：

| 阶段 | 时间 |
|------|------|
| dir_walker 阶段连接 | 52.080–52.151s（单连接 ≈ 70 ms） |
| Receiver workers 串行 mount（8 × 2 share，受 `STORAGE_PAIR_MOUNT_CONCURRENCY=2` 限流） | 52.495–58.551s ≈ 6 秒 |
| Sender workers 串行 mount | 54.758–58.638s ≈ 4 秒 |
| 实际数据复制 | 55.5–58.6s（实际 mkdir + put 1 个 4-byte 文件 ≈ 3 秒） |

**根因诊断**：

1. **mount semaphore 把握手串行化**：全局 `STORAGE_PAIR_MOUNT_CONCURRENCY=2`（`orchestrator.rs:360`）把 16 个本可并行的握手切成 8 批，每批 ~70 ms × 8 ≈ 6 秒。注释明确说"防止 portmapper 过载"——这是 NFS 特有（特权端口 + TIME_WAIT），CIFS/SMB **不存在 portmapper / 特权端口约束**，限流纯属误伤。
2. **每 worker 独立 Client 是正确设计，不该改**：smb-rs `Client` 内部是 `ParallelWorker` + 单一 outgoing channel + 响应路由 DashMap + per-session credits semaphore（`smb_client.rs:286-355` + `connection/worker/parallel/base.rs:254-302`）。若把多个 worker 共享同一 Client，所有 SMB 请求都经过同一个 async_backend 队列，丢失硬件并行度，锁竞争随 worker 数线性放大。
3. **stats_reporter 误报**：line 1180 硬编码 10s interval，启动 10ms 后就报 `Processed 0 entries (0 bytes) in 10s`（log line 109）—— 因为 reporter 的 `active_tasks: 0` 没考虑"还没 mount 完"这个状态。
4. **`create_dir_all` 逐级串行 mkdir**（`cifs.rs:878-895`）—— 3 层目录 = 3 个 round-trip，无缓存；多 receiver 重复对 `a/b/c` 发 mkdir（log line 2358, 2565）。
5. **读写块大小固定 2 MB**（`cifs.rs:138, 417`），现代 SMB3 server 通常支持 8 MB+ 单次 read/write；`read_data` (`cifs.rs:707-744`) 串行 read_at → tx.send → 下一次 read_at，**没有 inflight pipeline**。
6. **multi-channel 默认关闭**（log line 4776 `Multi-channel is not enabled in client configuration. Skipping setup`），单 NIC 单连接，浪费多核 + 多链路潜力。

**预期收益**：
- 单作业冷启动从 ~10 秒压到 ~2 秒（取消 NFS 风格 mount 限流，让 16 个握手并行 ≈ ~5.5s 节省）。
- **运行期并发不退化**：仍是 per-worker 独立 Client = 独立 async_backend，真硬件并行；不引入任何共享锁。
- 中大文件场景吞吐 2–4×（pipeline read + 8 MB 块 + multi-channel）。
- 大量小目录/小文件场景：mkdir cache 把目录层级开销从 N×depth 压到 ~N。
- handle lease 命中时，重复 open/close 同路径省 1 个 Create RT。

---

## 优化清单（按 ROI 排序）

### P0：取消 CIFS mount semaphore 限流（保留 per-worker 独立 Client）

**设计前提（明确不走的路）**：
- **不引入全局连接池 / Client pool**：smb-rs `Client` 内部是 `ParallelWorker` + 单一 outgoing 消息 channel + 响应路由 DashMap + per-session credits semaphore（`smb_client.rs:286-355` + `connection/worker/parallel/base.rs:254-302`）。多 worker 共享同一 Client = 全部 SMB 请求经过同一个 async_backend 队列，丢失硬件并行度，且 DashMap / credits 加锁竞争随 worker 数线性放大。
- **不做 StoragePair 单例**：理由同上，会形成单点队列。
- **不做 walker storage 让渡**：单握手收益 50–70 ms（在 P0 解锁限流后才可见），与改 dir_walker 接口的复杂度不匹配，明确不做。
- **保留"一个拷贝 worker = 一份独立 CifsStorage = 独立 Client = 独立 async_backend"** —— 这才是高并发 sync 的正确并发模型，与目前架构一致。

**问题诊断（不是连接数太多，是握手被人为串行化）**：
- 当前 17 次握手对 CIFS server 来说**完全不构成压力**（SMB 协议设计支持上千客户端同时连接）；性能问题是 **`STORAGE_PAIR_MOUNT_CONCURRENCY=2`** 把 16 个本可并行的握手切成 8 批，每批 ~70 ms × 8 批 ≈ 6 秒（与日志吻合）。
- `STORAGE_PAIR_MOUNT_CONCURRENCY` 注释明确说"防止 portmapper 过载"—— 这是 NFS 特有问题（`nfs.rs:510-555`：Windows 上 nfs-rs 绑特权端口失败后 TIME_WAIT，需限速避免端口耗尽）。**SMB 不存在 portmapper / 特权端口约束**，限流纯属误伤。

**改造方案**（仅 terrasync-rs 上层改动，不动 data-mover-rs 与 smb-rs）：

1. **按 storage 类型选择 mount semaphore 容量**（`crates/app/src/orchestrator.rs:360` 附近）：
   ```rust
   fn mount_concurrency(path: &str) -> usize {
       if path.starts_with("nfs://") { 2 }       // 维持 portmapper 限流
       else if path.starts_with("smb://") { 32 } // 实际放开（远大于典型 copy_concurrency=8）
       else { 32 }
   }
   // 取 min：双方都受限时，按更紧的那一侧走
   let mount_sem_cap = std::cmp::min(mount_concurrency(&c.src_path), mount_concurrency(&c.dest_path));
   let mount_semaphore = Arc::new(Semaphore::new(mount_sem_cap));
   ```
   - 单向 NFS↔CIFS 混合场景按 NFS 侧的限制走（取 min）。
   - 8 worker × 2 share 同时握手 → 16 并行 → wall-clock ~100 ms（单握手 RTT），相比 6 秒提升 ~60×。

2. **重试逻辑保留**：`create_storage_pair_with_retry`（`orchestrator.rs:110-157`）的指数退避 + semaphore 包装保持不变，仅 semaphore 容量解锁。

**预期收益**：
- 16 个握手从串行 6s → 并行 ~100 ms，**冷启动节省 ~5.5 秒**（这就是日志里大头）。
- 运行期吞吐**无任何影响**（仍 per-worker 独立 Client 真并发）。

**关键差异 vs 池化方案**：

| 维度 | 现状（限流串行） | 池化 / 单例（已否决） | **P0 推荐** |
|------|------|------|------|
| 握手数量 | 17 次串行 | 1–2 次 | 17 次**并行** |
| 握手 wall-clock | ~6s | ~100ms | **~100ms** |
| 运行期并发模型 | 16 独立 Client（真并行） | 1 共享 Client（async_backend 单点） | **16 独立 Client（真并行）** |
| 锁竞争 | 无 | 有（DashMap + credits） | **无** |
| 代码改动 | – | 大（动 smb-rs 或 data-mover 内部） | **小（仅 orchestrator 单点）** |

---

### P1：stats_reporter 启动期屏蔽 + warmup

**问题文件**：`crates/app/src/orchestrator.rs:1180-1197`

```rust
let mut interval = tokio::time::interval(Duration::from_secs(10));
// ...
if active_tasks != copy_concurrency { warn!(...); }
```

**改造方案**：
- 在 stats 状态里加 `mount_completed: bool` 旗标（mount_done_senders + mount_done_receivers == 2 × copy_concurrency 时置 true）。
- mount 未完成时，stats_reporter 只打 INFO 不打 WARN，或者跳过首条 tick。
- 把 `Duration::from_secs(10)` 提到 `consts.rs` 命名为 `STATS_REPORT_INTERVAL`，附 doc 说明。

**预期收益**：消除误报噪音，运维更易识别真问题。

---

### P1：CifsStorage 目录存在性缓存 + create_dir_all compound

**问题文件**：`~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs:878-895` `create_dir_all` / `:521-543` `mkdir_or_open`

**改造方案**：
- 在 `CifsStorage` struct 加入 `dir_exists_cache: Arc<DashSet<String>>`，记录已确认存在的相对路径。
- `create_dir_all` 入口先查缓存命中则直接返回；mkdir 成功 / `STATUS_OBJECT_NAME_COLLISION` 都加入缓存。
- 缓存**仅限当前 storage 实例内**（per-worker），不引入跨 worker 共享 —— 与 P0 设计原则一致。同一 worker 内多次写入同一目录树时命中率仍然高（典型 sync 任务 worker 串行处理一批文件，文件 path 局部性强）。
- 进阶：探测 smb-rs 是否支持 SMB2 compound request（同一 send 内 chain `Create(directory_file) + Close`）批量提交，能进一步减少 RT。当前 `client.create_file` 一次只提交一个请求，需在 smb-rs 层加 API。

**smb-rs 配套**（可选）：
- 在 `crates/smb/src/client/smb_client.rs` 加 `compound_create_close(unc_paths: &[(UncPath, FileCreateArgs)])`，利用 SMB2 `NextCommand` 字段把多个 Create 打包成一次网络写。
- 已有 transformer/worker 框架（log 中可见 `compress: true`）支持 chained 编码，主要工作在 `OutgoingMessage` 序列化。

**预期收益**：N 层目录从 N RT → 1–2 RT（缓存）或 N/3 RT（compound）。

---

### P1：SMB Handle Lease — 等价于 nfs.rs 的 file-handle 缓存

**背景对照**：
- `nfs.rs` 通过 `GLOBAL_CACHE: moka::Cache<(PathBuf, Bytes), Bytes>`（`nfs.rs:164-169`）缓存"路径 → NFS file handle"映射，由 `DepthAwareExpiry`（`nfs.rs:130-162`）按深度设差异化 TTI，所有 worker 通过 `SERVER_ID_REGISTRY`（`nfs.rs:174-191`）共享同一份 cache。`lookup_fh`（`nfs.rs:705-805`）命中 cache 即跳过 LOOKUP RPC。
- NFS 的 fh 是**无状态的**（不绑定 open lifetime），所以 cache 模型简单 ——「记下来就能复用」。
- SMB 的 `FileId(Persistent, Volatile)` 是**有状态的**：Open 返回，Close 即失效，**不能直接像 NFS 那样路径→FileId 缓存**。直接缓存 FileId 会拿到 `STATUS_FILE_CLOSED`。

**SMB 真正的等价机制：Handle Lease (SMB 2.1+)**
- 协议机制：在 Create 请求里附带 `RqLs` create context，要求 server 授予 read/handle/write caching lease（`smb-msg/src/create.rs:516, 588-625, 1019-1027`）。
- 拿到 `HandleCaching` lease 后，client **应用层 Close 时不真发 Close 到 server**，先在本地 lease table 留住 FileId；再次 Open 同一路径走 lease 路径直接复用，省 1 个 Create RT（也省 Close RT）。
- 收到 `OplockBreak` 通知时驱逐对应 lease，下次 Open 重新走完整流程（这是 NFS fh cache 没有的强一致保证）。
- 目录侧用 **Directory Lease**：scan 后 enumerate → 关闭目录句柄；接着对子项 mkdir / put 文件时，如果父目录还有 directory lease，QueryDirectory 失效感知 + create 同样可省 RT。

**smb-rs 现状**：
- Negotiate 阶段已声明 client capability：`with_leasing(true) + with_directory_leasing(true)`（`connection.rs:438, 442`）。
- `RequestLease` / `RequestLeaseV2` / `LeaseState` 数据结构已实现（`smb-msg/src/create.rs:516, 588-625`）。
- `client/smb_client.rs::create_file` **从来没把 lease context 塞进 `FileCreateArgs`**，server 实际没授予 lease（仅 `tests/` 用例和 binrw 序列化测试涉及）。
- 没有 client-side lease table，也没有 oplock break 通知处理。

**改造方案**（按依赖顺序）：

1. **smb-rs：暴露 lease API**
   - `FileCreateArgs` 增加 `lease: Option<LeaseRequest>` 字段（可选 lease key + 期望 state）。
   - `Client` 内部维护 `lease_table: DashMap<LeaseKey, LeaseSlot { unc, file_id, granted_state, last_seen }>`。
   - `create_file` 默认（当传入 lease 时）：
     - 入口检查 `lease_table` 是否已有匹配 unc + 满足期望 state 的 slot；命中 → 直接返回包装好的 `Resource` 共享 FileId（不发网络）。
     - 未命中 → 构造 `RqLs` v2 context 跟随 Create 发出，response 中带 `LeaseGranted` 时插表。
   - `Resource::close()` 改名/新增 `release()`：默认延迟关闭（lease 保护期内不真发 Close），lease 被 break 时真关。
   - 新增 `OplockBreak` 接收 task（worker 已有 channel），收到后 lookup `lease_table` 驱逐，并对当前 inflight 用户发"句柄需重新打开"信号。
   - 暴露 `Client::evict_lease(unc)` 给上层用于显式失效（e.g., delete 后）。

2. **data-mover-rs：cifs.rs 接入 lease + 路径侧 cache（per-storage）**
   - `CifsStorage` 新增 `handle_cache: Arc<moka::Cache<PathBuf, FileIdSlot>>`，**per-storage 持有**（与 P0 设计一致，不跨 worker 共享）。
   - 结构对照 `nfs.rs::GLOBAL_CACHE`（结构相同，但 nfs.rs 跨 worker 共享，cifs 这里刻意 per-worker —— 避免锁竞争）。
   - 复用 `DepthAwareExpiry` 风格的 TTI（浅层 1h、中层 10min、深层 10s）。
   - `read_file` / `read_data` / `write_file` / `write_data` / `set_entry_metadata` / `mkdir_or_open` 入口先查 `handle_cache`；命中且 lease 仍有效 → 用 cached FileId 跳过 Create RT。
   - 命中率分析：单 worker 处理一批文件时，对父目录 / 同前缀文件的重复 open 仍有显著命中（增量 sync 场景最受益）。

3. **terrasync-rs：scan→sync 阶段衔接**
   - dir_walker 扫到的每个目录在 cifs.rs `walk_scheduler` 里 enumerate 完后，**不要立刻 close**，把 directory handle 通过 lease 留下；后续 sender_worker 对子项写入时直接复用父目录上下文。
   - 仅在 sync 完整结束、`SyncOrchestrator::run` 退出 cleanup 时统一调用 `Client::flush_all_leases()` 释放。

**预期收益**：
- 增量场景（重复打开同一文件做 stat / set_metadata）：现状 3 RT/op → 0–1 RT/op，与 nfs.rs lookup_fh cache 命中率类似。
- 大规模目录树 sync：每个文件 copy 都至少省 1 个父目录 lookup RT。
- 强一致：靠 OplockBreak 失效，比 NFS fh cache 的 TTI-only 模型更安全。

**复用现有抽象**：
- `moka::sync::Cache` 已是 data-mover-rs 依赖（nfs.rs 使用），直接套用。
- `DashMap` + `OnceCell` 在 cifs.rs 中已用于 `dir_info_class` / `file_id_class`，lease_table 同等模式。
- nfs.rs 的 `DepthAwareExpiry` (130-162) 可抽到 `data-mover/src/cache.rs` 共享。

**实施风险**：
- Lease break 处理是 SMB client 的标准能力，但 smb-rs 现在没实现 → 改动面大，且需要正确的并发 / 重试语义。建议作为独立中等粒度 PR，前面 P0/P1（mount 限流 + mkdir cache）先落地。
- 部分老 Samba 实现 lease 行为有 bug，可加 `cifs?lease=false` query 参数关闭。

---

### P2：read_data / write_data 改为 inflight pipeline

**问题文件**：
- `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs:707-744` `read_data`
- `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs:828-870` `write_data`

**现状**：循环内 `read_at(buf, offset).await` → `tx.send(chunk).await` → 下一轮，**任何时刻只有 1 个 inflight 请求**。SMB credits 通常允许 32+ 个 inflight。

**改造方案**：
- `read_data`：用 `FuturesOrdered<Future<Output=(offset, Bytes)>>` 维持 `n` 个 inflight `read_at`（n=4 或 8），按 offset 顺序 await 后送 tx。
- `write_data`：从 rx 收齐 n 个 chunk 后并发 `write_at`，等全部完成再 ack。
- 默认 inflight 由 ClientConfig 传入，默认 4–8（保守值，避免穿透 server credits）。

**预期收益**：单文件读吞吐 2–4×；多并发文件 + 多 channel 后线性叠加。

---

### P2：动态 / 增大默认 block_size

**问题文件**：`~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs:138, 417`

```rust
const DEFAULT_BLOCK_SIZE: u64 = 2 * MB;
let effective_block_size = block_size.unwrap_or(DEFAULT_BLOCK_SIZE).min(DEFAULT_BLOCK_SIZE);
```

**问题**：硬上限 2 MB，且 `.min(DEFAULT_BLOCK_SIZE)` 把用户配置往下压。

**改造方案**：
- smb-rs `Connection::negotiate` 返回值含 `max_read_size` / `max_write_size`（每个 server 不同，常见 1/4/8 MB）。
- `CifsStorage::connect_only` 取 `min(user_config, server_max_read, server_max_write)`。
- 默认 `DEFAULT_BLOCK_SIZE` 提到 8 MB（与现代 Windows / Samba 默认一致）。
- 配套：data-mover 顶层 examples / docs 注明 block_size 含义。

**smb-rs 配套**：暴露 `Connection::negotiated_max_read_size() / max_write_size()`，如果还没暴露，加 getter。

**预期收益**：单文件读写从 ~16 个 2MB op → 4 个 8MB op，对中大文件带宽 1.5–2×。

---

### P2：开启 SMB Multi-Channel

**问题文件**：`~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs:398-405` `Client::new(ClientConfig { connection: ConnectionConfig { ... } })`

**现状日志**：`Multi-channel is not enabled in client configuration. Skipping setup`（line 4776）。

**改造方案**：
- `ConnectionConfig` 暴露 `multi_channel: bool` + `max_channels: usize`（默认 false / 1，保持向后兼容）。
- 在 `cifs.rs` `connect_only` 通过 SMB URL query 参数读：`smb://...?multi_channel=true&max_channels=4`，或顶层 AppConfig 全局开关。
- smb-rs 已有 `_setup_multi_channel`（`smb_client.rs:252`），只缺 config 旗标透传。需检查 `ClientConfig` 结构。

**预期收益**：server 支持时单文件可线性扩展到 NIC 数量，常见 2–4× 单流吞吐。

---

### P2：update_directory_metadata 内部 RT 压缩

**问题文件**：`crates/app/src/orchestrator.rs:1397-1451`

**现状**：已有 `DIR_META_CONCURRENCY=32` 并发，但每次 `set_entry_metadata` 内部对 CIFS 是 `open → set_basic_info → close` 三次 RT。

**改造方案**：
- 在 `data-mover-rs/src/cifs.rs::set_entry_metadata` 改用 smb-rs compound 把 Create+SetInfo+Close 打包为一次网络写（依赖前述 P1 smb-rs compound API）。
- 配合 P1 handle lease：若 lease 命中，open 阶段无 RT，整体压到 1 个 RT（仅 SetInfo）。

**预期收益**：目录元数据回写延迟 3× → 1×。

---

### P3：信令 / 加密策略评估

- 当前 `Negotiate` 协商 encryption + signing。内网信任环境可通过 `ConnectionConfig` 暴露开关关闭加密以提速（CPU 节省）。
- 不默认改，提供配置项与 doc，让运维按场景决定。

---

## 关键文件 / 行号速查

| 改动位置 | 路径 | 关键行 |
|---------|------|--------|
| CifsStorage 结构（per-worker，不池化） | `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs` | 248–274（struct）/ 392–430（connect_only）|
| storage 工厂 | `~/.cargo/git/checkouts/data-mover-rs-.../src/storage_enum.rs` | 1313–1319 |
| StoragePair | `crates/app/src/sync.rs` | 39–93 |
| mount semaphore | `crates/app/src/orchestrator.rs` | 360, 386–404, 491–509 |
| stats_reporter | `crates/app/src/orchestrator.rs` | 1180–1197 |
| update_directory_metadata | `crates/app/src/orchestrator.rs` | 1397–1451 |
| create_dir_all / mkdir | `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs` | 521–543, 878–895 |
| read_data / write_data | `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs` | 674–748, 828–871 |
| block_size 上限 | `~/.cargo/git/checkouts/data-mover-rs-.../src/cifs.rs` | 138, 417 |
| smb-rs share_connect 复用 | `~/.cargo/git/checkouts/smb-rs-.../crates/smb/src/client/smb_client.rs` | 238–355 |
| smb-rs multi-channel | `~/.cargo/git/checkouts/smb-rs-.../crates/smb/src/client/smb_client.rs` | 252（已有 `_setup_multi_channel`）|
| smb-rs lease capability 声明 | `~/.cargo/git/checkouts/smb-rs-.../crates/smb/src/connection.rs` | 438, 442 |
| smb-rs lease 数据结构（未接 API） | `~/.cargo/git/checkouts/smb-rs-.../crates/smb-msg/src/create.rs` | 516, 588–625 |
| nfs.rs file-handle cache 参考 | `~/.cargo/git/checkouts/data-mover-rs-.../src/nfs.rs` | 130–169（DepthAwareExpiry + GLOBAL_CACHE）/ 705–805（lookup_fh） |

---

## 复用已有抽象（避免重复造轮子）

- **terrasync-rs 已有 `Semaphore` + 重试**：`create_storage_pair_with_retry` 框架直接复用，仅需按 storage 类型调整 semaphore 容量。
- **`DashMap` / `DashSet` / `OnceCell` / `moka::Cache`** 已是 data-mover-rs workspace 依赖（cifs.rs 已用 `OnceCell` 缓存 `dir_info_class` / `file_id_class`，nfs.rs 已用 `moka::Cache` 缓存 fh），per-storage dir cache 与 handle lease cache 直接套用。
- **smb-rs 内部多 share 复用能力**：`Client::share_connects` map + `_with_tree` 已连接检查（`smb_client.rs:295-302`）—— 仅供单 worker 内 src+dest 同 host 时使用，**不跨 worker 共享 Client**。

---

## 验证 / Verification

**端到端**（用当前环境的实际 sync 命令，不走 e2e harness）：

> 注：`e2e-test-cifs-full-sync` skill 内的 share 配置 (192.168.50.x) 与当前环境 (10.131.7.203) 不匹配，统一改为手动 sync 命令复测。

复跑命令（改造前先存 baseline log，改造后比较）：

```powershell
.\target\release\terrasync.exe sync `
  smb://jay:xuanyuan=1@10.131.7.203/jay_cifs1 `
  smb://jay:xuanyuan=1@10.131.7.203/jay_cifs2 `
  -c .\examples\config.toml -l trace
```

对比指标：
- `Connecting to SMB share` 出现次数：**期望仍是 16-17 次**（per-worker Client 设计保留），但**全部并发完成**而不是串行 6 秒。
- 第一条 `Connecting to SMB share` → 最后一条 `Successfully connected to share`：期望从 ~6s 压到 ~150 ms。
- 端到端 wall-clock：期望 ~10 s 降到 ~2 s（仅 P0），更多 P1/P2 落地后小文件场景再降一档。
- `Processed 0 entries in 10s` 警告：期望消失。

完整正确性比对：
- 改造前先跑一次记录 ClickHouse `base_<job_id>` 表行数、源/目的 file count + total bytes 作 baseline。
- 改造后跑一次重新比对，行数与 byte 总数必须一致；任一 diff 视为回归。

吞吐基准：
- 准备一个 ~1 GB 单文件 share，测 throughput：期望 P2（pipeline + 8 MB 块 + multi-channel）全完成后单流 ≥ 200 MB/s（千兆链路），或 multi-channel 开启后 ≥ 单 NIC 上限的 80%。

**单元 / 集成测试**：
- data-mover-rs：新增 `tests/test_cifs_dir_cache.rs`，验证 per-storage `dir_exists_cache` 命中行为；如 handle lease 落地，加 `tests/test_cifs_lease.rs` 覆盖 OplockBreak 失效路径。
- terrasync-rs `crates/app/tests/`：mount semaphore 容量按 storage 类型选择的逻辑加 unit test（covers nfs/smb/混合三种组合的取值）。

**性能基准**：
- 用 `cargo run --release --example cifs_copy` + 1k 小文件目录树跑前后对比，记录到 `docs/perf/` 下。

---

## 实施建议顺序

1. **P0 取消 CIFS mount 限流 + P1 stats_reporter**：纯 terrasync-rs 单 PR（仅 `orchestrator.rs` 一个文件），**不动 data-mover-rs / smb-rs**，不引入任何共享锁，改完即拿到 ~70% 收益（消除冷启动 6 秒串行握手）。
2. **P1 mkdir cache**（data-mover-rs `cifs.rs::CifsStorage` 加 `dir_exists_cache: Arc<DashSet<String>>`；注意此 cache 是 **per-storage** 即 per-worker，与 P0 设计原则一致，不引入跨 worker 共享，独立 PR）。
3. **P2 block_size 自适应 + IO pipeline**（data-mover-rs，可能需 smb-rs 暴露 `negotiated_max_read_size` getter）。
4. **P2 multi-channel**（smb-rs `ConnectionConfig` 暴露开关 + data-mover-rs URL 参数透传）。
5. **P1 SMB Handle Lease 端到端**（smb-rs Create/Close API 接 lease + per-Client lease_table + OplockBreak 处理；data-mover-rs `CifsStorage::handle_cache` 接入。lease_table 同样是 **per-Client 即 per-worker**，与 nfs.rs GLOBAL_CACHE 跨 worker 共享略有差异 —— 我们刻意不跨 worker 共享以避免锁竞争）。
6. **P2 metadata compound**（依赖 smb-rs compound API，最后做）。

> 第 1 步占整体收益的大头且**改动面最小**（纯 terrasync-rs `orchestrator.rs` 单文件），建议先做并用上面那条 sync 命令复测后再推进 2–6。
>
> **核心设计原则贯穿全文：保留 per-worker 独立 Client，所有 cache / lease / dir-exists 状态都是 per-storage 持有，杜绝跨 worker 共享带来的锁开销。**

每个 PR 完成后用同样的 sync 命令复测一遍，确保没回归。
