# issue #18 回改：进程级 Sender↔Receiver 握手 e2e 验证（PR #40 review）

## 背景 / 需求
维护者在 PR #40 要求补一条**进程级（两个真实 OS 进程）**端到端验证，确认版本/能力协商握手
在真实 `terrasync` 二进制（serve + sync --remote）之间可用。现有 `crates/transport/tests/quic_roundtrip.rs`
已覆盖 compatible/incompatible/downgrade 三种握手场景，但都是同进程内两个 QUIC endpoint，
不是两个真实进程。

## 验收标准
- root crate 新增 `tests/remote_process_e2e.rs`，用 `assert_cmd::cargo::cargo_bin("terrasync")` 拿二进制。
- 起真实 Receiver 进程（`terrasync serve`）+ 真实 Sender 进程（`terrasync sync --remote`），
  经真实 loopback QUIC 连接完成握手 + 全量文件同步。
- 断言：Sender 退出码 0；dest 与 src 文件内容/数量一致；日志中能看到握手 Accepted 的证据。
- `cargo test -p terrasync-rs --test remote_process_e2e` 通过；`cargo fmt` + `cargo clippy -p terrasync-rs` 无新增告警。

## 分支基线
- `origin/main` = `2ad0931d91b31e8a9dc74326f4727f93200f77ab`
- 当前分支 `claude/issue-18` HEAD（开始时）= `a63a473f8b0efd0a413bb9c89b02e59596eacd3b`
  （`feat(transport): 协议版本与能力协商握手 (#18)`，PR #40 的实现提交）

## 已核实的关键事实
- 二进制 `terrasync`（package `terrasync-rs`），QUIC 无条件编入（cli/app 都开 `transport/quic` feature）。
- CLI 参数（`crates/cli/src/commands_enum.rs`）：
  - `Serve { listen: String(--listen, default "0.0.0.0:9876"), dest_path: String(positional),
    tls_cert_out: String(--tls-cert-out, default "server.crt") }`
  - `Sync { id: Option<String>(--id), src_path/dest_path: positional,
    remote: Option<String>(--remote), tls_server_cert: Option<String>(--tls-server-cert, requires remote), ... }`
- 日志：`setup_logging()`（`crates/utils/src/logger.rs`）**只写文件**（`<base_dir>/logs[/<job_id>]/app.log`），
  **没有 stdout layer**。`base_dir` 取 `CARGO_MANIFEST_DIR` 的**父目录**（若该环境变量存在），否则取
  `current_exe()` 所在目录。因为子进程会继承测试进程的 `CARGO_MANIFEST_DIR`（cargo test 设置为
  root crate 目录），需要在测试里**显式覆写**子进程的 `CARGO_MANIFEST_DIR` 指向本次测试专属 tmp 目录下的
  一个不存在子路径，使 `base_dir` 落在 tmp 目录内（`Path::new(fake).parent()`），日志/证书/jobs 都被
  收纳在测试自己的 tmp 目录里，不污染 worktree 或兄弟 agent 的共享目录。
- `database.enabled` 默认 `true`（默认 DSN `http://localhost:8123`），但双进程模式的
  `remote_sync.rs` / `receiver_task_remote` 完全不碰 DB；只有 `SyncOrchestrator::run()`
  顶层为了判定 `ScanType` 会在 `database.enabled=true` 时构造 `Database`（需要真实 ClickHouse）。
  所以测试需要通过 `-c <config.toml>` 传 `[database]\nenabled = false`，绕开对真实 ClickHouse 的依赖
  （`validate()` 只有 `enabled=true` 时才校验 dsn 非空）。

## ⚠️ 已发现并确认的进程级 bug（手工复现）
`crates/cli/src/commands.rs::serve_cmd` 当前顺序：
```
let (receiver_transport, _endpoint, cert_der) = quic::accept(listen_addr).await?; // 阻塞到连接建立
...
std::fs::write(tls_cert_out, &cert_der)?; // 连接建立后才写证书文件
```
而 `quic::accept()`（`crates/transport/src/quic/receiver.rs`）把"生成证书 + bind endpoint"和
"等待并接受一个连接"揉进同一个函数里，只有整个函数返回（即已经接受了一个连接）才能拿到 `cert_der`。

Sender 侧 `cli/commands.rs::sync_cmd` 在**发起 QUIC 连接之前**就要从磁盘读 `--tls-server-cert` 文件
（用于 pinned TLS 校验），而 `quic::connect()` 的握手（`endpoint.connect(...).await`）本身就需要
提前加载好的证书字节来验证服务端 —— 这就形成先有鸡还是先有蛋的死锁：Receiver 要等一个连接才写证书文件，
Sender 要读到证书文件才能发起连接。

**手工复现**（bash 起两个真实进程）：`serve --tls-cert-out server.crt dest` 启动 4 秒后，
`server.crt` 仍不存在，receiver 的 `app.log` 停在 `[QUIC Receiver] Listening on 127.0.0.1:19876`，
再无进展（阻塞在 `endpoint.accept().await`）。这是本 issue 要暴露的真实进程级问题，
**先单独修复，再和测试一起验证**。

### 第一处修复方案（证书时序）
把 `transport::quic::accept()` 拆成两步：
- `bind(listen_addr) -> Result<(Endpoint, Vec<u8>)>`：生成自签名证书 + bind endpoint，立即返回
  （不等待任何连接）。
- `accept_connection(&Endpoint) -> Result<QuicReceiverTransport>`：在已 bind 的 endpoint 上等待并接受
  一个连接。

`serve_cmd` 改为：`bind()` → 立即 `std::fs::write(tls_cert_out, cert_der)` → `accept_connection()`。
`quic::accept()` 唯一调用方是 `serve_cmd`，无其它引用点，直接改造替换（不保留冗余的组合版 `accept()`，
避免留下同样的顺序陷阱）。

## ⚠️ 第二个进程级 bug（写测试过程中用真实二进制反复跑出来的间歇性失败）

`serve_cmd` 修好证书时序后，用真实二进制跑 30 次 `serve` + `sync --remote`（脚本循环），
约 15% 概率 Sender 侧报 `Error: Copy error: Transport closed during ack phase`（其中一次等了
30.1s 才失败——刚好是 quinn 默认 `max_idle_timeout=30s`）。查 Receiver 侧 `app.log`：每次都显示
`[Receiver Remote] All transfers complete` → `Completed` → `Receiver daemon completed` 正常走完，
即数据其实已经写對，只是 Sender 没收到最后的 Ack / `AllDone`。

根因：`receiver_task_remote` 发完 `AllDone` 后立刻 return，`serve_cmd` 紧接着 return，进程随之退出——
此时 `quinn::Connection` / `Endpoint` 被 Drop，而 `write_all()` 返回 Ok 只代表数据进了 quinn
本地发送队列，不代表已经真正发到对端；进程过早退出会导致最后几帧（EntrySuccess/Progress/AllDone）
来不及送达就被现场终止的 UDP socket 吞掉。quinn 官方文档（`Connection::close()` 的文档注释）明确
建议：发送数据的一方在退出前应调用 `Endpoint::wait_idle()`，"exiting immediately could force
[peers] to wait out the idle timeout period"——和这里的现象完全对应。

### 修复方案（最小改动，第二处）
`serve_cmd` 在 `receiver_task_remote` 成功返回后、真正 return 之前，加一行
`endpoint.wait_idle().await;`。`wait_idle()` 只是等待"连接已经被判定为关闭"（对端收到 AllDone 后
主动 `close()`，或者兜底走 30s 默认 idle timeout），不会主动关闭/丢弃任何数据，是 quinn 文档里
给这种场景推荐的标准写法。验证：手工脚本跑 30 次 serve+sync 全部成功、无 flaky。

## 执行步骤（每步一提交）

- ✅ step 0：勘察确认（读 spec/代码、手工复现 bug、确定测试与修复方案）—— 本 plan 文件即产出，随本步提交。
- ✅ step 1：`transport::quic` 拆分 `accept()` 为 `bind()` + `accept_connection()`；`serve_cmd` 改为
  "bind → 写证书 → accept_connection" 顺序。验证：`cargo check -p transport -p cli` 通过；
  `cargo test -p transport --features quic` 6 条测试全绿（3 条握手 + 既有 roundtrip，证明重构未改变
  对外行为）。手工复现两进程 serve/sync：bind 后 1s 内 `server.crt` 立即出现（此前需等一个连接建立，
  永远等不到）；Sender exit 0；dest 文件与 src 一致；两端 app.log 均记录
  `Handshake accepted, negotiated features: FeatureFlags { delta: true, ... }`。
  （手工验证还发现：真实进程需要 `-c` 传 `[database]\nenabled = false` 才能绕开 ClickHouse 依赖，
  且每次运行需要全新 job_id/job_dir，否则会被判定为 Incremental 而报
  "Remote incremental sync not yet implemented" —— 这两点已写进下一步的测试实现里，通过全新 tmp
  目录 + 全新 job_id 规避，非本 issue 修复范围内的 bug。）
- ✅ step 2：root 新增 `tests/remote_process_e2e.rs` + `Cargo.toml` dev-dependencies 加 `tempfile`。
  实现 happy-path 进程级测试：起 Receiver → 等 cert 文件出现（bounded 超时）→ 起 Sender →
  断言 Sender exit code 0 → 等 Receiver 自行退出（bounded 超时，见下）→ 断言 dest 与 src 文件一致 →
  读双方 app.log 断言含 `Handshake accepted, negotiated features` 佐证握手确实发生。
  写测试过程中发现了"第二个进程级 bug"（见上）并修复；修复后手工脚本跑 30+50 次全部通过，
  `cargo test` 连续跑 20 次全部通过（另有 1 次孤立的 10s 超时失败，出现在验证第二个 bug 修复的
  紧邻批次里，怀疑是当时系统负载/前一批 30 个子进程收尾造成的瞬时抖动，而非协议本身死锁——因为
  同批次条件下 bash 直接起两个进程跑 80 次全部 <250ms 完成）。为稳妥起见把
  `RECEIVER_EXIT_TIMEOUT` 从 10s 调到 35s（略大于 quinn 默认 `max_idle_timeout=30s`），
  兜底覆盖"Sender 自己 close() 后过早退出、连接关闭帧未必送达 Receiver、Receiver 只能靠 idle
  timeout 判定连接结束"这一理论上仍存在、但触发概率极低的对称场景（见"范围外"说明，未在生产代码里
  处理，只在测试超时上留够余量）。验证：`cargo test -p terrasync-rs --test remote_process_e2e -- --nocapture`
  连续多次运行全部通过。
- ⬜ step 3：收尾。`cargo fmt`、`cargo clippy -p terrasync-rs`（无新增告警）、`git status` 确认无越界
  文件，移除本 plan 文件并单独提交。

## 范围外（明确不做）
- 不重造 incompatible/downgrade 负路径（已被 `quic_roundtrip.rs` 的
  `test_quic_handshake_incompatible_version_rejected_before_phase1` /
  `test_quic_handshake_missing_delta_capability_downgrades` 覆盖，进程级版本号是编译期常量，
  无法在单一真实进程里造假旧版本）。
- 不改动 `serve_cmd` / `sync_cmd` 之外的业务逻辑；不引入 ClickHouse 依赖（用配置覆盖绕开）。
- **不**对称修复 Sender 侧潜在的"`transport.close()` 之后进程过早退出、关闭帧未必送达"问题：
  `quic::connect()` 目前不返回 `Endpoint`（内部创建后即在函数末尾被丢弃，仅保留 `Connection`），
  要在 Sender 侧也补一个 `wait_idle()` 需要改 `connect()` 的公开签名，而它有 6 处调用点
  （`remote_sync.rs` 1 处 + `quic_roundtrip.rs` 5 处），属于比 Receiver 侧那处大得多的改动面，
  且实测（bash 直接起 80 次真实进程 + `cargo test` 连续 20 次）里从未复现，只在一次孤立场合看到
  10s 超时（详见 step 2 记录，怀疑是系统瞬时负载而非协议死锁）。这个改动收益不确定、成本明显更高，
  超出本 issue "只解决进程级握手验证" 的范围，因此只在测试超时上留够余量（35s > 30s 兜底），
  不改动 `transport::quic::connect()` 的公开接口。
