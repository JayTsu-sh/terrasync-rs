# TerraSync crate 重构实施方案

## 当前单文件复制重构范围（2026-11-25）

针对当前锁定的 data-mover `3988745`，首个可独立验收的切片为单进程复制一个无历史版本的普通文件，覆盖直接写入、断点续传和迁移模式下的删源判定。单进程指同一 TerraSync 进程访问源端和目标端，存储可以是远程服务。

从生产入口验证发布后按[已传输字节口径](../adr/0001-local-copy-transferred-bytes.md)记账、所需元数据失败时保留源文件，以及目标在删源前被替换时阻止误删。历史版本顺序、目录收尾和双进程协议留给后续切片。

## 原方案与基线（2026-11-18）

建议保留现有 workspace 成员。先让 CLI 和 Web 通过同一组任务入口使用 `app`，让 `transport` 专注两个进程之间交换的消息，再把本地复制和远端会话各自的执行状态收回 `app`。第一笔改动是迁移 callback 诊断契约，可以独立交付。整轮使用现有 `db` 和 data-mover 接口。

本轮提交以 `feat/full-migration-optimization` 的 `b54f997` 为基线，构建使用该分支锁定的 data-mover `e001d068`。受控遍历、复制及源端清理的本地改动仍在原工作树，该工作树使用 data-mover `349d59c1`。包含这些本地改动的验证结果只适用于当时的组合快照，候选提交以其独立工作树的验证结果为准。

## 目标结构与职责

| 所属 crate | 负责什么 | 调用方通过什么使用它 |
| --- | --- | --- |
| 根二进制、`cli` | 启动、命令参数、配置优先级、证书输入、面向用户的错误和退出码 | 同一 `terrasync` 提供本地命令、Sender 的 `sync --remote` 和 Receiver 的 `serve`。CLI 调用 app 用例，默认构建提供双进程功能 |
| `web` | HTTP／WS、任务管理、SQLite 执行记录、callback 接收和存储浏览 | 通过 app 启动任务，通过共享报告契约更新进度。端点检测和目录浏览可直接使用 data-mover |
| `app` | 执行一项产品任务，持有执行身份，协调存储、数据库、统计、取消和收尾 | 对外提供扫描、本地迁移、远端发送、远端接收、校验等用例。调用方只传任务请求与本次执行所需上下文 |
| `task-model` | CLI、Web、app、db 或协议真正共享的值类型及纯决策 | 保留观察、代次决策、源端策略，加入共用的诊断值类型。正常依赖限于模型及其所需基础库 |
| `transport` | wire 消息、编解码、版本协商、QUIC 多流、流量额度计算 | app 通过现有收发接口使用它。何时请求某项、是否重传、落盘结果能否确认，归远端会话 |
| `db`、`sync-delta`、`licensing`、`utils` | 依次负责执行事实与数据库布局、差分计算、许可、现有配置与公共设施 | 复用现有能力。db 在开启代次时调用 `CopyGenerationPlan::decide`，app 消费结果；db 暴露的 ClickHouse 配置类型仍由 utils 提供 |

`app` 内建议逐步形成四个模块。目录名表示最终归属，每个提交只移动能随调用方一起迁移的一组逻辑。

| 目标模块 | 从哪里收拢 | 由它掌握的决定 |
| --- | --- | --- |
| `scan` | `scan`、`scan_resume`、`scan/source` | 流式或持久化扫描、遍历顺序、完整目录确认、重放和累计统计 |
| `migration` | `sync`、`orchestrator`、`sender`、`receiver`、各 `remote_*` 模块 | `local` 管本地全量和增量，`remote` 管两端会话，条目复制、目标发布和元数据逻辑由实际共用的实现承载 |
| `execution` | `job_execution`、`copy_execution` 中已有的租约、故障监控与取消控制 | 执行身份、首个有类型故障、在途操作收束。扫描游标归 scan，复制代次提交归迁移流程 |
| `reporting` | `consumer/stats`、callback 及交付逻辑 | 进度投影、累计统计、批次身份、诊断发件箱及终态归档。Web 只依赖报告值和接收契约 |

本轮将远端入口收至 `app::remote_sync`，发送与接收会话分别由 `remote_sender_session` 和 `remote_receiver_session` 负责。Sender 使用本项目的源端观察、过滤和统计，Receiver 使用目标端元数据、校验、发布及错误类型。将来出现独立发布 Receiver 或第二个真实调用方，且会话可以通过少量输入、事件和结果独立运行时，再评估拆 crate。

有一处明确的范围边界。Web 的目录聚合查询会根据 ClickHouse 部署选择 `base_<job>` 或 `base`，并在集群模式追加过滤。目前 db 没有对应的聚合查询接口。本轮将这项布局知识限制在现有分析仓储；将来扩大 db 范围时，再让 Web 按任务、路径和统计维度查询。[分析查询实现](../../crates/web/src/infrastructure/analysis_repo.rs) · [复制代次决策](../../crates/db/src/catalogue/copy_generation.rs)

## 双进程与 NDX 的处理决定

候选提交的调用链是 `sync --remote → remote_sync::send → directory_pages → RemoteSenderSession`。对端通过 `serve → remote_sync::serve → remote_receiver_session::receive → RemoteReceiverSession::run` 进入消息循环。Sender 在发页前登记可请求条目，一边发清单一边处理请求和确认。Receiver 用自己的目标存储比较条目，选择跳过、全量或差分，等待写入结果并通知 Sender。可选的目标多余项删除由 Receiver 执行。远端 checkpoint 保存已完成路径，扫描 checkpoint 保存遍历恢复位置，两者分别验证。[远端入口](../../crates/app/src/remote_sync.rs) · [Sender 会话](../../crates/app/src/remote_sync/remote_sender_session.rs) · [Receiver 会话](../../crates/app/src/remote_receiver_session.rs)

| 机制 | 回答的问题 | 处置 |
| --- | --- | --- |
| data-mover `TraversalOrder::Blocks` 与 `DirectoryListed` | 哪些直接子项属于本次列举的目录，何时收到该目录的列举结果 | 远端 `directory_inventory` 已启用 Blocks。`directory_pages` 暂存并排序文件和子目录，然后编号发页。块边界可用于组页，不能证明目标已写入 |
| 每项 `ndx` | Receiver 的请求、payload、redo 和 ACK 对应本次会话的哪个已发布条目 | 保留关联 ID。重新遍历不能保证复现同一编号，跨会话恢复不能依赖它。把 `NdxTable` 的所有权移至 Sender 会话 |
| `ndx_start`、`gap_ndx` | 现有目录页携带的编号范围和预留位置 | 当前生产路径有写入，未发现业务读取。结构迁移期间保留 v14 编码；日后修改协议时一并核实所有读写点、调整版本与两端测试 |
| 在途条目窗口 | Sender 和 Receiver 何时能释放一项的明细 | 独立性能项目。先定义“对端不再请求或重试，迟到消息可判定，终态已记账”的条件，再设计释放。覆盖无需传输的条目、重传、重复 ACK、慢 Receiver、单个超大目录和断连重跑，测两端峰值 RSS 与随条目数增长的曲线 |

**Blocks 已经解决目录枚举分组；逐项 NDX 仍是双进程会话的关联依据。** 当前一页可含整个目录，页队列有界也限制不了单页大小。Sender 的 NDX 表、已完成路径集合和 Receiver 的终态记录要分别量测。`TransferDone` 在控制流上可能先于数据到达，不能用它或 `DirectoryListed` 驱逐仍可被引用的项。[清单生成](../../crates/app/src/remote_inventory.rs) · [Blocks 入口](../../crates/app/src/scan/source.rs) · [现行协议与索引](../../crates/transport/src/message.rs)

仓库另有 opaque advertisement 和 expert transfer 实现及测试。对应设计记录的是当时的 v6、v8。候选提交的协议版本和最低版本均为 v14，入口仍走上述目录页链路。合入审查时 GitHub `main` 的 `60775af` 使用 v12。切换清单格式、启用新的传输流程或调整 NDX 生命周期都应作为独立行为变更，分别给出两端失败和恢复证据。[候选协议](../../crates/transport/src/message.rs) · [Advertisement 设计](2026-09-01-remote-advertisement-design.md) · [Expert transfer 设计](2026-09-02-remote-expert-transfer-design.md)

## 实施顺序

按 A → B → C → D 交付。每个编号都包含实现、生产调用方和直接受影响的测试，可单独提交。开始下一步前先通过该步的验收；发生回归时回退该提交及依赖它的后续提交。恢复代码版本与处理运行中的任务是两件事，已经发布的目标或已按策略清理的源文件遵循原任务的恢复规则。

### A　收窄任务入口和共享报告契约

1. **先迁诊断值类型。** 从 `app/consumer/stats/diagnostics.rs` 将 `CallbackIdentity`、`DiagnosticEvent`、`DiagnosticBatch`、`CallbackAck`、双方校验用的限制和纯脱敏／校验逻辑迁入 `task-model`。将现有 crate 私有的 `DiagnosticEvent::is_bounded` 作为模型校验入口提供给 app，再同步更新发送端、Web 接收端和测试。`DiagnosticOutbox` 的队列、修订号、重试、归档留在 app。用同一组 JSON 样本对迁移前后序列化、缺省字段、重试批次 ID、重复投递、终态序号和累计数做对照。[诊断实现](../../crates/app/src/consumer/stats/diagnostics.rs) · [Web 回执事务测试](../../crates/web/src/infrastructure/callback_receipt_tests.rs)
2. **整理 Web 使用的进度报告入口。** 将 `ProgressReport`、`FinalStats` 等实际使用类型通过 app 的报告模块提供，逐个消除 Web 对 `app::consumer::stats` 的导入。`ScanSnapshot` 间接使用 data-mover 遍历计数，先由既有统计实现生成唯一快照，再导出报告。验收 Web 端的进度、终态和扫描分类数，同时检查 `task-model` 的正常依赖图。[报告定义](../../crates/app/src/report.rs) · [扫描统计](../../crates/app/src/scan/statistics.rs)
3. **合并本地迁移入口。** `sync` 和 `incremental_sync` 都调用 `new_local(config).run()`。统一到 `sync(SyncJobConfig)`，检索并迁移 CLI、Web、prelude、examples 和测试。扫描沿用 `scan(ScanRequest)`。保留 CLI 配置覆盖顺序、Web 重置语义和 app 的代次选择，对照有效请求与最终报告。远端 Sender 的配置和连接参数由远端用例接收。[同步入口](../../crates/app/src/sync.rs) · [CLI 参数组装](../../crates/cli/src/commands.rs) · [Web 任务执行](../../crates/web/src/infrastructure/task_runner.rs)

### B　让 transport 只承担跨进程通信

1. **移出存储 SDK 转换。** `DataChunk` 保留在 transport，在 app 的 `file_commit` 消费端构造 `PositionedChunk { offset, data }`，直接移动 `Bytes`。不能把原有 `From<DataChunk> for PositionedChunk` 原样搬进 app，Rust 的孤儿规则不允许。将 transport 中其余模型引用改到已锁定的 `data-mover-model`，共享测试夹具需要完整 SDK 时放在 dev-dependencies。验收正常依赖树中没有完整 `data-mover`，且大文件分块和偏移读写通过。[wire 数据块](../../crates/transport/src/chunk.rs) · [存储消费点](../../crates/app/src/file_commit.rs)
2. **移动会话状态。** 将 `NdxTable` 移入 app 远端 Sender，将 `DestIndex` 及目标分类移入 Receiver；`DirPageResult`、`NdxEntry`、`TransferDecision` 等实际在线上传输的类型留在 transport。同步移动索引测试。验收同一页的差分选择、源端失败、重试和 ACK 去重结果。[消息与索引实现](../../crates/transport/src/message.rs)
3. **移动落盘任务消息。** 将 `DiskCommitMsg`、`DcAck`、`FileOutcome` 移至 app 的远端落盘模块，保留向 wire ACK 的现有映射。`dual_process_streaming` 里直接驱动内部消息的用例可随模块迁入，保留断言；根包的真实双进程测试套件继续走公开命令。验收偏移、哈希错误、空文件、abort、重复 commit、清理及唯一完成结果。[落盘实现](../../crates/app/src/disk_commit.rs) · [流式落盘测试](../../crates/app/src/disk_commit/tests.rs)

B 批次先从改动前的 v14 编码记录代表性固定字节样本，包含 `CopyEntry`、`TarPacked` 之后仍在线上使用的枚举变体。序列化枚举顺序变化会改变 bincode 字节。迁移后应能解出原样本，并为同一消息编码出相同字节。该对照用于发现本轮结构变更引入的线协议变化。[QUIC 帧实现](../../crates/transport/src/quic/framing.rs) · [QUIC 往返测试](../../crates/transport/tests/quic_roundtrip.rs)

### C　收拢双进程会话并隔离本地队列

1. **合并 Receiver 会话入口和主循环。** 将 `receiver.rs` 中的 `recv_file_list_and_data_phase` 归入 `RemoteReceiverSession`，让同一会话拥有唯一读取循环、在途请求和落盘结果。保留容量 16 的落盘命令队列以及由主循环并发排空的无界 ACK 通道，避免两侧互等待，下游接纳数据后才归还额度。验证控制流先到 `TransferDone`、数据迟到、关闭中仍有磁盘任务等情况。最终进度和 `AllDone` 应在已请求项取得终态、落盘任务收束后发出。[会话实现与顺序测试](../../crates/app/src/remote_receiver_session.rs)
2. **让 CLI 调用两个远端用例。** 将 `serve_cmd` 中的目标存储创建、bind、证书写出、accept、单次接收和成功后的 `wait_idle` 收入 app 接收用例。CLI 保留参数解析；Sender 同步改为通过 app 远端发送入口启动。保留证书先于 accept 写出、写证书失败告警、版本和 token 校验、连接错误的类型及退出语义。远端仍拒绝源端删除和打包；逐项失败计入报告，会话失败通过调用结果返回。验收默认 CLI 启动两进程、证书与鉴权、断连退出、最终文件和报告。[CLI 现有时序](../../crates/cli/src/commands.rs) · [真实子进程测试](../../tests/remote_process_e2e.rs)
3. **替换普通本地全量路径上的消息队列。** 在 app 本地迁移模块内用现有有界 MPMC 通道表示条目、打包结果及错误。同步迁移 sender worker、`LocalCopyReceiver` 和 `AppError` 的相关分支。逐条核对容量 128、多个 worker 竞争领取、队列满时取消、生产者关闭后的排空和故障来源。复用 `LocalCopyExecutor`。远端协议测试仍可在内存 transport 上驱动。[本地队列与测试](../../crates/app/src/sender/copy_queue.rs) · [本地执行器](../../crates/app/src/receiver/in_process.rs) · [全量路径分派](../../crates/app/src/orchestrator/full.rs)
4. **最后接 Cargo feature。** 建立 `terrasync-rs/remote → cli/remote → app/remote → transport/quic`，由默认根构建启用。Web 的 app 依赖使用本地功能；若 cli 有默认 remote，根依赖 cli 时须关闭其默认 feature 并显式转发。删除 CLI 的直接 transport 依赖并迁移其错误转换；对 app 的远端模块、测试、examples 及相应错误分支按需加 feature 条件。分别检查独立 Web、本地根二进制、默认根二进制和 GUI 构建。目标是前两者的正常依赖图不启用 `transport/quic`，默认二进制仍提供两种远端角色。[根 features](../../Cargo.toml) · [app 依赖](../../crates/app/Cargo.toml) · [直接使用 QUIC 的 lab 测试](../../crates/app/tests/direct_cancel_real.rs)

### D　收拢本地迁移、扫描与执行配置

1. **按执行阶段拆本地编排。** 将 `run_local_prepared`、`run_planned_local`、全量与增量执行体依次迁入 local，沿用 `JobLease`、`CopyDatabaseExecution`、`copy_completion`、`source_cleanup`。先让全量和增量调用同一套已有收尾逻辑，再调整文件布局。普通文件取得目标就绪证据、复核源与身份后可清理源，和数据库批量冲刷并行。目录及根元数据有自己的提交条件。增量必须等待阶段 A 的扫描、复制和消费者冲刷成功后，才进入阶段 B 的删除／重命名。统计生命周期跨两阶段只开始和结束一次。[本地编排](../../crates/app/src/orchestrator.rs) · [提交收尾](../../crates/app/src/copy_completion.rs) · [迁移代次场景](../../tests/scenarios/migration-generations.md)
2. **以 scan 入口整合扫描实现。** 保留无数据库流式扫描和启用数据库时的持久化扫描路由。普通扫描用 Unordered，显式续扫用 Blocks；远端清单的 Blocks 选择独立于此。复用同一观察与过滤配置，持久化路径单独拥有确认游标、统计快照及重放去重。生产 checkpoint 至少间隔 60 秒且等完整目录块完成。验证大平铺目录中断、恢复后精确计数、用户取消与内部故障区分、在途数据库写结果未知的情况。[扫描入口](../../crates/app/src/scan.rs) · [持久化扫描](../../crates/app/src/scan/persisted.rs)
3. **最后调整配置读取。** 枚举 Web 写入全局 `AppConfig` 和 app 运行中再次读取的地点。先用一次“任务启动后更新配置”的对照记录当前行为。能在任务开始时确定的参数，通过已有请求或执行上下文传入有效配置；HDFS 角色配置继续在每次挂载时读取，作业查询和清理也读取当前数据库配置。这些调用只复制所需的配置段。任何改变进行中任务行为的决定单独提案。保留 db 使用的配置类型路径、CLI 覆盖优先级和许可检查时机。[全局配置](../../crates/utils/src/app_config.rs) · [Web 配置更新](../../crates/web/src/infrastructure/clickhouse_config.rs)

## 验收与交付

每个提交保存基线和改动后的输入、构建 feature、实际命令与结果。先运行触及模块的定向测试，再运行对应 CI。迁走内部测试后更新命令，并确认原有用例实际执行；被 feature 过滤成 0 个用例不能作为通过证据。

| 批次 | 定向命令，在仓库根运行 | 通过条件 |
| --- | --- | --- |
| A | `cargo test --locked -p task-model -p app -p web --features app/remote` | 报告 JSON、重试身份、脱敏、SQLite 回执幂等、终态排序和 CLI／Web 的迁移代次对照一致 |
| B | `cargo test --locked -p transport --features quic`；落盘及会话运行 `cargo test --locked -p app --features remote --lib`，逐项核对原用例；用 `cargo tree --locked -p transport -e normal` 查依赖 | wire 字节样本一致，落盘与 ACK 用例通过，完整 SDK 从 transport 正常依赖消失 |
| C | `cargo check --locked -p app --no-default-features --all-targets`、`cargo check --locked -p web`、`cargo check --locked -p terrasync-rs --no-default-features`、`cargo check --locked -p terrasync-rs --features gui,license`；`cargo test --locked --test remote_process_e2e -- --test-threads=1` | 单独用 `cargo tree --locked -p web -e normal,features` 和 `cargo tree --locked -p terrasync-rs --no-default-features -e normal,features` 检查 QUIC feature。默认 CLI 的远端命令、满队列取消、两端退出码、报告和文件均符合基线 |
| D | `cargo test --locked -p app --lib`（其中扫描定向运行 `cargo test --locked -p app --lib scan`）。按受影响路径执行代次矩阵、实验室故障与性能对照 | 全量／增量选择、源端留删、目标内容及元数据、checkpoint 与累计值一致；失败和取消后旧执行收束 |

- **真实续扫测试。** 在已配置的实验室，分别指定 Single／Cluster 的测试实例，允许测试创建和清理独立临时数据库。用 `cargo build --locked -p terrasync-rs --features lab-fault-injection` 构建被测 CLI，把 `TERRASYNC_TEST_BINARY` 指向该二进制的绝对路径，按测试设置 ClickHouse 环境变量。运行 `cargo test --locked -p app --features lab-fault-injection --test scan_resume -- --ignored --test-threads=1`。当前四个用例均需显式启用并记录执行结果。[续扫集成测试](../../crates/app/tests/scan_resume.rs)
- **数据库与恢复。** 触及持久化扫描或复制时，分别覆盖 ClickHouse Single 和 Cluster。注入源端、目标端或数据库的闪断、持续断连、超时及恢复，记录检测时间和故障识别后的时间；扫描的源端／ClickHouse 在识别后同一故障恢复预算约 3 秒。检查响应丢失后按执行身份处理、在途写入收束，以及终态可解释。触及通知时验证可用 ClickHouse 独立归档、callback 首次失败后最多再试 3 次、相同批次重试、HTTP 2xx 接收以及通知失败不改变任务结果。[故障约束](../external-dependency-failures.md) · [实验室约束](../../tests/lab/README.md)
- **性能。** 改到队列、遍历或收尾后，同数据、同并发、同扫描模式对照。不落库 Unordered 和 Blocks 各有自己的基线。全量及全量续扫吞吐损失目标不超过 5%，增量及增量续扫不超过 10%，增量分别测无变化和约 1% 增删改。报告净吞吐、续扫重放与整轮耗时、峰值 RSS 和持续增长，约 200 MiB 为优化目标。[性能基线](../perf/scan-performance-baseline.md) · [开发验收标准](../development.md)
- **合入。** 结构提交与行为修复分开。Web 的同步／校验任务取消传递、展示代次推断、创建任务时的目标路径参数，单列为待复现问题。每批评审实际依赖方向、跨模块公开类型和错误链，执行 Rust CI 中的 fmt、check、clippy、workspace 测试及串行远端 E2E。涉及前端或 MCP 契约时再执行对应流水线。软件、模拟故障和真实环境的结果分别记录。[架构审查](../architecture-review.md) · [CI 命令](../../.github/workflows/ci.yml)

首笔提交完成的判据很具体：app 与 Web 读写同一份诊断契约，双方无需了解对方的队列或 SQLite 实现，一份现有 callback 样本可在两端完成发送、接收与重复投递验收。它可以先合入，后面的协议和本地流水线迁移都能在此基础上独立验证。
