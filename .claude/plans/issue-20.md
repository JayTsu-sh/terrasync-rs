# Issue #20:QUIC 传输支持控制/数据/进度多路复用

## 分支基线

`origin/main` @ `c545a7c`(含 #18 握手 + #19 鉴权)。分支 `claude/issue-20` 从此创建。

## 需求 & 目标（摘自 approved spec）

将 QUIC 双进程传输从「单一 bidirectional stream + Mutex 串行化 + 三段式 barrier(file list →
request/data → ack)」改造为多路复用架构，使 progress/ack/error/redo 等控制消息能在大文件数据
传输期间实时穿插到达，不被数据流阻塞；同时 file list 生成、目标端 checksum 请求、源端数据发送
三者可流水线并发。

## 方案设计（关键决策，供恢复时对齐）

- **Stream 分类**：除控制 stream（Handshake/Auth/SessionConfig/TransferDone）外，新增
  `FileList`（FilePage/FileListError/FileListDone + TransferRequest/DeltaTransferRequest/
  MetadataUpdateRequest/RequestsDone）、`Data`（CreateDir/CreateSymlink/FileBegin/FileData/
  EndOfFile/DeltaData/DeltaMatch/TarPacked/SetAcl/CopyEntry）、`AckProgress`（EntrySuccess/
  EntryError/TarSuccess/Success/Redo/Error/Progress/AllDone）三条 stream，共 4 条 bidirectional
  QUIC stream。
- **Stream 发起方不对称（重要踩坑记录）**：一开始设计为 Sender 固定顺序 `open_bi` 全部 4 条、
  Receiver 固定顺序 `accept_bi` 全部 4 条，写测试时发现 `AckProgress` 永远读不到——QUIC 只有
  **发起方**实际写数据后对端才能感知该 stream 存在（quinn `RecvStream` 文档原文："Peers are not
  notified of streams until they or a later-numbered stream are used to send data"）。由于没有
  任何 `SenderMsg` variant 归类到 `AckProgress`（该类别只有 `ReceiverMsg` 使用），若这条 stream
  仍由 Sender `open_bi`，Sender 永远不会写它，Receiver 的 `accept_bi()` 会永远阻塞，Progress/Ack
  发不出去。**修正**：`Control`/`FileList`/`Data` 仍由 Sender `open_bi`（对应 `mux::CLIENT_INITIATED`
  固定顺序）；`AckProgress` 反过来由 **Receiver** `open_bi`、Sender 用 `accept_bi` 接——且这一步
  不能阻塞在 `connect()` 的关键路径上（Receiver 可能要到协议后期第一次发 Progress 才会
  `open_bi`），故用后台 task 异步接入（`mux::sender_setup` 内部 `tokio::spawn` 等 `accept_bi()`）。
- **recv() 多路 fan-in**：`framing::read_msg`（本质是 `read_exact`）**不是 cancel-safe 的**，不能
  直接在 `tokio::select!` 里对 4 条 stream 的读操作赛跑（否则被取消的分支会丢失半读的帧，永久
  错位该 stream 的边界）。改为：每条物理 stream 由独立后台 task 串行读到完整帧后转发进一个共享
  `mpsc` channel；`recv()` 只需 `channel.recv().await`，从根源避免 partial-read 取消问题，同时
  实现「大文件占满 Data stream 不影响 AckProgress stream 被及时读到」。
- **旧测试兼容**：`crates/transport/tests/quic_roundtrip.rs` 现有测试手工 `conn.accept_bi()` 一次，
  只收发 Handshake/Auth/SessionConfig/TransferDone/HandshakeAck/AuthResult/AllDone —— 这些消息全部
  归类到 `Control`（`CLIENT_INITIATED` 数组第 0 位，最先 open_bi），因此旧测试无需改动即可通过；
  新测试（步骤 6）里手工扮演 Receiver 的一侧，必须相应地对 `AckProgress` 改用 `open_bi()` 而非
  `accept_bi()`，与生产端 `mux::receiver_setup` 行为一致。
- **App 层并发化（避免"多个并发消费者抢同一个 recv()"的数据丢失/错乱风险）**：
  - Receiver（`receiver.rs`）：把 `recv_file_list_phase` + `recv_file_data_phase` **合并为一个**
    `recv_file_list_and_data_phase`，单一消费者循环内按 variant dispatch，去掉两阶段之间的
    顺序 barrier（`FileListDone` 时发 `RequestsDone` 但不再 break，继续循环处理数据消息）。
  - Sender（`remote_sync.rs`）：`send_file_list_phase`（只 `send()`，从不 `recv()`）与合并后的
    `process_requests_and_acks`（唯一的 `recv()` 消费者，处理 TransferRequest/DeltaTransferRequest/
    RequestsDone + EntrySuccess/EntryError/Progress/AllDone）通过 `tokio::try_join!` 并发运行，
    `NdxTable` 改用 `std::sync::Mutex` 支持并发读写（写者只有 file-list 任务，读者只有 request 任务，
    无二义性）。`RequestsDone` 到达时立即发 `TransferDone`（保持原有时序），但循环继续等 Ack 直到
    `AllDone`。
  - **不**引入"两个并发任务同时调用同一个 recv()"的设计（会导致消息被错误的消费者窃取/丢弃），
    这是本方案与 spec 描述"tokio::spawn 多个 consumer"字面表述的唯一偏离，原因见上，记录于此供
    review 参考。

## 验收标准

- 大文件传输期间仍能实时收到 progress/ack/error/redo（由 transport 层多 stream + 独立 reader task
  保证，`quic_roundtrip.rs` 新增专项测试验证）；file list/checksum 请求/数据发送可流水线并发
  （app 层 try_join! 验证）；单大文件不阻塞其他控制消息。
- 新增/修改测试全部通过，workspace 无回归。

## 执行步骤

- ✅ 步骤 1：`crates/transport/src/error.rs` 新增 `TransportError::StreamSetupFailed` variant。
- ✅ 步骤 2-5（合并一次提交，互相耦合无法拆分为独立可编译的中间态）：新增
  `crates/transport/src/quic/mux.rs`（`StreamKind` 分类 + `open_mux_streams`/
  `accept_mux_streams` + 后台 reader task fan-in 到共享 mpsc channel），`quic/mod.rs` 注册模块；
  改造 `QuicSenderTransport`/`QuicReceiverTransport` 用 4 条 stream + mux 路由 send/recv，`Drop`/
  `close()` 时 abort 后台 reader task；`cargo test -p transport --features quic` 现有 8 个测试
  全部通过（历史握手/鉴权测试在新 mux 架构下行为不变，证明向后兼容）。
- ✅ 步骤 6：`quic_roundtrip.rs` 新增 `test_quic_mux_large_data_stream_does_not_block_ack_progress_stream`：
  后台任务持续往 Data stream 写不可压缩数据（8MiB，远超 quinn 默认 ~1.19MiB 单 stream 接收窗口，
  对端故意不读触发 flow control 阻塞，用 `is_finished()==false` 断言阻塞确实发生），同时对端在
  AckProgress stream 回一条 Progress，断言 `sender.recv()` 在 2s 超时内读到——过程中发现并修正了
  上面记录的"stream 发起方不对称"设计问题。`cargo test -p transport --features quic` 全部 9 个
  测试通过，连跑 2 次无 flake。
- ✅ 步骤 7：`app` 层 Sender（`remote_sync.rs`）：合并 `process_requests` + `process_acks` 为
  `process_requests_and_acks`，与 `send_file_list_phase` 用 `tokio::try_join!` 并发运行，
  `NdxTable` 改 `std::sync::Mutex`（写者=文件列表任务，读者=请求处理任务）。
  `cargo check -p app` + `cargo clippy -p app --no-deps`（无新增警告）+
  `cargo test -p app`（27 个既有测试全部通过，无回归）。
- ✅ 步骤 8：`app` 层 Receiver（`receiver.rs`）：合并 `recv_file_list_phase` + `recv_file_data_phase`
  为 `recv_file_list_and_data_phase`，单一消费者循环内按 variant dispatch，去除阶段 barrier
  （`FileListDone` 时发 `RequestsDone` 但不 break，继续处理数据消息直到 `TransferDone`）。
  `cargo check -p app` + `cargo clippy -p app --no-deps`（receiver.rs 无新增警告）+
  `cargo test -p app`（27 个既有测试全部通过）。
- ✅ 步骤 9：`app` 层新增测试 `remote_sync::tests::sender_receiver_pipeline_roundtrip_in_process`：
  基于 `transport::in_process` 双端联调（Sender 侧直接调用 remote_sync.rs 内部的
  `send_file_list_phase`/`process_requests_and_acks` + Receiver 侧调用公开的
  `receiver_task_remote`，走真实 tempdir 本地存储），验证并发路径正确、
  success/error 计数与文件数吻合（无丢消息）、dest==src。新增 `tempfile` dev-dependency。
  `cargo test -p app`（28 个测试全部通过，连跑 2 次无 flake）+ `cargo clippy -p app --no-deps
  --tests`（remote_sync.rs 无新增警告）。
- ✅ 步骤 10：端到端：扩展 `tests/remote_process_e2e.rs` 新增
  `test_remote_process_e2e_large_multi_chunk_file_mux`（10MiB 跨多 chunk 大文件 + 既有小文件，
  验证 mux Data stream 正确性，dest==src）。**这一步过程中用真实两进程联调暴露并修正了 2 个
  单进程 transport 单测完全测不出来的严重 bug**（记录于此，供 review / 恢复时对齐）：
  1. **`FileList`/`Data` 后台 accept 竞态**：`receiver_setup` 最初为 `FileList`/`Data` 各起一个
     独立后台 task 并发 `accept_bi()`（仿照 `AckProgress` 的写法）。quinn 的"按创建顺序 yield"
     只在同一个调用序列内成立，两个 task 并发调用时哪个拿到哪条物理 stream 完全不确定——真实
     两进程联调时实际触发：`Data` 标签的 task 抢到了本该属于 `FileList` 的物理 stream，导致
     `FileList` 的 `send_routed()` 永远等不到 accept 完成，死锁到 QUIC 连接 30s 空闲超时才「解开」
     （此时数据已经乱套）。**修正**：`FileList`/`Data` 必须在同一个后台 task 里按 Sender 的
     `open_bi()` 顺序依次 `accept_bi()`（`mux::spawn_lazy_accept_file_list_and_data`）。
  2. **`TransferDone` 与实际数据完成解耦（更隐蔽，正确性 bug）**：`TransferDone` 走 `Control`
     stream，真正的文件数据走独立的 `Data` stream——多路复用后二者是完全独立的物理 stream，
     不再有"同一根 stream 严格 FIFO"的隐含顺序保证；体积很小的 `TransferDone` 完全可能抢在
     大文件的 `FileData`/`EndOfFile` 之前被 Receiver 处理，若仍然"收到 `TransferDone` 就 break"，
     会把还在 `Data` stream 上飞的文件直接丢弃（真实复现：4 个文件只收到 1 个）。**修正**：
     Receiver 显式计数 `requested_count`（发出过的 `TransferRequest`/`DeltaTransferRequest` 数）
     与 `completed_count`（收到过的对应 `EndOfFile`/`CreateSymlink` 完成数），只有二者相等
     **且**已经见过 `TransferDone` 才真正结束循环（`recv_file_list_and_data_phase`）。
  3. （附带）`tests/remote_process_e2e.rs::free_loopback_port` 原先用 TCP 探测端口再释放给
     UDP（QUIC 实际协议）使用，探测协议本就不匹配，且多个测试在同一 `cargo test` 进程内并发跑时
     有 TOCTOU 窗口可能撞到同一端口号；改为 UDP 探测 + 进程内 `ALLOCATED_PORTS` 去重集合，
     消除这一类端口冲突（测试基础设施小改动，不影响任何测试的可观察行为）。
  `cargo test -p terrasync-rs --test remote_process_e2e`：4 个测试（3 个既有 + 1 个新增）全部
  通过，连跑 5 次全部 pass（个别 run 因 QUIC 连接收尾偶发命中 30s 空闲超时兜底导致单次耗时
  变长，但结果始终是 4/4 pass，不是新增的 flaky failure；这一兜底路径是既有测试
  `RECEIVER_EXIT_TIMEOUT=35s` 注释里本就承认并预留余量的已知特性，非本次改动引入的回归）。
- ⬜ 步骤 11：全量验证收尾：`cargo fmt`、`cargo test -p transport --features quic`、
  `cargo test -p app`、`cargo test -p terrasync-rs --test remote_process_e2e`（连跑 2 次）；
  `git status` 确认无越界文件；移除本 plan 文件。
