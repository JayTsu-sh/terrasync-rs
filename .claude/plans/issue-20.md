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
- ⬜ 步骤 8：`app` 层 Receiver（`receiver.rs`）：合并 `recv_file_list_phase` + `recv_file_data_phase`
  为 `recv_file_list_and_data_phase`，去除阶段 barrier。
- ⬜ 步骤 9：`app` 层新增测试：基于 `transport::in_process` 双端联调（Sender 侧调用 remote_sync.rs
  内部函数 + Receiver 侧调用公开的 `receiver_task_remote`），验证并发路径正确、无丢消息、
  dest==src。`cargo test -p app`。
- ⬜ 步骤 10：端到端：扩展 `tests/remote_process_e2e.rs` 新增多 chunk 大文件场景（验证 mux Data
  stream 正确性，dest==src），跑 2 次确认不 flake。
- ⬜ 步骤 11：全量验证收尾：`cargo fmt`、`cargo test -p transport --features quic`、
  `cargo test -p app`、`cargo test -p terrasync-rs --test remote_process_e2e`（连跑 2 次）；
  `git status` 确认无越界文件；移除本 plan 文件。
