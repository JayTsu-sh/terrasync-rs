//! QUIC 多路复用：控制 / 文件列表 / 数据 / 进度+ack 四条逻辑 stream
//!
//! 改造前 Sender/Receiver 各只用一条 bidirectional stream（`Mutex<SendStream>` +
//! `Mutex<RecvStream>`），大文件 `FileData` 占满该 stream 时，progress/ack 等控制消息
//! 即使已经写到对端的接收缓冲区，也要等当前这轮读取循环空出来才会被读到。
//!
//! 本模块把逻辑消息按类别拆到 4 条独立的 QUIC bidirectional stream 上，每类流量互不
//! 阻塞对方；`recv()` 侧用「每条物理 stream 一个后台 task 串行读帧 + 转发进共享
//! `mpsc` channel」的方式做多路 fan-in —— 这是必须的，因为 `framing::read_msg`
//! （底层 `RecvStream::read_exact`）**不是 cancel-safe 的**：如果直接在
//! `tokio::select!` 里对多条 stream 的读操作赛跑，被取消的分支会丢失已读到一半的
//! 帧头/帧体字节，永久错位该 stream 后续的帧边界。放到独立 task 里让每条 stream
//! 的读循环各自跑到完整帧再转发，从根源上规避这个问题。
//!
//! fan-in channel 用**无界** `mpsc::unbounded_channel`：若改用有界 channel，某条 stream
//! （典型如大文件 `Data` stream）密集写入把 channel 挤满后，会反过来拖慢其它 stream 的
//! reader task 写入 channel（有界 channel 的发送方需要排队等待可用容量），重新引入
//! 「大数据流阻塞控制消息」的问题，与本模块要解决的目标相悖。真正的背压仍然存在于
//! QUIC 每条 stream 各自独立的接收窗口上，无界 channel 只是避免在这之上再叠加一层
//! 跨 stream 共享的人为瓶颈。
//!
//! ## Stream 发起方不对称 + 延迟 accept（两个真实进程联调时踩过的坑）
//!
//! `Control`/`FileList`/`Data` 三类由 **Sender** `open_bi`，因为 Sender 在这 3 条上都会
//! 写消息（Receiver 在其反方向写回复：`HandshakeAck`/`AuthResult` 走 `Control` 反向，
//! `TransferRequest` 等走 `FileList` 反向）。`AckProgress` 则反过来由 **Receiver**
//! `open_bi`、Sender 用 `accept_bi` 接：没有任何 `SenderMsg` variant 归类到
//! `AckProgress`，如果也让 Sender 发起（`open_bi` 却从不写），QUIC 只有发起方实际写入
//! 数据后对端才能感知该 stream 存在（见 quinn `RecvStream` 文档），Receiver 的
//! `accept_bi()` 就会永远等不到这条 stream 被 reveal，导致 Progress/Ack 永远发不出去。
//!
//! 同样的道理，Receiver 侧也**不能**依次同步 `accept_bi()` 全部 `Control`/`FileList`/`Data`
//! 三条：`FileList`/`Data` 要到协议后期（文件列表 / 数据传输阶段）Sender 才会第一次往上面
//! 写东西，若同步等它们 reveal，会在真正收到 `Handshake` 之前就卡住——而 Sender 又在
//! `connect()` 之后立即等 `HandshakeAck` 才会继续，形成死锁（两个真实子进程联调
//! `tests/remote_process_e2e.rs` 时发现：单进程内的 transport 单测因为收发双方共享同一个
//! tokio runtime、调度顺序凑巧没有触发这个问题）。修正为：Receiver 只同步 `accept_bi()`
//! `Control`（Sender 建立连接后立即写 `Handshake`，很快就绪）；`FileList`/`Data` 用后台 task
//! 异步 `accept_bi()`，对应的发送半部分通过 [`StreamSlot::Pending`] + `oneshot` 延迟交付给
//! `send_routed()`——这在语义上是安全的：`FileList` 的发送半部分（`TransferRequest` 等）只会
//! 在收到对应 `FilePage` 之后才用到，而收到 `FilePage` 就意味着该 stream 已经被 accept，
//! `oneshot` 早已就绪，不会真的阻塞。
//!
//! **`FileList`/`Data` 这两条后台异步 accept 的 stream 必须在同一个 task 里按顺序依次
//! `accept_bi()`**，不能像 `AckProgress` 那样各开一个独立 task 并发调用——这也是两个真实
//! 子进程联调时踩到的第二个坑：quinn 的“按创建顺序 yield”只在**同一个调用序列**内成立，
//! 两个 task 并发调用 `accept_bi()` 时，哪个 task 拿到哪条物理 stream 完全不确定，曾经
//! 实际观察到 `Data` 标签的 task 抢到了本该属于 `FileList` 的物理 stream，导致 `FileList`
//! 的 `send_routed()` 永远等不到 accept 完成、死锁到 QUIC 连接空闲超时（30s）。详见
//! [`spawn_lazy_accept_file_list_and_data`]。

// 外部 crate
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

// 内部模块
use super::framing;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};

/// 逻辑 stream 分类，每类对应一条独立的 QUIC bidirectional stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamKind {
    /// 握手 / 鉴权 / `SessionConfig` / `TransferDone` 等控制消息
    Control,
    /// 文件列表（`FilePage` 等）+ 传输请求（`TransferRequest` 等）+ 分类信号（`Classified`）
    FileList,
    /// 文件/目录/符号链接数据流
    Data,
    /// Entry/NDX 级 ack + 进度上报
    AckProgress,
}

impl StreamKind {
    /// 作为 `Vec` 下标使用
    fn index(self) -> usize {
        match self {
            StreamKind::Control => 0,
            StreamKind::FileList => 1,
            StreamKind::Data => 2,
            StreamKind::AckProgress => 3,
        }
    }
}

/// 由 Sender 发起（`open_bi`）的 3 类 stream，固定顺序建立
const CLIENT_INITIATED: [StreamKind; 3] = [StreamKind::Control, StreamKind::FileList, StreamKind::Data];

/// `SenderMsg` 按 variant 归类到对应的逻辑 stream（永远落在 [`CLIENT_INITIATED`] 三类之一）
pub(crate) fn sender_stream_kind(msg: &SenderMsg) -> StreamKind {
    match msg {
        SenderMsg::Handshake(_)
        | SenderMsg::Auth { .. }
        | SenderMsg::SessionConfig(_)
        | SenderMsg::TransferDone
        | SenderMsg::EntryError { .. } => StreamKind::Control,

        SenderMsg::FilePage(_) | SenderMsg::FileListError { .. } | SenderMsg::FileListDone => StreamKind::FileList,

        SenderMsg::CopyEntry { .. }
        | SenderMsg::CreateDir { .. }
        | SenderMsg::CreateSymlink { .. }
        | SenderMsg::FileBegin { .. }
        | SenderMsg::FileData { .. }
        | SenderMsg::EndOfFile { .. }
        | SenderMsg::DeltaData { .. }
        | SenderMsg::DeltaMatch { .. }
        | SenderMsg::TarPacked { .. }
        | SenderMsg::SetAcl { .. } => StreamKind::Data,
    }
}

/// `ReceiverMsg` 按 variant 归类到对应的逻辑 stream
pub(crate) fn receiver_stream_kind(msg: &ReceiverMsg) -> StreamKind {
    match msg {
        ReceiverMsg::HandshakeAck(_) | ReceiverMsg::AuthResult { .. } => StreamKind::Control,

        ReceiverMsg::TransferRequest { .. }
        | ReceiverMsg::DeltaTransferRequest { .. }
        | ReceiverMsg::MetadataUpdateRequest { .. }
        | ReceiverMsg::Classified { .. }
        | ReceiverMsg::RequestsDone => StreamKind::FileList,

        ReceiverMsg::EntrySuccess { .. }
        | ReceiverMsg::EntryError { .. }
        | ReceiverMsg::TarSuccess { .. }
        | ReceiverMsg::Success { .. }
        | ReceiverMsg::Redo { .. }
        | ReceiverMsg::Error { .. }
        | ReceiverMsg::Progress(_)
        | ReceiverMsg::AllDone => StreamKind::AckProgress,
    }
}

/// 某个逻辑 stream 的发送半部分：要么已就绪，要么还在等后台 `accept_bi()` 完成
pub(crate) enum StreamSlot {
    Ready(quinn::SendStream),
    Pending(oneshot::Receiver<quinn::SendStream>),
}

/// Sender 侧建立 4 条逻辑 stream：`Control`/`FileList`/`Data` 由本端 `open_bi`（均为本地
/// 操作，立即就绪，不依赖对端）；`AckProgress` 由对端（Receiver）发起，本端用
/// `accept_bi` 接——放到后台 task 里异步接入（见模块文档），就绪后再挂到共享
/// fan-in channel 上。
///
/// 返回：`send_streams`（长度 4，下标见 [`StreamKind::index`]，`send()` 据此路由）、
/// 统一的接收 channel、各 reader task 的 `JoinHandle`。
pub(crate) async fn sender_setup(
    conn: &quinn::Connection,
) -> Result<(
    Vec<Mutex<StreamSlot>>,
    UnboundedReceiver<ReceiverMsg>,
    Vec<JoinHandle<()>>,
)> {
    let mut send_streams = Vec::with_capacity(CLIENT_INITIATED.len() + 1);
    let mut recv_streams = Vec::with_capacity(CLIENT_INITIATED.len());
    for kind in CLIENT_INITIATED {
        let (s, r) = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::StreamSetupFailed(format!("open_bi({kind:?}): {e}")))?;
        send_streams.push(Mutex::new(StreamSlot::Ready(s)));
        recv_streams.push(r);
    }

    let (tx, rx) = mpsc::unbounded_channel::<ReceiverMsg>();
    let mut reader_tasks = Vec::with_capacity(CLIENT_INITIATED.len() + 1);
    for (idx, stream) in recv_streams.into_iter().enumerate() {
        let t = tx.clone();
        reader_tasks.push(tokio::spawn(reader_loop::<ReceiverMsg>(
            stream,
            t,
            CLIENT_INITIATED[idx],
        )));
    }

    // AckProgress：Receiver 侧是同步 open_bi（见 receiver_setup），不依赖任何应用层消息，
    // 这里放到后台只是为了写法统一，实际很快就会就绪。
    let ack_slot = spawn_lazy_accept::<ReceiverMsg>(conn.clone(), StreamKind::AckProgress, tx, &mut reader_tasks);
    send_streams.push(Mutex::new(ack_slot));

    Ok((send_streams, rx, reader_tasks))
}

/// Receiver 侧建立 4 条逻辑 stream：`Control` 同步 `accept_bi`（Sender 建立连接后立即写
/// `Handshake`，很快就绪）；`FileList`/`Data` 后台异步 `accept_bi`（Sender 要到协议后期
/// 才会往上面写东西，同步等待会死锁，见模块文档）；`AckProgress` 由本端 `open_bi`
/// （本地操作，无需等待对端，立即可写）。
///
/// 返回：`send_streams`（长度 4，下标见 [`StreamKind::index`]，`send()` 据此路由）、
/// 统一的接收 channel、各 reader task 的 `JoinHandle`。
pub(crate) async fn receiver_setup(
    conn: &quinn::Connection,
) -> Result<(
    Vec<Mutex<StreamSlot>>,
    UnboundedReceiver<SenderMsg>,
    Vec<JoinHandle<()>>,
)> {
    let (tx, rx) = mpsc::unbounded_channel::<SenderMsg>();
    let mut reader_tasks = Vec::with_capacity(CLIENT_INITIATED.len() + 1);

    // Control：同步 accept，握手消息几乎立即到达
    let (control_send, control_recv) = conn
        .accept_bi()
        .await
        .map_err(|e| TransportError::StreamSetupFailed(format!("accept_bi({:?}): {e}", StreamKind::Control)))?;
    reader_tasks.push(tokio::spawn(reader_loop::<SenderMsg>(
        control_recv,
        tx.clone(),
        StreamKind::Control,
    )));

    // FileList/Data：Sender 要到文件列表/数据传输阶段才会第一次写入，同步 accept 会在
    // Handshake 之前就卡住，形成死锁（Sender 正等 HandshakeAck 才继续）。改为后台异步——
    // 但二者必须在**同一个**后台 task 里按顺序依次 accept_bi()，不能像 AckProgress 那样
    // 各开一个独立 task 并发调用：quinn 的 accept_bi() 只在同一个调用序列内保证"按创建
    // 顺序 yield"，两个 task 并发调用时哪个 task 拿到哪条物理 stream 完全不确定，曾经
    // 导致 FileList/Data 被错误对调、形成死锁（两个真实子进程联调时发现）。
    let (file_list_slot, data_slot) = spawn_lazy_accept_file_list_and_data(conn.clone(), tx, &mut reader_tasks);

    // AckProgress：本端发起，不需要等待对端；反方向没有任何 SenderMsg 会写入，
    // 对应的 RecvStream 直接丢弃即可（Drop 会发 STOP_SENDING，但 Sender 本来就不会往这写）。
    let (ack_send, _ack_recv_unused) = conn
        .open_bi()
        .await
        .map_err(|e| TransportError::StreamSetupFailed(format!("open_bi({:?}): {e}", StreamKind::AckProgress)))?;

    let send_streams = vec![
        Mutex::new(StreamSlot::Ready(control_send)),
        Mutex::new(file_list_slot),
        Mutex::new(data_slot),
        Mutex::new(StreamSlot::Ready(ack_send)),
    ];

    Ok((send_streams, rx, reader_tasks))
}

/// 后台异步 `accept_bi()` 一条逻辑 stream：完成后把发送半部分通过 `oneshot` 交付
/// （对应的 `StreamSlot::Pending` 由 `send_routed()` 消费），并立即为接收半部分启动
/// `reader_loop`（转发进共享 fan-in channel）。返回一个待消费的 [`StreamSlot::Pending`]。
///
/// **只能用于同一 `Connection` 上不会再有其它并发 `accept_bi()` 调用的场景**（当前
/// 仅 Sender 侧的 `AckProgress` 满足：它是该连接上唯一一个后台异步 accept 的 stream）。
/// 若有多条 stream 都需要后台异步 accept，必须用同一个 task 顺序调用（见
/// [`spawn_lazy_accept_file_list_and_data`] 的文档说明原因）。
fn spawn_lazy_accept<T>(
    conn: quinn::Connection, kind: StreamKind, tx: UnboundedSender<T>, reader_tasks: &mut Vec<JoinHandle<()>>,
) -> StreamSlot
where
    T: DeserializeOwned + Send + 'static,
{
    let (deliver_tx, deliver_rx) = oneshot::channel::<quinn::SendStream>();
    reader_tasks.push(tokio::spawn(async move {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                // 发送半部分先交付，接收半部分再开始读循环：保证 send_routed() 需要用到
                // 该 stream 时（必然是先从这条 stream 收到过消息之后），oneshot 早已就绪。
                let _ = deliver_tx.send(send);
                reader_loop::<T>(recv, tx, kind).await;
            }
            Err(e) => warn!("[QUIC mux] accept {:?} stream failed: {e}", kind),
        }
    }));
    StreamSlot::Pending(deliver_rx)
}

/// 后台异步依次 `accept_bi()` `FileList` 与 `Data` 两条 stream（Receiver 侧专用）。
///
/// **不能**像 [`spawn_lazy_accept`] 那样为这两条各开一个独立 task 并发调用
/// `accept_bi()`：quinn 只保证「按创建顺序 yield」是针对**同一个调用序列**而言的
/// （见模块顶部文档引用的 quinn `RecvStream` 文档），如果两个不同 task 并发调用
/// `accept_bi()`，哪个 task 拿到哪条物理 stream 完全不确定——两个真实子进程联调时
/// 曾经实际触发过：`Data` 标签的 task 抢到了本该属于 `FileList` 的物理 stream，
/// 导致 `FileList` 的 `send_routed()` 永远等不到对应的 stream 被 accept，死锁。
/// 因此改为在**同一个**后台 task 里，严格按 `[FileList, Data]` 顺序依次
/// `accept_bi()`（与 Sender 侧 `open_bi()` 的顺序一致），每条 stream 一 accept 完成
/// 就立即交付发送半部分 + 单独 spawn 对应的 `reader_loop`（不阻塞去 accept 下一条）。
fn spawn_lazy_accept_file_list_and_data(
    conn: quinn::Connection, tx: UnboundedSender<SenderMsg>, reader_tasks: &mut Vec<JoinHandle<()>>,
) -> (StreamSlot, StreamSlot) {
    let (file_list_tx, file_list_rx) = oneshot::channel::<quinn::SendStream>();
    let (data_tx, data_rx) = oneshot::channel::<quinn::SendStream>();

    reader_tasks.push(tokio::spawn(async move {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let _ = file_list_tx.send(send);
                let t = tx.clone();
                tokio::spawn(reader_loop::<SenderMsg>(recv, t, StreamKind::FileList));
            }
            Err(e) => {
                warn!("[QUIC mux] accept {:?} stream failed: {e}", StreamKind::FileList);
                return;
            }
        }

        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let _ = data_tx.send(send);
                reader_loop::<SenderMsg>(recv, tx, StreamKind::Data).await;
            }
            Err(e) => warn!("[QUIC mux] accept {:?} stream failed: {e}", StreamKind::Data),
        }
    }));

    (StreamSlot::Pending(file_list_rx), StreamSlot::Pending(data_rx))
}

/// 单条物理 stream 的读循环：读到完整帧就转发进 channel，stream 结束或出错则退出
async fn reader_loop<T>(mut stream: quinn::RecvStream, tx: UnboundedSender<T>, kind: StreamKind)
where
    T: DeserializeOwned + Send + 'static,
{
    loop {
        match framing::read_msg::<T>(&mut stream).await {
            Ok(Some(msg)) => {
                if tx.send(msg).is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                warn!("[QUIC mux] {:?} stream read error: {e}", kind);
                break;
            }
        }
    }
}

/// 按消息类别把 `msg` 写到 `send_streams` 中对应的物理 stream；若该 stream 仍处于
/// [`StreamSlot::Pending`]（后台 `accept_bi()` 尚未完成），先等待其就绪。
pub(crate) async fn send_routed<T: Serialize>(
    send_streams: &[Mutex<StreamSlot>], kind: StreamKind, msg: &T,
) -> Result<()> {
    let mut guard = send_streams[kind.index()].lock().await;
    if let StreamSlot::Pending(_) = &*guard {
        // 取出 Pending 里的 oneshot receiver 消费掉（需要先放一个占位值进去满足借用检查）
        let (_placeholder_tx, placeholder_rx) = oneshot::channel();
        let StreamSlot::Pending(deliver_rx) = std::mem::replace(&mut *guard, StreamSlot::Pending(placeholder_rx))
        else {
            unreachable!("刚匹配过 Pending 分支")
        };
        let stream = deliver_rx
            .await
            .map_err(|_| TransportError::StreamSetupFailed(format!("{kind:?} accept task 提前退出")))?;
        *guard = StreamSlot::Ready(stream);
    }
    let StreamSlot::Ready(stream) = &mut *guard else {
        unreachable!("上面已确保此时必为 Ready")
    };
    framing::write_msg(stream, msg)
        .await
        .map_err(|e| TransportError::SendFailed(format!("{e}")))
}
