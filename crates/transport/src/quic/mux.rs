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
//! ## Stream 发起方不对称
//!
//! `Control`/`FileList`/`Data` 三类由 **Sender** `open_bi`，因为 Sender 在这 3 条上都会
//! 写消息（Receiver 在其反方向写回复：`HandshakeAck`/`AuthResult` 走 `Control` 反向，
//! `TransferRequest` 等走 `FileList` 反向）。`AckProgress` 则反过来由 **Receiver**
//! `open_bi`、Sender 用 `accept_bi` 接：没有任何 `SenderMsg` variant 归类到
//! `AckProgress`，如果也让 Sender 发起（`open_bi` 却从不写），QUIC 只有发起方实际写入
//! 数据后对端才能感知该 stream 存在（见 quinn `RecvStream` 文档），Receiver 的
//! `accept_bi()` 就会永远等不到这条 stream 被 reveal，导致 Progress/Ack 永远发不出去。

// 外部 crate
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
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
    /// 文件列表（`FilePage` 等）+ 传输请求（`TransferRequest` 等）
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

/// Sender 侧建立 4 条逻辑 stream：`Control`/`FileList`/`Data` 由本端 `open_bi`；
/// `AckProgress` 由对端（Receiver）发起，本端用 `accept_bi` 接——这一步不能阻塞在
/// `connect()` 的关键路径上（Receiver 可能要到协议后期第一次发 `Progress`/
/// `EntrySuccess` 才会 `open_bi`），因此放到后台 task 里异步接入，就绪后再挂到
/// 共享 fan-in channel 上。
///
/// 返回：`send_streams`（长度 3，下标对应 [`CLIENT_INITIATED`]，`send()` 据此路由）、
/// 统一的接收 channel、各 reader task 的 `JoinHandle`。
pub(crate) async fn sender_setup(
    conn: &quinn::Connection,
) -> Result<(
    Vec<Mutex<quinn::SendStream>>,
    UnboundedReceiver<ReceiverMsg>,
    Vec<JoinHandle<()>>,
)> {
    let mut send_streams = Vec::with_capacity(CLIENT_INITIATED.len());
    let mut recv_streams = Vec::with_capacity(CLIENT_INITIATED.len());
    for kind in CLIENT_INITIATED {
        let (s, r) = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::StreamSetupFailed(format!("open_bi({kind:?}): {e}")))?;
        send_streams.push(Mutex::new(s));
        recv_streams.push(r);
    }

    let (tx, rx) = mpsc::unbounded_channel::<ReceiverMsg>();
    let mut reader_tasks = Vec::with_capacity(CLIENT_INITIATED.len() + 1);
    for (idx, stream) in recv_streams.into_iter().enumerate() {
        let tx = tx.clone();
        reader_tasks.push(tokio::spawn(reader_loop::<ReceiverMsg>(
            stream,
            tx,
            CLIENT_INITIATED[idx],
        )));
    }

    let ack_conn = conn.clone();
    reader_tasks.push(tokio::spawn(async move {
        match ack_conn.accept_bi().await {
            // Sender 侧永远不写 AckProgress，发送半部分保留但不使用（Drop 时正常 finish，无副作用）
            Ok((_send_unused, recv)) => reader_loop::<ReceiverMsg>(recv, tx, StreamKind::AckProgress).await,
            Err(e) => warn!("[QUIC mux] accept AckProgress stream failed: {e}"),
        }
    }));

    Ok((send_streams, rx, reader_tasks))
}

/// Receiver 侧建立 4 条逻辑 stream：`Control`/`FileList`/`Data` 用 `accept_bi` 等待
/// 对端（Sender）发起；`AckProgress` 由本端 `open_bi`（无需等待对端，立即可写）。
///
/// 返回：`send_streams`（长度 4，下标见 [`StreamKind::index`]，`send()` 据此路由）、
/// 统一的接收 channel、各 reader task 的 `JoinHandle`。
pub(crate) async fn receiver_setup(
    conn: &quinn::Connection,
) -> Result<(
    Vec<Mutex<quinn::SendStream>>,
    UnboundedReceiver<SenderMsg>,
    Vec<JoinHandle<()>>,
)> {
    let mut send_streams = Vec::with_capacity(CLIENT_INITIATED.len() + 1);
    let mut recv_streams = Vec::with_capacity(CLIENT_INITIATED.len());
    for kind in CLIENT_INITIATED {
        let (s, r) = conn
            .accept_bi()
            .await
            .map_err(|e| TransportError::StreamSetupFailed(format!("accept_bi({kind:?}): {e}")))?;
        send_streams.push(Mutex::new(s));
        recv_streams.push(r);
    }

    // AckProgress：本端发起，不需要等待对端；反方向没有任何 SenderMsg 会写入，
    // 对应的 RecvStream 直接丢弃即可（Drop 会发 STOP_SENDING，但 Sender 本来就不会往这写）。
    let (ack_send, _ack_recv_unused) = conn
        .open_bi()
        .await
        .map_err(|e| TransportError::StreamSetupFailed(format!("open_bi({:?}): {e}", StreamKind::AckProgress)))?;
    send_streams.push(Mutex::new(ack_send));

    let (tx, rx) = mpsc::unbounded_channel::<SenderMsg>();
    let mut reader_tasks = Vec::with_capacity(CLIENT_INITIATED.len());
    for (idx, stream) in recv_streams.into_iter().enumerate() {
        let tx = tx.clone();
        reader_tasks.push(tokio::spawn(reader_loop::<SenderMsg>(
            stream,
            tx,
            CLIENT_INITIATED[idx],
        )));
    }

    Ok((send_streams, rx, reader_tasks))
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

/// 按消息类别把 `msg` 写到 `send_streams` 中对应的物理 stream
pub(crate) async fn send_routed<T: Serialize>(
    send_streams: &[Mutex<quinn::SendStream>], kind: StreamKind, msg: &T,
) -> Result<()> {
    let mut stream = send_streams[kind.index()].lock().await;
    framing::write_msg(&mut stream, msg)
        .await
        .map_err(|e| TransportError::SendFailed(format!("{e}")))
}
