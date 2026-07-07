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

// 外部 crate
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::warn;

// 内部模块
use super::framing;
use crate::error::{Result, TransportError};
use crate::message::{ReceiverMsg, SenderMsg};

/// 共享 mpsc channel 的容量（4 条 stream fan-in 到一个 channel，需要一定缓冲避免
/// 某条 stream 数据密集到达时阻塞其后台 reader task）
const INCOMING_CHANNEL_CAPACITY: usize = 256;

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
    /// 固定顺序：Sender `open_bi` 与 Receiver `accept_bi` 必须严格按此顺序逐一建立
    /// （quinn 按创建顺序 yield stream，双端顺序不一致会导致消息路由错乱）
    const ALL: [StreamKind; 4] = [
        StreamKind::Control,
        StreamKind::FileList,
        StreamKind::Data,
        StreamKind::AckProgress,
    ];

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

/// `SenderMsg` 按 variant 归类到对应的逻辑 stream
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

/// 按 [`StreamKind::ALL`] 固定顺序依次 `open_bi`，返回 4 条 stream 的发送/接收半部分
///
/// 调用方（Sender）必须与对端 [`accept_mux_streams`] 使用相同的顺序，否则双方对
/// 同一物理 stream 的分类理解会错位。
pub(crate) async fn open_mux_streams(
    conn: &quinn::Connection,
) -> Result<(Vec<quinn::SendStream>, Vec<quinn::RecvStream>)> {
    let mut sends = Vec::with_capacity(StreamKind::ALL.len());
    let mut recvs = Vec::with_capacity(StreamKind::ALL.len());
    for kind in StreamKind::ALL {
        let (s, r) = conn
            .open_bi()
            .await
            .map_err(|e| TransportError::StreamSetupFailed(format!("open_bi({kind:?}): {e}")))?;
        sends.push(s);
        recvs.push(r);
    }
    Ok((sends, recvs))
}

/// 按 [`StreamKind::ALL`] 固定顺序依次 `accept_bi`，返回 4 条 stream 的发送/接收半部分
///
/// 调用方（Receiver）必须与对端 [`open_mux_streams`] 使用相同的顺序。
pub(crate) async fn accept_mux_streams(
    conn: &quinn::Connection,
) -> Result<(Vec<quinn::SendStream>, Vec<quinn::RecvStream>)> {
    let mut sends = Vec::with_capacity(StreamKind::ALL.len());
    let mut recvs = Vec::with_capacity(StreamKind::ALL.len());
    for kind in StreamKind::ALL {
        let (s, r) = conn
            .accept_bi()
            .await
            .map_err(|e| TransportError::StreamSetupFailed(format!("accept_bi({kind:?}): {e}")))?;
        sends.push(s);
        recvs.push(r);
    }
    Ok((sends, recvs))
}

/// 为每条 `RecvStream` 各起一个后台 task 串行读帧，全部转发进同一个共享 `mpsc` channel
///
/// 返回共享的 `mpsc::Receiver`（多路 fan-in 后的统一消息入口）与各 task 的 `JoinHandle`
/// （供调用方在 `close()`/`Drop` 时 abort，避免连接结束后残留读循环）。
pub(crate) fn spawn_reader_tasks<T>(recv_streams: Vec<quinn::RecvStream>) -> (mpsc::Receiver<T>, Vec<JoinHandle<()>>)
where
    T: DeserializeOwned + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<T>(INCOMING_CHANNEL_CAPACITY);
    let mut handles = Vec::with_capacity(recv_streams.len());
    for (idx, stream) in recv_streams.into_iter().enumerate() {
        let tx = tx.clone();
        let kind = StreamKind::ALL[idx];
        handles.push(tokio::spawn(reader_loop::<T>(stream, tx, kind)));
    }
    (rx, handles)
}

/// 单条物理 stream 的读循环：读到完整帧就转发进 channel，stream 结束或出错则退出
async fn reader_loop<T>(mut stream: quinn::RecvStream, tx: mpsc::Sender<T>, kind: StreamKind)
where
    T: DeserializeOwned + Send + 'static,
{
    loop {
        match framing::read_msg::<T>(&mut stream).await {
            Ok(Some(msg)) => {
                if tx.send(msg).await.is_err() {
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
