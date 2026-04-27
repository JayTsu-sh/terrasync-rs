// 外部 crate
use thiserror::Error;

/// Transport 层错误类型
#[derive(Error, Debug)]
pub enum TransportError {
    /// 通道已关闭（发送端或接收端已 drop）
    #[error("Channel closed")]
    ChannelClosed,

    /// 发送消息失败
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// 接收消息失败
    #[error("Recv failed: {0}")]
    RecvFailed(String),

    /// 序列化/反序列化错误（双进程模式下使用）
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// 底层存储操作错误
    #[error("Storage error: {0}")]
    StorageError(#[from] data_mover::error::StorageError),
}

pub type Result<T> = std::result::Result<T, TransportError>;
