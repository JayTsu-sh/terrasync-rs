// 外部 crate
use thiserror::Error;

/// QUIC 传输层错误类型
#[derive(Error, Debug)]
pub enum QuicTransportError {
    /// QUIC 连接错误
    #[error("QUIC connection error: {0}")]
    ConnectionError(#[from] quinn::ConnectionError),

    /// QUIC 连接建立错误
    #[error("QUIC connect error: {0}")]
    ConnectError(#[from] quinn::ConnectError),

    /// QUIC 读取错误
    #[error("QUIC read error: {0}")]
    ReadError(#[from] quinn::ReadExactError),

    /// QUIC 写入错误
    #[error("QUIC write error: {0}")]
    WriteError(#[from] quinn::WriteError),

    /// bincode 序列化/反序列化错误
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// TLS 证书错误
    #[error("TLS error: {0}")]
    TlsError(String),

    /// IO 错误
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Transport 层错误
    #[error("Transport error: {0}")]
    TransportError(#[from] crate::error::TransportError),
}

pub type Result<T> = std::result::Result<T, QuicTransportError>;

impl From<QuicTransportError> for crate::error::TransportError {
    fn from(e: QuicTransportError) -> Self {
        crate::error::TransportError::SendFailed(format!("{}", e))
    }
}
