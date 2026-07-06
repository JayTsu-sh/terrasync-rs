pub mod error;
pub mod in_process;
pub mod message;
pub mod traits;

/// QUIC 传输层实现（启用 "quic" feature 时编译）
#[cfg(feature = "quic")]
pub mod quic;

pub mod prelude {
    pub use crate::error::{Result, TransportError};
    pub use crate::in_process::{
        InProcessReceiverTransport, InProcessSenderTransport, create_in_process_pair,
        create_in_process_pair_with_capacity,
    };
    pub use crate::message::{
        BlockSignature, DestIndex, DiskCommitMsg, FeatureFlags, HandshakeResult, HashAlgorithm,
        MIN_SUPPORTED_PROTOCOL_VERSION, NdxTable, PROTOCOL_VERSION, ProgressSnapshot, ProtocolHandshake, ReceiverMsg,
        SenderMsg, SessionConfig, TransferDecision,
    };
    #[cfg(feature = "quic")]
    pub use crate::quic::{QuicReceiverTransport, QuicSenderTransport};
    pub use crate::traits::{ReceiverTransport, SenderTransport};
}
