//! 消息帧协议：length-prefix framing over QUIC streams
//!
//! 帧格式：[4 bytes LE total_length][1 byte flags][payload]
//! - flags bit 0: zstd 压缩（1=compressed, 0=raw）
//! - total_length = 1 (flags) + payload.len()

use bincode::config;
use quinn::{RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::error::{QuicTransportError, Result};

/// 最大消息大小（256 MiB）
const MAX_MESSAGE_SIZE: u32 = 256 * 1024 * 1024;

/// 压缩阈值：payload 超过此大小才压缩（避免小消息压缩开销）
const COMPRESSION_THRESHOLD: usize = 1024;

/// flags bit 定义
const FLAG_ZSTD: u8 = 0x01;

/// zstd 压缩级别（3 是 zstd 默认，平衡速度和压缩率）
const ZSTD_LEVEL: i32 = 3;

fn bincode_config() -> impl config::Config {
    config::standard()
}

/// 将消息序列化 + 可选压缩后写入 QUIC stream
pub async fn write_msg<T: Serialize>(stream: &mut SendStream, msg: &T) -> Result<()> {
    let raw = bincode::serde::encode_to_vec(msg, bincode_config())
        .map_err(|e| QuicTransportError::SerializationError(format!("encode: {}", e)))?;

    let (payload, flags) = if raw.len() >= COMPRESSION_THRESHOLD {
        // zstd 压缩
        let compressed = zstd::encode_all(raw.as_slice(), ZSTD_LEVEL)
            .map_err(|e| QuicTransportError::SerializationError(format!("zstd compress: {}", e)))?;
        // 只在压缩后更小时使用
        if compressed.len() < raw.len() {
            (compressed, FLAG_ZSTD)
        } else {
            (raw, 0u8)
        }
    } else {
        (raw, 0u8)
    };

    // 写入 length prefix（total = 1 flags + payload）
    let total_len = (1 + payload.len()) as u32;
    stream
        .write_all(&total_len.to_le_bytes())
        .await
        .map_err(QuicTransportError::WriteError)?;

    // 写入 flags
    stream
        .write_all(&[flags])
        .await
        .map_err(QuicTransportError::WriteError)?;

    // 写入 payload
    stream
        .write_all(&payload)
        .await
        .map_err(QuicTransportError::WriteError)?;

    Ok(())
}

/// 从 QUIC stream 读取消息帧 + 可选解压 + 反序列化
pub async fn read_msg<T: DeserializeOwned>(stream: &mut RecvStream) -> Result<Option<T>> {
    // 读取 length prefix
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(e) => return Err(QuicTransportError::ReadError(e)),
    }

    let total_len = u32::from_le_bytes(len_buf);
    if total_len > MAX_MESSAGE_SIZE || total_len < 1 {
        return Err(QuicTransportError::SerializationError(format!(
            "invalid frame size: {} bytes",
            total_len
        )));
    }

    // 读取 flags
    let mut flags_buf = [0u8; 1];
    stream
        .read_exact(&mut flags_buf)
        .await
        .map_err(QuicTransportError::ReadError)?;
    let flags = flags_buf[0];

    // 读取 payload
    let payload_len = (total_len - 1) as usize;
    let mut payload = vec![0u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(QuicTransportError::ReadError)?;

    // 解压（如果压缩了）
    let raw = if flags & FLAG_ZSTD != 0 {
        zstd::decode_all(payload.as_slice())
            .map_err(|e| QuicTransportError::SerializationError(format!("zstd decompress: {}", e)))?
    } else {
        payload
    };

    // 反序列化
    let (msg, _) = bincode::serde::decode_from_slice(&raw, bincode_config())
        .map_err(|e| QuicTransportError::SerializationError(format!("decode: {}", e)))?;

    Ok(Some(msg))
}
