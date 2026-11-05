//! 传输帧的数据块；进入存储引擎时移动 Bytes，不复制 payload。
use bytes::Bytes;
use data_mover::storage::PositionedChunk;
use serde::{Deserialize, Serialize};

/// 数据块结构体 - 用于表示文件传输过程中的数据片段
///
/// 该结构体封装了文件数据的一个数据块及其在原始文件中的偏移量，
/// 主要用于在文件复制过程中进行数据分块传输和处理。
#[derive(Debug, Serialize, Deserialize)]
pub struct DataChunk {
    /// 文件中的偏移量，表示该数据块在原文件中的起始位置
    pub offset: u64,
    /// 数据块的实际内容
    pub data: Bytes,
}

impl From<DataChunk> for PositionedChunk {
    fn from(chunk: DataChunk) -> Self {
        Self {
            offset: chunk.offset,
            data: chunk.data,
        }
    }
}
