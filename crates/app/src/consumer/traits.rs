//! 消费者接口定义模块
//!
//! 该模块定义了所有消费者必须实现的核心接口，是整个消费者系统的基础。
//! 消费者负责处理系统产生的各种消息，如存储条目、统计信息等。

// 外部crate
use async_trait::async_trait;
use storage_v2::StorageEntryMessage;
use tokio::sync::mpsc;

// 内部模块
use crate::error::Result;

/// 消费者接口定义
///
/// 所有消费者必须实现此trait，以便被消费者管理器统一管理。
/// 该trait定义了消费者的核心行为：启动和标识。
#[async_trait]
pub trait Consumer {
    /// 启动消费者，开始处理消息
    ///
    /// 该方法启动消费者任务，开始从通道接收器接收和处理消息。
    /// 当所有发送端被丢弃时，`recv()` 返回 None，消费者应刷新剩余数据后退出。
    ///
    /// # 参数
    /// - `receiver`: mpsc 通道接收器，带背压，用于接收待处理的消息
    ///
    /// # 返回值
    /// - 成功时返回包含异步任务句柄的`Ok`，任务句柄可以用于等待消费者完成
    /// - 失败时返回包含错误信息的`Err`
    async fn start(
        &mut self, receiver: mpsc::Receiver<StorageEntryMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>>;

    /// 获取消费者名称
    ///
    /// 返回消费者的唯一标识符，用于日志记录和调试。
    ///
    /// # 返回值
    /// - 消费者名称的静态字符串引用
    fn name(&self) -> &'static str;
}
