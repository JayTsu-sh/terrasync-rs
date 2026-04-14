//! 消费者模块
//!
//! 该模块负责管理和实现不同类型的消费者，用于处理扫描和复制过程中产生的消息。
//! 主要包括：
//! 1. 消费者接口定义
//! 2. 消费者管理器
//! 3. 存储条目消费者
//! 4. 统计信息消费者

// 子模块声明（按字母顺序排序）
/// 消费者管理器，用于协调和管理多个消费者
mod manager;
/// 统计信息消费者，用于收集和展示任务统计信息
pub mod stats;

// 公共模块
/// 存储条目消费者，用于处理存储条目消息
pub mod database;
/// 消费者接口定义，所有消费者必须实现此接口
pub mod traits;

// 重新导出重要的类型（按字母顺序排序）

pub use database::DatabaseConsumer;
pub use manager::ConsumerManager;
pub use stats::{JobResult, ProgressReport, StatisticConsumer};
pub use traits::Consumer;
