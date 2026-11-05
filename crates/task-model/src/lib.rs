//! CLI、Web、数据库和传输共用的纯任务契约。
//! 观察始终使用原生类型；逐版本视图借用共享观察，不复制历史。
pub mod error;
mod event;
mod facts;
pub mod observation;
mod transfer;
pub use error::{Result, TaskModelError};
pub use event::{ChangeKind, EntryChange, ErrorEvent, StorageEntryMessage};
pub use facts::ObservedEntryExt;
pub use transfer::TransferMode;

#[cfg(test)]
#[path = "../../../tests/support/observed.rs"]
mod observed_fixtures;
