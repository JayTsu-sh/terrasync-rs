//! TerraSync 的存储边界：产品数据投影及旧会话与 data-mover 角色接口之间的转换。
//! 所有后端 IO、复制、恢复与原子发布由 data-mover 执行。
use data_mover::model::BackendKind;
use task_model::{ErrorEvent, ObservedEntryExt, StorageEntryMessage, TransferMode};
mod archive;
mod connected;
mod copied_metadata;
pub use data_mover::model::ObservedEntry;
pub mod hdfs;
mod options;
pub use hdfs::HdfsRecoverableCopyOptions;
pub mod async_receiver;
pub mod error;
mod integrity_check;
mod projection;
mod scanning;
mod streaming;
mod traversal_recovery;
pub use async_receiver::AsyncReceiver;
pub use connected::*;
pub use data_mover::storage::{HdfsConfig, HdfsKerberosCredentials, S3BucketInfo};
pub use data_mover::traversal::{RemoteScanFailure, RemoteScanFailureKind};
pub use data_mover::{FilterFieldDef, filter, get_filter_field_definitions, qos, redact_storage_url};
pub use error::{Result, StorageError};
pub use integrity_check::*;
pub use options::*;
pub use scanning::{
    DeleteDirIterator, DeleteEvent, DirectoryInventory, ResumableScan, ResumableScanItem, ScanSourceMode,
};
pub use streaming::*;
pub type WalkDirAsyncIterator = AsyncReceiver<StorageEntryMessage>;

pub fn canonicalize_path(value: &str) -> std::io::Result<String> {
    if backend_kind_from_location(value) == BackendKind::Local {
        Ok(soft_canonicalize::soft_canonicalize(value)?
            .to_string_lossy()
            .into_owned())
    } else {
        Ok(value.into())
    }
}

#[cfg(test)]
#[path = "../../../tests/support/observed.rs"]
mod observed_fixtures;
