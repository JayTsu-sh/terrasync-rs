//! ClickHouse 请求、恢复及取消机制，供扫描和目录持久化共用。
use crate::{ClickHouseConfig, Result};
use clickhouse::Client;

pub(crate) mod queries;
pub(crate) mod recovery;
pub(crate) mod transport;
pub use queries::{ScanQueryCancellation, with_scan_cancellation};
pub use recovery::{ConnectivityWatch, ScanDatabaseExecution};
pub(crate) mod read;

pub(crate) fn base_client(config: &ClickHouseConfig) -> Result<Client> {
    transport::build_client(config, None)
}
