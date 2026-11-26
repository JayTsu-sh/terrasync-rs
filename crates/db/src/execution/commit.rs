//! 代次发布和回查共用一次数据库请求恢复窗口。

use std::{
    ops::AsyncFn,
    sync::{Arc, OnceLock},
};

use clickhouse::query::Query;
use task_model::GenerationCommitResult;

use super::{
    queries::{check_scan_cancellation, execute_request},
    recovery::RequestSafety,
};
use crate::{DatabaseError, Result};

/// 发送后结果不明时仅回查。连接层确认未发送时仍可按执行预算重试。
pub(crate) async fn commit_with_readback(
    query: Query, readback: impl AsyncFn() -> Result<bool>, outcome: &mut GenerationCommitResult,
) -> Result<()> {
    let unconfirmed = OnceLock::<Option<Arc<DatabaseError>>>::new();
    let result = execute_request(RequestSafety::IdempotentWrite, || async {
        check_scan_cancellation()?;
        if let Some(Some(source)) = unconfirmed.get() {
            if readback().await? {
                return Ok(());
            }
            // 未见记录时，旧请求仍可能执行。仅重试回查，不再次发出提交。
            return Err(DatabaseError::ScanRequestFailed {
                source: Arc::clone(source),
            });
        }
        match query
            .clone()
            .execute()
            .await
            .map_err(DatabaseError::scan_commit_response)
        {
            Err(error)
                if error.is_confirmed_scan_cancellation()
                    || matches!(error, DatabaseError::ScanTerminationUnconfirmed { .. }) =>
            {
                // 此时请求已经进入发送路径，取消确认不能证明没有写入。
                let _ = unconfirmed.set(None);
                Err(error)
            }
            Err(error) if error.is_transient_connectivity_failure() && !error.request_was_not_sent() => {
                let source = Arc::new(error);
                let _ = unconfirmed.set(Some(source.clone()));
                Err(DatabaseError::ScanRequestFailed { source })
            }
            result => result,
        }
    })
    .await;
    if result.is_ok() {
        *outcome = GenerationCommitResult::Committed;
    } else if unconfirmed.get().is_some() {
        *outcome = GenerationCommitResult::PendingConfirmation;
    }
    result
}
