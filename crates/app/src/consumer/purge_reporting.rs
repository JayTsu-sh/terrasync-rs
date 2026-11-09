//! 将永久清理的逐版本原因交付给现有诊断日志；不重复累计 entry／任务失败。
use std::sync::Arc;

use data_mover::model::SourceVersion;
use data_mover::storage::StorageRoleFailure;
use task_model::{ErrorEvent, StorageEntryMessage};
use tokio::sync::Mutex;

use super::stats::{StatisticConsumer, journal};
use crate::error::{AppError, Result, StorageError};

pub(super) async fn record(consumer: &Arc<Mutex<StatisticConsumer>>, error: &AppError) -> Result<()> {
    let Some((path, failure)) = error.source_cleanup_entry() else {
        return Ok(());
    };
    let Some(StorageError::SourcePurgeRefused { source, .. }) = failure.storage_error() else {
        return Ok(());
    };
    let first = source.version.as_ref().map(|version| (version, source.source.as_ref()));
    for (version, cause) in first
        .into_iter()
        .chain(source.additional.iter().map(|(version, cause)| (version, cause)))
    {
        let version = match version {
            SourceVersion::Current => "current",
            SourceVersion::Id(id) => id.as_str(),
        };
        let diagnostic = match cause {
            StorageRoleFailure::Entry(cause) => cause.diagnostic(),
            StorageRoleFailure::Session(cause) => cause.diagnostic(),
        };
        journal::record(
            consumer,
            &StorageEntryMessage::Diagnostic {
                event: ErrorEvent::Delete,
                path: path.to_path_buf(),
                reason: format!("source_version={version} phase=SourceCleanup: {cause}; {diagnostic}"),
            },
        )
        .await?;
    }
    Ok(())
}
