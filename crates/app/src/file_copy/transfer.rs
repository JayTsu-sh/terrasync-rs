//! 将任务策略编译为唯一的原生传输请求；恢复和发布仍由 data-mover 负责。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use data_mover::metadata::MetadataPlan;
use data_mover::model::{EntryKind, ObservedEntry};
use data_mover::transfer::{
    self, MetadataFailurePolicy, MetadataTransferMode, ReadBackVerification, TransferIdentity, TransferOutcome,
    TransferRequest,
};
use task_model::TransferMode;
use task_model::observation::{EntryStatistics, ObservedItem};

use super::metadata::captured_metadata_plan;
use super::{CopyOptions, transfer_policy};
use crate::error::storage::{Result, StorageError};
use crate::storage_factory::StorageContext;

pub(crate) fn validate_transfer_mode(destination: &StorageContext, mode: TransferMode) -> Result<()> {
    let staged = destination.roles.staged_destination()?;
    let reason = match mode {
        TransferMode::Direct if !staged.supports_direct() => {
            Some("direct writes are unavailable; select --resume or a supported backend")
        }
        TransferMode::Checkpoint if !staged.resumable_at_destination() => {
            Some("destination checkpoints are unavailable; select a supported backend")
        }
        _ => None,
    };
    if let Some(reason) = reason {
        return Err(StorageError::UnsupportedTransferMode {
            backend: destination.roles.kind(),
            mode,
            reason,
        });
    }
    Ok(())
}

pub(crate) fn validate_stream_transfer_mode(destination: &StorageContext, mode: TransferMode) -> Result<()> {
    validate_transfer_mode(destination, mode)?;
    if mode == TransferMode::Direct && !destination.roles.staged_destination()?.supports_sparse_patch() {
        return Err(StorageError::UnsupportedTransferMode {
            backend: destination.roles.kind(),
            mode,
            reason: "remote Direct requires sparse patch support; use --resume or a supported backend",
        });
    }
    Ok(())
}

pub(crate) async fn copy_entry(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, options: CopyOptions,
) -> Result<()> {
    let keep_history = entry.history().is_some() && destination.roles.staged_destination()?.keeps_history().await?;
    for item in ObservedItem::all(entry) {
        if item
            .view()
            .version()
            .is_some_and(|record| !keep_history && (!record.is_latest() || record.is_delete_marker()))
        {
            continue;
        }
        copy_item(source, destination, &item, &options, None).await?;
    }
    Ok(())
}

pub(crate) async fn copy_item(
    source: &StorageContext, destination: &StorageContext, item: &ObservedItem, options: &CopyOptions,
    identity: Option<&str>,
) -> Result<()> {
    validate_transfer_mode(destination, options.transfer_mode)?;
    let view = item.view();
    if view.version().is_some_and(|record| record.is_delete_marker()) {
        transfer::write_delete_marker(
            destination.roles.clone(),
            view.source_path()?,
            options.cancel.clone().unwrap_or_default(),
        )
        .await?;
        return Ok(());
    }
    execute_file(source, destination, &item.entry, item.version_index, options, identity).await?;
    Ok(())
}

pub(crate) async fn copy_current(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, options: &CopyOptions,
    identity: Option<&str>,
) -> Result<TransferOutcome> {
    validate_transfer_mode(destination, options.transfer_mode)?;
    execute_file(source, destination, entry, None, options, identity).await
}

pub(crate) async fn copy_checkpointed(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, mut options: CopyOptions,
) -> Result<()> {
    options.transfer_mode = TransferMode::Checkpoint;
    let outcome = copy_current(source, destination, entry, &options, None).await?;
    tracing::debug!(transferred_bytes = outcome.transferred_bytes, reused_bytes = outcome.reused_bytes, recovery = ?outcome.recovery, "checkpointed transfer completed");
    Ok(())
}

async fn execute_file(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, version_index: Option<usize>,
    options: &CopyOptions, identity: Option<&str>,
) -> Result<TransferOutcome> {
    let item = ObservedItem {
        entry: Arc::clone(entry),
        version_index,
    };
    let view = item.view().payload();
    // 获取／计划错误延后返回，仍尝试复制可读取的数据；两个阶段失败时保留全部原因。
    let metadata = captured_metadata_plan(destination.storage(), view).map(Arc::new);
    let result = execute_native(
        source,
        destination,
        entry,
        version_index,
        options,
        identity,
        metadata.as_ref().ok(),
    )
    .await;
    let (outcome, plan) = match (result, metadata) {
        (Ok(outcome), Ok(plan)) => (outcome, plan),
        (Ok(_), Err(metadata)) => return Err(metadata),
        (Err(data), Ok(_)) => return Err(data),
        (Err(data), Err(metadata)) => {
            return Err(StorageError::TransferAndMetadata {
                data: Box::new(data),
                metadata: Box::new(metadata),
            });
        }
    };
    let target = plan
        .verify_published(
            destination.storage(),
            &view.source_path()?,
            outcome.destination_version.as_deref(),
            options.cancel.clone().unwrap_or_default(),
        )
        .await?;
    if target.kind != EntryKind::File || target.size != Some(view.get_size()) {
        return Err(StorageError::TargetPublicationChanged { path: target.path });
    }
    Ok(outcome)
}

async fn execute_native(
    source: &StorageContext, destination: &StorageContext, entry: &Arc<ObservedEntry>, version_index: Option<usize>,
    options: &CopyOptions, identity: Option<&str>, metadata: Option<&Arc<MetadataPlan>>,
) -> Result<TransferOutcome> {
    let mut request = TransferRequest::from_shared_observation(
        source.roles.clone(),
        Arc::clone(entry),
        version_index,
        destination.roles.clone(),
        options.cancel.clone().unwrap_or_default(),
    )?
    .with_transfer_policy(transfer_policy(options.transfer_mode))
    .with_metadata_mode(MetadataTransferMode::CallerManaged)
    .with_read_back_verification(if options.enable_integrity_check {
        ReadBackVerification::Enabled
    } else {
        ReadBackVerification::Disabled
    });
    if let Some(metadata) = metadata {
        request = request
            .with_metadata_plan(Arc::clone(metadata))
            .with_metadata_failure_policy(MetadataFailurePolicy::PublishData);
    }
    if let Some(identity) = identity {
        request = request.with_identity_override(TransferIdentity::from_label(identity)?);
    }
    if let Some(qos) = &options.qos {
        request = request.with_source_qos(qos.source_group(source.block_size)?);
    }
    // 这里只建立目标事实；源清理由任务层在就绪复核后决定。
    let result = transfer::transfer(request).await;
    let data = match &result {
        Ok(outcome) => Some(outcome),
        Err(failure) => failure.published_data(),
    };
    if let Some(data) = data
        && let Some(counter) = &options.bytes_counter
    {
        counter.fetch_add(data.transferred_bytes, Ordering::Relaxed);
    }
    match result {
        Ok(outcome) => Ok(outcome),
        Err(failure) => Err(failure
            .cleanup_for_policy(transfer_policy(options.transfer_mode))
            .await
            .into()),
    }
}
