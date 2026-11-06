//! 将任务策略编译为唯一的原生传输请求；恢复和发布仍由 data-mover 负责。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use data_mover::model::ObservedEntry;
use data_mover::transfer::{self, ReadBackVerification, TransferIdentity, TransferOutcome, TransferRequest};
use task_model::TransferMode;
use task_model::observation::ObservedItem;

use super::{CopyOptions, transfer_policy};
use crate::error::storage::{Result, StorageError};
use crate::storage_factory::StorageContext;

pub(crate) fn validate_transfer_mode(destination: &StorageContext, mode: TransferMode) -> Result<()> {
    let staged = destination.roles.staged_destination()?;
    let reason = match mode {
        TransferMode::Direct if !staged.supports_direct() => {
            Some("direct writes are unavailable; select --resume or --in-place")
        }
        TransferMode::Checkpoint if !staged.resumable_at_destination() => {
            Some("destination checkpoints are unavailable; select --in-place or a supported backend")
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
            reason: "remote Direct requires sparse patch support; use --resume or --in-place",
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
    let mut request = TransferRequest::from_shared_observation(
        source.roles.clone(),
        Arc::clone(entry),
        version_index,
        destination.roles.clone(),
        options.cancel.clone().unwrap_or_default(),
    )?
    .with_transfer_policy(transfer_policy(options.transfer_mode))
    .with_read_back_verification(if options.enable_integrity_check {
        ReadBackVerification::Enabled
    } else {
        ReadBackVerification::Disabled
    });
    if let Some(identity) = identity {
        request = request.with_identity_override(TransferIdentity::from_label(identity)?);
    }
    if let Some(qos) = &options.qos {
        request = request.with_source_qos(qos.source_group(source.block_size)?);
    }
    let outcome = if options.is_source_reserved {
        transfer::transfer(request).await?
    } else {
        transfer::move_file(request).await?
    };
    if let Some(counter) = &options.bytes_counter {
        counter.fetch_add(outcome.transferred_bytes, Ordering::Relaxed);
    }
    Ok(outcome)
}
