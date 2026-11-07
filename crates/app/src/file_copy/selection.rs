//! 目标无法表示的目录对象由任务层跳过；协议能力判断复用存储库。
use std::sync::{Arc, Weak};

use data_mover::model::ObservedEntry;
use data_mover::transfer::{DirectoryObjectAction, directory_object_action};
use task_model::StorageEntryMessage;
use tracing::info;

use crate::broadcast::BroadcastForwarder;
use crate::error::Result;
use crate::storage_factory::StorageContext;

pub(crate) fn unrepresentable_object(entry: &ObservedEntry, destination: &StorageContext) -> Option<&'static str> {
    if destination.storage().holds_directory_objects() {
        return None;
    }
    match directory_object_action(entry, destination.storage()) {
        DirectoryObjectAction::Skip(reason) => Some(reason),
        _ => None,
    }
}

pub(crate) async fn skip_unrepresentable(
    entry: &Arc<ObservedEntry>, destination: &StorageContext, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<bool> {
    let Some(reason) = unrepresentable_object(entry, destination) else {
        return Ok(false);
    };
    report_skip(entry, reason, broadcaster).await?;
    Ok(true)
}

/// 增量分派按 entry 连续交付所选版本；仅借用前一个 entry 的身份，不保留历史集合。
#[derive(Default)]
pub(crate) struct SkipTracker {
    previous: Weak<ObservedEntry>,
}

impl SkipTracker {
    pub(crate) async fn skip(
        &mut self, entry: &Arc<ObservedEntry>, destination: &StorageContext,
        broadcaster: &BroadcastForwarder<StorageEntryMessage>,
    ) -> Result<bool> {
        let Some(reason) = unrepresentable_object(entry, destination) else {
            return Ok(false);
        };
        if self
            .previous
            .upgrade()
            .is_none_or(|previous| !Arc::ptr_eq(&previous, entry))
        {
            report_skip(entry, reason, broadcaster).await?;
            self.previous = Arc::downgrade(entry);
        }
        Ok(true)
    }
}

async fn report_skip(
    entry: &Arc<ObservedEntry>, reason: &'static str, broadcaster: &BroadcastForwarder<StorageEntryMessage>,
) -> Result<()> {
    info!(path = %entry.path(), %reason, "Copy skipped; source retained");
    broadcaster
        .broadcast(StorageEntryMessage::CopySkipped {
            entry: entry.clone(),
            reason: reason.into(),
        })
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;
