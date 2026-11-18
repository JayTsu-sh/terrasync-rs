//! 任务层授权目标清理；数据库的未见记录不是源端已删除的证据。
use std::time::Duration;

use data_mover::model::ObservedEntry;
use data_mover::storage::{NamespaceReadOutcome, NamespaceReadPolicy, read_namespace_entry};
use task_model::{SourcePresence, StorageEntryMessage, TargetRetention, TargetRetentionReason};
use tokio_util::sync::CancellationToken;

use crate::broadcast::BroadcastForwarder;
use crate::error::{Result, StorageError};
use crate::storage_factory::StorageContext;

#[derive(Clone, Copy)]
pub(crate) struct SourceAbsenceCheck {
    read_policy: NamespaceReadPolicy,
}

impl SourceAbsenceCheck {
    pub fn new(operation_timeout: Duration, recovery_window: Duration) -> Self {
        Self {
            read_policy: NamespaceReadPolicy {
                operation_timeout,
                recovery_window,
                retry_interval: Duration::from_millis(100),
            },
        }
    }

    /// 这里只确认被查询的当前路径；历史行与虚拟目录不能升级为整 key／子树删除授权。
    pub async fn confirm(
        self, source: &StorageContext, entry: &ObservedEntry, events: &BroadcastForwarder<StorageEntryMessage>,
    ) -> Result<bool> {
        if entry.path().as_str().is_empty() || entry.history().is_some() || entry.directory_marker_object().is_some() {
            preserve(
                events,
                entry,
                SourcePresence::Unknown,
                TargetRetentionReason::SourceDeletionUnconfirmed,
                "未获得根目录、完整版本历史或目录对象的缺失证明，保留目标",
            )
            .await?;
            return Ok(false);
        }
        match read_namespace_entry(
            source.storage(),
            entry.path(),
            None,
            self.read_policy,
            &CancellationToken::new(),
        )
        .await
        {
            Ok(NamespaceReadOutcome::Descriptor { descriptor: None, .. }) => Ok(true),
            Ok(_) => {
                preserve(
                    events,
                    entry,
                    SourcePresence::Present,
                    TargetRetentionReason::SourceDeletionUnconfirmed,
                    "源路径仍存在；本轮未命中不代表源端已删除，保留目标",
                )
                .await?;
                Ok(false)
            }
            Err(error) => {
                preserve(
                    events,
                    entry,
                    SourcePresence::Unknown,
                    TargetRetentionReason::SourceQueryFailed,
                    format!("源端存在性确认失败，保留目标并停止清理: {error}"),
                )
                .await?;
                events
                    .broadcast(StorageEntryMessage::TargetPreserved {
                        retention: Box::new(TargetRetention::source_range(
                            TargetRetentionReason::IncompleteEnumeration,
                        )),
                        reason: "源端确认失败后停止目标清理；剩余候选保留，未逐项核验存在性".into(),
                    })
                    .await?;
                Err(StorageError::from(error).into())
            }
        }
    }
}

pub(crate) async fn preserve(
    events: &BroadcastForwarder<StorageEntryMessage>, entry: &ObservedEntry, presence: SourcePresence,
    reason: TargetRetentionReason, detail: impl Into<String>,
) -> Result<()> {
    let mut retention = TargetRetention::entry(entry.path(), reason);
    retention.source_presence = presence;
    events
        .broadcast(StorageEntryMessage::TargetPreserved {
            retention: Box::new(retention),
            reason: detail.into(),
        })
        .await?;
    Ok(())
}
