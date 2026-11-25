//! 保留目标的决策事实；存在性未知不能被解释为删除成功。
use data_mover_model::StoragePath;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePresence {
    Unknown,
    Present,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetDeletionOutcome {
    Deleted,
    AlreadyAbsent,
    Preserved,
    Failed,
}

impl TargetDeletionOutcome {
    /// 目标删除已得到成功或目标原已缺失的证据。
    pub const fn confirms_absence(self) -> bool {
        matches!(self, Self::Deleted | Self::AlreadyAbsent)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionScope {
    Entry,
    SourceRange,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetRetentionReason {
    SourceDeletionUnconfirmed,
    SourceCleanupPolicy,
    FilteredSelection,
    IncompleteEnumeration,
    AmbiguousIdentity,
    SourceQueryFailed,
    BaselineUnavailable,
    StaleEvidence,
    TargetPersistenceUnconfirmed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TargetRetention {
    pub source_path: Option<String>,
    pub target_path: Option<String>,
    pub scope: RetentionScope,
    pub source_presence: SourcePresence,
    pub reason: TargetRetentionReason,
}

impl TargetRetention {
    pub fn entry(path: &StoragePath, reason: TargetRetentionReason) -> Self {
        Self {
            source_path: Some(path.as_str().to_owned()),
            target_path: Some(path.as_str().to_owned()),
            scope: RetentionScope::Entry,
            source_presence: SourcePresence::Unknown,
            reason,
        }
    }

    pub fn source_range(reason: TargetRetentionReason) -> Self {
        Self {
            source_path: None,
            target_path: None,
            scope: RetentionScope::SourceRange,
            source_presence: SourcePresence::Unknown,
            reason,
        }
    }
}
