//! 迁移代次的纯策略；不访问配置、数据库或存储。
use serde::{Deserialize, Serialize};

use crate::{Result, SourceCleanupPolicy, TaskModelError, TransferMode};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceBinding {
    pub backend_kind: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PathMapping {
    Relative,
    Packaged { depth: usize },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyBinding {
    pub source: NamespaceBinding,
    pub target: NamespaceBinding,
    pub target_history: bool,
    pub mapping: PathMapping,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileSelection {
    pub path: String,
    pub digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopySelection {
    pub include: Option<String>,
    pub exclude: Option<String>,
    pub file_list: Option<FileSelection>,
    /// 原生有序 AST／规则版本及清单内容的指纹，不包含本轮时间。
    pub definition_key: String,
    pub relative_time: bool,
}

impl CopySelection {
    pub fn is_filtered(&self) -> bool {
        self.include.is_some() || self.exclude.is_some() || self.file_list.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationLevel {
    Quick,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyContract {
    pub acl: bool,
    pub verification: VerificationLevel,
    pub transfer: TransferMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyRequestDescriptor {
    pub execution_id: String,
    pub binding: CopyBinding,
    pub source_cleanup: SourceCleanupPolicy,
    pub selection: CopySelection,
    pub contract: CopyContract,
    pub filter_evaluated_at: i64,
    pub delete_target: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationMode {
    Full,
    Incremental,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetCleanupPolicy {
    Preserve,
    ConfirmAbsentWithinScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationReason {
    FirstGeneration,
    PreviousSourceDeleted,
    BaselineUnavailable,
    RetainedSourceFacts,
    RelativeTimeWindow,
    FilteredSelection,
    SelectionChanged,
    ContractChanged,
    SourceCleanup,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyGenerationPlan {
    pub mode: MigrationMode,
    pub target_cleanup: TargetCleanupPolicy,
    pub reasons: Vec<MigrationReason>,
    pub force_copy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyGenerationPhase {
    Accepted,
    Prepared,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyGeneration {
    pub sequence: u64,
    pub request: CopyRequestDescriptor,
    pub plan: CopyGenerationPlan,
    pub phase: CopyGenerationPhase,
    /// 准备完成后失败／取消也可复用已确认事实；仅 Accepted 不能证明准备完成。
    pub baseline_prepared: bool,
}

impl CopyGenerationPlan {
    pub fn decide(
        request: &CopyRequestDescriptor, previous: Option<&CopyGeneration>, baseline_available: bool,
    ) -> Result<Self> {
        if request.execution_id.is_empty() || request.selection.definition_key.is_empty() {
            return Err(TaskModelError::InvalidMigrationDescriptor);
        }
        if previous.is_some_and(|previous| previous.request.binding != request.binding) {
            return Err(TaskModelError::CopyBindingChanged);
        }
        let (mode, reason) = match previous {
            None => (MigrationMode::Full, MigrationReason::FirstGeneration),
            Some(previous) if !previous.request.source_cleanup.retains_source() => {
                (MigrationMode::Full, MigrationReason::PreviousSourceDeleted)
            }
            Some(previous)
                if previous.phase == CopyGenerationPhase::Accepted
                    || !previous.baseline_prepared
                    || !baseline_available =>
            {
                (MigrationMode::Full, MigrationReason::BaselineUnavailable)
            }
            Some(_) => (MigrationMode::Incremental, MigrationReason::RetainedSourceFacts),
        };
        let mut reasons = vec![reason];
        let selection_changed = previous
            .is_some_and(|previous| previous.request.selection.definition_key != request.selection.definition_key);
        if request.selection.relative_time {
            reasons.push(MigrationReason::RelativeTimeWindow);
        }
        if request.selection.is_filtered() {
            reasons.push(MigrationReason::FilteredSelection);
        }
        if selection_changed {
            reasons.push(MigrationReason::SelectionChanged);
        }
        if !request.source_cleanup.retains_source() {
            reasons.push(MigrationReason::SourceCleanup);
        }
        let force_copy = mode == MigrationMode::Incremental
            && previous.is_some_and(|previous| previous.request.contract != request.contract);
        if force_copy {
            reasons.push(MigrationReason::ContractChanged);
        }
        let target_cleanup = if mode == MigrationMode::Full
            || request.selection.is_filtered()
            || request.selection.relative_time
            || selection_changed
            || !request.source_cleanup.retains_source()
        {
            TargetCleanupPolicy::Preserve
        } else {
            TargetCleanupPolicy::ConfirmAbsentWithinScope
        };
        if request.delete_target && target_cleanup == TargetCleanupPolicy::Preserve {
            return Err(TaskModelError::TargetCleanupConflict);
        }
        Ok(Self {
            mode,
            target_cleanup,
            reasons,
            force_copy,
        })
    }
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
