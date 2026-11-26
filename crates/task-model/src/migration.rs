//! 迁移代次的纯策略；不访问配置、数据库或存储。
use serde::{Deserialize, Serialize};

use crate::{Result, SourceCleanupPolicy, TargetRetention, TargetRetentionReason, TaskModelError, TransferMode};

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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRuntimeSettings {
    pub transfer_mode: TransferMode,
    pub scan_concurrency: usize,
    pub copy_concurrency: usize,
    pub block_size: Option<String>,
    pub delta_size_threshold: Option<String>,
    pub qos: Option<String>,
    pub peak_qos_rate: f32,
    pub iops: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRequestDescriptor {
    pub execution_id: String,
    pub binding: CopyBinding,
    pub source_cleanup: SourceCleanupPolicy,
    pub selection: CopySelection,
    pub contract: CopyContract,
    pub runtime: CopyRuntimeSettings,
    pub filter_evaluated_at: i64,
    pub delete_target: bool,
}

impl CopyRequestDescriptor {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("execution_id", self.execution_id.as_str()),
            ("selection.definition_key", self.selection.definition_key.as_str()),
            ("source.backend_kind", self.binding.source.backend_kind.as_str()),
            ("source.namespace", self.binding.source.namespace.as_str()),
            ("target.backend_kind", self.binding.target.backend_kind.as_str()),
            ("target.namespace", self.binding.target.namespace.as_str()),
        ] {
            if value.is_empty() || value.contains('\0') {
                return Err(TaskModelError::InvalidMigrationDescriptor { field });
            }
        }
        if self.runtime.scan_concurrency == 0 || self.runtime.copy_concurrency == 0 {
            return Err(TaskModelError::InvalidMigrationDescriptor { field: "concurrency" });
        }
        if !self.runtime.peak_qos_rate.is_finite() || self.runtime.peak_qos_rate <= 0.0 {
            return Err(TaskModelError::InvalidMigrationDescriptor { field: "peak_qos_rate" });
        }
        Ok(())
    }
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyGeneration {
    pub sequence: u64,
    pub revision: u64,
    pub request: CopyRequestDescriptor,
    pub plan: CopyGenerationPlan,
    pub phase: CopyGenerationPhase,
    /// 准备完成后失败／取消也可复用已确认事实；仅 Accepted 不能证明准备完成。
    pub baseline_prepared: bool,
    /// 只有已知执行才记录目标逐项对账是否收束；旧执行可能没有此证据。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_reconciliation_completed: Option<bool>,
    /// 扫描前捕获的根目录原值；数据库以独立二进制列保存，不挤占请求 JSON 的大小预算。
    /// 根不是普通迁移 entry，也不作为端点删除候选；callback 不包含此原值。
    #[serde(skip)]
    pub root_snapshot: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationReport {
    pub generation: Option<u64>,
    pub execution_id: String,
    pub filter_evaluated_at: i64,
    pub source_cleanup: SourceCleanupPolicy,
    pub target_history: bool,
    pub plan: CopyGenerationPlan,
    pub phase: CopyGenerationPhase,
    pub baseline_prepared: bool,
}

impl CopyGeneration {
    pub fn incomplete_target_preservation(&self) -> Option<TargetRetention> {
        (self.target_reconciliation_completed == Some(false)
            && matches!(self.phase, CopyGenerationPhase::Failed | CopyGenerationPhase::Cancelled))
        .then(|| TargetRetention::source_range(TargetRetentionReason::IncompleteEnumeration))
    }

    pub fn target_preservation(&self) -> Option<TargetRetention> {
        self.plan
            .target_preservation()
            .or_else(|| self.incomplete_target_preservation())
    }

    pub fn report(&self) -> MigrationReport {
        MigrationReport {
            generation: Some(self.sequence),
            execution_id: self.request.execution_id.clone(),
            filter_evaluated_at: self.request.filter_evaluated_at,
            source_cleanup: self.request.source_cleanup,
            target_history: self.request.binding.target_history,
            plan: self.plan.clone(),
            phase: self.phase,
            baseline_prepared: self.baseline_prepared,
        }
    }
}

impl CopyGenerationPlan {
    /// 整段保留以已接受的计划为依据。没有逐项核验时只报告范围。
    pub fn target_preservation(&self) -> Option<TargetRetention> {
        if self.target_cleanup != TargetCleanupPolicy::Preserve {
            return None;
        }
        let reason = if self.reasons.contains(&MigrationReason::SourceCleanup)
            || self.reasons.contains(&MigrationReason::PreviousSourceDeleted)
        {
            TargetRetentionReason::SourceCleanupPolicy
        } else if self.reasons.contains(&MigrationReason::FilteredSelection)
            || self.reasons.contains(&MigrationReason::RelativeTimeWindow)
            || self.reasons.contains(&MigrationReason::SelectionChanged)
        {
            TargetRetentionReason::FilteredSelection
        } else {
            TargetRetentionReason::BaselineUnavailable
        };
        Some(TargetRetention::source_range(reason))
    }

    pub fn decide(
        request: &CopyRequestDescriptor, previous: Option<&CopyGeneration>, baseline_available: bool,
    ) -> Result<Self> {
        request.validate()?;
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
