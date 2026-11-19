//! 元数据观察与 opaque 编码；运行时直接交付原生观察，不投影为后端条目。
use crate::error::storage::{Result, StorageError, operation};
use crate::storage_factory::path;
use data_mover::model::*;
use data_mover::storage::StorageRoleFailure;
use data_mover::storage::{
    NamespaceReadOutcome, NamespaceReadPolicy, Storage, observe_descriptor, read_namespace_entry,
};
use data_mover::transfer::{DirectoryObjectAction, DirectoryObjectDestination, directory_object_action};
use std::path::Path;
use task_model::metadata::MetadataScope;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(crate) struct ObservationValidation {
    required_tags: bool,
    scope: MetadataScope,
}

impl ObservationValidation {
    pub fn new(required_tags: bool, scope: MetadataScope) -> Self {
        Self { required_tags, scope }
    }

    pub fn validate(self, entry: &ObservedEntry) -> Result<()> {
        if !self.required_tags {
            return Ok(());
        }
        let omitted = !self.scope.directory_object_output
            && matches!(
                directory_object_action(entry, DirectoryObjectDestination::new(false, None)),
                DirectoryObjectAction::Skip(_)
            );
        if !omitted {
            validate_required_tags(entry, self.scope.latest_version_only)?;
        }
        Ok(())
    }
}

/// 必须复制的事实失败后不能继续发布观察或把缺失事实解释成 Omit。
pub(crate) fn validate_required_tags(entry: &ObservedEntry, latest_only: bool) -> Result<()> {
    fn check(path: &StoragePath, facts: &MetadataObservations) -> Result<()> {
        let (class, transience) = match facts.tags() {
            MetadataObservation::Value { .. } | MetadataObservation::NotApplicable => return Ok(()),
            MetadataObservation::Unsupported => (FailureClass::Unsupported, Transience::Permanent),
            MetadataObservation::Failed { class, transience } => (*class, *transience),
            MetadataObservation::NotRequested => (FailureClass::InvalidInput, Transience::Permanent),
        };
        Err(StorageRoleFailure::Entry(EntryOperationFailure::new(
            path.clone(),
            Operation::Metadata,
            class,
            transience,
            "required copy tags were not observed",
        )?)
        .into())
    }
    if let Some(history) = entry.history() {
        for record in history
            .records()
            .iter()
            .filter(|record| !record.is_delete_marker() && (!latest_only || record.is_latest()))
        {
            if let Some(metadata) = record.metadata() {
                check(entry.path(), metadata)?;
            } else {
                check(entry.path(), &MetadataObservations::default())?;
            }
        }
    } else if let Some(marker) = entry.directory_marker_object() {
        check(marker.path(), marker.metadata())?;
    } else if entry.kind() != EntryKind::Directory {
        check(entry.path(), entry.metadata())?;
    }
    Ok(())
}

pub(crate) fn observation_plan(tags: bool) -> ObservationPlan {
    ObservationPlan::default()
        .with_timestamps(ObservationMode::InlineOnly)
        .with_ownership_mode(ObservationMode::InlineOnly)
        .with_tags(if tags {
            ObservationMode::Required
        } else {
            ObservationMode::Omit
        })
}

pub(crate) fn tags_required(storage: &Storage, include_tags: bool) -> Result<bool> {
    Ok(include_tags
        && !matches!(
            storage
                .metadata()?
                .observation_capabilities()
                .support(MetadataFamily::Tags),
            ObservationSupport::Unsupported | ObservationSupport::NotApplicable
        ))
}

/// 捕获单个任务路径所需的复制事实；IO 和模型构造由原生角色完成。
pub async fn observe_path(storage: &Storage, value: &Path) -> Result<ObservedEntry> {
    let plan = observation_plan(tags_required(storage, true)?);
    observe_path_with_plan(storage, value, plan).await
}

pub(crate) async fn observe_path_with_plan(
    storage: &Storage, value: &Path, plan: ObservationPlan,
) -> Result<ObservedEntry> {
    let descriptor = storage.namespace()?.stat(&path(value)?, false).await?;
    Ok(observe_descriptor(storage, descriptor, plan).await?)
}

/// 文件列表的点查询复用原生只读恢复；不在任务层复制协议重试。
pub(crate) async fn observe_path_with_read_policy(
    storage: &Storage, value: &Path, plan: Option<ObservationPlan>, policy: NamespaceReadPolicy,
    cancel: &CancellationToken,
) -> Result<ObservedEntry> {
    let plan = match plan {
        Some(plan) => plan,
        None => observation_plan(tags_required(storage, true)?),
    };
    match read_namespace_entry(storage, &path(value)?, Some(plan), policy, cancel).await? {
        NamespaceReadOutcome::Observation { entry, .. } => {
            entry.ok_or_else(|| StorageError::FileNotFound(value.display().to_string()))
        }
        NamespaceReadOutcome::Descriptor { .. } => Err(operation("source metadata read returned no observation")),
    }
}

/// 验证原生历史分段顺序，不积累整段历史，也不复制逐版本条目。
#[derive(Default)]
pub(crate) struct HistorySequence {
    path: Option<StoragePath>,
    next_segment: u32,
}
impl HistorySequence {
    pub fn check(&mut self, entry: &ObservedEntry, validation: ObservationValidation) -> Result<()> {
        validation.validate(entry)?;
        if let Some(history) = entry.history() {
            if self.path.is_none() {
                self.path = Some(entry.path().clone());
                self.next_segment = 0;
            }
            if self.path.as_ref() != Some(entry.path()) || history.segment() != self.next_segment {
                return Err(operation("out-of-order object history"));
            }
            self.next_segment = self
                .next_segment
                .checked_add(1)
                .ok_or_else(|| operation("history segment overflow"))?;
            if history.is_complete() {
                self.path = None;
            }
        } else if self.path.is_some() {
            return Err(operation("interrupted object history"));
        }
        Ok(())
    }
    pub fn is_complete(&self) -> bool {
        self.path.is_none()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod required_tags_tests {
    use super::*;
    use crate::error::StorageError;
    use crate::observed_fixtures::{directory_marker, observation};

    #[test]
    fn failed_required_tags_block_the_object_and_keep_the_typed_cause() {
        let entry = directory_marker("folder", Vec::new());
        assert!(validate_required_tags(&entry, false).is_ok());
        let marker = entry.directory_marker_object().unwrap();
        let facts = MetadataObservations::new(
            MetadataObservation::NotApplicable,
            MetadataObservation::NotApplicable,
            MetadataObservation::Failed {
                class: FailureClass::Timeout,
                transience: Transience::Transient,
            },
            MetadataObservation::NotApplicable,
            MetadataObservation::NotRequested,
        )
        .unwrap();
        let marker = marker.clone().with_metadata(facts);
        let entry = entry.with_directory_marker_object(Some(Box::new(marker))).unwrap();
        match validate_required_tags(&entry, false) {
            Err(StorageError::Role(StorageRoleFailure::Entry(error))) => {
                assert_eq!(error.path().as_str(), "folder/");
                assert_eq!(error.class(), FailureClass::Timeout);
                assert_eq!(error.transience(), Transience::Transient);
            }
            result => panic!("expected the typed tag failure, got {result:?}"),
        }
    }

    #[test]
    fn absent_required_tags_cannot_admit_a_history_version() {
        let entry = observation(
            BackendKind::S3,
            "file".into(),
            EntryKind::File,
            1,
            [123; 3],
            None,
            None,
            None,
            Some((Some("v1".into()), true, false)),
        );
        assert!(validate_required_tags(&entry, false).is_err());
        let structural = directory_marker("folder", Vec::new())
            .with_directory_marker_object(None)
            .unwrap();
        assert!(validate_required_tags(&structural, false).is_ok());
        assert_eq!(
            observation_plan(true).mode(MetadataFamily::Tags),
            ObservationMode::Required
        );
    }
}

#[cfg(test)]
#[path = "observation_failures_tests.rs"]
mod failure_tests;

#[cfg(test)]
#[path = "observation_scope_tests.rs"]
mod scope_tests;
