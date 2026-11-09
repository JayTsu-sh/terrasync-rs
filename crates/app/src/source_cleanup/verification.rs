//! 删源前重读已捕获的源属性；不扩大元数据范围，也不把 atime 或自身删子项造成的目录 mtime 变化当作冲突。
use super::SourceCleanupCheck;
use crate::error::storage::operation;
use data_mover::model::{
    EntryKind, EntryOperationFailure, FailureClass, MetadataFamily, MetadataObservation, MetadataObservations,
    ObservationMode, ObservationPlan, ObservedEntry, Operation, SourceVersion, Transience, VersionRecord,
};
use data_mover::storage::{NamespaceReadOutcome, read_namespace_entry};
use data_mover::storage::{SourceDescriptor, StorageRoleFailure};
use task_model::metadata::metadata_digests;
use task_model::observation::ObservedView;
use tokio::time::timeout;

use crate::error::{Result, StorageError};
use crate::storage_factory::StorageContext;

fn requested<T>(fact: &MetadataObservation<T>) -> ObservationMode {
    if fact.value().is_some() {
        ObservationMode::Required
    } else {
        ObservationMode::Omit
    }
}

pub(super) fn source_plan(facts: &MetadataObservations) -> ObservationPlan {
    ObservationPlan::default()
        .with_acl(requested(facts.acl()))
        .with_xattrs(requested(facts.xattrs()))
        .with_tags(requested(facts.tags()))
        .with_ownership_mode(requested(facts.ownership_mode()))
        .with_timestamps(ObservationMode::InlineOnly)
}

pub(super) fn changed_families(expected: &MetadataObservations, current: &MetadataObservations) -> Vec<MetadataFamily> {
    let mut changed = Vec::new();
    if expected.ownership_mode().value().is_some()
        && expected.ownership_mode().value() != current.ownership_mode().value()
    {
        changed.push(MetadataFamily::OwnershipMode);
    }
    let wanted = [
        expected.acl().value().is_some(),
        expected.xattrs().value().is_some(),
        expected.tags().value().is_some(),
    ];
    if wanted.iter().any(|wanted| *wanted) {
        let before = metadata_digests(expected);
        let after = metadata_digests(current);
        for (index, family) in [MetadataFamily::Acl, MetadataFamily::Xattrs, MetadataFamily::Tags]
            .into_iter()
            .enumerate()
        {
            if wanted[index] && before[index] != after[index] {
                changed.push(family);
            }
        }
    }
    changed
}

/// false 表示源已缺失，调用方仍须先确认目标；不能用这个结果授权删除目标。
pub(super) async fn verify_source_metadata(
    source: &StorageContext, expected: &ObservedEntry, check: &SourceCleanupCheck,
) -> Result<bool> {
    let result = read_namespace_entry(
        source.storage(),
        expected.path(),
        Some(source_plan(expected.metadata())),
        check.read_policy,
        &check.cancel,
    )
    .await
    .map_err(StorageError::from)?;
    let current = match result {
        NamespaceReadOutcome::Observation {
            entry: Some(current), ..
        } => current,
        NamespaceReadOutcome::Observation { entry: None, .. } => return Ok(false),
        NamespaceReadOutcome::Descriptor { .. } => {
            return Err(operation("source metadata read returned no observation").into());
        }
    };
    let failures: Vec<_> = current.metadata().failures().collect();
    if !failures.is_empty() {
        return Err(StorageError::SourceMetadataAcquisitionFailed {
            path: expected.path().clone(),
            version: None,
            failures,
        }
        .into());
    }
    let families = changed_families(expected.metadata(), current.metadata());
    if !families.is_empty() {
        return Err(StorageError::SourceMetadataChanged {
            path: expected.path().clone(),
            families,
        }
        .into());
    }
    Ok(true)
}

/// 选中标记是没有数据体的单版本描述，不能由 size=None 或虚拟前缀单独推断。
pub(super) fn selected_marker(descriptor: &SourceDescriptor) -> Option<&VersionRecord> {
    let history = descriptor.listing.history.as_ref()?;
    (descriptor.listing.listing_only
        && descriptor.size.is_none()
        && descriptor.content_version.is_none()
        && history.records().len() == 1)
        .then(|| &history.records()[0])
        .filter(|record| record.is_delete_marker() && record.identity() == &descriptor.source_identity)
}

/// 指定历史版本使用已有 read／metadata role，不把旧版本属性与当前源混比。
pub(super) async fn verify_source_version(
    source: &StorageContext, expected: ObservedView<'_>, check: &SourceCleanupCheck,
) -> Result<()> {
    let path = expected.source_path()?;
    let operation = async {
        let failures: Vec<_> = expected.metadata().failures().collect();
        if !failures.is_empty() {
            return Err(StorageError::SourceMetadataAcquisitionFailed {
                path: path.clone(),
                version: Some(expected.version_id().into()),
                failures,
            }
            .into());
        }
        let version = SourceVersion::Id(expected.version_id().into());
        let descriptor = source
            .storage()
            .read_source()?
            .describe_version(&path, &version)
            .await?;
        if descriptor.kind != EntryKind::File
            || descriptor.version != version
            || descriptor.size != expected.version().and_then(|record| record.size())
            || &descriptor.source_identity != expected.source_identity()
        {
            return Err(StorageError::HistoryCleanupUnconfirmed {
                path: path.clone(),
                reason: "源历史版本身份或大小发生变化",
            }
            .into());
        }
        if let Some(wanted) = expected.version().filter(|record| record.is_delete_marker()) {
            let confirmed = selected_marker(&descriptor).is_some_and(|actual| {
                if actual.version_id() != wanted.version_id() {
                    return false;
                }
                if wanted.version_id().is_some() {
                    return true;
                }
                // null 槽可被替换；按双方共同精度比较修改时间，缺失证据时保留源。
                wanted.modified().zip(actual.modified()).is_some_and(|(before, after)| {
                    let quantum = before
                        .precision()
                        .quantum_nanos()
                        .max(after.precision().quantum_nanos());
                    before.unix_nanos().div_euclid(quantum) == after.unix_nanos().div_euclid(quantum)
                })
            });
            return if confirmed {
                Ok(())
            } else {
                Err(StorageError::HistoryCleanupUnconfirmed {
                    path: path.clone(),
                    reason: "源删除标记缺少匹配的类型或修改证据",
                }
                .into())
            };
        }
        let facts = source
            .storage()
            .metadata()?
            .observe_copy_bound_version(
                &path,
                expected.source_identity(),
                &version,
                source_plan(expected.metadata()),
            )
            .await?
            .observations;
        let failures: Vec<_> = facts.failures().collect();
        if !failures.is_empty() {
            return Err(StorageError::SourceMetadataAcquisitionFailed {
                path: path.clone(),
                version: Some(expected.version_id().into()),
                failures,
            }
            .into());
        }
        let families = changed_families(expected.metadata(), &facts);
        if !families.is_empty() {
            return Err(StorageError::SourceMetadataChanged {
                path: path.clone(),
                families,
            }
            .into());
        }
        Ok(())
    };
    tokio::select! {
        biased;
        () = check.cancel.cancelled() => Err(StorageError::Cancelled.into()),
        result = timeout(check.read_policy.operation_timeout, operation) => match result {
            Ok(result) => result,
            Err(_) => Err(StorageRoleFailure::Entry(EntryOperationFailure::new(
                path, Operation::Observe, FailureClass::Timeout, Transience::Transient,
                "historical source verification exceeded operation timeout",
            ).map_err(StorageError::from)?).into()),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::file_copy::{CopyOptions, copy_file};
    use crate::scan::observe_path_with_plan;
    use crate::source_cleanup::{SourceCleanupCheck, remove_entry};
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use async_trait::async_trait;
    use data_mover::model::FailureClass;
    use data_mover::model::{
        EntryOperationFailure, ExtendedAttribute, MetadataCapabilities, Operation, StoragePath, Transience,
    };
    use data_mover::storage::StorageRoleFailure;
    use data_mover::storage::{Metadata, MetadataMutation, StorageBuilder};
    use std::time::Duration;
    use std::{path::Path, sync::Arc};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn failed_captured_source_metadata_blocks_cleanup_before_any_source_mutation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"retained payload")?;
        let source = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let entry =
            Arc::new(observe_path_with_plan(source.storage(), Path::new("file"), ObservationPlan::default()).await?);
        copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
        let facts = entry.metadata();
        let failed = entry.as_ref().clone().with_metadata(
            MetadataObservations::new(
                MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                },
                facts.xattrs().clone(),
                facts.tags().clone(),
                facts.ownership_mode().clone(),
                facts.timestamps().clone(),
            )
            .map_err(StorageError::from)?,
        );
        let error = remove_entry(
            &source,
            &target,
            &failed,
            &SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new()),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AppError::StorageError(StorageError::SourceMetadataAcquisitionFailed { .. })
        ));
        assert_eq!(std::fs::read(root.path().join("file"))?, b"retained payload");
        assert_eq!(std::fs::read(target_root.path().join("file"))?, b"retained payload");
        Ok(())
    }

    struct MissingAttribute(Arc<dyn Metadata>);

    #[async_trait]
    impl Metadata for MissingAttribute {
        fn observation_capabilities(&self) -> MetadataCapabilities {
            self.0.observation_capabilities()
        }
        async fn observe(
            &self, path: &StoragePath, _: ObservationPlan,
        ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
            Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Metadata,
                    FailureClass::NotFound,
                    Transience::Permanent,
                    "attribute read failed after successful stat",
                )
                .unwrap(),
            ))
        }
        async fn apply(
            &self, path: &StoragePath, mutation: MetadataMutation, cancel: CancellationToken,
        ) -> std::result::Result<(), StorageRoleFailure> {
            self.0.apply(path, mutation, cancel).await
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn attribute_not_found_does_not_prove_that_the_source_path_was_removed() -> Result<()> {
        let root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"payload")?;
        let mut source = create_storage(root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        let target = create_storage(target_root.path().to_str().unwrap(), CreateStorageOptions::default()).await?;
        source
            .roles
            .metadata()?
            .apply(
                &StoragePath::new("file").unwrap(),
                MetadataMutation::Xattrs(vec![
                    ExtendedAttribute::new(b"user.cleanup-proof".to_vec(), b"before".to_vec()).unwrap(),
                ]),
                CancellationToken::new(),
            )
            .await?;
        let entry = Arc::new(
            observe_path_with_plan(
                source.storage(),
                Path::new("file"),
                ObservationPlan::default()
                    .with_xattrs(ObservationMode::Required)
                    .with_timestamps(ObservationMode::InlineOnly)
                    .with_ownership_mode(ObservationMode::InlineOnly),
            )
            .await?,
        );
        copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
        source.roles = StorageBuilder::new(source.roles.identity().clone(), source.roles.capabilities().clone())
            .namespace(source.roles.namespace()?)
            .read_source(source.roles.read_source()?)
            .staged_destination(source.roles.staged_destination()?)
            .metadata(Arc::new(MissingAttribute(source.roles.metadata()?)))
            .build()
            .unwrap();
        assert!(
            remove_entry(
                &source,
                &target,
                &entry,
                &SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new())
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(root.path().join("file"))?, b"payload");
        assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
        Ok(())
    }
}

#[cfg(test)]
#[path = "marker_tests.rs"]
mod marker_tests;
