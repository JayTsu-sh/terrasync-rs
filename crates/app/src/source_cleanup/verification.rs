//! 删源前重读已捕获的源属性；不扩大元数据范围，也不把 atime 或自身删子项造成的目录 mtime 变化当作冲突。
use super::SourceCleanupCheck;
use data_mover::model::{
    MetadataFamily, MetadataObservation, MetadataObservations, ObservationMode, ObservationPlan, ObservedEntry,
};
use data_mover::storage::read_source_observation;
use task_model::metadata::metadata_digests;

use crate::error::{Result, StorageError};
use crate::storage_factory::StorageContext;

fn requested<T>(fact: &MetadataObservation<T>) -> ObservationMode {
    if fact.value().is_some() {
        ObservationMode::Required
    } else {
        ObservationMode::Omit
    }
}

fn source_plan(facts: &MetadataObservations) -> ObservationPlan {
    ObservationPlan::default()
        .with_acl(requested(facts.acl()))
        .with_xattrs(requested(facts.xattrs()))
        .with_tags(requested(facts.tags()))
        .with_ownership_mode(requested(facts.ownership_mode()))
        .with_timestamps(ObservationMode::InlineOnly)
}

fn changed_families(expected: &MetadataObservations, current: &MetadataObservations) -> Vec<MetadataFamily> {
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
    let Some(current) = read_source_observation(
        source.storage(),
        expected.path(),
        source_plan(expected.metadata()),
        check.read_policy,
        &check.cancel,
    )
    .await
    .map_err(StorageError::from)?
    .entry
    else {
        return Ok(false);
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
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
