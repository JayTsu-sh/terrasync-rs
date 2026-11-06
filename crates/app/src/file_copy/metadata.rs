//! 从已捕获的原生观察编译复制元数据计划。
use crate::error::storage::{Result, StorageError, operation};
use crate::storage_factory::path;
use data_mover::model::*;
use data_mover::storage::Storage;
use std::path::Path;
use tokio_util::sync::CancellationToken;

use data_mover::{
    metadata::{
        AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
        OwnershipTarget, TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    },
    storage::{CopiedOwnershipTarget, CopiedTimestampTarget, CopiedValueTarget},
};
use task_model::observation::ObservedView;

/// 从已捕获的值编译计划；不回读源端，也不修改最终路径。
pub(crate) fn copied_metadata_plan(destination: &Storage, entry: ObservedView<'_>) -> Result<MetadataPlan> {
    let staged = destination.staged_destination()?;
    let target = staged.copied_metadata_target();
    let ownership = entry.metadata().ownership_mode().value();
    let mode = entry.mode().map(|mode| mode & 0o7777);
    let owner_permitted = ownership.is_none_or(|owner| staged.may_set_owner(owner.uid, owner.gid));
    let ownership_target = match target.map(|target| target.ownership) {
        Some(CopiedOwnershipTarget::Numeric) if owner_permitted => OwnershipTarget::Numeric,
        Some(CopiedOwnershipTarget::Numeric) => OwnershipTarget::NotPermitted,
        Some(CopiedOwnershipTarget::ModeOnly) => OwnershipTarget::ModeOnly,
        _ => OwnershipTarget::NotApplicable,
    };
    let timestamps = match target.map(|target| target.timestamps) {
        Some(CopiedTimestampTarget::Stored(precision)) => TimestampTargetCapability::Supported(TimestampTarget {
            precision,
            accessed: true,
            modified: true,
            created: false,
        }),
        _ => TimestampTargetCapability::Unsupported,
    };
    let policies = MetadataPolicies::default()
        .with_ownership_mode(
            if ownership.is_some() && owner_permitted && ownership_target == OwnershipTarget::Numeric {
                MetadataPolicy::RequireExact
            } else if ownership.is_none() && mode.is_none() {
                MetadataPolicy::BestEffort
            } else {
                MetadataPolicy::AllowKnownLoss
            },
        )
        .with_timestamps(
            if entry.metadata().timestamps().value().is_some()
                && matches!(timestamps, TimestampTargetCapability::Supported(_))
            {
                MetadataPolicy::AllowKnownLoss
            } else {
                MetadataPolicy::BestEffort
            },
        )
        .with_tags(if entry.metadata().tags().value().is_some() {
            MetadataPolicy::BestEffort
        } else {
            MetadataPolicy::Omit
        });
    Ok(compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: entry.metadata(),
            target: MetadataTarget {
                acl: AclTarget::NotApplicable,
                xattrs: ValueTarget::NotApplicable,
                tags: if target.is_some_and(|target| target.tags == CopiedValueTarget::Supported) {
                    ValueTarget::Supported
                } else {
                    ValueTarget::Unsupported
                },
                ownership_mode: ownership_target,
                timestamps,
            },
            policies,
            principal_mapper: None,
        },
        ownership.is_none().then_some(mode).flatten(),
        false,
        false,
        entry.entry.kind(),
    )?)
}

pub(crate) async fn set_metadata(
    storage: &Storage, value: &Path, atime: Option<i64>, mtime: Option<i64>, uid: Option<u32>, gid: Option<u32>,
    mode: Option<u32>,
) -> Result<()> {
    let destination = storage.staged_destination()?;
    let capabilities = destination
        .copied_metadata_target()
        .ok_or_else(|| operation("destination does not advertise copied metadata support"))?;
    let metadata = storage.metadata()?;
    let p = path(value)?;
    let cancel = CancellationToken::new();
    // 旧 NAS mode 包含文件类型位；角色层的修改接口只接受权限位。
    let mode = mode.map(|value| value & 0o7777);
    if let (CopiedOwnershipTarget::Numeric, Some(uid), Some(gid), Some(mode)) = (capabilities.ownership, uid, gid, mode)
        && destination.may_set_owner(uid, gid)
    {
        metadata
            .apply(
                &p,
                MetadataMutation::NumericOwnership(OwnershipMode { uid, gid, mode }),
                cancel.clone(),
            )
            .await?;
    } else if let Some(mode) = mode
        && capabilities.ownership != CopiedOwnershipTarget::Unsupported
    {
        metadata.apply(&p, MetadataMutation::Mode(mode), cancel.clone()).await?;
    }
    if let CopiedTimestampTarget::Stored(precision) = capabilities.timestamps
        && (atime.is_some() || mtime.is_some())
    {
        let aligned = |value: i64| {
            let quantum = precision.quantum_nanos();
            StorageTimestamp::new(i128::from(value).div_euclid(quantum) * quantum, precision)
                .unwrap_or_else(|_| unreachable!("quantized timestamp is aligned"))
        };
        metadata
            .apply(
                &p,
                MetadataMutation::Timestamps(TimestampMetadata {
                    accessed: atime.map(aligned),
                    modified: mtime.map(aligned),
                    created: None,
                }),
                cancel,
            )
            .await?;
    }
    Ok(())
}

pub(crate) async fn set_entry_metadata(storage: &Storage, entry: &ObservedEntry) -> Result<()> {
    let view = ObservedView {
        entry,
        version_index: None,
    }
    .payload();
    let plan = copied_metadata_plan(storage, view)?;
    plan.apply(
        storage.metadata()?.as_ref(),
        &view.source_path()?,
        CancellationToken::new(),
    )
    .await?;
    Ok(())
}

pub(crate) async fn copy_xattr(from: &Storage, to: &Storage, value: &Path) -> Result<()> {
    let p = path(value)?;
    let facts = from
        .metadata()?
        .observe(&p, ObservationPlan::default().with_xattrs(ObservationMode::BestEffort))
        .await?;
    let parts: MetadataObservationParts = facts.into();
    if let MetadataObservation::Value { value: attrs, .. } = parts.xattrs {
        to.metadata()?
            .apply(&p, MetadataMutation::Xattrs(attrs), CancellationToken::new())
            .await?;
    }
    Ok(())
}

pub(crate) async fn apply_acl(storage: &Storage, value: &Path, acl: AclMetadata) -> Result<()> {
    let target = storage.staged_destination()?.copied_metadata_target();
    let acl_target = match target.map(|v| v.acl) {
        Some(CopiedAclTarget::Encoding(encoding)) => AclTarget::Encoding(encoding),
        _ => AclTarget::Unsupported,
    };
    let observations = MetadataObservations::new(
        MetadataObservation::Value {
            value: acl,
            provenance: MetadataProvenance::AdditionalCall,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations,
            target: MetadataTarget {
                acl: acl_target,
                xattrs: ValueTarget::NotApplicable,
                tags: ValueTarget::NotApplicable,
                ownership_mode: OwnershipTarget::NotApplicable,
                timestamps: TimestampTargetCapability::NotApplicable,
            },
            policies: MetadataPolicies::default().with_acl(MetadataPolicy::BestEffort),
            principal_mapper: None,
        },
        None,
        false,
        true,
        EntryKind::File,
    )
    .map_err(StorageError::from)?;
    plan.apply(&*storage.metadata()?, &path(value)?, CancellationToken::new())
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

use data_mover::storage::{CopiedAclTarget, MetadataMutation};

pub(crate) async fn observe_acl(storage: &Storage, value: &Path) -> Result<Option<AclMetadata>> {
    let facts = storage
        .metadata()?
        .observe(
            &path(value)?,
            ObservationPlan::default().with_acl(ObservationMode::BestEffort),
        )
        .await?;
    let parts: MetadataObservationParts = facts.into();
    Ok(match parts.acl {
        MetadataObservation::Value { value, .. } => Some(value),
        _ => None,
    })
}

pub(crate) async fn copy_acl(from: &Storage, to: &Storage, value: &Path) -> Result<()> {
    if let Some(acl) = observe_acl(from, value).await? {
        apply_acl(to, value, acl).await?;
    }
    Ok(())
}
