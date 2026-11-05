//! 从已捕获的原生观察编译复制元数据计划。
use crate::{ConnectedStorage, Result, observed_scan::ObservedView};
use data_mover::{
    metadata::{
        AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
        OwnershipTarget, TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    },
    storage::{CopiedOwnershipTarget, CopiedTimestampTarget, CopiedValueTarget},
};

/// 从已捕获的值编译计划；不回读源端，也不修改最终路径。
pub(crate) fn copied_metadata_plan(destination: &ConnectedStorage, entry: ObservedView<'_>) -> Result<MetadataPlan> {
    let staged = destination.roles.staged_destination()?;
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
