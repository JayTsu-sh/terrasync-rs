//! 扫描前统一选择可迁移的元数据；worker 不再各自推断或追加源端读取。

use data_mover::model::{MetadataCapabilities, MetadataFamily, ObservationMode, ObservationPlan, ObservationSupport};
use data_mover::storage::{CopiedAclTarget, CopiedMetadataTarget, CopiedValueTarget};
use data_mover::time_util::now_secs;
use task_model::metadata::MetadataScope;

use crate::error::Result;
use crate::storage_factory::StorageContext;

#[derive(Clone, Debug)]
pub(crate) struct MigrationPlan {
    pub filter_time_anchor: i64,
    pub observation: ObservationPlan,
    pub scope: MetadataScope,
    pub additional_reads: Vec<MetadataRead>,
    pub root_metadata: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_factory::{CreateStorageOptions, create_storage};
    use data_mover::model::AclEncoding;
    use data_mover::storage::{CopiedOwnershipTarget, CopiedTimestampTarget};

    pub(super) const OBJECT_TARGET: CopiedMetadataTarget = CopiedMetadataTarget {
        timestamps: CopiedTimestampTarget::NotStored,
        ownership: CopiedOwnershipTarget::Unsupported,
        acl: CopiedAclTarget::Unsupported,
        xattrs: CopiedValueTarget::Unsupported,
        tags: CopiedValueTarget::Supported,
    };

    #[test]
    fn filesystem_to_object_omits_unusable_acl_and_xattr_reads() {
        let source = MetadataCapabilities::default()
            .with(MetadataFamily::Acl, ObservationSupport::Supported)
            .with(MetadataFamily::Xattrs, ObservationSupport::Supported)
            .with(MetadataFamily::Tags, ObservationSupport::NotApplicable);
        let plan = MigrationPlan::from_capabilities(source, Some(OBJECT_TARGET), false);
        for read in plan.additional_reads {
            assert!(!read.requested);
            assert_eq!(read.extra_calls, Some(0));
            assert_eq!(plan.observation.mode(read.family), ObservationMode::Omit);
        }
    }

    #[test]
    fn selected_reads_allow_data_delivery_after_metadata_failure() {
        let target = CopiedMetadataTarget {
            acl: CopiedAclTarget::Encoding(AclEncoding::NfsV4),
            xattrs: CopiedValueTarget::Supported,
            ..OBJECT_TARGET
        };
        let plan = MigrationPlan::from_capabilities(MetadataCapabilities::default(), Some(target), true);
        for read in plan.additional_reads {
            assert!(read.requested);
            assert_eq!(plan.observation.mode(read.family), ObservationMode::BestEffort);
            assert_eq!(read.extra_calls, None);
        }
    }

    #[test]
    fn object_destination_omits_acl_even_when_acl_copy_is_enabled() {
        let plan = MigrationPlan::from_capabilities(MetadataCapabilities::default(), Some(OBJECT_TARGET), true);
        assert_eq!(plan.observation.acl(), ObservationMode::Omit);
        assert_eq!(plan.observation.xattrs(), ObservationMode::Omit);
    }

    #[tokio::test]
    async fn filesystem_destination_selects_latest_version_and_omits_content_directory_object_attributes() -> Result<()>
    {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
        let destination =
            create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
        let plan = MigrationPlan::compile(&source, &destination, false).await?;
        assert!(plan.scope.latest_version_only);
        assert!(!plan.scope.directory_object_output);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRead {
    pub family: MetadataFamily,
    pub requested: bool,
    pub reason: &'static str,
    /// 每个被选中版本的独立请求估计；基础 listing/stat 不重复计费。
    pub extra_calls: Option<usize>,
    pub call_rule: &'static str,
}

impl MigrationPlan {
    pub async fn compile(source: &StorageContext, destination: &StorageContext, acl: bool) -> Result<Self> {
        let root_metadata =
            !source.storage().holds_directory_objects() && !destination.storage().holds_directory_objects();
        let source = source.storage().metadata()?.observation_capabilities();
        let target = destination.storage().staged_destination()?.copied_metadata_target();
        let mut plan = Self::from_capabilities(source, target, acl);
        plan.scope = Self::scope_for(destination).await?;
        plan.root_metadata = root_metadata;
        Ok(plan)
    }

    pub async fn scope_for(destination: &StorageContext) -> Result<MetadataScope> {
        Ok(MetadataScope {
            latest_version_only: !destination.storage().staged_destination()?.keeps_history().await?,
            directory_object_output: destination.storage().holds_directory_objects(),
        })
    }

    fn from_capabilities(source: MetadataCapabilities, target: Option<CopiedMetadataTarget>, acl: bool) -> Self {
        let can_read = |family| {
            !matches!(
                source.support(family),
                ObservationSupport::Unsupported | ObservationSupport::NotApplicable
            )
        };
        let target_acl = target.is_some_and(|target| matches!(target.acl, CopiedAclTarget::Encoding(_)));
        let requested = [
            (
                MetadataFamily::Acl,
                acl && target_acl && source.support(MetadataFamily::Acl) != ObservationSupport::NotApplicable,
            ),
            (
                MetadataFamily::Xattrs,
                acl && target.is_some_and(|target| target.xattrs == CopiedValueTarget::Supported)
                    && source.support(MetadataFamily::Xattrs) != ObservationSupport::NotApplicable,
            ),
            (
                MetadataFamily::Tags,
                target.is_some_and(|target| target.tags == CopiedValueTarget::Supported)
                    && can_read(MetadataFamily::Tags),
            ),
        ];
        let mut observation = ObservationPlan::default()
            .with_timestamps(ObservationMode::InlineOnly)
            .with_ownership_mode(ObservationMode::InlineOnly);
        let additional_reads = requested
            .into_iter()
            .map(|(family, requested)| {
                observation = observation.with(
                    family,
                    if requested {
                        ObservationMode::BestEffort
                    } else {
                        ObservationMode::Omit
                    },
                );
                MetadataRead {
                    family,
                    requested,
                    reason: if requested && !can_read(family) {
                        "源端已声明无法获取该必需属性；交付数据，报告元数据失败并禁止删源"
                    } else if requested {
                        "复制与增量比较共用扫描原值；失败仍交付数据复制"
                    } else {
                        "本计划没有对应复制需求，省略源端额外读取"
                    },
                    extra_calls: if requested && can_read(family) { None } else { Some(0) },
                    call_rule: match family {
                        MetadataFamily::Acl => "基础属性回读及 ACL 查询；按实际协议合并能力计费",
                        MetadataFamily::Xattrs => "属性列表及逐属性值读取；成本随属性数量和大小增加",
                        MetadataFamily::Tags => "版本绑定的标签查询，必要时额外 HEAD；角色调用不等于一次 RPC",
                        _ => "复用 listing/stat；无法复用的成本由协议观察报告",
                    },
                }
            })
            .collect();
        Self {
            root_metadata: false,
            filter_time_anchor: now_secs(),
            observation,
            scope: MetadataScope::default(),
            additional_reads,
        }
    }

    pub fn log(&self) {
        tracing::info!(
            filter_time_anchor_epoch = self.filter_time_anchor,
            latest_metadata_only = self.scope.latest_version_only,
            directory_object_metadata = self.scope.directory_object_output,
            root_metadata = self.root_metadata,
            root_source_stat_calls = usize::from(self.root_metadata),
            "Migration version metadata selection"
        );
        if self.root_metadata {
            tracing::info!(
                "根目录在扫描前单独获取一次 stat 及本计划选中的原值；全部子项完成后回读目标身份并应用／校验根属性。删源还需前后持久性复核。此计费按角色调用描述，实际 RPC 取决于协议、ACL 和 xattr 大小，不按每个 entry 叠加"
            );
        }
        if !self.scope.directory_object_output {
            tracing::info!("已知非空尾斜杠对象省略输出元数据额外读取；过滤依赖仍按计划获取");
        }
        for read in &self.additional_reads {
            tracing::info!(family = ?read.family, requested = read.requested, extra_calls_per_version = ?read.extra_calls,
                reason = read.reason, call_rule = read.call_rule, "Migration metadata plan (listing/stat baseline plus selected extra requests)");
        }
    }
}

#[cfg(all(test, unix))]
#[path = "migration_plan_nfs_real_tests.rs"]
mod nfs_real_tests;
