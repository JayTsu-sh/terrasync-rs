//! 测试用完整请求；每次调用代表新的执行，绑定与选择分别显式构造。
use task_model::migration::{
    CopyBinding, CopyContract, CopyRequestDescriptor, CopyRuntimeSettings, CopySelection, NamespaceBinding,
    PathMapping, VerificationLevel,
};
use task_model::{SourceCleanupPolicy, TransferMode};
use uuid::Uuid;

pub fn request(binding: &str, source_reserved: bool) -> CopyRequestDescriptor {
    CopyRequestDescriptor {
        execution_id: Uuid::new_v4().to_string(),
        binding: CopyBinding {
            source: NamespaceBinding {
                backend_kind: "local".into(),
                namespace: format!("fixture-source/{binding}"),
            },
            target: NamespaceBinding {
                backend_kind: "local".into(),
                namespace: format!("fixture-target/{binding}"),
            },
            target_history: false,
            mapping: PathMapping::Relative,
        },
        source_cleanup: SourceCleanupPolicy::from_reserved(source_reserved),
        selection: CopySelection {
            include: None,
            exclude: None,
            file_list: None,
            definition_key: "fixture-unfiltered-v1".into(),
            relative_time: false,
        },
        contract: CopyContract {
            acl: false,
            verification: VerificationLevel::Quick,
        },
        runtime: CopyRuntimeSettings {
            transfer_mode: TransferMode::Direct,
            scan_concurrency: 1,
            copy_concurrency: 1,
            block_size: None,
            delta_size_threshold: None,
            qos: None,
            peak_qos_rate: 2.0,
            iops: None,
        },
        filter_evaluated_at: 1_000_000,
        delete_target: false,
    }
}
