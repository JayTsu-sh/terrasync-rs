use data_mover_model::ModelValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskModelError {
    #[error("迁移请求字段无效: {field}")]
    InvalidMigrationDescriptor { field: &'static str },
    #[error("相同 job ID 的源／目标绑定或路径映射发生变化；保留原代次和基线，请使用原绑定或新的 job ID")]
    CopyBindingChanged,
    #[error("目标镜像删除与本轮保留目标策略冲突")]
    TargetCleanupConflict,
    #[error(transparent)]
    Observation(#[from] ModelValueError),
}

pub type Result<T> = std::result::Result<T, TaskModelError>;
