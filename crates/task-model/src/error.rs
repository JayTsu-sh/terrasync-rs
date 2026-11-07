use data_mover_model::ModelValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskModelError {
    #[error("迁移请求缺少执行身份或过滤定义指纹")]
    InvalidMigrationDescriptor,
    #[error("相同 job ID 的源／目标绑定或路径映射发生变化；保留原代次和基线，请使用原绑定或新的 job ID")]
    CopyBindingChanged,
    #[error("目标镜像删除与本轮保留目标策略冲突")]
    TargetCleanupConflict,
    #[error(transparent)]
    Observation(#[from] ModelValueError),
}

pub type Result<T> = std::result::Result<T, TaskModelError>;
