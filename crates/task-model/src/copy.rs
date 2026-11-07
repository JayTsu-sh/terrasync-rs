//! 复制完成事实与精确目标版本选择；数据完成不等同于整项就绪。
use crate::observation::ObservedItem;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyReadiness {
    /// 数据已发布并完成所选校验，但元数据尚未就绪；禁止删源。
    DataOnly,
    Ready,
}

#[derive(Clone, Debug)]
pub struct CopyCompletion {
    pub item: ObservedItem,
    pub target_version: Option<String>,
    pub readiness: CopyReadiness,
}

impl From<ObservedItem> for CopyCompletion {
    fn from(item: ObservedItem) -> Self {
        Self {
            item,
            target_version: None,
            readiness: CopyReadiness::Ready,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataTargetVersion {
    Current,
    Bound(String),
    /// 历史目标缺少精确版本映射；不能通过重新发布旧数据来修复。
    Unconfirmed,
}
