//! 源端清理属于本次任务的显式策略，执行期间不随全局配置变动。
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCleanupPolicy {
    Retain,
    DeleteSelectedReady,
}

impl SourceCleanupPolicy {
    pub const fn from_reserved(reserved: bool) -> Self {
        if reserved {
            Self::Retain
        } else {
            Self::DeleteSelectedReady
        }
    }

    pub const fn retains_source(self) -> bool {
        matches!(self, Self::Retain)
    }
}
