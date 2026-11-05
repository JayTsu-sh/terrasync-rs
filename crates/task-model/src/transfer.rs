use serde::{Deserialize, Serialize};

/// sync 的显式写入模式，原样传递给上游，不按文件大小或后端静默降级。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferMode {
    Checkpoint,
    #[default]
    Atomic,
    Direct,
}

impl TransferMode {
    pub const fn from_flags(resume: bool, in_place: bool) -> Self {
        if resume {
            Self::Checkpoint
        } else if in_place {
            Self::Atomic
        } else {
            Self::Direct
        }
    }
}
