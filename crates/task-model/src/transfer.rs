use serde::{Deserialize, Serialize};

/// 文件续传独立于扫描；未启用续传时直接写入目标，不按大小或后端静默降级。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferMode {
    Checkpoint,
    #[default]
    Direct,
}

impl TransferMode {
    pub const fn from_resume(resume: bool) -> Self {
        if resume { Self::Checkpoint } else { Self::Direct }
    }
}
