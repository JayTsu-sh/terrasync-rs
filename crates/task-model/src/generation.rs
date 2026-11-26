//! 持久化任务代次的提交结果。

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationCommitResult {
    Committed,
    NotCommitted,
    PendingConfirmation,
}

impl fmt::Display for GenerationCommitResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Committed => "committed",
            Self::NotCommitted => "not_committed",
            Self::PendingConfirmation => "pending_confirmation",
        })
    }
}
