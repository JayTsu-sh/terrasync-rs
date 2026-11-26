//! 扫描持久化与恢复契约，独立于单机和集群布局。
use crate::{DatabaseError, Result};
use serde::{Deserialize, Serialize};

pub const RUNNING: u8 = 0;
pub const FINALIZING: u8 = 1;
pub const COMPLETED: u8 = 2;
pub const ABANDONED: u8 = 3;
/// 恢复或放弃前暂停发布，直到已到达数据库的旧请求退出。
pub const RECOVERING: u8 = 4;

#[derive(Clone, Copy, Debug)]
pub enum ScanRecovery {
    Resume,
    Restart,
}

#[derive(Clone, Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct ScanResumeState {
    pub id: u8,
    pub schema_version: u8,
    pub version: u64,
    pub request_key: String,
    pub generation: u8,
    pub incremental: bool,
    pub phase: u8,
    /// 本轮幂等事件日志的时间戳下界，不包含边界。
    pub event_time: i64,
    pub observed_entries: u64,
    pub checkpoint: Vec<u8>,
    pub statistics: String,
    #[serde(default)]
    pub baseline_snapshot: String,
    #[serde(default)]
    pub checkpoint_snapshot: String,
}

impl ScanResumeState {
    pub(crate) fn validate_envelope(&self) -> Result<()> {
        if self.schema_version != 1 || self.id != 1 || self.generation > 1 || self.phase > RECOVERING {
            return Err(DatabaseError::ConversionError(
                "Unsupported or invalid scan resume state".into(),
            ));
        }
        Ok(())
    }

    pub fn validate(&self, request_key: &str) -> Result<()> {
        self.validate_envelope()?;
        if self.request_key != request_key {
            return Err(DatabaseError::ConfigError(
                "Scan resume source/options differ from the saved scan; use the original source/options, reset this job's scan records, or choose a new job id".into(),
            ));
        }
        Ok(())
    }

    /// 恢复和回查都要求轮次身份与代次同时匹配。
    pub(crate) fn matches_commit(&self, generation: u8, epoch: i64) -> Result<bool> {
        self.validate_envelope()?;
        if epoch == self.event_time && self.needs_commit_reconciliation() && generation == self.generation {
            return Ok(true);
        }
        if epoch >= self.event_time || generation != 1 - self.generation {
            return Err(DatabaseError::TransactionError(
                "Saved scan round conflicts with the committed generation".into(),
            ));
        }
        Ok(false)
    }

    pub fn advance(&mut self) -> Result<()> {
        self.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| DatabaseError::ConversionError("Scan resume version overflow".into()))?;
        Ok(())
    }

    pub const fn is_active_phase(phase: u8) -> bool {
        matches!(phase, RUNNING | FINALIZING | RECOVERING)
    }

    pub const fn active(&self) -> bool {
        Self::is_active_phase(self.phase)
    }

    pub const fn needs_commit_reconciliation(&self) -> bool {
        matches!(self.phase, FINALIZING | RECOVERING)
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, clickhouse::Row)]
pub struct ScanResumeSummary {
    pub dirs: u64,
    pub dir_bytes: i64,
    pub files: u64,
    pub file_bytes: i64,
    pub links: u64,
    pub link_bytes: i64,
    pub total_depth: i64,
    pub max_depth: u64,
    pub total_name_length: i64,
    pub max_name_length: u64,
}
#[derive(Debug, Deserialize, PartialEq, Eq, clickhouse::Row)]
pub struct ScanEventSummary {
    pub operation_type: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub is_regular_file: bool,
    pub entries: u64,
    pub bytes: i64,
}

pub type ScanEventStatistics = Vec<ScanEventSummary>;

pub struct ScanResumeStatistics {
    pub summary: ScanResumeSummary,
    pub extensions: Vec<(String, i64, u64)>,
    pub ages: Vec<(u8, i64, u64)>,
    pub sizes: Vec<(u8, i64, u64)>,
    pub events: ScanEventStatistics,
}
