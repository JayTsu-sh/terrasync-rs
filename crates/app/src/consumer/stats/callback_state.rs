//! 回调状态独立于可回退的扫描游标；ACK 只清明细，不清累计错误或恢复状态。
use super::report::ProgressReport;
use super::{
    accumulator::{ErrorStats, StatsKind},
    diagnostics::{CallbackIdentity, DiagnosticOutbox},
};
use crate::error::{AppError, Result};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub(crate) struct StatisticsCheckpoint {
    pub version: u8,
    pub stats: StatsKind,
    pub report: ProgressReport,
    pub callback: CallbackState,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum SavedStatistics {
    Current(StatisticsCheckpoint),
    Previous((StatsKind, ProgressReport)),
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanRuntimePhase {
    #[default]
    Running,
    Recovering,
    Failed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ScanRuntimeState {
    pub state: ScanRuntimePhase,
    pub network_faults: u64,
    pub recovery_attempts: u64,
    pub successful_recoveries: u64,
    pub current_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CallbackState {
    pub identity: CallbackIdentity,
    pub outbox: DiagnosticOutbox,
    pub runtime: ScanRuntimeState,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| value.as_millis().min(u128::from(u64::MAX)) as u64)
}

impl Default for CallbackState {
    fn default() -> Self {
        let round_id = Uuid::new_v4().to_string();
        let attempt_id = Uuid::new_v4().to_string();
        let now = now_millis();
        Self {
            outbox: DiagnosticOutbox::new(round_id.clone(), attempt_id.clone(), 512, 64),
            identity: CallbackIdentity {
                round_id,
                attempt_id,
                round_started_at_millis: now,
                attempt_started_at_millis: now,
                sequence: 0,
            },
            runtime: ScanRuntimeState {
                state: ScanRuntimePhase::Running,
                ..Default::default()
            },
        }
    }
}

impl CallbackState {
    pub(crate) fn initialize_scan(&mut self, job_id: &str, round: Option<i64>) -> Result<()> {
        if let Some(round) = round {
            let round_id = format!("{job_id}:{round}");
            if self.identity.round_id != round_id {
                self.outbox = DiagnosticOutbox::new(round_id.clone(), self.identity.attempt_id.clone(), 512, 64);
            }
            self.identity.round_id = round_id;
            self.identity.round_started_at_millis = u64::try_from(round / 1_000_000)?;
        }
        Ok(())
    }

    pub(crate) fn restore_checkpoint(&mut self, saved: Self) -> Result<()> {
        if !saved
            .outbox
            .validate_identity(&saved.identity.round_id, &saved.identity.attempt_id)
        {
            return Err(AppError::ScanError("Saved callback outbox identity is invalid".into()));
        }
        self.identity.round_id = saved.identity.round_id;
        self.identity.round_started_at_millis = saved.identity.round_started_at_millis;
        self.outbox = saved.outbox;
        self.outbox.set_attempt(self.identity.attempt_id.clone());
        self.runtime = saved.runtime;
        Ok(())
    }
}

pub(crate) fn errors(stats: &StatsKind) -> &ErrorStats {
    match stats {
        StatsKind::Full(stats) => &stats.error_stats,
        StatsKind::Incremental(stats) => &stats.error_stats,
    }
}

#[cfg(test)]
#[path = "callback_state_tests.rs"]
mod tests;
