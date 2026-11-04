//! 回调状态独立于可回退的扫描游标；ACK 只清明细，不清累计错误或恢复状态。
use super::report::ProgressReport;
use super::{
    accumulator::{ErrorStats, StatsKind},
    diagnostics::{CallbackIdentity, DiagnosticOutbox},
};
use crate::error::{AppError, Result};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{OpenOptions, create_dir_all, read, rename},
    io::AsyncWriteExt,
};
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
    #[serde(skip)]
    journal: Option<PathBuf>,
}

#[derive(Deserialize, Serialize)]
struct Journal {
    job_id: String,
    callback: CallbackState,
    errors: ErrorStats,
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
            journal: None,
        }
    }
}

impl CallbackState {
    pub(crate) async fn initialize_scan(
        &mut self, job_id: &str, job_dir: &str, round: Option<i64>, stats: &mut StatsKind,
    ) -> Result<()> {
        if let Some(round) = round {
            let round_id = format!("{job_id}:{round}");
            if self.identity.round_id != round_id {
                self.outbox = DiagnosticOutbox::new(round_id.clone(), self.identity.attempt_id.clone(), 512, 64);
            }
            self.identity.round_id = round_id;
            self.identity.round_started_at_millis = u64::try_from(round / 1_000_000)?;
        }
        if job_dir.is_empty() {
            return Ok(());
        }
        create_dir_all(job_dir).await?;
        // 轮次文件保留未确认明细；下一轮不会把旧轮次计数混入新统计。
        let digest = blake3::hash(self.identity.round_id.as_bytes());
        let path = Path::new(job_dir).join(format!("scan-callback-{}.json", digest.to_hex()));
        match read(&path).await {
            Ok(bytes) => {
                if bytes.len() > 8 * 1024 * 1024 {
                    return Err(AppError::ScanError("Scan callback journal exceeds size limit".into()));
                }
                let saved: Journal = serde_json::from_slice(&bytes)?;
                if saved.job_id != job_id
                    || saved.callback.identity.round_id != self.identity.round_id
                    || !saved
                        .callback
                        .outbox
                        .validate_identity(&saved.callback.identity.round_id, &saved.callback.identity.attempt_id)
                {
                    return Err(AppError::ScanError("Scan callback journal identity is invalid".into()));
                }
                self.runtime.network_faults = self.runtime.network_faults.max(saved.callback.runtime.network_faults);
                self.runtime.recovery_attempts = self
                    .runtime
                    .recovery_attempts
                    .max(saved.callback.runtime.recovery_attempts);
                self.runtime.successful_recoveries = self
                    .runtime
                    .successful_recoveries
                    .max(saved.callback.runtime.successful_recoveries);
                if saved.callback.outbox.revision() > self.outbox.revision() {
                    self.outbox = saved.callback.outbox;
                    self.outbox.set_attempt(self.identity.attempt_id.clone());
                    self.runtime.state = saved.callback.runtime.state;
                    self.runtime.current_error = saved.callback.runtime.current_error;
                }
                errors_mut(stats).merge_cumulative(&saved.errors);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.journal = Some(path);
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

    pub(crate) async fn persist(&self, job_id: &str, errors: &ErrorStats) -> Result<()> {
        let Some(path) = &self.journal else { return Ok(()) };
        let bytes = serde_json::to_vec(&Journal {
            job_id: job_id.into(),
            callback: self.clone(),
            errors: errors.clone(),
        })?;
        let temporary = path.with_extension(format!("{}.tmp", self.identity.attempt_id));
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        rename(&temporary, path).await?;
        Ok(())
    }
}

pub(crate) fn errors(stats: &StatsKind) -> &ErrorStats {
    match stats {
        StatsKind::Full(stats) => &stats.error_stats,
        StatsKind::Incremental(stats) => &stats.error_stats,
    }
}
pub(crate) fn errors_mut(stats: &mut StatsKind) -> &mut ErrorStats {
    match stats {
        StatsKind::Full(stats) => &mut stats.error_stats,
        StatsKind::Incremental(stats) => &mut stats.error_stats,
    }
}

#[cfg(test)]
#[path = "callback_state_tests.rs"]
mod tests;
