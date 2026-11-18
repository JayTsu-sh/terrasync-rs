//! 回调状态独立于可回退的扫描游标；ACK 只清明细，不清累计错误或恢复状态。
use super::journal::{self, DiagnosticJournal};
use super::{
    accumulator::{ErrorStats, StatsKind},
    diagnostics::DiagnosticOutbox,
};
use crate::error::{AppError, Result};
use crate::report::{ProgressReport, ScanRuntimePhase, ScanRuntimeState};
use db::{ClickHouseDatabase, NotificationRecord};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use task_model::diagnostics::{CallbackIdentity, MAX_DIAGNOSTIC_BATCH_EVENTS};
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CallbackState {
    pub identity: CallbackIdentity,
    pub outbox: DiagnosticOutbox,
    pub runtime: ScanRuntimeState,
    pub archived_diagnostics: bool,
    pub diagnostics_incomplete: bool,
    pub task_error_count: u64,
    pub target_preserved_count: u64,
    pub pending_acknowledgements: Vec<NotificationRecord>,
    pub unconfirmed_archive: Vec<NotificationRecord>,
    #[serde(skip)]
    pub(crate) journal: DiagnosticJournal,
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
            archived_diagnostics: false,
            diagnostics_incomplete: false,
            task_error_count: 0,
            target_preserved_count: 0,
            pending_acknowledgements: Vec::new(),
            unconfirmed_archive: Vec::new(),
            journal: DiagnosticJournal::default(),
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
    pub(crate) fn with_database(database: Option<ClickHouseDatabase>) -> Self {
        Self {
            journal: DiagnosticJournal::new(database),
            ..Self::default()
        }
    }
    pub(crate) fn initialize_scan(&mut self, job_id: &str, round: Option<i64>) -> Result<()> {
        if let Some(round) = round {
            let round_id = format!("{job_id}:{round}");
            if self.identity.round_id != round_id {
                self.archived_diagnostics = false;
                self.diagnostics_incomplete = false;
                self.task_error_count = 0;
                self.target_preserved_count = 0;
                self.pending_acknowledgements.clear();
                self.unconfirmed_archive.clear();
                self.journal.reset_delivery();
                self.outbox = DiagnosticOutbox::new(round_id.clone(), self.identity.attempt_id.clone(), 512, 64);
            }
            self.identity.round_id = round_id;
            self.identity.round_started_at_millis = u64::try_from(round / 1_000_000)?;
        }
        Ok(())
    }

    pub(crate) fn restore_checkpoint(&mut self, saved: Self) -> Result<()> {
        if saved.pending_acknowledgements.len() > journal::MAX_PENDING_CONFIRMATIONS
            || saved.unconfirmed_archive.len() > MAX_DIAGNOSTIC_BATCH_EVENTS
        {
            return Err(AppError::DiagnosticArchiveInvalid {
                field: "checkpoint_record_count",
            });
        }
        for row in saved.pending_acknowledgements.iter().chain(&saved.unconfirmed_archive) {
            row.validate()?;
            if row.round_id() != saved.identity.round_id {
                return Err(AppError::DiagnosticArchiveInvalid {
                    field: "checkpoint_record_round",
                });
            }
        }
        for rows in [&saved.pending_acknowledgements, &saved.unconfirmed_archive] {
            journal::batch(rows)?;
        }
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
        // checkpoint 之后也可能已有明细落库；恢复不能只依据旧快照中的布尔值跳过查询。
        self.archived_diagnostics = saved.archived_diagnostics || self.journal.database.is_some();
        self.diagnostics_incomplete = saved.diagnostics_incomplete;
        self.task_error_count = saved.task_error_count;
        self.target_preserved_count = saved.target_preserved_count;
        self.pending_acknowledgements = saved.pending_acknowledgements;
        self.unconfirmed_archive = saved.unconfirmed_archive;
        self.journal.reset_delivery();
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
