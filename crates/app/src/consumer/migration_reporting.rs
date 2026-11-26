//! 将迁移终态的独立原因映射到现有有界诊断日志。
use std::sync::Arc;
use std::{collections::HashSet, ptr};

use db::DatabaseError;
use task_model::diagnostics::safe_diagnostic_text;
use tokio::sync::Mutex;

use super::purge_reporting;
use super::stats::{StatisticConsumer, journal};
use crate::error::{AppError, StorageError};

enum Cause<'a> {
    App(&'a AppError),
    Database(&'a DatabaseError),
    Storage(&'a StorageError),
}

impl Cause<'_> {
    fn code(&self) -> String {
        match self {
            Self::Database(error) => {
                let (class, server_code) = error.diagnostic_class();
                match server_code {
                    Some(code) => format!("clickhouse_server_{code}"),
                    None => format!("clickhouse_{class}"),
                }
            }
            Self::Storage(StorageError::NamespaceRead(_)) => "namespace_read".into(),
            Self::Storage(_) => "storage_failure".into(),
            Self::App(AppError::CopyWorkerFailed { .. } | AppError::TaskJoinError(_)) => "worker_join".into(),
            Self::App(
                AppError::CopyQueueClosed | AppError::CopyHandlerChannelClosed | AppError::ConsumerDisconnected { .. },
            ) => "channel_closed".into(),
            Self::App(AppError::IoError(error)) => format!("io_{:?}", error.kind()),
            Self::App(AppError::CopyEntryFailed { .. }) => "copy_entry_failure".into(),
            Self::App(_) => "migration_failure".into(),
        }
    }

    fn message(&self) -> String {
        match self {
            Self::App(error) => error.to_string(),
            Self::Database(error) => error.to_string(),
            Self::Storage(error) => error.to_string(),
        }
    }

    fn path(&self) -> Option<&str> {
        match self {
            Self::App(AppError::CopyEntryFailed { path, .. } | AppError::CopyWorkerFailed { path, .. }) => {
                path.to_str()
            }
            _ => None,
        }
    }
}

#[derive(Default)]
struct Failures<'a> {
    // 只保留本次收尾的指针身份；不同请求即使原因文字相同也分别交付。
    app: HashSet<usize>,
    databases: HashSet<usize>,
    storage: HashSet<usize>,
    independent: Vec<(&'static str, Cause<'a>)>,
    uncertain_termination: Vec<(&'static str, Cause<'a>)>,
    stop: Option<(&'static str, Cause<'a>)>,
    already_reported: bool,
}

impl<'a> Failures<'a> {
    fn visit(&mut self, phase: &'static str, error: &'a AppError) {
        if !self.app.insert(ptr::from_ref(error).addr()) {
            return;
        }
        match error {
            AppError::CopyPhaseFailure { phase, source } => self.visit(phase, source),
            AppError::CopyPhaseFailures { source, additional } => {
                self.visit(phase, source);
                for error in additional {
                    self.visit(phase, error);
                }
            }
            AppError::CopyExecutionStopped { phase, source } => self.visit(phase, source),
            AppError::ReportedCopyFailure { .. } | AppError::DiagnosticJournalStopped { .. } => {
                self.already_reported = true;
            }
            AppError::CopyEntryFailed { source, .. } if source.is_reported_copy_failure() => self.visit(phase, source),
            AppError::MigrationAndRootMetadataFailed { source, metadata } => {
                self.visit(phase, source);
                self.visit("root_metadata", metadata);
            }
            AppError::MigrationAndSourceCleanupFailed { source, cleanup } => {
                self.visit(phase, source);
                self.visit("source_cleanup", cleanup);
            }
            AppError::MigrationReportingFailed { source, reporting } => {
                self.visit(phase, source);
                self.visit("reporting", reporting);
            }
            AppError::MigrationFinalizationFailed { source, finalization } => {
                self.visit(phase, source);
                self.database("generation_finalization", finalization);
            }
            AppError::CopyExecutionAndDatabaseFailed { source, database } => {
                self.visit(phase, source);
                self.database("database_execution", database);
            }
            AppError::CopyStartupAndTraversalFailed { source, startup } => {
                self.visit("startup", startup);
                self.visit("enumeration", source);
            }
            AppError::CopyAndSourceMetadataFailed { source, metadata } => {
                self.visit(phase, source);
                self.storage("source_metadata", metadata);
            }
            AppError::CopyRootMetadataFailure { phase, source, .. } => {
                if self.storage.insert(ptr::from_ref(source.as_ref()).addr()) {
                    self.independent.push((phase, Cause::App(error)));
                }
            }
            AppError::DatabaseError(error) => self.database(phase, error),
            AppError::StorageError(error) => self.storage(phase, error),
            error if error.is_copy_stop_consequence() => {
                self.stop.get_or_insert((phase, Cause::App(error)));
            }
            error => self.independent.push((phase, Cause::App(error))),
        }
    }

    fn database(&mut self, phase: &'static str, error: &'a DatabaseError) {
        if !self.databases.insert(ptr::from_ref(error).addr()) {
            return;
        }
        match error {
            DatabaseError::ScanRequestFailed { source } => self.database(phase, source),
            DatabaseError::RequestFailureInPhase { phase, source } => self.database(phase, source),
            DatabaseError::CatalogueBatchCleanupFailed { source, cleanup } => {
                self.database(phase, source);
                self.database("catalogue_cleanup", cleanup);
            }
            DatabaseError::CopyBaseRetirementFailed { source, .. } => {
                self.database("catalogue_retirement", source);
            }
            DatabaseError::CopyBaseRetirementReportingFailed { cleanup, reporting } => {
                self.database("catalogue_retirement", cleanup);
                self.database("generation_finalization", reporting);
            }
            DatabaseError::ScanDatabaseUnavailable {
                source, termination, ..
            } => {
                self.database(phase, source);
                if let Some(error) = termination {
                    self.database(phase, error);
                }
            }
            // 远端结束仍未确认须交付。它描述原故障的收尾，不再累计同一次失败。
            DatabaseError::ScanTerminationUnconfirmed { .. } => {
                self.uncertain_termination.push((phase, Cause::Database(error)));
            }
            error if error.is_confirmed_scan_cancellation() => {
                self.stop.get_or_insert((phase, Cause::Database(error)));
            }
            error => self.independent.push((phase, Cause::Database(error))),
        }
    }

    fn storage(&mut self, phase: &'static str, error: &'a StorageError) {
        if self.storage.insert(ptr::from_ref(error).addr()) {
            if error.is_cancelled() {
                self.stop.get_or_insert((phase, Cause::Storage(error)));
            } else {
                self.independent.push((phase, Cause::Storage(error)));
            }
        }
    }
}

pub(super) async fn record(consumer: &Arc<Mutex<StatisticConsumer>>, error: &AppError) {
    let mut failures = Failures::default();
    failures.visit("migration", error);
    if failures.independent.is_empty() && !failures.already_reported {
        failures.independent.extend(failures.stop);
        if failures.independent.is_empty()
            && let Some(termination) = failures.uncertain_termination.pop()
        {
            failures.independent.push(termination);
        }
    }

    let counted = failures
        .independent
        .into_iter()
        .map(|(phase, cause)| (phase, cause, true));
    let details = failures
        .uncertain_termination
        .into_iter()
        .map(|(phase, cause)| (phase, cause, false));
    let deadline = journal::diagnostic_deadline();
    for (phase, cause, count) in counted.chain(details) {
        let code = cause.code();
        let message = safe_diagnostic_text(&cause.message());
        if let Err(error) = journal::record_diagnostic(consumer, Some(deadline), |state| {
            let callback = &mut state.callback;
            callback.task_error_count = callback.task_error_count.saturating_add(u64::from(count));
            callback.diagnostics_incomplete |= !callback.outbox.enqueue(phase, &code, &message, cause.path());
            callback.runtime.current_error = Some(message);
        })
        .await
        {
            StatisticConsumer::diagnostic_failure(consumer, error).await;
        }
        if let Cause::App(error) = cause
            && let Err(reporting) = purge_reporting::record(consumer, error, Some(deadline)).await
        {
            StatisticConsumer::diagnostic_failure(consumer, reporting).await;
        }
    }
}
