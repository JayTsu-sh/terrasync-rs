//! 复制执行的数据库作用域；最终报告使用独立传输。
use std::future::Future;

use db::{
    ClickHouseDatabase, DatabaseError, DatabaseExecutionMonitor,
    scan::{ScanQueryCancellation, with_scan_cancellation},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::{AppError, Result};

#[derive(Clone)]
pub(crate) struct CopyDatabaseExecution {
    queries: ScanQueryCancellation,
    cancel: CancellationToken,
}

impl CopyDatabaseExecution {
    pub(crate) fn prepare(database: &mut ClickHouseDatabase, cancel: CancellationToken) -> Result<Self> {
        let (scoped, queries) = database.with_query_cancellation()?;
        *database = scoped;
        Ok(Self { queries, cancel })
    }

    pub(crate) fn token(&self) -> &CancellationToken {
        &self.cancel
    }

    pub(crate) fn cancellation_error(&self) -> DatabaseError {
        let execution = self.queries.execution();
        execution.failure().map_or(DatabaseError::ScanCancelled, |source| {
            DatabaseError::ScanDatabaseUnavailable {
                recovery_window: execution.recovery_window(),
                source,
                termination: None,
            }
        })
    }

    pub(crate) fn start_monitor(&self) -> Result<DatabaseExecutionMonitor> {
        Ok(DatabaseExecutionMonitor::start(
            self.queries.clone(),
            self.cancel.clone(),
        )?)
    }

    pub(crate) async fn scope<T>(&self, work: impl Future<Output = T>) -> T {
        with_scan_cancellation(self.cancel.clone(), Some(self.queries.execution()), work).await
    }

    pub(crate) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        let monitor = self.start_monitor()?;
        let execution = self.queries.execution();
        let result = self.scope(work).await;
        let monitored = monitor.finish().await;
        // 接收通道关闭与故障同时发生时，不能让较晚的成功返回掩盖故障。
        let result = if let Some(source) = execution.failure() {
            let termination = match result {
                Ok(()) => None,
                Err(AppError::DatabaseError(error)) => Some(Box::new(error)),
                Err(error) => return Err(error),
            };
            Err(DatabaseError::ScanDatabaseUnavailable {
                recovery_window: execution.recovery_window(),
                source,
                termination,
            }
            .into())
        } else {
            result
        };
        match (result, monitored) {
            (Err(source), Err(monitoring)) => {
                warn!(error = %monitoring, "Database consumer monitor also failed during shutdown");
                Err(source)
            }
            (Err(source), Ok(())) => Err(source),
            (Ok(()), monitoring) => monitoring.map_err(Into::into),
        }
    }
}
