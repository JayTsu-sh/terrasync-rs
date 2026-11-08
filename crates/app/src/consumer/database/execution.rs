//! 事实消费者的恢复作用域；通知和代次收尾使用原目录的独立传输。
use std::future::Future;

use db::{
    ClickHouseDatabase, DatabaseError, DatabaseExecutionMonitor,
    scan::{ScanQueryCancellation, with_scan_cancellation},
};
use task_model::StorageEntryMessage;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::{AppError, Result};

pub(super) struct ConsumerExecution {
    queries: Option<ScanQueryCancellation>,
    cancel: CancellationToken,
}

impl ConsumerExecution {
    pub(super) fn prepare(database: &mut ClickHouseDatabase, copy: bool, cancel: CancellationToken) -> Result<Self> {
        let queries = if copy {
            let (scoped, queries) = database.with_query_cancellation()?;
            *database = scoped;
            Some(queries)
        } else {
            None
        };
        Ok(Self { queries, cancel })
    }

    pub(super) fn stops_on_failure(&self) -> bool {
        self.queries.is_some()
    }

    pub(super) async fn receive(
        &self, receiver: &mut mpsc::Receiver<StorageEntryMessage>,
    ) -> Result<Option<StorageEntryMessage>> {
        if self.queries.is_none() {
            return Ok(receiver.recv().await);
        }
        select! {
            biased;
            () = self.cancel.cancelled() => {
                let error = self.queries.as_ref().and_then(|queries| {
                    let execution = queries.execution();
                    execution.failure().map(|source| DatabaseError::ScanDatabaseUnavailable {
                        recovery_window: execution.recovery_window(), source, termination: None,
                    })
                }).unwrap_or(DatabaseError::ScanCancelled);
                Err(error.into())
            }
            message = receiver.recv() => Ok(message),
        }
    }

    pub(super) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        let Some(queries) = &self.queries else {
            return work.await;
        };
        let monitor = DatabaseExecutionMonitor::start(queries.clone(), self.cancel.clone())?;
        let execution = queries.execution();
        let result = with_scan_cancellation(self.cancel.clone(), Some(execution.clone()), work).await;
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
