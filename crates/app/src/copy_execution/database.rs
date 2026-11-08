//! 复制执行的数据库作用域；最终报告使用独立传输。
use std::future::Future;
use std::sync::{Arc, OnceLock};

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
    child_failure: Arc<OnceLock<Arc<AppError>>>,
}

impl CopyDatabaseExecution {
    pub(crate) fn prepare(database: &mut ClickHouseDatabase, cancel: CancellationToken) -> Result<Self> {
        let (scoped, queries) = database.with_query_cancellation()?;
        *database = scoped;
        Ok(Self {
            queries,
            cancel,
            child_failure: Arc::new(OnceLock::new()),
        })
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

    pub(crate) fn stopped_error(&self) -> AppError {
        match self.child_failure.get() {
            Some(source) => AppError::CopyExecutionStopped {
                source: Arc::clone(source),
            },
            None => self.cancellation_error().into(),
        }
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

    /// 子任务不另起 monitor，最终数据库故障立即停止同一执行的新请求。
    pub(crate) async fn run_child(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        match self.scope(work).await {
            Ok(()) => Ok(()),
            Err(error) => {
                // 先保存原始类型再唤醒其他 worker，避免诱发的取消覆盖最初原因。
                let source = self.child_failure.get_or_init(|| Arc::new(error));
                self.cancel.cancel();
                Err(AppError::CopyExecutionStopped {
                    source: Arc::clone(source),
                })
            }
        }
    }

    pub(crate) async fn run_optional_child(
        execution: Option<Self>, work: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        match execution {
            Some(execution) => execution.run_child(work).await,
            None => work.await,
        }
    }

    pub(crate) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        let monitor = self.start_monitor()?;
        let execution = self.queries.execution();
        let result = self.scope(work).await;
        let monitored = monitor.finish().await;
        let result = if result.is_ok() && self.child_failure.get().is_some() {
            Err(self.stopped_error())
        } else {
            result
        };
        // 接收通道关闭与故障同时发生时，不能让较晚的成功返回掩盖故障。
        let result = if let Some(source) = execution.failure() {
            let termination = match result {
                Ok(()) => None,
                Err(AppError::DatabaseError(error)) => Some(Box::new(error)),
                Err(error) => {
                    return Err(AppError::CopyExecutionAndDatabaseFailed {
                        source: Box::new(error),
                        database: DatabaseError::ScanDatabaseUnavailable {
                            recovery_window: execution.recovery_window(),
                            source,
                            termination: None,
                        },
                    });
                }
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
