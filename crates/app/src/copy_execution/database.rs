//! 复制执行的数据库作用域；最终报告使用独立传输。
use std::future::Future;
use std::sync::{Arc, OnceLock};

use db::{
    ClickHouseDatabase, DatabaseError, DatabaseExecutionMonitor,
    scan::{ScanQueryCancellation, with_scan_cancellation},
};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, Result};
use crate::task_stream::combine_copy_results;

#[derive(Clone)]
pub(crate) struct CopyDatabaseExecution {
    queries: ScanQueryCancellation,
    cancel: CancellationToken,
    child_failure: Arc<OnceLock<(&'static str, Arc<AppError>)>>,
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
            Some(&(phase, ref source)) => AppError::CopyExecutionStopped {
                phase,
                source: Arc::clone(source),
            },
            None => self.cancellation_error().into(),
        }
    }

    pub(crate) fn start_monitor(&self) -> Result<DatabaseExecutionMonitor> {
        Ok(DatabaseExecutionMonitor::start(
            self.queries.clone(),
            self.cancel.clone(),
            Some("database_monitor"),
        )?)
    }

    pub(crate) async fn scope<T>(&self, work: impl Future<Output = T>) -> T {
        with_scan_cancellation(self.cancel.clone(), Some(self.queries.execution()), work).await
    }

    /// 子任务不另起 monitor，最终数据库故障立即停止同一执行的新请求。
    pub(crate) async fn run_child(&self, phase: &'static str, work: impl Future<Output = Result<()>>) -> Result<()> {
        let database = self.queries.execution().with_request_phase(phase);
        // 在 worker 边界堆存管线 future，避免阶段和数据库作用域叠加大状态机。
        let work = Box::pin(work);
        match with_scan_cancellation(self.cancel.clone(), Some(database), work).await {
            Ok(()) => Ok(()),
            // 已逐项报告的复制结果仍使整轮失败，但不能中断待冲刷的完成事实。
            Err(error) if error.is_reported_copy_failure() => Err(error),
            Err(error) if self.cancel.is_cancelled() && error.is_copy_stop_consequence() => Err(self.stopped_error()),
            Err(error) => {
                // 先保存原始类型再唤醒其他 worker，避免诱发的取消覆盖最初原因。
                let (phase, source) = match error {
                    AppError::CopyExecutionStopped { phase, source } => (phase, source),
                    error => (phase, Arc::new(error)),
                };
                let _ = self.child_failure.set((phase, Arc::clone(&source)));
                self.cancel.cancel();
                Err(AppError::CopyExecutionStopped { phase, source })
            }
        }
    }

    pub(crate) async fn run_optional_child(
        execution: Option<Self>, phase: &'static str, work: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        match execution {
            Some(execution) => execution.run_child(phase, work).await,
            None => work.await,
        }
    }

    pub(crate) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        let monitor = self.start_monitor()?;
        let execution = self.queries.execution();
        let result = self.scope(work).await;
        let monitored = monitor.finish().await;
        let terminal = execution.failure();
        let probe = execution
            .unresolved_probe_failure()
            .await
            .filter(|probe| terminal.as_ref().is_none_or(|source| !Arc::ptr_eq(probe, source)));
        // 已交付的逐项失败不能遮住其他 worker 保存的执行级根因。
        let result = if (result.is_ok() || result.as_ref().is_err_and(AppError::is_reported_copy_failure))
            && self.child_failure.get().is_some()
        {
            Err(self.stopped_error())
        } else {
            result
        };
        // 接收通道关闭与故障同时发生时，不能让较晚的成功返回掩盖故障。
        let result = if let Some(source) = terminal {
            let unavailable = |termination| DatabaseError::ScanDatabaseUnavailable {
                recovery_window: execution.recovery_window(),
                source: Arc::clone(&source),
                termination,
            };
            match result {
                Ok(()) => Err(unavailable(None).into()),
                Err(AppError::DatabaseError(error)) => Err(unavailable(Some(Box::new(error))).into()),
                Err(error) => Err(AppError::CopyExecutionAndDatabaseFailed {
                    source: Box::new(error),
                    database: unavailable(None),
                }),
            }
        } else {
            result
        };
        combine_copy_results([
            result,
            monitored.map_err(|error| AppError::from(error).in_copy_phase("database_monitor")),
            probe.map_or(Ok(()), |source| {
                Err(AppError::from(DatabaseError::ScanRequestFailed { source }).in_copy_phase("database_monitor"))
            }),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};
    use utils::app_config::ClickhouseConfig;

    #[tokio::test]
    async fn reported_entry_failure_keeps_the_fact_scope_open_for_drain() -> Result<()> {
        for data_failed in [false, true] {
            // 只构造执行身份，不连接数据库；事实冲刷在真实故障回归中验证。
            let config = ClickhouseConfig {
                mode: Default::default(),
                cluster: None,
                dsn: "http://127.0.0.1:9".into(),
                database: "fixture".into(),
                username: "fixture".into(),
                password: None,
                dial_timeout: 1,
                read_timeout: 1,
            };
            let token = CancellationToken::new();
            let mut database = ClickHouseDatabase::new(&config, "reported_entry")?;
            let execution = CopyDatabaseExecution::prepare(&mut database, token.clone())?;
            let result = execution
                .run_child("fixture", async {
                    Err(AppError::ReportedCopyFailure {
                        source: Box::new(IoError::new(ErrorKind::PermissionDenied, "owned entry refusal").into()),
                        data_failed,
                    })
                })
                .await;
            assert!(
                !token.is_cancelled(),
                "reported entry outcome must not cancel pending fact writes"
            );
            assert!(execution.child_failure.get().is_none());
            assert!(
                matches!(result, Err(AppError::ReportedCopyFailure { source, data_failed: actual })
                if actual == data_failed && matches!(source.as_ref(), AppError::IoError(error) if error.kind() == ErrorKind::PermissionDenied))
            );
            execution.run_child("fixture", async { Ok(()) }).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn fatal_peer_failure_is_not_hidden_by_an_earlier_reported_entry_result() -> Result<()> {
        let config = ClickhouseConfig {
            mode: Default::default(),
            cluster: None,
            dsn: "http://127.0.0.1:9".into(),
            database: "fixture".into(),
            username: "fixture".into(),
            password: None,
            dial_timeout: 1,
            read_timeout: 1,
        };
        let token = CancellationToken::new();
        let mut database = ClickHouseDatabase::new(&config, "reported_then_fatal")?;
        let execution = CopyDatabaseExecution::prepare(&mut database, token.clone())?;
        let result = execution
            .run(async {
                let reported = execution
                    .run_child("fixture", async {
                        Err(AppError::ReportedCopyFailure {
                            source: Box::new(IoError::new(ErrorKind::PermissionDenied, "owned entry refusal").into()),
                            data_failed: false,
                        })
                    })
                    .await;
                let fatal = execution
                    .run_child("fixture", async {
                        Err(IoError::new(ErrorKind::ConnectionAborted, "owned fatal peer").into())
                    })
                    .await;
                assert!(matches!(fatal, Err(AppError::CopyExecutionStopped { .. })));
                reported
            })
            .await;
        assert!(token.is_cancelled());
        assert!(matches!(result, Err(AppError::CopyExecutionStopped { source, .. })
            if matches!(source.as_ref(), AppError::IoError(error) if error.kind() == ErrorKind::ConnectionAborted)));
        Ok(())
    }
}
