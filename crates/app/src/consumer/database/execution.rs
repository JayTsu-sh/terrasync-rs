//! 消费者在执行作用域内接收事实，非复制消费者保持原有接收行为。
use std::future::Future;

use db::ClickHouseDatabase;
use task_model::StorageEntryMessage;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::copy_execution::CopyDatabaseExecution;
use crate::error::{AppError, Result};

pub(super) struct ConsumerExecution {
    database: Option<CopyDatabaseExecution>,
    owns_monitor: bool,
}

impl ConsumerExecution {
    pub(super) fn prepare(
        database: &mut ClickHouseDatabase, copy: bool, cancel: CancellationToken, shared: Option<CopyDatabaseExecution>,
    ) -> Result<Self> {
        if let Some(shared) = shared {
            if !copy {
                return Err(AppError::ConfigError("非复制消费者不能复用复制执行域".into()));
            }
            return Ok(Self {
                database: Some(shared),
                owns_monitor: false,
            });
        }
        Ok(Self {
            database: copy
                .then(|| CopyDatabaseExecution::prepare(database, cancel))
                .transpose()?,
            owns_monitor: true,
        })
    }

    pub(super) fn stops_on_failure(&self) -> bool {
        self.database.is_some()
    }

    pub(super) async fn receive(
        &self, receiver: &mut mpsc::Receiver<StorageEntryMessage>,
    ) -> Result<Option<StorageEntryMessage>> {
        let Some(database) = &self.database else {
            return Ok(receiver.recv().await);
        };
        select! {
            biased;
            () = database.token().cancelled() => Err(database.stopped_error()),
            message = receiver.recv() => Ok(message),
        }
    }

    pub(super) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        match &self.database {
            Some(database) if self.owns_monitor => database.run(work).await,
            Some(database) => database.run_child(work).await,
            None => work.await,
        }
    }
}
