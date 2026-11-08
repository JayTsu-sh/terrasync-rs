//! 消费者在执行作用域内接收事实，非复制消费者保持原有接收行为。
use std::future::Future;

use db::ClickHouseDatabase;
use task_model::StorageEntryMessage;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::copy_execution::CopyDatabaseExecution;
use crate::error::Result;

pub(super) struct ConsumerExecution {
    database: Option<CopyDatabaseExecution>,
}

impl ConsumerExecution {
    pub(super) fn prepare(database: &mut ClickHouseDatabase, copy: bool, cancel: CancellationToken) -> Result<Self> {
        Ok(Self {
            database: copy
                .then(|| CopyDatabaseExecution::prepare(database, cancel))
                .transpose()?,
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
            () = database.token().cancelled() => Err(database.cancellation_error().into()),
            message = receiver.recv() => Ok(message),
        }
    }

    pub(super) async fn run(&self, work: impl Future<Output = Result<()>>) -> Result<()> {
        match &self.database {
            Some(database) => database.run(work).await,
            None => work.await,
        }
    }
}
