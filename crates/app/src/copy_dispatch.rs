//! 复制流水线按对象路径分派：同一对象的历史分段和操作保持源顺序，队列总量有界。
use std::hash::{DefaultHasher, Hash, Hasher};

use async_channel::Sender;
use tokio::task::JoinHandle;
use {storage_adapter::AsyncReceiver, task_model::StorageEntryMessage};

use crate::error::{AppError, Result};

pub(crate) struct CopyQueues {
    senders: Vec<Sender<StorageEntryMessage>>,
}
impl CopyQueues {
    pub(crate) fn new(workers: usize, capacity: usize) -> (Self, Vec<AsyncReceiver<StorageEntryMessage>>) {
        let (senders, receivers) = (0..workers.max(1))
            .map(|_| {
                let (sender, receiver) = async_channel::bounded(capacity.max(1));
                (sender, AsyncReceiver::new(receiver))
            })
            .unzip();
        (Self { senders }, receivers)
    }

    pub(crate) async fn send(&self, message: StorageEntryMessage) -> Result<()> {
        let mut hash = DefaultHasher::new();
        match &message {
            StorageEntryMessage::Scanned(entry)
            | StorageEntryMessage::Packaged(entry)
            | StorageEntryMessage::Deleted(entry)
            | StorageEntryMessage::IntegrityChecked(entry) => entry.path().as_str().hash(&mut hash),
            StorageEntryMessage::New(item) | StorageEntryMessage::Changed { entry: item, .. } => {
                item.entry.path().as_str().hash(&mut hash)
            }
            StorageEntryMessage::Renamed((_, to)) => to.path().as_str().hash(&mut hash),
            StorageEntryMessage::Error { path, .. } => path.hash(&mut hash),
            StorageEntryMessage::TarManifest { tar_path, .. } => tar_path.hash(&mut hash),
        }
        let index = hash.finish() as usize % self.senders.len();
        self.senders[index]
            .send(message)
            .await
            .map_err(|_| AppError::ConsumerError("Copy handler channel closed".into()))
    }
}

pub(crate) fn partition_scan(
    source: AsyncReceiver<StorageEntryMessage>, workers: usize, capacity: usize,
) -> (Vec<AsyncReceiver<StorageEntryMessage>>, JoinHandle<Result<()>>) {
    let (queues, receivers) = CopyQueues::new(workers, capacity);
    let task = tokio::spawn(async move {
        while let Some(message) = source.next().await {
            queues.send(message).await?;
        }
        Ok(())
    });
    (receivers, task)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::observed_fixtures::observation;
    use data_mover::model::{BackendKind, EntryKind};
    use std::sync::Arc;
    use task_model::ObservedEntryExt;

    #[tokio::test]
    async fn a_path_keeps_delivery_order_and_other_paths_can_run_in_parallel() {
        let (queues, receivers) = CopyQueues::new(4, 64);
        let mut tasks = Vec::new();
        for receiver in receivers {
            tasks.push(tokio::spawn(async move {
                let mut received = Vec::new();
                while let Some(StorageEntryMessage::Scanned(entry)) = receiver.next().await {
                    received.push((entry.path().as_str().to_owned(), entry.get_size()));
                }
                received
            }));
        }
        for index in 0..100 {
            for path in ["a", "b", "c", "d"] {
                let entry = Arc::new(observation(
                    BackendKind::S3,
                    path.into(),
                    EntryKind::File,
                    index,
                    [0; 3],
                    None,
                    None,
                    None,
                    None,
                ));
                queues.send(StorageEntryMessage::Scanned(entry)).await.unwrap();
            }
        }
        drop(queues);
        let mut all = Vec::new();
        for task in tasks {
            all.extend(task.await.unwrap());
        }
        for path in ["a", "b", "c", "d"] {
            assert_eq!(
                all.iter()
                    .filter(|(key, _)| key == path)
                    .map(|(_, value)| *value)
                    .collect::<Vec<_>>(),
                (0..100).collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn a_failed_worker_stops_dispatch_without_retaining_a_source_history() {
        let (queues, receivers) = CopyQueues::new(2, 1);
        drop(receivers);
        let entry = Arc::new(observation(
            BackendKind::Local,
            "file".into(),
            EntryKind::File,
            1,
            [0; 3],
            None,
            None,
            None,
            None,
        ));
        assert!(queues.send(StorageEntryMessage::Scanned(entry)).await.is_err());
    }
}
