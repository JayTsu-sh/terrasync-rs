use crate::error::storage::{Result, StorageError, operation};
use crate::file_copy::metadata::copied_metadata_plan;
use crate::file_copy::transfer as copy_transfer;
use crate::file_copy::transfer_policy;
use crate::storage_factory::StorageContext;
use data_mover::model::*;
use task_model::TransferMode;

use crate::byte_resume::CommitCallback;
use blake3::Hash;
use data_mover::storage::{PositionedChunk, WriteProgress, WriteProgressObserver};
use data_mover::{
    storage::{self, PositionedByteStream, SourceDescriptor},
    transfer::{
        DirectPatchChunk, DirectPatchInput, DirectPatchSession, ExpertDestinationRequest, ExpertDestinationSession,
        ExpertDestinationTransferred, ExpertSourceEvidence,
    },
};
use futures::{Stream, StreamExt, stream};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use task_model::observation::{EntryStatistics, ObservedView};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

type Input = Pin<Box<dyn Stream<Item = StreamChunk> + Send>>;

/// basis 提示只由 Receiver 的重建器产生；同位置引用已读取并计入完整摘要。
pub struct StreamChunk {
    pub chunk: PositionedChunk,
    pub unchanged_basis: bool,
}
impl From<PositionedChunk> for StreamChunk {
    fn from(chunk: PositionedChunk) -> Self {
        Self {
            chunk,
            unchanged_basis: false,
        }
    }
}

struct WriteState {
    session: Option<ExpertDestinationSession>,
    direct: Option<DirectPatchSession>,
    written_bytes: u64,
    mode: TransferMode,
    transferred: Option<ExpertDestinationTransferred>,
    digest: [u8; 32],
    bytes: u64,
    input_ended: bool,
    identity_key: EntryIdentityKey,
    size: u64,
    observer: Arc<Mutex<Option<CommitCallback>>>,
}
#[derive(Clone)]
pub struct StreamHandle {
    state: Arc<AsyncMutex<WriteState>>,
    cancel: CancellationToken,
}
impl StreamHandle {
    /// 无需等待提交锁即可请求取消；实际资源由正在运行的引擎阶段收尾。
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    #[cfg(test)]
    pub async fn written_bytes(&self) -> u64 {
        self.state.lock().await.written_bytes
    }
    pub async fn received_size(&self) -> u64 {
        self.state.lock().await.bytes
    }
    pub async fn input_ended(&self) -> bool {
        self.state.lock().await.input_ended
    }
    pub async fn received_hash(&self) -> String {
        Hash::from_bytes(self.state.lock().await.digest).to_hex().to_string()
    }
    pub async fn discard(&self) -> Result<()> {
        self.cancel();
        let mut state = self.state.lock().await;
        state.direct.take();
        if let Some(session) = state.session.take() {
            if state.mode != TransferMode::Checkpoint {
                session.discard().await?;
            }
        }
        if let Some(transferred) = state.transferred.take() {
            let evidence = ExpertSourceEvidence {
                source_size: state.size,
                blake3: state.digest,
                identity_key: state.identity_key,
                source_qos: Default::default(),
            };
            if let Err(failure) = transferred.complete(evidence).await {
                if state.mode != TransferMode::Checkpoint {
                    failure.discard_stage().await?;
                }
            }
        }
        Ok(())
    }
}
#[cfg(test)]
pub async fn prepare_stream(dest: &StorageContext, entry: &ObservedEntry) -> Result<StreamHandle> {
    prepare_stream_with_mode(dest, entry, TransferMode::Atomic, CancellationToken::new()).await
}

#[cfg(test)]
pub async fn prepare_stream_with_mode(
    dest: &StorageContext, entry: &ObservedEntry, mode: TransferMode, cancel: CancellationToken,
) -> Result<StreamHandle> {
    prepare_observed_stream(
        dest,
        ObservedView {
            entry,
            version_index: entry.history().map(|_| 0),
        },
        mode,
        cancel,
    )
    .await
}
pub async fn prepare_observed_stream(
    dest: &StorageContext, entry: ObservedView<'_>, mode: TransferMode, cancel: CancellationToken,
) -> Result<StreamHandle> {
    let entry = entry.payload();
    let cancel = cancel.child_token();
    copy_transfer::validate_stream_transfer_mode(dest, mode)?;
    let p = entry.source_path()?;
    let identity = entry.source_identity().clone();
    let observed = ObservedEntry::new(
        p.clone(),
        EntryKind::File,
        Some(entry.get_size()),
        entry.version().map_or(entry.entry.modified(), VersionRecord::modified),
        identity,
    )?;
    let metadata_plan = copied_metadata_plan(&dest.roles, entry)?;
    let identity_key = observed.identity_key();
    let observer = Arc::new(Mutex::new(None::<CommitCallback>));
    let callback = observer.clone();
    let progress = WriteProgressObserver::new(move |event| {
        if let WriteProgress::Confirmed { offset, bytes } = event {
            let callback = callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(callback) = callback {
                callback(offset, bytes);
            }
        }
    });
    let (session, direct) = if mode == TransferMode::Direct {
        let source = SourceDescriptor::new(
            p,
            EntryKind::File,
            Some(entry.get_size()),
            observed.source_identity().clone(),
        );
        (
            None,
            Some(DirectPatchSession::prepare(dest.roles.clone(), source, metadata_plan, cancel.clone()).await?),
        )
    } else {
        let session = ExpertDestinationSession::prepare(
            ExpertDestinationRequest::new(
                observed,
                dest.block_size as usize,
                dest.roles.clone(),
                p.clone(),
                cancel.clone(),
            )
            .with_transfer_policy(transfer_policy(mode))
            .with_metadata_plan(metadata_plan)
            .with_write_observer(progress),
        )
        .await?;
        (Some(session), None)
    };
    let state = WriteState {
        session,
        direct,
        written_bytes: 0,
        mode,
        transferred: None,
        digest: *blake3::hash(&[]).as_bytes(),
        bytes: 0,
        input_ended: false,
        identity_key,
        size: entry.get_size(),
        observer,
    };
    Ok(StreamHandle {
        state: Arc::new(AsyncMutex::new(state)),
        cancel,
    })
}
#[cfg(test)]
pub async fn write_chunk_stream(
    rx: mpsc::Receiver<PositionedChunk>, handle: &StreamHandle, on_committed: CommitCallback,
) -> Result<()> {
    let input: Input = Box::pin(stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|chunk| (StreamChunk::from(chunk), rx))
    }));
    write_input(input, handle, on_committed).await
}

pub async fn write_planned_stream(rx: mpsc::Receiver<StreamChunk>, handle: &StreamHandle) -> Result<()> {
    let input: Input = Box::pin(stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|chunk| (chunk, rx))
    }));
    write_input(input, handle, Arc::new(|_, _| {})).await
}

async fn write_input(incoming: Input, handle: &StreamHandle, on_committed: CommitCallback) -> Result<()> {
    let direct = handle.state.lock().await.direct.take();
    if let Some(mut direct) = direct {
        let mut input: DirectPatchInput = Box::pin(incoming.map(|input| DirectPatchChunk {
            offset: input.chunk.offset,
            data: input.chunk.data,
            unchanged_basis: input.unchanged_basis,
        }));
        let result = direct.receive(&mut input).await;
        let progress = direct.progress();
        let mut state = handle.state.lock().await;
        state.bytes = progress.bytes;
        state.digest = progress.blake3;
        state.input_ended = progress.ended;
        state.direct = Some(direct);
        return result.map_err(StorageError::from);
    }
    let (session, mode) = {
        let mut state = handle.state.lock().await;
        *state.observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(on_committed);
        (
            state
                .session
                .take()
                .ok_or_else(|| operation("writer already started"))?,
            state.mode,
        )
    };
    let input: PositionedByteStream = Box::pin(incoming.map(|input| Ok(input.chunk)));
    let write = session.write_positioned(input).await;
    let mut state = handle.state.lock().await;
    state.bytes = write.progress.bytes;
    state.input_ended = write.progress.ended;
    state.digest = write.progress.blake3;
    match write.outcome {
        Ok(transferred) => {
            state.transferred = Some(transferred);
            Ok(())
        }
        Err(failure) => Err(failure.cleanup_for_policy(transfer_policy(mode)).await.into()),
    }
}
pub async fn commit_chunk_stream(handle: StreamHandle) -> Result<()> {
    let mut state = handle.state.lock().await;
    if let Some(direct) = state.direct.take() {
        state.written_bytes = direct.complete().await?;
        return Ok(());
    }
    let evidence = ExpertSourceEvidence {
        source_size: state.size,
        blake3: state.digest,
        identity_key: state.identity_key,
        source_qos: storage::SourceQosStats {
            logical_bytes: state.bytes,
            ..Default::default()
        },
    };
    let result = state
        .transferred
        .take()
        .ok_or_else(|| operation("no successful staged write"))?
        .complete(evidence)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(failure) => Err(failure.cleanup_for_policy(transfer_policy(state.mode)).await.into()),
    }
}
#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
