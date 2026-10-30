use crate::{
    connected::{path, timestamp},
    error::{StreamTransferDiagnostic, operation},
    *,
};
use blake3::{Hash, Hasher};
use data_mover::storage::{StorageRoleFailure, WriteProgress, WriteProgressObserver};
use data_mover::{
    metadata::{
        AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
        OwnershipTarget, TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    },
    model::*,
    storage::{
        self, ByteStream, CopiedOwnershipTarget, CopiedTimestampTarget, CopiedValueTarget, SourceDescriptor,
        SourceQosGroup, SourceQosPolicy,
    },
    transfer::{
        DirectPatchChunk, DirectPatchInput, DirectPatchSession, ExpertDestinationRequest, ExpertDestinationSession,
        ExpertDestinationTransferred, ExpertSourceEvidence, TransferFailure,
    },
};
use futures::{Stream, StreamExt, stream};
use moka::sync::Cache;
use qos::{QosConfig, QosManager, parse_bandwidth_string};
use std::pin::Pin;
use std::{
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

type Input = Pin<Box<dyn Stream<Item = StreamChunk> + Send>>;

pub trait ConsistencyCheck {
    fn finalize(self) -> String;
}
#[derive(Clone, Default)]
pub struct HashCalculator(pub(crate) blake3::Hasher);
impl ConsistencyCheck for HashCalculator {
    fn finalize(self) -> String {
        self.0.finalize().to_hex().to_string()
    }
}
pub(crate) fn source_qos(qos: &QosManager, maximum: u64) -> Result<SourceQosGroup> {
    type Groups = Cache<usize, (Weak<QosConfig>, SourceQosGroup)>;
    static GROUPS: OnceLock<Groups> = OnceLock::new();
    let config = qos.config();
    let key = Arc::as_ptr(&config) as usize;
    let groups = GROUPS.get_or_init(|| Cache::builder().support_invalidation_closures().build());
    // 配置释放后移除预算；活跃配置的预算不能被容量或时间驱逐。
    let _ = groups.invalidate_entries_if(|_, (owner, _)| owner.strong_count() == 0);
    if let Some((_, group)) = groups.get(&key) {
        return Ok(group);
    }
    let bandwidth = config.bandwidth.as_deref().map(parse_bandwidth_string).transpose()?;
    let hard = config
        .hard_bandwidth
        .as_deref()
        .map(parse_bandwidth_string)
        .transpose()?;
    let policy = SourceQosPolicy::new(
        bandwidth.map(|soft| {
            (
                soft,
                hard.unwrap_or_else(|| (soft as f64 * f64::from(config.peak_rate)) as u64)
                    .max(soft),
                config.peak_duration.unwrap_or(Duration::ZERO),
            )
        }),
        config.max_io_bytes.unwrap_or(maximum).max(1),
        config.iops.map(|soft| {
            (
                soft,
                config.hard_iops.unwrap_or(soft).max(soft),
                config.iops_peak_duration.unwrap_or(Duration::ZERO),
            )
        }),
    )?;
    // get_with 合并并发初始化，确保相同配置始终共用一个限流预算。
    let (_, group) = groups.get_with(key, || (Arc::downgrade(&config), SourceQosGroup::new(policy)));
    Ok(group)
}
impl StorageEnum {
    pub fn read_chunk_stream(
        storage: &Self, entry: &EntryEnum, intervals: Option<Vec<(u64, u64)>>, qos: Option<QosManager>, hash: bool,
        capacity: usize,
    ) -> (mpsc::Receiver<DataChunk>, JoinHandle<Result<Option<HashCalculator>>>) {
        let storage = storage.clone();
        let entry = entry.clone();
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let task = tokio::spawn(async move {
            let source = storage.roles.read_source()?;
            let p = path(entry.get_relative_path())?;
            let version = entry
                .get_version_id()
                .map_or(SourceVersion::Current, |v| SourceVersion::Id(v.into()));
            let descriptor = source.describe_version(&p, &version).await?;
            let size = descriptor.size.ok_or_else(|| operation("unknown source size"))?;
            let ranges = intervals.unwrap_or_else(|| vec![(0, size)]);
            let mut hasher = hash.then(HashCalculator::default);
            let budget = qos
                .as_ref()
                .map(|q| source_qos(q, storage.block_size))
                .transpose()?
                .map(|g| g.transfer_budget());
            for (start, end) in ranges {
                if start > end || end > size {
                    return Err(operation("source range outside object"));
                }
                if start == end {
                    continue;
                }
                let mut offset = start;
                let mut stream = source
                    .read(storage::ReadRequest {
                        path: p.clone(),
                        range: Some(start..end),
                        expected_source: Some(descriptor.source_identity.clone()),
                        maximum_chunk_bytes: source
                            .maximum_read_chunk_bytes()
                            .min(storage.block_size as usize)
                            .max(1),
                        read_inflight: source.read_concurrency(),
                        read_budget: None,
                        cancel: CancellationToken::new(),
                        source_qos: budget.clone(),
                        version: version.clone(),
                    })
                    .await?;
                while let Some(data) = stream.next().await {
                    let data = data?;
                    if offset + data.len() as u64 > end {
                        return Err(operation("source read exceeded requested range"));
                    }
                    if let Some(hasher) = hasher.as_mut() {
                        hasher.0.update(&data);
                    }
                    let len = data.len() as u64;
                    if tx.send(DataChunk { offset, data }).await.is_err() {
                        return Err(StorageError::Cancelled);
                    }
                    offset += len;
                }
                if offset != end {
                    return Err(operation("source returned a short read"));
                }
            }
            Ok(hasher)
        });
        (rx, task)
    }
}
/// 从 wire 已捕获的值编译计划；不回读源端，也不修改最终路径。
fn stream_metadata_plan(destination: &ConnectedStorage, entry: &EntryEnum) -> Result<MetadataPlan> {
    let staged = destination.roles.staged_destination()?;
    let target = staged.copied_metadata_target();
    let mode = entry.get_mode().map(|value| value & 0o7777);
    let ownership = match (entry.get_uid(), entry.get_gid(), mode) {
        (Some(uid), Some(gid), Some(mode)) => Some(OwnershipMode { uid, gid, mode }),
        _ => None,
    };
    let owner_permitted = ownership.is_none_or(|owner| staged.may_set_owner(owner.uid, owner.gid));
    let tags = entry
        .get_tags()
        .map(|tags| {
            tags.iter()
                .map(|tag| ObjectTag::new(tag.key.clone(), tag.value.clone()))
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .transpose()?;
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        captured(tags),
        captured(ownership),
        captured(Some(TimestampMetadata {
            accessed: Some(timestamp(entry.get_atime())),
            modified: Some(timestamp(entry.get_mtime())),
            created: None,
        })),
    )?;
    let ownership_target = match target.map(|target| target.ownership) {
        Some(CopiedOwnershipTarget::Numeric) if owner_permitted => OwnershipTarget::Numeric,
        Some(CopiedOwnershipTarget::Numeric) => OwnershipTarget::NotPermitted,
        Some(CopiedOwnershipTarget::ModeOnly) => OwnershipTarget::ModeOnly,
        _ => OwnershipTarget::NotApplicable,
    };
    let timestamps = match target.map(|target| target.timestamps) {
        Some(CopiedTimestampTarget::Stored(precision)) => TimestampTargetCapability::Supported(TimestampTarget {
            precision,
            accessed: true,
            modified: true,
            created: false,
        }),
        _ => TimestampTargetCapability::Unsupported,
    };
    let policies = MetadataPolicies::default()
        .with_ownership_mode(
            if ownership.is_some() && owner_permitted && ownership_target == OwnershipTarget::Numeric {
                MetadataPolicy::RequireExact
            } else if ownership.is_none() && mode.is_none() {
                MetadataPolicy::BestEffort
            } else {
                MetadataPolicy::AllowKnownLoss
            },
        )
        .with_timestamps(if matches!(timestamps, TimestampTargetCapability::Supported(_)) {
            MetadataPolicy::AllowKnownLoss
        } else {
            MetadataPolicy::BestEffort
        })
        .with_tags(if entry.get_tags().is_some() {
            MetadataPolicy::BestEffort
        } else {
            MetadataPolicy::Omit
        });
    Ok(compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations,
            target: MetadataTarget {
                acl: AclTarget::NotApplicable,
                xattrs: ValueTarget::NotApplicable,
                tags: if target.is_some_and(|target| target.tags == CopiedValueTarget::Supported) {
                    ValueTarget::Supported
                } else {
                    ValueTarget::Unsupported
                },
                ownership_mode: ownership_target,
                timestamps,
            },
            policies,
            principal_mapper: None,
        },
        ownership.is_none().then_some(mode).flatten(),
        false,
        false,
    )?)
}

fn captured<T>(value: Option<T>) -> MetadataObservation<T> {
    value.map_or(MetadataObservation::NotRequested, |value| MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::Inline,
    })
}

/// basis 提示只由 Receiver 的重建器产生；同位置引用已读取并计入完整摘要。
pub struct StreamChunk {
    pub chunk: DataChunk,
    pub unchanged_basis: bool,
}
impl From<DataChunk> for StreamChunk {
    fn from(chunk: DataChunk) -> Self {
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
    resume_offset: u64,
    transferred: Option<ExpertDestinationTransferred>,
    digest: [u8; 32],
    bytes: u64,
    input_ended: bool,
    identity_key: EntryIdentityKey,
    size: u64,
    cancel: CancellationToken,
    observer: Arc<Mutex<Option<CommitCallback>>>,
}
#[derive(Clone)]
pub struct StreamHandle(Arc<AsyncMutex<WriteState>>);
impl StreamHandle {
    pub async fn written_bytes(&self) -> u64 {
        self.0.lock().await.written_bytes
    }
    pub async fn received_size(&self) -> u64 {
        self.0.lock().await.bytes
    }
    pub async fn input_ended(&self) -> bool {
        self.0.lock().await.input_ended
    }
    pub async fn received_hash(&self) -> String {
        Hash::from_bytes(self.0.lock().await.digest).to_hex().to_string()
    }
    pub async fn discard(&self) -> Result<()> {
        let mut state = self.0.lock().await;
        state.direct.take();
        if let Some(session) = state.session.take() {
            if state.mode != TransferMode::Checkpoint {
                session.discard().await?;
            }
        }
        if let Some(transferred) = state.transferred.take() {
            state.cancel.cancel();
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
impl StorageEnum {
    pub async fn prepare_stream(dest: &Self, entry: &EntryEnum) -> Result<StreamHandle> {
        Self::prepare_stream_with_mode(dest, entry, TransferMode::Atomic).await
    }

    pub async fn prepare_stream_with_mode(dest: &Self, entry: &EntryEnum, mode: TransferMode) -> Result<StreamHandle> {
        dest.validate_stream_transfer_mode(mode)?;
        let p = path(entry.get_relative_path())?;
        // wire 元数据固定本轮源描述；目标检查点由上游绑定并校验，传输仍接收全量摘要证据。
        let identity = SourceIdentity::new(
            BackendIdentity::new(BackendKind::Local, "terrasync-wire-v10")?,
            IdentityStrength::PathScoped,
            blake3::hash(p.as_str().as_bytes()).as_bytes(),
        )?;
        let observed = ObservedEntry::new(
            p.clone(),
            EntryKind::File,
            Some(entry.get_size()),
            Some(timestamp(entry.get_mtime())),
            identity,
        )?;
        let metadata_plan = stream_metadata_plan(dest, entry)?;
        let identity_key = observed.identity_key();
        let cancel = CancellationToken::new();
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
        let (session, direct, resume_offset) = if mode == TransferMode::Direct {
            let source = SourceDescriptor::new(
                p,
                EntryKind::File,
                Some(entry.get_size()),
                observed.source_identity().clone(),
            );
            (
                None,
                Some(DirectPatchSession::prepare(dest.roles.clone(), source, metadata_plan).await?),
                0,
            )
        } else {
            let session = ExpertDestinationSession::prepare(
                ExpertDestinationRequest::new(
                    observed.clone(),
                    dest.block_size as usize,
                    dest.roles.clone(),
                    p.clone(),
                    cancel.clone(),
                )
                .with_transfer_policy(mode.policy())
                .with_metadata_plan(metadata_plan)
                .with_write_observer(progress),
            )
            .await?;
            let offset = session.write_offset();
            (Some(session), None, offset)
        };
        let state = WriteState {
            session,
            direct,
            written_bytes: 0,
            mode,
            resume_offset,
            transferred: None,
            digest: *blake3::hash(&[]).as_bytes(),
            bytes: 0,
            input_ended: false,
            identity_key,
            size: entry.get_size(),
            cancel,
            observer,
        };
        Ok(StreamHandle(Arc::new(AsyncMutex::new(state))))
    }
    pub async fn write_chunk_stream(
        _dest: &Self, _entry: &EntryEnum, rx: mpsc::Receiver<DataChunk>, handle: &StreamHandle,
        _qos: Option<QosManager>, on_committed: CommitCallback,
    ) -> Result<()> {
        let input: Input = Box::pin(stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|chunk| (StreamChunk::from(chunk), rx))
        }));
        Self::write_input(input, handle, on_committed).await
    }

    pub async fn write_planned_stream(rx: mpsc::Receiver<StreamChunk>, handle: &StreamHandle) -> Result<()> {
        let input: Input = Box::pin(stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|chunk| (chunk, rx))
        }));
        Self::write_input(input, handle, Arc::new(|_, _| {})).await
    }

    async fn write_input(incoming: Input, handle: &StreamHandle, on_committed: CommitCallback) -> Result<()> {
        let direct = handle.0.lock().await.direct.take();
        if let Some(mut direct) = direct {
            let mut input: DirectPatchInput = Box::pin(incoming.map(|input| DirectPatchChunk {
                offset: input.chunk.offset,
                data: input.chunk.data,
                unchanged_basis: input.unchanged_basis,
            }));
            let result = direct.receive(&mut input).await;
            let progress = direct.progress();
            let mut state = handle.0.lock().await;
            state.bytes = progress.bytes;
            state.digest = progress.blake3;
            state.input_ended = progress.ended;
            state.direct = Some(direct);
            return result.map_err(StorageError::from);
        }
        let (session, mode, resume_offset) = {
            let mut state = handle.0.lock().await;
            *state.observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(on_committed);
            (
                state
                    .session
                    .take()
                    .ok_or_else(|| operation("writer already started"))?,
                state.mode,
                state.resume_offset,
            )
        };
        let stats = Arc::new(Mutex::new((0u64, Hasher::new(), false)));
        let tally = stats.clone();
        let input: ByteStream = Box::pin(stream::unfold((incoming, false), move |(mut rx, failed)| {
            let stats = stats.clone();
            async move {
                if failed {
                    return None;
                }
                let result = loop {
                    let chunk = match rx.next().await {
                        Some(input) => input.chunk,
                        None => {
                            stats.lock().unwrap_or_else(std::sync::PoisonError::into_inner).2 = true;
                            return None;
                        }
                    };
                    let mut tally = stats.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if chunk.offset != tally.0 {
                        break Err(role_failure("non-contiguous payload"));
                    }
                    tally.0 += chunk.data.len() as u64;
                    tally.1.update(&chunk.data);
                    let skip = resume_offset.saturating_sub(chunk.offset).min(chunk.data.len() as u64) as usize;
                    if skip < chunk.data.len() {
                        break Ok(chunk.data.slice(skip..));
                    }
                };
                let failed = result.is_err();
                Some((result, (rx, failed)))
            }
        }));
        let result = session.write(input).await;
        let (bytes, digest, input_ended) = {
            let stats = tally.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (stats.0, *stats.1.finalize().as_bytes(), stats.2)
        };
        let mut state = handle.0.lock().await;
        state.bytes = bytes;
        state.input_ended = input_ended;
        state.digest = digest;
        match result {
            Ok(transferred) => {
                state.transferred = Some(transferred);
                Ok(())
            }
            Err(failure) => Err(cleanup_stream_failure(failure, mode).await),
        }
    }
    pub async fn commit_chunk_stream(_dest: &Self, _entry: &EntryEnum, _size: u64, handle: StreamHandle) -> Result<()> {
        let mut state = handle.0.lock().await;
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
            Err(failure) => Err(cleanup_stream_failure(failure, state.mode).await),
        }
    }
}
/// Checkpoint 保留未发布状态；Atomic 先复制结构化诊断，再清理原始失败持有的 stage。
async fn cleanup_stream_failure(failure: TransferFailure, mode: TransferMode) -> StorageError {
    if mode == TransferMode::Checkpoint && failure.has_unpublished_stage() {
        return failure.into();
    }
    let mut diagnostic = StreamTransferDiagnostic::capture(&failure);
    diagnostic.cleanup_failure = if failure.has_pending_cleanup() {
        failure.cleanup_published_stage().await.err()
    } else if failure.has_unpublished_stage() {
        failure.discard_stage().await.err()
    } else {
        None
    };
    StorageError::StreamTransfer(Box::new(diagnostic))
}

pub(crate) fn role_failure(message: &str) -> storage::StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Write,
            FailureClass::Protocol,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("固定诊断")),
    )
}
impl ConnectedStorage {
    pub async fn compute_hash(&self, p: &Path, _size: u64) -> Result<String> {
        let source = self.roles.read_source()?;
        let p = path(p)?;
        let desc = source.describe(&p).await?;
        let mut stream = source
            .read(storage::ReadRequest {
                path: p,
                range: None,
                expected_source: Some(desc.source_identity),
                maximum_chunk_bytes: source.maximum_read_chunk_bytes(),
                read_inflight: source.read_concurrency(),
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: None,
                version: SourceVersion::Current,
            })
            .await?;
        let mut hasher = Hasher::new();
        while let Some(bytes) = stream.next().await {
            hasher.update(&bytes?);
        }
        Ok(hasher.finalize().to_hex().to_string())
    }
}

#[cfg(test)]
#[path = "streaming_metadata_tests.rs"]
mod tests;
