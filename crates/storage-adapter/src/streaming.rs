use crate::{error::operation, *};
use data_mover::{
    model::*,
    storage::{self, ByteStream, SourceQosGroup, SourceQosPolicy},
    transfer::{
        ExpertDestinationRequest, ExpertDestinationSession, ExpertDestinationTransferred, ExpertSourceEvidence,
        TransferPolicy,
    },
};
use futures::StreamExt;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

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
pub(crate) fn source_qos(qos: &qos::QosManager, maximum: u64) -> Result<SourceQosGroup> {
    type Groups = HashMap<usize, (Weak<qos::QosConfig>, SourceQosGroup)>;
    static GROUPS: OnceLock<Mutex<Groups>> = OnceLock::new();
    let config = qos.config();
    let key = Arc::as_ptr(&config) as usize;
    let mut groups = GROUPS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    groups.retain(|_, (owner, _)| owner.strong_count() > 0);
    if let Some((_, group)) = groups.get(&key) {
        return Ok(group.clone());
    }
    let bandwidth = config
        .bandwidth
        .as_deref()
        .map(qos::parse_bandwidth_string)
        .transpose()?;
    let hard = config
        .hard_bandwidth
        .as_deref()
        .map(qos::parse_bandwidth_string)
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
    )
    .map_err(operation)?;
    let group = SourceQosGroup::new(policy);
    groups.insert(key, (Arc::downgrade(&config), group.clone()));
    Ok(group)
}
impl StorageEnum {
    pub fn read_chunk_stream(
        storage: &Self, entry: &EntryEnum, intervals: Option<Vec<(u64, u64)>>, qos: Option<qos::QosManager>,
        hash: bool, capacity: usize,
    ) -> (mpsc::Receiver<DataChunk>, JoinHandle<Result<Option<HashCalculator>>>) {
        let storage = storage.clone();
        let entry = entry.clone();
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let task = tokio::spawn(async move {
            let source = storage.roles.read_source()?;
            let p = crate::connected::path(entry.get_relative_path())?;
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
struct WriteState {
    session: Option<ExpertDestinationSession>,
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
pub struct StreamHandle(Arc<tokio::sync::Mutex<WriteState>>);
impl StreamHandle {
    pub async fn received_size(&self) -> u64 {
        self.0.lock().await.bytes
    }
    pub async fn input_ended(&self) -> bool {
        self.0.lock().await.input_ended
    }
    pub async fn received_hash(&self) -> String {
        blake3::Hash::from_bytes(self.0.lock().await.digest)
            .to_hex()
            .to_string()
    }
    pub async fn discard(&self) -> Result<()> {
        let mut state = self.0.lock().await;
        if let Some(session) = state.session.take() {
            session.discard().await?;
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
                failure.discard_stage().await?;
            }
        }
        Ok(())
    }
}
impl StorageEnum {
    pub async fn prepare_stream(dest: &Self, entry: &EntryEnum) -> Result<StreamHandle> {
        let p = crate::connected::path(entry.get_relative_path())?;
        // 旧 wire 不携带 opaque observation；该产品会话只接受从零写入，不能授权恢复。
        let identity = SourceIdentity::new(
            BackendIdentity::new(BackendKind::Local, "terrasync-wire-v9")?,
            IdentityStrength::PathScoped,
            blake3::hash(p.as_str().as_bytes()).as_bytes(),
        )?;
        let observed = ObservedEntry::new(
            p.clone(),
            EntryKind::File,
            Some(entry.get_size()),
            Some(crate::connected::timestamp(entry.get_mtime())),
            identity,
        )?;
        let identity_key = observed.identity_key();
        let cancel = CancellationToken::new();
        let observer = Arc::new(Mutex::new(None::<CommitCallback>));
        let callback = observer.clone();
        let progress = storage::WriteProgressObserver::new(move |event| {
            if let storage::WriteProgress::Confirmed { offset, bytes } = event {
                let callback = callback
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(callback) = callback {
                    callback(offset, bytes);
                }
            }
        });
        let session = ExpertDestinationSession::prepare(
            ExpertDestinationRequest::new(
                observed,
                dest.block_size as usize,
                dest.roles.clone(),
                p,
                cancel.clone(),
            )
            .with_transfer_policy(TransferPolicy::AtomicReplace)
            .with_write_observer(progress),
        )
        .await?;
        let state = WriteState {
            session: Some(session),
            transferred: None,
            digest: *blake3::hash(&[]).as_bytes(),
            bytes: 0,
            input_ended: false,
            identity_key,
            size: entry.get_size(),
            cancel,
            observer,
        };
        Ok(StreamHandle(Arc::new(tokio::sync::Mutex::new(state))))
    }
    pub async fn write_chunk_stream(
        _dest: &Self, _entry: &EntryEnum, rx: mpsc::Receiver<DataChunk>, handle: &StreamHandle,
        _qos: Option<qos::QosManager>, on_committed: CommitCallback,
    ) -> Result<()> {
        let session = {
            let mut state = handle.0.lock().await;
            *state.observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(on_committed);
            state
                .session
                .take()
                .ok_or_else(|| operation("writer already started"))?
        };
        let stats = Arc::new(Mutex::new((0u64, blake3::Hasher::new(), false)));
        let tally = stats.clone();
        let input: ByteStream = Box::pin(futures::stream::unfold((rx, false), move |(mut rx, failed)| {
            let stats = stats.clone();
            async move {
                if failed {
                    return None;
                }
                let chunk = match rx.recv().await {
                    Some(chunk) => chunk,
                    None => {
                        stats.lock().unwrap_or_else(std::sync::PoisonError::into_inner).2 = true;
                        return None;
                    }
                };
                let result = {
                    let mut tally = stats.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if chunk.offset != tally.0 {
                        Err(role_failure("non-contiguous payload"))
                    } else {
                        tally.0 += chunk.data.len() as u64;
                        tally.1.update(&chunk.data);
                        Ok(chunk.data)
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
            Err(failure) => {
                let message = failure.to_string();
                failure.discard_stage().await?;
                Err(operation(message))
            }
        }
    }
    pub async fn commit_chunk_stream(_dest: &Self, _entry: &EntryEnum, _size: u64, handle: StreamHandle) -> Result<()> {
        let mut state = handle.0.lock().await;
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
            Err(failure) => {
                let message = failure.to_string();
                if failure.has_pending_cleanup() {
                    failure.cleanup_published_stage().await?;
                } else {
                    failure.discard_stage().await?;
                }
                Err(operation(message))
            }
        }
    }
}
fn role_failure(message: &str) -> storage::StorageRoleFailure {
    storage::StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Write,
            FailureClass::Protocol,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic")),
    )
}
impl ConnectedStorage {
    pub async fn compute_hash(&self, p: &Path, _size: u64) -> Result<String> {
        let source = self.roles.read_source()?;
        let p = crate::connected::path(p)?;
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
        let mut hasher = blake3::Hasher::new();
        while let Some(bytes) = stream.next().await {
            hasher.update(&bytes?);
        }
        Ok(hasher.finalize().to_hex().to_string())
    }
}
