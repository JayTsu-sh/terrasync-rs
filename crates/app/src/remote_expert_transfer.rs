//! Remote expert-transfer orchestration.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use data_mover::metadata::MetadataPlan;
use data_mover::model::{EntryIdentityKey, ObservedEntry, StoragePath};
use data_mover::storage::{ExistingDestinationPolicy, SourceQosGroup, SourceQosStats, Storage};
use data_mover::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceEvidence, ExpertSourceRequest, ExpertSourceSession,
    InflightLimits, RecoveryProvider, Resumability, TransferIdentity, TransferOutcome, TransferPhase, TransferSide,
};
use futures::stream;
use tokio_util::sync::CancellationToken;
use transport::flow_control::{DEFAULT_CREDIT_WINDOW_BYTES, ReceiverCreditState, payload_credit_cost};
use transport::message::{
    ReceiverMsg, RemoteDestinationEvent, RemoteSourceEvent, RemoteStageDisposition, RemoteTransferFailure,
    RemoteTransferPhase, RemoteTransferSide, RemoteTransferTerminal, SenderMsg, SessionNdx, SourceQosSnapshot,
};
use transport::traits::{ReceiverTransport, SenderTransport};

use crate::error::{AppError, Result};
use crate::local_transfer::{EntryRecoveryState, lazy_entry_recovery_provider};

/// Source-process inputs for one selected advertised observation.
pub struct RemoteExpertSourceRequest {
    ndx: SessionNdx,
    identity: TransferIdentity,
    source: Storage,
    observation: ObservedEntry,
    inflight: InflightLimits,
    cancel: CancellationToken,
    source_qos: Option<SourceQosGroup>,
}

impl RemoteExpertSourceRequest {
    #[must_use]
    pub fn new(
        ndx: SessionNdx, identity: TransferIdentity, source: Storage, observation: ObservedEntry,
        inflight: InflightLimits, cancel: CancellationToken,
    ) -> Self {
        Self {
            ndx,
            identity,
            source,
            observation,
            inflight,
            cancel,
            source_qos: None,
        }
    }

    #[must_use]
    pub fn with_source_qos(mut self, source_qos: SourceQosGroup) -> Self {
        self.source_qos = Some(source_qos);
        self
    }
}

/// Destination-process inputs for one selected advertised observation.
pub struct RemoteExpertDestinationRequest {
    ndx: SessionNdx,
    identity: TransferIdentity,
    observation: ObservedEntry,
    destination: Storage,
    final_path: StoragePath,
    inflight: InflightLimits,
    cancel: CancellationToken,
    existing_destination: ExistingDestinationPolicy,
    resumability: Resumability,
    recovery: Option<Arc<dyn EntryRecoveryState>>,
    metadata_plan: Option<MetadataPlan>,
}

impl RemoteExpertDestinationRequest {
    #[must_use]
    pub fn new(
        ndx: SessionNdx, identity: TransferIdentity, observation: ObservedEntry, destination: Storage,
        final_path: StoragePath, inflight: InflightLimits, cancel: CancellationToken,
    ) -> Self {
        Self {
            ndx,
            identity,
            observation,
            destination,
            final_path,
            inflight,
            cancel,
            existing_destination: ExistingDestinationPolicy::default(),
            resumability: Resumability::default(),
            recovery: None,
            metadata_plan: None,
        }
    }

    #[must_use]
    pub fn with_existing_destination_policy(mut self, policy: ExistingDestinationPolicy) -> Self {
        self.existing_destination = policy;
        self
    }

    #[must_use]
    pub fn with_recovery(mut self, resumability: Resumability, recovery: Option<Arc<dyn EntryRecoveryState>>) -> Self {
        self.resumability = resumability;
        self.recovery = recovery;
        self
    }

    /// Supplies metadata semantics compiled from the advertised observation before target I/O.
    #[must_use]
    pub fn with_metadata_plan(mut self, plan: MetadataPlan) -> Self {
        self.metadata_plan = Some(plan);
        self
    }
}

/// Completed source-side accounting for a remote transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteExpertSourceReport {
    pub transferred_bytes: u64,
    pub source_qos: SourceQosStats,
}

/// Serves one receiver-selected NDX and streams only bytes after its durable prefix.
pub async fn serve_remote_source(
    transport: Arc<dyn SenderTransport>, request: RemoteExpertSourceRequest,
) -> Result<RemoteExpertSourceReport> {
    let ndx = request.ndx;
    let cancel = request.cancel.clone();
    match serve_remote_source_inner(Arc::clone(&transport), request).await {
        Ok(report) => Ok(report),
        Err(error) => {
            if let AppError::RemoteExpertTransferFailure { source, .. } = &error {
                let event = if cancel.is_cancelled() {
                    RemoteSourceEvent::Cancelled { ndx }
                } else {
                    RemoteSourceEvent::Failed {
                        ndx,
                        failure: project_failure(source),
                    }
                };
                let _ = transport.send(SenderMsg::RemoteSource(event)).await;
            }
            Err(error)
        }
    }
}

async fn serve_remote_source_inner(
    transport: Arc<dyn SenderTransport>, request: RemoteExpertSourceRequest,
) -> Result<RemoteExpertSourceReport> {
    expect_transfer_request(transport.recv().await, request.ndx)?;
    let mut source_request = ExpertSourceRequest::new(
        request.identity,
        request.source,
        request.observation,
        request.inflight,
        request.cancel,
    );
    if let Some(source_qos) = request.source_qos {
        source_request = source_request.with_source_qos(source_qos);
    }
    let session = ExpertSourceSession::open(source_request)
        .await
        .map_err(|source| transfer_failure(request.ndx, source))?;
    let offer = session.offer();
    transport
        .send(SenderMsg::RemoteSource(RemoteSourceEvent::Offer {
            ndx: request.ndx,
            source_size: offer.source_size,
            maximum_chunk_bytes: u64::try_from(offer.maximum_chunk_bytes)
                .map_err(|_| protocol(request.ndx, "source chunk ceiling exceeds wire range"))?,
            identity_key: *offer.identity_key.as_bytes(),
        }))
        .await?;
    let (durable_prefix, requested_chunk) =
        expect_payload_request(transport.recv().await, request.ndx, offer.source_size)?;
    let mut payload = session
        .stream_from(durable_prefix)
        .map_err(|source| transfer_failure(request.ndx, source))?;
    let mut offset = durable_prefix;
    while let Some(chunk) = payload
        .next_chunk()
        .await
        .map_err(|source| transfer_failure(request.ndx, source))?
    {
        for part in split_bytes(&chunk, requested_chunk) {
            let length =
                u64::try_from(part.len()).map_err(|_| protocol(request.ndx, "payload length exceeds wire range"))?;
            transport
                .send(SenderMsg::RemoteSource(RemoteSourceEvent::Payload {
                    ndx: request.ndx,
                    offset,
                    data: part,
                }))
                .await?;
            offset = offset
                .checked_add(length)
                .ok_or_else(|| protocol(request.ndx, "payload offset overflow"))?;
        }
    }
    let evidence = payload
        .finish()
        .await
        .map_err(|source| transfer_failure(request.ndx, source))?;
    transport
        .send(SenderMsg::RemoteSource(RemoteSourceEvent::Completed {
            ndx: request.ndx,
            source_size: evidence.source_size,
            blake3: evidence.blake3,
            identity_key: *evidence.identity_key.as_bytes(),
            source_qos: qos_to_wire(evidence.source_qos),
        }))
        .await?;
    expect_destination_terminal(transport.recv().await, request.ndx, &evidence)?;
    Ok(RemoteExpertSourceReport {
        transferred_bytes: evidence.source_size,
        source_qos: evidence.source_qos,
    })
}

/// Receives one source stream into a staged destination, verifies it, then publishes it.
pub async fn receive_remote_destination(
    transport: Arc<dyn ReceiverTransport>, request: RemoteExpertDestinationRequest,
) -> Result<TransferOutcome> {
    let ndx = request.ndx;
    let cancel = request.cancel.clone();
    match receive_remote_destination_inner(Arc::clone(&transport), request).await {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            if let Some(failure) = project_destination_failure(&error) {
                let terminal = if cancel.is_cancelled() {
                    RemoteTransferTerminal::Cancelled { ndx }
                } else {
                    RemoteTransferTerminal::Failed { ndx, failure }
                };
                let _ = transport
                    .send(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(
                        terminal,
                    )))
                    .await;
            }
            Err(error)
        }
    }
}

async fn receive_remote_destination_inner(
    transport: Arc<dyn ReceiverTransport>, request: RemoteExpertDestinationRequest,
) -> Result<TransferOutcome> {
    validate_recovery(&request)?;
    transport
        .send(ReceiverMsg::RemoteDestination(
            RemoteDestinationEvent::TransferRequested { ndx: request.ndx },
        ))
        .await?;
    let (source_size, source_chunk) = expect_offer(transport.recv().await, request.ndx, &request.observation)?;
    let identity_key = request.observation.identity_key();
    let (provider, recovery_opened) = recovery_provider(&request);
    let mut destination_request = ExpertDestinationRequest::new(
        request.identity,
        request.observation.clone(),
        source_chunk,
        request.destination,
        request.final_path,
        request.inflight,
        request.cancel,
    )
    .with_existing_destination_policy(request.existing_destination)
    .with_recovery(request.resumability, provider);
    if let Some(plan) = request.metadata_plan.clone() {
        destination_request = destination_request.with_metadata_plan(plan);
    }
    let destination = ExpertDestinationSession::prepare(destination_request)
        .await
        .map_err(|source| transfer_failure(request.ndx, source))?;
    let durable_prefix = destination.write_offset();
    let credit_window = usize::try_from(DEFAULT_CREDIT_WINDOW_BYTES)
        .map_err(|_| protocol(request.ndx, "credit window exceeds platform range"))?;
    let destination_chunk = destination.maximum_chunk_bytes().min(credit_window);
    transport
        .send(ReceiverMsg::RemoteDestination(
            RemoteDestinationEvent::PayloadRequested {
                ndx: request.ndx,
                durable_prefix,
                maximum_chunk_bytes: u64::try_from(destination_chunk)
                    .map_err(|_| protocol(request.ndx, "destination chunk ceiling exceeds wire range"))?,
            },
        ))
        .await?;

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let wire_stream: data_mover::storage::ByteStream = Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let write = destination.write(wire_stream);
    let receive = receive_payload(
        Arc::clone(&transport),
        request.ndx,
        source_size,
        durable_prefix,
        destination_chunk,
        identity_key,
        tx,
    );
    let (write_result, receive_result) = tokio::join!(write, receive);
    let (transferred, evidence) = match (write_result, receive_result) {
        (Ok(transferred), Ok(evidence)) => (transferred, evidence),
        (Err(destination), Err(peer)) => {
            return Err(AppError::RemoteExpertPayloadInterrupted {
                ndx: request.ndx.0,
                peer: Box::new(peer),
                destination,
            });
        }
        (Err(source), Ok(_)) => return Err(transfer_failure(request.ndx, source)),
        (Ok(_), Err(peer)) => return Err(peer),
    };
    let outcome = transferred
        .complete(evidence)
        .await
        .map_err(|source| transfer_failure(request.ndx, source))?;
    if recovery_opened.load(Ordering::Acquire)
        && let Some(recovery) = request.recovery
    {
        recovery
            .completed(&request.observation)
            .await
            .map_err(|source| AppError::RemoteExpertRecoveryCompletion {
                ndx: request.ndx.0,
                source_qos: outcome.source_qos,
                source: Box::new(source),
            })?;
    }
    transport
        .send(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(
            RemoteTransferTerminal::Completed {
                ndx: request.ndx,
                transferred_bytes: outcome.transferred_bytes,
                blake3: outcome.blake3,
            },
        )))
        .await?;
    Ok(outcome)
}

fn expect_destination_terminal(
    message: Option<ReceiverMsg>, ndx: SessionNdx, evidence: &ExpertSourceEvidence,
) -> Result<()> {
    match message {
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(RemoteTransferTerminal::Completed {
            ndx: received,
            transferred_bytes,
            blake3,
        }))) if received == ndx && transferred_bytes == evidence.source_size && blake3 == evidence.blake3 => Ok(()),
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(RemoteTransferTerminal::Failed {
            ndx: received,
            failure,
        }))) if received == ndx => Err(peer_failure(ndx, failure)),
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(RemoteTransferTerminal::Cancelled {
            ndx: received,
        }))) if received == ndx => Err(AppError::RemoteExpertPeerCancelled { ndx: ndx.0 }),
        _ => Err(protocol(ndx, "expected matching destination terminal")),
    }
}

fn expect_transfer_request(message: Option<ReceiverMsg>, ndx: SessionNdx) -> Result<()> {
    match message {
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::TransferRequested { ndx: received }))
            if received == ndx =>
        {
            Ok(())
        }
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(RemoteTransferTerminal::Failed {
            ndx: received,
            failure,
        }))) if received == ndx => Err(peer_failure(ndx, failure)),
        Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(RemoteTransferTerminal::Cancelled {
            ndx: received,
        }))) if received == ndx => Err(AppError::RemoteExpertPeerCancelled { ndx: ndx.0 }),
        _ => Err(protocol(ndx, "expected matching transfer request")),
    }
}

fn expect_payload_request(message: Option<ReceiverMsg>, ndx: SessionNdx, source_size: u64) -> Result<(u64, usize)> {
    if let Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::Terminal(terminal))) = message {
        return match terminal {
            RemoteTransferTerminal::Failed { ndx: received, failure } if received == ndx => {
                Err(peer_failure(ndx, failure))
            }
            RemoteTransferTerminal::Cancelled { ndx: received } if received == ndx => {
                Err(AppError::RemoteExpertPeerCancelled { ndx: ndx.0 })
            }
            _ => Err(protocol(ndx, "unexpected destination terminal")),
        };
    }
    let Some(ReceiverMsg::RemoteDestination(RemoteDestinationEvent::PayloadRequested {
        ndx: received,
        durable_prefix,
        maximum_chunk_bytes,
    })) = message
    else {
        return Err(protocol(ndx, "expected matching payload request"));
    };
    if received != ndx || durable_prefix > source_size || maximum_chunk_bytes == 0 {
        return Err(protocol(ndx, "invalid payload request"));
    }
    let maximum_chunk_bytes = usize::try_from(maximum_chunk_bytes)
        .map_err(|_| protocol(ndx, "requested chunk ceiling exceeds platform range"))?;
    Ok((durable_prefix, maximum_chunk_bytes))
}

fn expect_offer(message: Option<SenderMsg>, ndx: SessionNdx, observation: &ObservedEntry) -> Result<(u64, usize)> {
    if let Some(SenderMsg::RemoteSource(event)) = message {
        match event {
            RemoteSourceEvent::Failed { ndx: received, failure } if received == ndx => {
                return Err(peer_failure(ndx, failure));
            }
            RemoteSourceEvent::Cancelled { ndx: received } if received == ndx => {
                return Err(AppError::RemoteExpertPeerCancelled { ndx: ndx.0 });
            }
            RemoteSourceEvent::Offer {
                ndx: received,
                source_size,
                maximum_chunk_bytes,
                identity_key,
            } => {
                return validate_offer(
                    received,
                    source_size,
                    maximum_chunk_bytes,
                    identity_key,
                    ndx,
                    observation,
                );
            }
            _ => return Err(protocol(ndx, "expected matching source offer")),
        }
    }
    Err(protocol(ndx, "expected matching source offer"))
}

fn validate_offer(
    received: SessionNdx, source_size: u64, maximum_chunk_bytes: u64, identity_key: [u8; 32], ndx: SessionNdx,
    observation: &ObservedEntry,
) -> Result<(u64, usize)> {
    if received != ndx
        || Some(source_size) != observation.size()
        || identity_key != *observation.identity_key().as_bytes()
        || maximum_chunk_bytes == 0
    {
        return Err(protocol(ndx, "source offer differs from advertised observation"));
    }
    let maximum_chunk_bytes = usize::try_from(maximum_chunk_bytes)
        .map_err(|_| protocol(ndx, "source chunk ceiling exceeds platform range"))?;
    Ok((source_size, maximum_chunk_bytes))
}

async fn receive_payload(
    transport: Arc<dyn ReceiverTransport>, ndx: SessionNdx, source_size: u64, durable_prefix: u64,
    maximum_chunk_bytes: usize, identity_key: EntryIdentityKey,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Bytes, data_mover::storage::StorageRoleFailure>>,
) -> Result<ExpertSourceEvidence> {
    let mut expected_offset = durable_prefix;
    let mut credit = ReceiverCreditState::default();
    loop {
        match transport.recv().await {
            Some(SenderMsg::RemoteSource(RemoteSourceEvent::Payload {
                ndx: received,
                offset,
                data,
            })) => {
                let length =
                    u64::try_from(data.len()).map_err(|_| protocol(ndx, "payload length exceeds wire range"))?;
                if received != ndx
                    || data.is_empty()
                    || data.len() > maximum_chunk_bytes
                    || offset != expected_offset
                    || offset.checked_add(length).is_none_or(|end| end > source_size)
                {
                    credit.flush(transport.as_ref()).await?;
                    return Err(protocol(ndx, "payload offset, size, or NDX is invalid"));
                }
                let bytes = payload_credit_cost(&data);
                if tx.send(Ok(data)).await.is_err() {
                    credit.flush(transport.as_ref()).await?;
                    return Err(protocol(ndx, "destination stopped accepting payload"));
                }
                credit.accepted(transport.as_ref(), bytes).await?;
                expected_offset += length;
            }
            Some(SenderMsg::RemoteSource(RemoteSourceEvent::Completed {
                ndx: received,
                source_size: completed_size,
                blake3,
                identity_key: completed_identity,
                source_qos,
            })) => {
                if received != ndx
                    || completed_size != source_size
                    || completed_identity != *identity_key.as_bytes()
                    || expected_offset != source_size
                {
                    credit.flush(transport.as_ref()).await?;
                    return Err(protocol(ndx, "source completion evidence is inconsistent"));
                }
                credit.flush(transport.as_ref()).await?;
                drop(tx);
                return Ok(ExpertSourceEvidence {
                    source_size,
                    blake3,
                    identity_key,
                    source_qos: qos_from_wire(source_qos),
                });
            }
            Some(SenderMsg::RemoteSource(RemoteSourceEvent::Failed { ndx: received, failure })) if received == ndx => {
                credit.flush(transport.as_ref()).await?;
                return Err(peer_failure(ndx, failure));
            }
            Some(SenderMsg::RemoteSource(RemoteSourceEvent::Cancelled { ndx: received })) if received == ndx => {
                credit.flush(transport.as_ref()).await?;
                return Err(AppError::RemoteExpertPeerCancelled { ndx: ndx.0 });
            }
            _ => {
                credit.flush(transport.as_ref()).await?;
                return Err(protocol(ndx, "unexpected message while receiving payload"));
            }
        }
    }
}

fn recovery_provider(request: &RemoteExpertDestinationRequest) -> (Option<Arc<dyn RecoveryProvider>>, Arc<AtomicBool>) {
    request.recovery.as_ref().map_or_else(
        || (None, Arc::new(AtomicBool::new(false))),
        |store| {
            let (provider, opened) = lazy_entry_recovery_provider(Arc::clone(store), request.observation.clone());
            (Some(provider), opened)
        },
    )
}

fn validate_recovery(request: &RemoteExpertDestinationRequest) -> Result<()> {
    match (request.resumability, request.recovery.is_some()) {
        (Resumability::Enabled, false) => Err(AppError::ConfigError(
            "enabled remote resumability requires entry recovery persistence".to_string(),
        )),
        (Resumability::Disabled, true) => Err(AppError::ConfigError(
            "disabled remote resumability cannot accept entry recovery persistence".to_string(),
        )),
        _ => Ok(()),
    }
}

fn split_bytes(bytes: &Bytes, maximum: usize) -> Vec<Bytes> {
    let mut parts = Vec::with_capacity(bytes.len().div_ceil(maximum));
    let mut start = 0;
    while start < bytes.len() {
        let end = start.saturating_add(maximum).min(bytes.len());
        parts.push(bytes.slice(start..end));
        start = end;
    }
    parts
}

fn qos_to_wire(stats: SourceQosStats) -> SourceQosSnapshot {
    SourceQosSnapshot {
        logical_bytes: stats.logical_bytes,
        client_streamed_shaped_bytes: stats.client_streamed_shaped_bytes,
        native_bytes: stats.native_bytes,
        source_read_operations: stats.source_read_operations,
        native_requests: stats.native_requests,
        native_payload_shaped: stats.native_payload_shaped,
    }
}

fn qos_from_wire(stats: SourceQosSnapshot) -> SourceQosStats {
    SourceQosStats {
        logical_bytes: stats.logical_bytes,
        client_streamed_shaped_bytes: stats.client_streamed_shaped_bytes,
        native_bytes: stats.native_bytes,
        source_read_operations: stats.source_read_operations,
        native_requests: stats.native_requests,
        native_payload_shaped: stats.native_payload_shaped,
    }
}

fn project_failure(failure: &data_mover::transfer::TransferFailure) -> RemoteTransferFailure {
    let stage = if failure.has_pending_cleanup() {
        RemoteStageDisposition::PublishedCleanupPending
    } else if failure.has_recoverable_stage() {
        RemoteStageDisposition::Recoverable
    } else if failure.has_unpublished_stage() {
        RemoteStageDisposition::UnpublishedEphemeral
    } else {
        RemoteStageDisposition::None
    };
    RemoteTransferFailure {
        phase: match failure.phase() {
            TransferPhase::Preflight => RemoteTransferPhase::Preflight,
            TransferPhase::Describe => RemoteTransferPhase::Describe,
            TransferPhase::Prepare => RemoteTransferPhase::Prepare,
            TransferPhase::RecoveryRegistration => RemoteTransferPhase::RecoveryRegistration,
            TransferPhase::Transfer => RemoteTransferPhase::Transfer,
            TransferPhase::Checkpoint => RemoteTransferPhase::Checkpoint,
            TransferPhase::Verify => RemoteTransferPhase::Verify,
            TransferPhase::Metadata => RemoteTransferPhase::Metadata,
            TransferPhase::Publish => RemoteTransferPhase::Publish,
        },
        side: match failure.side() {
            TransferSide::Source => RemoteTransferSide::Source,
            TransferSide::Destination => RemoteTransferSide::Destination,
            TransferSide::Orchestration => RemoteTransferSide::Orchestration,
        },
        stage,
        final_destination_changed: failure.final_destination_changed(),
        source_qos: qos_to_wire(failure.source_qos()),
        diagnostic: failure.to_string(),
    }
}

fn project_destination_failure(error: &AppError) -> Option<RemoteTransferFailure> {
    match error {
        AppError::RemoteExpertTransferFailure { source, .. } => Some(project_failure(source)),
        AppError::RemoteExpertRecoveryCompletion { source_qos, source, .. } => Some(RemoteTransferFailure {
            phase: RemoteTransferPhase::RecoveryCompletion,
            side: RemoteTransferSide::Orchestration,
            stage: RemoteStageDisposition::None,
            final_destination_changed: true,
            source_qos: qos_to_wire(*source_qos),
            diagnostic: source.to_string(),
        }),
        AppError::RemoteExpertPayloadInterrupted { destination, .. } => Some(project_failure(destination)),
        _ => None,
    }
}

fn peer_failure(ndx: SessionNdx, failure: RemoteTransferFailure) -> AppError {
    AppError::RemoteExpertPeerFailure { ndx: ndx.0, failure }
}

fn protocol(ndx: SessionNdx, reason: impl Into<String>) -> AppError {
    AppError::RemoteExpertTransferProtocol {
        ndx: ndx.0,
        reason: reason.into(),
    }
}

fn transfer_failure(ndx: SessionNdx, source: data_mover::transfer::TransferFailure) -> AppError {
    AppError::RemoteExpertTransferFailure { ndx: ndx.0, source }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use data_mover::metadata::{
        AclTarget, ApplicationOutcome, MetadataPlanRequest, MetadataPolicies, MetadataTarget, OwnershipTarget,
        TimestampTargetCapability, ValueTarget, compile_metadata_plan,
    };
    use data_mover::model::{BackendIdentity, BackendKind, ObservedEntry, StoragePath};
    use data_mover::storage::{
        BackendConfig, LocalBackendConfig, PreflightPolicy, SourceQosGroup, SourceQosPolicy, Storage, connect_backend,
    };
    use data_mover::transfer::{
        InflightLimits, RecoveryRegistrar, RecoveryRegistrationFailure, Resumability, TransferIdentity,
    };
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;
    use transport::message::{
        AdvertisedObservation, AdvertisementEvent, AdvertisementTerminal, HandshakeResult, ObservationPage,
        ProtocolHandshake, ReceiverMsg, RemoteSourceEvent, SenderMsg, SessionNdx,
    };
    use transport::prelude::create_in_process_pair_with_capacity;
    use transport::quic;
    use transport::traits::{ReceiverTransport as _, SenderTransport as _};

    use super::{
        RemoteExpertDestinationRequest, RemoteExpertSourceRequest, receive_remote_destination, serve_remote_source,
    };
    use crate::local_transfer::{EntryRecoveryState, RecoveryRegistration};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    struct ScriptedReceiver {
        incoming: tokio::sync::Mutex<VecDeque<SenderMsg>>,
    }

    #[async_trait::async_trait]
    impl transport::traits::ReceiverTransport for ScriptedReceiver {
        async fn recv(&self) -> Option<SenderMsg> {
            self.incoming.lock().await.pop_front()
        }

        async fn send(&self, _msg: ReceiverMsg) -> transport::error::Result<()> {
            Ok(())
        }

        async fn close(&self) -> transport::error::Result<()> {
            Ok(())
        }
    }

    async fn local_storage(root: &std::path::Path, stable_id: &str) -> TestResult<Storage> {
        Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
            root: root.to_path_buf(),
            identity: BackendIdentity::new(BackendKind::Local, stable_id)?,
            read_concurrency: NonZeroUsize::new(2).ok_or("read concurrency")?,
            write_concurrency: NonZeroUsize::new(2).ok_or("write concurrency")?,
        }))
        .await?)
    }

    async fn observe(storage: &Storage, path: &str) -> TestResult<ObservedEntry> {
        let source = storage.read_source(&PreflightPolicy::production())?;
        let descriptor = source.describe(&StoragePath::new(path)?).await?;
        Ok(ObservedEntry::new(
            descriptor.path,
            descriptor.kind,
            descriptor.size,
            None,
            descriptor.source_identity,
        )?)
    }

    #[tokio::test]
    async fn capacity_one_transport_streams_local_file_through_expert_halves() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        let payload = (0..(256 * 1024 + 7))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        tokio::fs::write(source_root.path().join("payload.bin"), &payload).await?;
        let source = local_storage(source_root.path(), "remote-expert-source").await?;
        let destination = local_storage(destination_root.path(), "remote-expert-destination").await?;
        let observation = observe(&source, "payload.bin").await?;
        let metadata_plan = compile_metadata_plan(&MetadataPlanRequest {
            observations: observation.metadata(),
            target: MetadataTarget {
                acl: AclTarget::NotApplicable,
                xattrs: ValueTarget::NotApplicable,
                tags: ValueTarget::NotApplicable,
                ownership_mode: OwnershipTarget::NotApplicable,
                timestamps: TimestampTargetCapability::NotApplicable,
            },
            policies: MetadataPolicies::default(),
            principal_mapper: None,
        })?;
        let identity = TransferIdentity::new("remote-expert-capacity-one")?;
        let cancel = CancellationToken::new();
        let (sender_transport, receiver_transport) = create_in_process_pair_with_capacity(1);

        let source_task = tokio::spawn(serve_remote_source(
            Arc::new(sender_transport),
            RemoteExpertSourceRequest::new(
                SessionNdx(4),
                identity.clone(),
                source,
                observation.clone(),
                InflightLimits::new(2, 256 * 1024, 2)?,
                cancel.clone(),
            )
            .with_source_qos(SourceQosGroup::new(SourceQosPolicy::new(None, 32 * 1024, None)?)),
        ));
        let destination_task = tokio::spawn(receive_remote_destination(
            Arc::new(receiver_transport),
            RemoteExpertDestinationRequest::new(
                SessionNdx(4),
                identity,
                observation,
                destination,
                StoragePath::new("copied.bin")?,
                InflightLimits::new(2, 64 * 1024, 2)?,
                cancel,
            )
            .with_recovery(Resumability::Disabled, None)
            .with_metadata_plan(metadata_plan),
        ));

        let source_report = source_task.await??;
        let destination_outcome = destination_task.await??;
        assert_eq!(source_report.transferred_bytes, payload.len() as u64);
        assert_eq!(
            source_report.source_qos.client_streamed_shaped_bytes,
            payload.len() as u64
        );
        assert!(source_report.source_qos.source_read_operations > 1);
        assert_eq!(destination_outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(destination_outcome.source_qos, source_report.source_qos);
        assert!(destination_outcome.metadata.as_ref().is_some_and(|report| {
            report
                .outcomes()
                .iter()
                .all(|item| item.outcome == ApplicationOutcome::OmittedByPolicy)
        }));
        assert_eq!(destination_outcome.blake3, *blake3::hash(&payload).as_bytes());
        assert_eq!(
            tokio::fs::read(destination_root.path().join("copied.bin")).await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn source_change_fails_both_processes_without_waiting_for_payload() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        tokio::fs::write(source_root.path().join("changed.bin"), b"advertised").await?;
        let source = local_storage(source_root.path(), "remote-changed-source").await?;
        let destination = local_storage(destination_root.path(), "remote-changed-destination").await?;
        let observation = observe(&source, "changed.bin").await?;
        tokio::fs::write(source_root.path().join("changed.bin"), b"changed-after-advertisement").await?;
        let identity = TransferIdentity::new("remote-source-changed")?;
        let limits = InflightLimits::new(1, 64 * 1024, 1)?;
        let cancel = CancellationToken::new();
        let (sender_transport, receiver_transport) = create_in_process_pair_with_capacity(1);
        let mut source_task = tokio::spawn(serve_remote_source(
            Arc::new(sender_transport),
            RemoteExpertSourceRequest::new(
                SessionNdx(8),
                identity.clone(),
                source,
                observation.clone(),
                limits,
                cancel.clone(),
            ),
        ));
        let mut destination_task = tokio::spawn(receive_remote_destination(
            Arc::new(receiver_transport),
            RemoteExpertDestinationRequest::new(
                SessionNdx(8),
                identity,
                observation,
                destination,
                StoragePath::new("must-not-exist.bin")?,
                limits,
                cancel,
            )
            .with_recovery(Resumability::Disabled, None),
        ));

        let completed = tokio::time::timeout(Duration::from_secs(2), async {
            let source_result = (&mut source_task).await;
            let destination_result = (&mut destination_task).await;
            (source_result, destination_result)
        })
        .await;
        let Ok((source_result, destination_result)) = completed else {
            source_task.abort();
            destination_task.abort();
            panic!("source failure did not terminate both remote halves")
        };
        assert!(source_result?.is_err());
        assert!(matches!(
            destination_result?,
            Err(crate::error::AppError::RemoteExpertPeerFailure { ndx: 8, .. })
        ));
        assert!(!destination_root.path().join("must-not-exist.bin").exists());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_is_a_typed_peer_terminal_and_never_publishes() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        tokio::fs::write(source_root.path().join("cancelled.bin"), vec![1; 128 * 1024]).await?;
        let source = local_storage(source_root.path(), "remote-cancel-source").await?;
        let destination = local_storage(destination_root.path(), "remote-cancel-destination").await?;
        let observation = observe(&source, "cancelled.bin").await?;
        let identity = TransferIdentity::new("remote-cancelled")?;
        let limits = InflightLimits::new(1, 64 * 1024, 1)?;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (sender_transport, receiver_transport) = create_in_process_pair_with_capacity(1);
        let source_task = tokio::spawn(serve_remote_source(
            Arc::new(sender_transport),
            RemoteExpertSourceRequest::new(
                SessionNdx(12),
                identity.clone(),
                source,
                observation.clone(),
                limits,
                cancel.clone(),
            ),
        ));
        let destination_task = tokio::spawn(receive_remote_destination(
            Arc::new(receiver_transport),
            RemoteExpertDestinationRequest::new(
                SessionNdx(12),
                identity,
                observation,
                destination,
                StoragePath::new("cancelled-output.bin")?,
                limits,
                cancel,
            )
            .with_recovery(Resumability::Disabled, None),
        ));

        let (source_result, destination_result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(source_task, destination_task)
        })
        .await?;
        assert!(source_result?.is_err());
        assert!(matches!(
            destination_result?,
            Err(crate::error::AppError::RemoteExpertPeerCancelled { ndx: 12 })
        ));
        assert!(!destination_root.path().join("cancelled-output.bin").exists());
        Ok(())
    }

    #[derive(Default)]
    struct RecoveryCounts {
        opened: AtomicUsize,
        registered: AtomicUsize,
        completed: AtomicUsize,
    }

    struct TestRecoveryRegistrar(Arc<RecoveryCounts>);

    #[async_trait::async_trait]
    impl RecoveryRegistrar for TestRecoveryRegistrar {
        async fn register(
            &self, _identity: data_mover::storage::RecoveryIdentity,
        ) -> std::result::Result<(), RecoveryRegistrationFailure> {
            self.0.registered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct TestRecoveryState {
        counts: Arc<RecoveryCounts>,
        completion_fails: bool,
    }

    #[async_trait::async_trait]
    impl EntryRecoveryState for TestRecoveryState {
        async fn open(&self, _entry: &ObservedEntry) -> crate::error::Result<RecoveryRegistration> {
            self.counts.opened.fetch_add(1, Ordering::SeqCst);
            let registrar: Arc<dyn RecoveryRegistrar> = Arc::new(TestRecoveryRegistrar(Arc::clone(&self.counts)));
            Ok(RecoveryRegistration::new(None, [7; 32], registrar))
        }

        async fn completed(&self, _entry: &ObservedEntry) -> crate::error::Result<()> {
            if self.completion_fails {
                return Err(crate::error::AppError::CheckpointError(
                    "injected durable completion failure".to_string(),
                ));
            }
            self.counts.completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn recoverable_remote_transfer_registers_before_payload_and_completes_after_publish() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        let payload = vec![0x6d; 192 * 1024 + 3];
        tokio::fs::write(source_root.path().join("recoverable.bin"), &payload).await?;
        let source = local_storage(source_root.path(), "remote-recovery-source").await?;
        let destination = local_storage(destination_root.path(), "remote-recovery-destination").await?;
        let observation = observe(&source, "recoverable.bin").await?;
        let identity = TransferIdentity::new("remote-recovery")?;
        let limits = InflightLimits::new(1, 64 * 1024, 1)?;
        let cancel = CancellationToken::new();
        let counts = Arc::new(RecoveryCounts::default());
        let recovery: Arc<dyn EntryRecoveryState> = Arc::new(TestRecoveryState {
            counts: Arc::clone(&counts),
            completion_fails: false,
        });
        let (sender_transport, receiver_transport) = create_in_process_pair_with_capacity(1);

        let source_task = tokio::spawn(serve_remote_source(
            Arc::new(sender_transport),
            RemoteExpertSourceRequest::new(
                SessionNdx(16),
                identity.clone(),
                source,
                observation.clone(),
                limits,
                cancel.clone(),
            ),
        ));
        let destination_task = tokio::spawn(receive_remote_destination(
            Arc::new(receiver_transport),
            RemoteExpertDestinationRequest::new(
                SessionNdx(16),
                identity,
                observation,
                destination,
                StoragePath::new("recovered.bin")?,
                limits,
                cancel,
            )
            .with_recovery(Resumability::Enabled, Some(recovery)),
        ));
        let (source_result, destination_result) = tokio::join!(source_task, destination_task);
        source_result??;
        destination_result??;

        assert_eq!(counts.opened.load(Ordering::SeqCst), 1);
        assert_eq!(counts.registered.load(Ordering::SeqCst), 1);
        assert_eq!(counts.completed.load(Ordering::SeqCst), 1);
        assert_eq!(
            tokio::fs::read(destination_root.path().join("recovered.bin")).await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn durable_completion_failure_reaches_source_after_publication() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        let payload = vec![0x2a; 128 * 1024 + 1];
        tokio::fs::write(source_root.path().join("completion.bin"), &payload).await?;
        let source = local_storage(source_root.path(), "remote-completion-source").await?;
        let destination = local_storage(destination_root.path(), "remote-completion-destination").await?;
        let observation = observe(&source, "completion.bin").await?;
        let identity = TransferIdentity::new("remote-completion-failure")?;
        let limits = InflightLimits::new(1, 64 * 1024, 1)?;
        let cancel = CancellationToken::new();
        let counts = Arc::new(RecoveryCounts::default());
        let recovery: Arc<dyn EntryRecoveryState> = Arc::new(TestRecoveryState {
            counts,
            completion_fails: true,
        });
        let (sender_transport, receiver_transport) = create_in_process_pair_with_capacity(1);
        let source_task = tokio::spawn(serve_remote_source(
            Arc::new(sender_transport),
            RemoteExpertSourceRequest::new(
                SessionNdx(18),
                identity.clone(),
                source,
                observation.clone(),
                limits,
                cancel.clone(),
            ),
        ));
        let destination_task = tokio::spawn(receive_remote_destination(
            Arc::new(receiver_transport),
            RemoteExpertDestinationRequest::new(
                SessionNdx(18),
                identity,
                observation,
                destination,
                StoragePath::new("published-but-unrecorded.bin")?,
                limits,
                cancel,
            )
            .with_recovery(Resumability::Enabled, Some(recovery)),
        ));

        let (source_result, destination_result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(source_task, destination_task)
        })
        .await?;
        let Err(source_error) = source_result? else {
            panic!("source must observe destination terminal failure")
        };
        let crate::error::AppError::RemoteExpertPeerFailure { failure, .. } = source_error else {
            panic!("source received the wrong terminal failure")
        };
        assert!(failure.final_destination_changed);
        assert!(destination_result?.is_err());
        assert_eq!(
            tokio::fs::read(destination_root.path().join("published-but-unrecorded.bin")).await?,
            payload
        );
        Ok(())
    }

    #[tokio::test]
    async fn mid_payload_peer_failure_retains_recoverable_destination_stage() -> TestResult {
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        let payload = vec![0x55; 128 * 1024 + 1];
        tokio::fs::write(source_root.path().join("interrupted.bin"), &payload).await?;
        let source = local_storage(source_root.path(), "remote-interrupted-source").await?;
        let destination = local_storage(destination_root.path(), "remote-interrupted-destination").await?;
        let observation = observe(&source, "interrupted.bin").await?;
        let counts = Arc::new(RecoveryCounts::default());
        let recovery: Arc<dyn EntryRecoveryState> = Arc::new(TestRecoveryState {
            counts,
            completion_fails: false,
        });
        let peer_failure = transport::message::RemoteTransferFailure {
            phase: transport::message::RemoteTransferPhase::Transfer,
            side: transport::message::RemoteTransferSide::Source,
            stage: transport::message::RemoteStageDisposition::None,
            final_destination_changed: false,
            source_qos: transport::message::SourceQosSnapshot::default(),
            diagnostic: "injected source read failure".to_string(),
        };
        let transport: Arc<dyn transport::traits::ReceiverTransport> = Arc::new(ScriptedReceiver {
            incoming: tokio::sync::Mutex::new(VecDeque::from([
                SenderMsg::RemoteSource(RemoteSourceEvent::Offer {
                    ndx: SessionNdx(22),
                    source_size: payload.len() as u64,
                    maximum_chunk_bytes: 64 * 1024,
                    identity_key: *observation.identity_key().as_bytes(),
                }),
                SenderMsg::RemoteSource(RemoteSourceEvent::Payload {
                    ndx: SessionNdx(22),
                    offset: 0,
                    data: Bytes::copy_from_slice(&payload[..64 * 1024]),
                }),
                SenderMsg::RemoteSource(RemoteSourceEvent::Failed {
                    ndx: SessionNdx(22),
                    failure: peer_failure,
                }),
            ])),
        });

        let result = receive_remote_destination(
            transport,
            RemoteExpertDestinationRequest::new(
                SessionNdx(22),
                TransferIdentity::new("remote-interrupted")?,
                observation,
                destination,
                StoragePath::new("interrupted-output.bin")?,
                InflightLimits::new(1, 64 * 1024, 1)?,
                CancellationToken::new(),
            )
            .with_recovery(Resumability::Enabled, Some(recovery)),
        )
        .await;
        let Err(crate::error::AppError::RemoteExpertPayloadInterrupted { peer, destination, .. }) = result else {
            panic!("mid-payload failure must retain both peer and destination errors")
        };
        assert!(matches!(*peer, crate::error::AppError::RemoteExpertPeerFailure { .. }));
        assert!(destination.has_recoverable_stage());
        assert!(!destination_root.path().join("interrupted-output.bin").exists());
        Ok(())
    }

    #[tokio::test]
    async fn real_quic_runs_the_same_local_expert_state_machine() -> TestResult {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let source_root = tempdir()?;
        let destination_root = tempdir()?;
        let payload = vec![0x3c; 160 * 1024 + 11];
        tokio::fs::write(source_root.path().join("quic.bin"), &payload).await?;
        let source = local_storage(source_root.path(), "remote-quic-source").await?;
        let destination = local_storage(destination_root.path(), "remote-quic-destination").await?;
        let observation = observe(&source, "quic.bin").await?;
        let snapshot = observation.encode_snapshot().as_bytes().to_vec();
        let identity = TransferIdentity::new("remote-real-quic")?;
        let limits = InflightLimits::new(2, 128 * 1024, 2)?;
        let cancel = CancellationToken::new();

        let (endpoint, cert) = quic::bind("[::1]:0".parse()?)?;
        let address = quic::receiver::local_addr(&endpoint)?;
        let accept = tokio::spawn(async move { quic::accept_connection(&endpoint).await });
        let sender = quic::connect(address, "localhost", Some(quic::CertificateDer::from(cert))).await?;
        sender.send(SenderMsg::Handshake(ProtocolHandshake::current())).await?;
        let receiver = accept.await??;
        let Some(SenderMsg::Handshake(remote_handshake)) = receiver.recv().await else {
            return Err("missing QUIC handshake".into());
        };
        receiver
            .send(ReceiverMsg::HandshakeAck(
                ProtocolHandshake::current().negotiate(&remote_handshake),
            ))
            .await?;
        assert!(matches!(
            sender.recv().await,
            Some(ReceiverMsg::HandshakeAck(HandshakeResult::Accepted { .. }))
        ));
        sender
            .send(SenderMsg::Advertisement(AdvertisementEvent::Page(ObservationPage {
                sequence: 0,
                entries: vec![AdvertisedObservation {
                    ndx: SessionNdx(20),
                    snapshot,
                }],
            })))
            .await?;
        sender
            .send(SenderMsg::Advertisement(AdvertisementEvent::Terminal(
                AdvertisementTerminal::Completed {
                    observed_entries: 1,
                    entry_failures: 0,
                },
            )))
            .await?;
        assert!(matches!(
            receiver.recv().await,
            Some(SenderMsg::Advertisement(AdvertisementEvent::Page(_)))
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(SenderMsg::Advertisement(AdvertisementEvent::Terminal(
                AdvertisementTerminal::Completed { .. }
            )))
        ));

        let sender: Arc<dyn transport::traits::SenderTransport> = Arc::new(sender);
        let receiver: Arc<dyn transport::traits::ReceiverTransport> = Arc::new(receiver);
        let source_task = tokio::spawn(serve_remote_source(
            Arc::clone(&sender),
            RemoteExpertSourceRequest::new(
                SessionNdx(20),
                identity.clone(),
                source,
                observation.clone(),
                limits,
                cancel.clone(),
            ),
        ));
        let destination_task = tokio::spawn(receive_remote_destination(
            Arc::clone(&receiver),
            RemoteExpertDestinationRequest::new(
                SessionNdx(20),
                identity,
                observation,
                destination,
                StoragePath::new("quic-output.bin")?,
                limits,
                cancel,
            )
            .with_recovery(Resumability::Disabled, None),
        ));
        source_task.await??;
        destination_task.await??;
        assert_eq!(
            tokio::fs::read(destination_root.path().join("quic-output.bin")).await?,
            payload
        );
        sender.close().await?;
        receiver.close().await?;
        Ok(())
    }
}
