//! Checkpoint测试的真实Local角色委托；不制造stage、恢复事实或持久化前缀。
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use data_mover::model::StoragePath;
use data_mover::storage::{
    ByteStream, CheckpointObservation, CopiedMetadataTarget, DestinationPrepareRequest, MetadataBatchFailure,
    MetadataMutation, PositionedByteStream, PrepareFact, PreparedStage, PublicationEvidence, PublicationFailure,
    PublishRequest, ReadBackMethod, StagedDestination, StorageBuilder, StorageRoleFailure, UploadDigest,
    VerificationEvidence, VerificationPoint, VerifyRequest, WriteEvidence,
};
use futures::StreamExt;
use serde::Serialize;
use tokio::time::{Instant, interval, timeout};
use tokio_util::sync::CancellationToken;

use super::{HOLD_AT, TestResult};
use crate::storage_factory::StorageContext;

#[derive(Clone, Debug, Serialize)]
pub(super) struct PrepareSeen {
    pub(super) offset: u64,
    pub(super) restored: bool,
    pub(super) resumed: bool,
    pub(super) reused_bytes: u64,
    pub(super) fact: String,
    pub(super) transfer_identity: [u8; 32],
    pub(super) recovery_binding: [u8; 32],
}

#[derive(Default)]
pub(super) struct Facts {
    pub(super) prepares: Mutex<Vec<PrepareSeen>>,
    pub(super) actual_forwarded_bytes: AtomicU64,
    pub(super) durable_prefix: AtomicU64,
    pub(super) input_held: AtomicBool,
    pub(super) checkpointed_writes: AtomicUsize,
    pub(super) cancellation_millis: AtomicU64,
    pub(super) advertised_interval: Option<u64>,
}

struct DestinationProbe {
    inner: Arc<dyn StagedDestination>,
    facts: Arc<Facts>,
    interrupt: bool,
    cancel: CancellationToken,
}

impl DestinationProbe {
    async fn interrupt_after_durable_pointer(&self, stage: &PreparedStage) -> TestResult {
        let started = Instant::now();
        timeout(Duration::from_secs(30), async {
            let mut poll = interval(Duration::from_millis(5));
            loop {
                poll.tick().await;
                if !self.facts.input_held.load(Ordering::SeqCst) || !stage.recovery_enabled() {
                    continue;
                }
                // observe_checkpoint真实重开pointer并验证绑定/长度，不能用内存prefix代替。
                let observed = self.inner.observe_checkpoint(stage).await?;
                if observed.durable_prefix == 0 {
                    continue;
                }
                assert!(observed.durable_prefix < stage.checkpoint_size());
                self.facts
                    .durable_prefix
                    .store(observed.durable_prefix, Ordering::SeqCst);
                self.facts
                    .cancellation_millis
                    .store(started.elapsed().as_millis() as u64, Ordering::SeqCst);
                self.cancel.cancel();
                return TestResult::Ok(());
            }
        })
        .await?
    }
}

#[async_trait]
impl StagedDestination for DestinationProbe {
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        self.inner.automatic_checkpoint_interval_bytes()
    }
    fn resumable_at_destination(&self) -> bool {
        self.inner.resumable_at_destination()
    }
    fn supports_positioned_write(&self) -> bool {
        self.inner.supports_positioned_write()
    }
    fn supports_hash_checkpoint(&self) -> bool {
        self.inner.supports_hash_checkpoint()
    }
    fn stage_supports_hash_checkpoint(&self, stage: &PreparedStage) -> bool {
        self.inner.stage_supports_hash_checkpoint(stage)
    }
    fn requires_hash_intent(&self) -> bool {
        self.inner.requires_hash_intent()
    }
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        self.inner.copied_metadata_target()
    }
    fn may_set_owner(&self, uid: u32, gid: u32) -> bool {
        self.inner.may_set_owner(uid, gid)
    }
    fn final_refusal(&self, path: &StoragePath, size: Option<u64>) -> Option<String> {
        self.inner.final_refusal(path, size)
    }
    fn verification_point(&self, stage: &PreparedStage) -> VerificationPoint {
        self.inner.verification_point(stage)
    }
    fn read_back_method(&self, stage: &PreparedStage) -> ReadBackMethod {
        self.inner.read_back_method(stage)
    }
    fn upload_digest(&self, stage: &PreparedStage) -> UploadDigest {
        self.inner.upload_digest(stage)
    }

    async fn prepare_at_destination(
        &self, request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let transfer_identity = request.transfer_identity;
        let recovery_binding = request.prepare.recovery_binding;
        let stage = self.inner.prepare_at_destination(request).await?;
        let mut seen = self.facts.prepares.lock().unwrap();
        assert!(seen.len() < 8);
        seen.push(PrepareSeen {
            offset: stage.write_offset(),
            restored: stage.restored_checkpoint().is_some(),
            resumed: matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }),
            reused_bytes: stage.prepare_fact().reused_bytes(),
            fact: format!("{:?}", stage.prepare_fact()),
            transfer_identity,
            recovery_binding,
        });
        Ok(stage)
    }
    async fn write_checkpointed(
        &self, stage: &PreparedStage, input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.facts.checkpointed_writes.fetch_add(1, Ordering::SeqCst);
        let facts = self.facts.clone();
        let interrupt = self.interrupt;
        let cancel = self.cancel.clone();
        let input = Box::pin(input.then(move |item| {
            let facts = facts.clone();
            let cancel = cancel.clone();
            async move {
                if interrupt && item.as_ref().is_ok_and(|chunk| chunk.offset >= HOLD_AT) {
                    facts.input_held.store(true, Ordering::SeqCst);
                    cancel.cancelled().await;
                }
                if let Ok(chunk) = &item {
                    facts
                        .actual_forwarded_bytes
                        .fetch_add(chunk.data.len() as u64, Ordering::SeqCst);
                }
                item
            }
        }));
        let writing = self.inner.write_checkpointed(stage, input);
        if self.interrupt {
            let (written, confirmed) = tokio::join!(writing, self.interrupt_after_durable_pointer(stage));
            confirmed.unwrap();
            written
        } else {
            writing.await
        }
    }
    async fn write_positioned(
        &self, stage: &PreparedStage, input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.inner.write_positioned(stage, input).await
    }
    async fn write(&self, stage: &PreparedStage, input: ByteStream) -> Result<WriteEvidence, StorageRoleFailure> {
        self.inner.write(stage, input).await
    }
    async fn observe_checkpoint(&self, stage: &PreparedStage) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.inner.observe_checkpoint(stage).await
    }
    async fn persist_hash_checkpoint(&self, stage: &PreparedStage) -> Result<(), StorageRoleFailure> {
        self.inner.persist_hash_checkpoint(stage).await
    }
    async fn release_hash_checkpoint(&self, stage: &PreparedStage) -> Result<(), StorageRoleFailure> {
        self.inner.release_hash_checkpoint(stage).await
    }
    async fn verify(
        &self, stage: &PreparedStage, request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        self.inner.verify(stage, request).await
    }
    async fn apply_metadata(
        &self, stage: &PreparedStage, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.inner.apply_metadata(stage, mutation, cancel).await
    }
    async fn apply_metadata_batch(
        &self, stage: &PreparedStage, mutations: Vec<MetadataMutation>, cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        self.inner.apply_metadata_batch(stage, mutations, cancel).await
    }
    async fn validate_recovered_metadata(
        &self, stage: &PreparedStage, mutations: Vec<MetadataMutation>, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.inner.validate_recovered_metadata(stage, mutations, cancel).await
    }
    async fn publish(
        &self, stage: &PreparedStage, request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        self.inner.publish(stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.inner.discard(stage).await
    }
}

pub(super) fn install(
    context: &mut StorageContext, cancel: CancellationToken, interrupt: bool,
) -> TestResult<Arc<Facts>> {
    let roles = context.storage();
    let inner = roles.staged_destination()?;
    let facts = Arc::new(Facts {
        advertised_interval: inner.automatic_checkpoint_interval_bytes(),
        ..Facts::default()
    });
    context.roles = StorageBuilder::new(roles.identity().clone(), roles.capabilities().clone())
        .read_source(roles.read_source()?)
        .namespace(roles.namespace()?)
        .metadata(roles.metadata()?)
        .staged_destination(Arc::new(DestinationProbe {
            inner,
            facts: facts.clone(),
            interrupt,
            cancel,
        }))
        .build()?;
    Ok(facts)
}
