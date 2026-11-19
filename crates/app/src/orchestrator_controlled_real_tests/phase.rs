//! 只在实际角色调用前握手，身份和错误由原始存储角色返回。
use std::{
    fmt::Write,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use data_mover::{
    model::{
        EntryIdentityKey, MetadataCapabilities, MetadataObservations, ObservationPlan, PermissionFacts, SourceIdentity,
        SourceVersion, StoragePath, StorageTimestamp, SymlinkTarget,
    },
    storage::{
        CopiedMetadataObservation, CreateSymlinkRequest, ListRequest, ListingStream, Metadata, MetadataBatchFailure,
        MetadataMutation, Namespace, SourceDescriptor, StorageBuilder, StorageRoleFailure,
    },
};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{Case, TestResult};
use crate::storage_factory::StorageContext;

#[derive(Clone)]
pub(super) struct Control {
    pub(super) address: SocketAddr,
    pub(super) run: String,
    pub(super) token: String,
    pub(super) case: Case,
}

impl Control {
    pub(super) async fn phase(&self, phase: &str, fresh_call: usize) -> TestResult {
        assert!(self.address.ip().is_loopback(), "test control must stay on loopback");
        let body = serde_json::to_vec(&serde_json::json!({
            "run": self.run, "case": self.case.name(), "phase": phase,
            "path": "dir", "fresh": true, "fresh_call": fresh_call
        }))?;
        timeout(Duration::from_secs(10), async {
            let mut stream = TcpStream::connect(self.address).await?;
            stream.write_all(format!(
                "POST /phase HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nX-Run-Token: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                self.address, self.token, body.len()
            ).as_bytes()).await?;
            stream.write_all(&body).await?;
            let mut response = Vec::new();
            stream.take(65_537).read_to_end(&mut response).await?;
            assert!(response.len() <= 65_536, "control response exceeded its bound");
            assert!(response.starts_with(b"HTTP/1.1 204 ") || response.starts_with(b"HTTP/1.0 204 "), "owned control did not confirm the phase");
            TestResult::Ok(())
        }).await?
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct StatTrace {
    pub(super) fresh: bool,
    pub(super) fresh_call: usize,
    pub(super) identity: Option<String>,
    pub(super) mode: Option<u32>,
    pub(super) modified_unix_nanos: Option<i128>,
    pub(super) failure: Option<String>,
}

#[derive(Default)]
pub(super) struct PhaseEvidence {
    pub(super) calls: Mutex<Vec<StatTrace>>,
    pub(super) confirmed: AtomicBool,
    pub(super) bindings: AtomicUsize,
    fresh_phases: AtomicUsize,
}

impl PhaseEvidence {
    async fn fresh_phase(&self, point: Case, control: Option<&Control>) -> usize {
        let ordinal = self.fresh_phases.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(control) = control
            && control.case == point
            && control.case.trigger() == Some(ordinal)
        {
            // 控制异常是测试故障，不能伪造成触发预期拒绝的存储错误。
            control.phase(control.case.phase(), ordinal).await.unwrap();
            self.confirmed.store(true, Ordering::SeqCst);
        }
        ordinal
    }
}

struct PhaseNamespace {
    inner: Arc<dyn Namespace>,
    control: Option<Control>,
    evidence: Arc<PhaseEvidence>,
}

#[async_trait]
impl Namespace for PhaseNamespace {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
        let watched = path.as_str() == "dir";
        let ordinal = if fresh && watched {
            self.evidence.fresh_phase(Case::TargetPost, self.control.as_ref()).await
        } else {
            0
        };
        let result = self.inner.stat(path, fresh).await;
        if watched {
            let mut calls = self.evidence.calls.lock().unwrap();
            assert!(calls.len() < 32, "owned fixture exceeded stat evidence bound");
            calls.push(StatTrace {
                fresh,
                fresh_call: ordinal,
                identity: result.as_ref().ok().map(|value| {
                    value.source_identity.identity_key().as_bytes().iter().fold(
                        String::with_capacity(64),
                        |mut text, byte| {
                            write!(text, "{byte:02x}").unwrap();
                            text
                        },
                    )
                }),
                mode: result
                    .as_ref()
                    .ok()
                    .and_then(|value| value.inline_mode)
                    .map(|mode| mode & 0o7777),
                modified_unix_nanos: result
                    .as_ref()
                    .ok()
                    .and_then(|value| value.inline_timestamps)
                    .and_then(|value| value.modified)
                    .map(StorageTimestamp::unix_nanos),
                failure: result.as_ref().err().map(ToString::to_string),
            });
        }
        result
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.inner.create_directory(path).await
    }
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
    async fn create_symlink(&self, request: CreateSymlinkRequest) -> Result<(), StorageRoleFailure> {
        self.inner.create_symlink(request).await
    }
    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.inner.delete(path).await
    }
    async fn persist_observed(
        &self, expected: &SourceDescriptor, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.inner.persist_observed(expected, cancel).await
    }
}

struct PhaseMetadata {
    inner: Arc<dyn Metadata>,
    control: Option<Control>,
    evidence: Arc<PhaseEvidence>,
}

#[async_trait]
impl Metadata for PhaseMetadata {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }

    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        self.inner.copied_metadata_observation_plan()
    }

    fn observe_listed(
        &self, descriptor: &SourceDescriptor, plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        self.inner.observe_listed(descriptor, plan)
    }

    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.inner.observe(path, plan).await
    }

    async fn observe_copy_bound_version(
        &self, path: &StoragePath, expected: &SourceIdentity, version: &SourceVersion, plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        self.inner
            .observe_copy_bound_version(path, expected, version, plan)
            .await
    }

    async fn permissions(&self, descriptor: &SourceDescriptor) -> Result<PermissionFacts, StorageRoleFailure> {
        self.inner.permissions(descriptor).await
    }

    async fn apply(
        &self, path: &StoragePath, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.inner.apply(path, mutation, cancel).await
    }

    fn supports_bound_directory_metadata(&self) -> bool {
        self.inner.supports_bound_directory_metadata()
    }

    async fn apply_bound_directory(
        &self, path: &StoragePath, identity: EntryIdentityKey, mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        if path.as_str() == "dir" {
            // 阶段 2 是底层绑定目录对象；前后阶段仍记录真实 namespace stat。
            self.evidence.bindings.fetch_add(1, Ordering::SeqCst);
            self.evidence.fresh_phase(Case::TargetPre, self.control.as_ref()).await;
        }
        self.inner
            .apply_bound_directory(path, identity, mutations, cancel)
            .await
    }
}

pub(super) fn install(context: &mut StorageContext, control: Option<Control>) -> TestResult<Arc<PhaseEvidence>> {
    let evidence = Arc::new(PhaseEvidence::default());
    let roles = context.storage();
    context.roles = StorageBuilder::new(roles.identity().clone(), roles.capabilities().clone())
        .read_source(roles.read_source()?)
        .metadata(Arc::new(PhaseMetadata {
            inner: roles.metadata()?,
            control: control.clone(),
            evidence: evidence.clone(),
        }))
        .staged_destination(roles.staged_destination()?)
        .namespace(Arc::new(PhaseNamespace {
            inner: roles.namespace()?,
            control,
            evidence: evidence.clone(),
        }))
        .build()?;
    Ok(evidence)
}
