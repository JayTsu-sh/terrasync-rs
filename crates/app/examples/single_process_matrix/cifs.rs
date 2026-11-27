//! 私有 FAS2750 实验目录的所有权校验和清理策略。

use std::env;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use data_mover::model::{EntryKind, EntryOperationFailure, FailureClass, ModelValueError, StoragePath};
use data_mover::storage::{
    BackendConfig, BackendConnectError, CapabilityUnavailable, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy,
    CreateDirectoryAllFailure, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeTerminalFailure,
    Namespace, Storage, StorageRoleFailure, connect_backend, create_directory_all, delete_tree,
};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::diagnostic::{Cause, emit};

const SHARE: &str = "terrasync_nightly";
const USERNAME: &str = r"LIZYAD\tsnightly";
const MATRIX_DIR: &str = "single-process-matrix";

#[derive(Debug, Error)]
pub(super) enum CifsError {
    #[error("CIFS matrix requires the dedicated Nightly account and a nonblank password")]
    Credentials,
    #[error("invalid CIFS matrix scope, cell path or endpoint role")]
    InvalidScope,
    #[error("CIFS matrix ownership receipt is missing or invalid")]
    Receipt,
    #[error("CIFS run ownership could not be verified")]
    Ownership,
    #[error("the exclusive CIFS run scope create has an unresolved outcome")]
    ClaimOutcomeUnknown,
    #[error("CIFS run scope still exists after cleanup")]
    ScopeRemaining,
    #[error("CIFS operation cancelled")]
    Cancelled,
    #[error("CIFS deletion returned inconsistent completion evidence")]
    Incomplete,
    #[error("CIFS ownership receipt encoding failed")]
    ReceiptJson(#[from] serde_json::Error),
    #[error("CIFS ownership receipt I/O failed")]
    ReceiptIo(#[from] io::Error),
    #[error(transparent)]
    Path(#[from] ModelValueError),
    #[error(transparent)]
    Connect(#[from] BackendConnectError),
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error(transparent)]
    Role(#[from] StorageRoleFailure),
    #[error(transparent)]
    CreateCell(#[from] CreateDirectoryAllFailure),
    #[error(transparent)]
    DeleteEntry(#[from] EntryOperationFailure),
    #[error(transparent)]
    DeleteTerminal(#[from] DeleteTreeTerminalFailure),
}

impl CifsError {
    fn cause(&self) -> Cause {
        match self {
            Self::Connect(error) => Cause::connect(error, true),
            Self::Role(error) | Self::CreateCell(CreateDirectoryAllFailure::Role(error)) => Cause::role(error),
            Self::DeleteEntry(error) => Cause::entry(error),
            Self::DeleteTerminal(DeleteTreeTerminalFailure::Session(error)) => Cause::session(error),
            Self::Credentials => Cause::class(FailureClass::Authentication),
            Self::InvalidScope | Self::Path(_) => Cause::class(FailureClass::InvalidInput),
            Self::Ownership | Self::ScopeRemaining => Cause::class(FailureClass::Conflict),
            Self::ClaimOutcomeUnknown => Cause::unknown_outcome(FailureClass::Conflict),
            Self::Cancelled => Cause::class(FailureClass::Cancelled),
            _ => Cause::class(FailureClass::Internal),
        }
    }
}

#[derive(Debug, Error)]
#[error("CIFS matrix {phase} failed")]
pub(super) struct CifsFailure {
    phase: &'static str,
    #[source]
    source: CifsError,
}

fn at<T, E: Into<CifsError>>(phase: &'static str, result: Result<T, E>) -> Result<T, CifsFailure> {
    result.map_err(|error| CifsFailure {
        phase,
        source: error.into(),
    })
}

#[derive(Clone, Copy)]
enum Role {
    Source,
    Destination,
}

impl Role {
    fn parse(value: &str) -> Result<Self, CifsError> {
        match value {
            "source" => Ok(Self::Source),
            "destination" => Ok(Self::Destination),
            _ => Err(CifsError::InvalidScope),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }

    fn server(self) -> &'static str {
        match self {
            Self::Source => "10.128.61.200",
            Self::Destination => "10.128.61.201",
        }
    }

    fn config(self, root: Option<String>, username: String, password: String) -> Result<CifsBackendConfig, CifsError> {
        if !username.eq_ignore_ascii_case(USERNAME) || password.trim().is_empty() {
            return Err(CifsError::Credentials);
        }
        Ok(CifsBackendConfig {
            server: self.server().into(),
            share: SHARE.into(),
            root,
            username,
            password,
            ensure_dir: false,
            signing_policy: CifsSigningPolicy::Required,
            guest_policy: CifsGuestPolicy::Deny,
        })
    }

    async fn connect(self, root: Option<String>) -> Result<Storage, CifsError> {
        let username = env::var("LAB_CIFS_NIGHTLY_USERNAME").map_err(|_| CifsError::Credentials)?;
        let password = env::var("LAB_CIFS_NIGHTLY_PASSWORD").map_err(|_| CifsError::Credentials)?;
        Ok(connect_backend(BackendConfig::Cifs(self.config(root, username, password)?)).await?)
    }
}

fn nonce(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_run_id(run_id: &str) -> Result<(), CifsError> {
    let suffix = run_id
        .strip_prefix("nightly-")
        .or_else(|| run_id.strip_prefix("release-"));
    if suffix.is_some_and(|value| {
        (1..=80).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }) {
        Ok(())
    } else {
        Err(CifsError::InvalidScope)
    }
}

fn cell_path(role: Role, cell: &str) -> Result<StoragePath, CifsError> {
    let (source, destination) = cell.split_once("__").ok_or(CifsError::InvalidScope)?;
    let profile = |name: &str| {
        (1..=40).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            && !name.contains("__")
    };
    let endpoint = match role {
        Role::Source => source,
        Role::Destination => destination,
    };
    if endpoint != "cifs_fas2750" || !profile(source) || !profile(destination) {
        return Err(CifsError::InvalidScope);
    }
    Ok(StoragePath::new(format!("{MATRIX_DIR}/{cell}/{}", role.name()))?)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum Phase {
    Prepared,
    // 旧回执在发出 CREATE_NEW 前后都可能停留在 Planned。
    #[serde(alias = "Planned")]
    Creating,
    Created,
    Claimed,
    Ready,
    Finalizing,
}

#[derive(Deserialize, Serialize)]
struct OwnedScope {
    run_id: String,
    scope: String,
    token: String,
    phase: Phase,
}

impl OwnedScope {
    fn prepare(run_id: &str, path: &Path) -> Result<(), CifsError> {
        validate_run_id(run_id)?;
        Self {
            run_id: run_id.to_owned(),
            scope: format!("ts-matrix-{}", Uuid::new_v4().simple()),
            token: Uuid::new_v4().simple().to_string(),
            phase: Phase::Prepared,
        }
        .persist(path, false)
    }

    fn from_receipt(run_id: &str, path: &Path) -> Result<Self, CifsError> {
        validate_run_id(run_id)?;
        let owned: Self = serde_json::from_reader(fs::File::open(path)?)?;
        if owned.run_id != run_id || !owned.scope.strip_prefix("ts-matrix-").is_some_and(nonce) || !nonce(&owned.token)
        {
            return Err(CifsError::Receipt);
        }
        Ok(owned)
    }

    fn persist(&self, path: &Path, replace: bool) -> Result<(), CifsError> {
        let parent = path.parent().ok_or(CifsError::Receipt)?;
        let mut file = NamedTempFile::new_in(parent)?;
        serde_json::to_writer(file.as_file_mut(), self)?;
        file.as_file().sync_all()?;
        if replace {
            file.persist(path)
        } else {
            file.persist_noclobber(path)
        }
        .map_err(|error| error.error)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn advance(&mut self, phase: Phase, path: &Path) -> Result<(), CifsFailure> {
        self.phase = phase;
        at("persist_receipt", self.persist(path, true))
    }

    fn marker_name(&self) -> String {
        format!(".owner-{}", self.token)
    }

    fn paths(&self) -> Result<(StoragePath, StoragePath), CifsFailure> {
        let scope = at("load_receipt", StoragePath::new(&self.scope))?;
        let marker = at(
            "load_receipt",
            StoragePath::new(format!("{scope}/{}", self.marker_name())),
        )?;
        Ok((scope, marker))
    }

    async fn claim(&mut self, namespace: &dyn Namespace, receipt: &Path) -> Result<(), CifsFailure> {
        at(
            "claim_scope",
            (self.phase == Phase::Prepared).then_some(()).ok_or(CifsError::Receipt),
        )?;
        let (scope, marker) = self.paths()?;
        // 请求前持久化意图。只有 CREATE_NEW 的明确成功才能授权创建标记。
        self.advance(Phase::Creating, receipt)?;
        at("create_scope", namespace.create_directory(&scope).await)?;
        self.advance(Phase::Created, receipt)?;
        at("create_marker", namespace.create_directory(&marker).await)?;
        self.advance(Phase::Claimed, receipt)
    }

    fn check_claim_outcome(&self) -> Result<(), CifsFailure> {
        if self.phase == Phase::Creating {
            // 即使此刻查不到目录，服务端也可能稍后提交未确认的创建。
            return at("check_ownership", Err(CifsError::ClaimOutcomeUnknown));
        }
        Ok(())
    }

    async fn confirm_absence(&self, storage: &Storage) -> Result<(), CifsFailure> {
        self.check_claim_outcome()?;
        let namespace = at("confirm_absence", storage.namespace())?;
        let (scope, _) = self.paths()?;
        if at("confirm_absence", directory_exists(namespace.as_ref(), &scope).await)? {
            return at("confirm_absence", Err(CifsError::ScopeRemaining));
        }
        Ok(())
    }

    async fn cleanup(&mut self, storage: &Storage, receipt: &Path) -> Result<(), CifsFailure> {
        self.check_claim_outcome()?;
        let namespace = at("observe_scope", storage.namespace())?;
        let (scope, marker) = self.paths()?;
        if !at("observe_scope", directory_exists(namespace.as_ref(), &scope).await)? {
            return Ok(());
        }
        // 已存在的目录必须有独占创建的确认记录。未知创建结果不能授权删除。
        at(
            "check_ownership",
            (self.phase != Phase::Prepared)
                .then_some(())
                .ok_or(CifsError::Ownership),
        )?;
        let marker_present = at("observe_marker", directory_exists(namespace.as_ref(), &marker).await)?;
        if !marker_present && !matches!(self.phase, Phase::Created | Phase::Finalizing) {
            return at("check_ownership", Err(CifsError::Ownership));
        }
        let data = at("observe_data", StoragePath::new(format!("{scope}/{MATRIX_DIR}")))?;
        if at("observe_data", directory_exists(namespace.as_ref(), &data).await)? {
            // 只有两端已验证的范围及仍存在的标记才能授权递归删除。
            at(
                "check_ownership",
                (self.phase == Phase::Ready && marker_present)
                    .then_some(())
                    .ok_or(CifsError::Ownership),
            )?;
            at("delete_data", delete_data(storage, data.clone()).await)?;
            if at("observe_data", directory_exists(namespace.as_ref(), &data).await)? {
                return at("observe_data", Err(CifsError::ScopeRemaining));
            }
        }
        // 两端指向同一专用卷。保存删除标记的授权后，才允许删除标记。
        // Finalizing 只允许重试删除标记及根目录，不允许重新删除数据树。
        if self.phase != Phase::Finalizing {
            self.advance(Phase::Finalizing, receipt)?;
        }
        if marker_present {
            at("delete_marker", namespace.delete(&marker).await)?;
        }
        at("delete_scope", namespace.delete(&scope).await)?;
        self.confirm_absence(storage).await
    }
}

async fn verify_marker(namespace: &dyn Namespace, marker: &StoragePath) -> Result<(), CifsError> {
    if directory_exists(namespace, marker).await? {
        Ok(())
    } else {
        Err(CifsError::Ownership)
    }
}

async fn directory_exists(namespace: &dyn Namespace, path: &StoragePath) -> Result<bool, CifsError> {
    match namespace.stat(path, true).await {
        Ok(entry) if entry.kind == EntryKind::Directory => Ok(true),
        Ok(_) => Err(CifsError::Ownership),
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn delete_data(storage: &Storage, root: StoragePath) -> Result<(), CifsError> {
    let limit = NonZeroUsize::new(4).ok_or(CifsError::Incomplete)?;
    let mut deletion = delete_tree(
        storage,
        DeleteTreeRequest {
            root,
            delete_root: true,
            max_inflight_operations: limit,
            max_buffered_items: limit,
            cancel: CancellationToken::new(),
        },
    )?;
    let mut failure = None;
    while let Some(item) = deletion.next_item().await {
        if let DeleteTreeItem::EntryFailure(error) = item {
            failure.get_or_insert(error);
        }
    }
    match deletion.finish().await? {
        DeleteTreeOutcome::Cancelled => Err(CifsError::Cancelled),
        DeleteTreeOutcome::Completed(result) => match failure {
            Some(error) => Err(error.into()),
            None if result.entry_failures == 0 => Ok(()),
            None => Err(CifsError::Incomplete),
        },
    }
}

fn receipt_path() -> Result<PathBuf, CifsError> {
    env::var_os("TS_SINGLE_CIFS_RECEIPT")
        .map(PathBuf::from)
        .ok_or(CifsError::Receipt)
}

pub(super) fn diagnostic_server(profile: &str, role: &str) -> Option<&'static str> {
    if profile == "cifs_fas2750" {
        Role::parse(role).ok().map(Role::server)
    } else {
        None
    }
}

async fn prepare_cell(
    storage: &Storage, marker: &StoragePath, cell: &StoragePath, cancel: &CancellationToken,
) -> Result<(), CifsFailure> {
    let namespace = at("observe_marker", storage.namespace())?;
    tokio::select! {
        biased;
        () = cancel.cancelled() => return at("observe_marker", Err(CifsError::Cancelled)),
        result = verify_marker(namespace.as_ref(), marker) => at("observe_marker", result)?,
    }
    // 取消后不再推进逐级创建。在途请求仍可能提交，由已确认拥有的运行范围负责清理。
    if cancel.is_cancelled() {
        return at("observe_marker", Err(CifsError::Cancelled));
    }
    tokio::select! {
        biased;
        () = cancel.cancelled() => at("create_cell", Err(CifsError::Cancelled)),
        result = create_directory_all(storage, cell) => at("create_cell", result),
    }
}

pub(super) async fn connect_cell(role: &str, cell: &str, cancel: &CancellationToken) -> Result<Storage, CifsFailure> {
    let role = at("cell_path", Role::parse(role))?;
    let result = async {
        let cell = at("cell_path", cell_path(role, cell))?;
        let receipt = at("load_receipt", receipt_path())?;
        let run_id = at(
            "load_receipt",
            env::var("TS_SINGLE_MATRIX_RUN_ID").map_err(|_| CifsError::Receipt),
        )?;
        let owned = at("load_receipt", OwnedScope::from_receipt(&run_id, &receipt))?;
        at(
            "check_ownership",
            (owned.phase == Phase::Ready).then_some(()).ok_or(CifsError::Ownership),
        )?;
        let marker = at("load_receipt", StoragePath::new(owned.marker_name()))?;
        let storage = tokio::select! {
            biased;
            () = cancel.cancelled() => return at("connect", Err(CifsError::Cancelled)),
            result = role.connect(Some(owned.scope.clone())) => at("connect", result)?,
        };
        prepare_cell(&storage, &marker, &cell, cancel).await?;
        tokio::select! {
            biased;
            () = cancel.cancelled() => at("connect_cell", Err(CifsError::Cancelled)),
            result = role.connect(Some(format!("{}/{cell}", owned.scope))) => at("connect_cell", result),
        }
    }
    .await;
    report_failure(role, &result);
    result
}

fn report_failure<T>(role: Role, result: &Result<T, CifsFailure>) {
    if let Err(error) = result {
        let mutation = matches!(
            error.phase,
            "create_scope" | "create_marker" | "create_cell" | "delete_data" | "delete_marker" | "delete_scope"
        );
        emit(
            error.phase,
            role.name(),
            Some(role.server()),
            error.source.cause(),
            mutation,
        );
    }
}

pub(super) enum Action {
    Prepare,
    Claim,
    Verify,
    Delete,
    Confirm,
}

impl Action {
    pub(super) fn parse(command: &str) -> Option<Self> {
        match command {
            "--prepare-cifs-run" => Some(Self::Prepare),
            "--claim-cifs-run" => Some(Self::Claim),
            "--verify-cifs-run" => Some(Self::Verify),
            "--delete-cifs-run" => Some(Self::Delete),
            "--confirm-cifs-run" => Some(Self::Confirm),
            _ => None,
        }
    }

    pub(super) async fn run(self, run_id: &str, role: &str) -> Result<(), CifsFailure> {
        let role = at("endpoint", Role::parse(role))?;
        let result = self.execute(run_id, role).await;
        report_failure(role, &result);
        result
    }

    async fn execute(self, run_id: &str, role: Role) -> Result<(), CifsFailure> {
        let receipt = at("load_receipt", receipt_path())?;
        match (&self, role) {
            (Self::Prepare, Role::Source) => return at("persist_receipt", OwnedScope::prepare(run_id, &receipt)),
            (Self::Claim, Role::Source) | (Self::Verify, Role::Destination) | (Self::Delete | Self::Confirm, _) => (),
            _ => return at("endpoint", Err(CifsError::InvalidScope)),
        }
        let mut owned = at("load_receipt", OwnedScope::from_receipt(run_id, &receipt))?;
        if matches!(self, Self::Delete | Self::Confirm) {
            owned.check_claim_outcome()?;
        }
        let storage = at("connect", role.connect(None).await)?;
        match self {
            Self::Claim => {
                let namespace = at("claim_scope", storage.namespace())?;
                owned.claim(namespace.as_ref(), &receipt).await
            }
            Self::Verify => {
                at(
                    "check_ownership",
                    (owned.phase == Phase::Claimed)
                        .then_some(())
                        .ok_or(CifsError::Ownership),
                )?;
                let (_, marker) = owned.paths()?;
                let namespace = at("observe_marker", storage.namespace())?;
                at("observe_marker", verify_marker(namespace.as_ref(), &marker).await)?;
                owned.advance(Phase::Ready, &receipt)
            }
            Self::Delete => owned.cleanup(&storage, &receipt).await,
            Self::Confirm => owned.confirm_absence(&storage).await,
            Self::Prepare => at("endpoint", Err(CifsError::InvalidScope)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use data_mover::model::{BackendIdentity, BackendKind, Operation, Transience};
    use data_mover::storage::{
        BackendCapabilities, CapabilityAvailability, ListRequest, ListingStream, LocalBackendConfig, SourceDescriptor,
        StorageBuilder, UnsupportedReason,
    };
    use tempfile::tempdir;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
    const RUN_ID: &str = "nightly-100-1";

    #[test]
    fn only_the_dedicated_identity_and_share_are_configured() -> TestResult {
        for (role, server) in [(Role::Source, "10.128.61.200"), (Role::Destination, "10.128.61.201")] {
            let config = role.config(None, USERNAME.into(), "fixture-password".into())?;
            assert_eq!((config.server.as_str(), config.share.as_str()), (server, SHARE));
            assert!(!config.ensure_dir);
            assert_eq!(config.guest_policy, CifsGuestPolicy::Deny);
            assert_eq!(config.signing_policy, CifsSigningPolicy::Required);
            assert!(matches!(
                role.config(None, r"LIZYAD\peer".into(), "secret".into()),
                Err(CifsError::Credentials)
            ));
        }
        Ok(())
    }

    #[test]
    fn cell_paths_accept_new_profiles_without_allowing_path_injection() -> TestResult {
        assert_eq!(
            cell_path(Role::Source, "cifs_fas2750__future_backend")?.as_str(),
            "single-process-matrix/cifs_fas2750__future_backend/source",
        );
        assert_eq!(
            cell_path(Role::Destination, "cifs_fas2750__cifs_fas2750")?.as_str(),
            "single-process-matrix/cifs_fas2750__cifs_fas2750/destination",
        );
        for invalid in [
            "",
            "local__cifs_fas2750",
            "cifs_fas2750__../local",
            "cifs_fas2750__local%2foutside",
            "cifs_fas2750__local__local",
        ] {
            assert!(cell_path(Role::Source, invalid).is_err(), "{invalid}");
        }
        Ok(())
    }

    async fn local(root: &Path) -> TestResult<Storage> {
        Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            root: root.to_owned(),
            read_concurrency: NonZeroUsize::MIN,
            write_concurrency: NonZeroUsize::MIN,
        }))
        .await?)
    }

    fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
        StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Namespace,
                class,
                Transience::Unknown,
                "injected fixture failure",
            )
            .unwrap(),
        )
    }

    #[derive(Clone)]
    enum Fault {
        CreateScopeAck,
        CreateScopeDelayed,
        CreateMarkerAck,
        DeleteMarkerAck,
        DeleteScopeAck,
        ObserveMarkerDenied,
        CancelOnMarker(CancellationToken),
        CancelOnCreate(CancellationToken),
    }

    struct FaultyNamespace {
        inner: Arc<dyn Namespace>,
        fault: Fault,
    }

    fn marker(path: &StoragePath) -> bool {
        path.as_str()
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with(".owner-"))
    }

    #[async_trait]
    impl Namespace for FaultyNamespace {
        async fn stat(&self, path: &StoragePath, fresh: bool) -> Result<SourceDescriptor, StorageRoleFailure> {
            assert!(fresh);
            if matches!(self.fault, Fault::ObserveMarkerDenied) && marker(path) {
                return Err(failure(path, FailureClass::PermissionDenied));
            }
            let result = self.inner.stat(path, fresh).await;
            if let Fault::CancelOnMarker(cancel) = &self.fault {
                assert!(marker(path));
                cancel.cancel();
            }
            result
        }

        async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
            if matches!(self.fault, Fault::CreateScopeDelayed) {
                return Err(failure(path, FailureClass::Timeout));
            }
            self.inner.create_directory(path).await?;
            if let Fault::CancelOnCreate(cancel) = &self.fault {
                assert_eq!(path.as_str(), MATRIX_DIR);
                cancel.cancel();
                return std::future::pending().await;
            }
            if matches!(
                (&self.fault, marker(path)),
                (Fault::CreateScopeAck, false) | (Fault::CreateMarkerAck, true)
            ) {
                return Err(failure(path, FailureClass::Timeout));
            }
            Ok(())
        }

        fn list(&self, request: ListRequest) -> ListingStream {
            self.inner.list(request)
        }

        async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
            self.inner.delete(path).await?;
            if matches!(
                (&self.fault, path.as_str().contains('/'), marker(path)),
                (Fault::DeleteMarkerAck, true, true) | (Fault::DeleteScopeAck, false, false)
            ) {
                return Err(failure(path, FailureClass::Timeout));
            }
            Ok(())
        }
    }

    fn faulty(storage: &Storage, fault: Fault) -> TestResult<Storage> {
        let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused")?);
        Ok(StorageBuilder::new(
            BackendIdentity::new(BackendKind::Local, "cifs-matrix-test")?,
            BackendCapabilities::new(
                unavailable.clone(),
                unavailable.clone(),
                CapabilityAvailability::Supported,
                unavailable,
            ),
        )
        .namespace(Arc::new(FaultyNamespace {
            inner: storage.namespace()?,
            fault,
        }))
        .build()?)
    }

    async fn fixture() -> TestResult<(tempfile::TempDir, PathBuf, PathBuf, OwnedScope, Storage)> {
        let temp = tempdir()?;
        let receipt = temp.path().join("receipt");
        let share = temp.path().join("share");
        fs::create_dir(&share)?;
        OwnedScope::prepare(RUN_ID, &receipt)?;
        let owned = OwnedScope::from_receipt(RUN_ID, &receipt)?;
        let source = local(&share).await?;
        Ok((temp, receipt, share, owned, source))
    }

    #[tokio::test]
    async fn collision_and_unconfirmed_scope_create_never_authorize_recursive_delete() -> TestResult {
        for collide in [true, false] {
            let (_temp, receipt, share, mut owned, source) = fixture().await?;
            let root = share.join(&owned.scope);
            if collide {
                fs::create_dir(&root)?;
            }
            let source = faulty(&source, Fault::CreateScopeAck)?;
            let error = owned.claim(source.namespace()?.as_ref(), &receipt).await.unwrap_err();
            assert_eq!(error.phase, "create_scope");
            let mut reload = OwnedScope::from_receipt(RUN_ID, &receipt)?;
            assert_eq!(reload.phase, Phase::Creating);
            assert!(OwnedScope::from_receipt("release-100-1", &receipt).is_err());
            let sentinel = root.join(MATRIX_DIR).join("foreign-data");
            fs::create_dir_all(sentinel.parent().unwrap())?;
            fs::write(&sentinel, "peer data")?;
            assert!(matches!(
                reload.cleanup(&source, &receipt).await.unwrap_err().source,
                CifsError::ClaimOutcomeUnknown
            ));
            assert_eq!(fs::read_to_string(sentinel)?, "peer data");
        }
        Ok(())
    }

    #[tokio::test]
    async fn absent_scope_cannot_clear_an_unconfirmed_create_that_commits_later() -> TestResult {
        let (_temp, receipt, share, mut owned, source) = fixture().await?;
        let peer = local(&share).await?;
        owned.cleanup(&source, &receipt).await?;
        let fault = faulty(&source, Fault::CreateScopeDelayed)?;
        let error = owned.claim(fault.namespace()?.as_ref(), &receipt).await.unwrap_err();
        assert_eq!(error.phase, "create_scope");
        for endpoint in [&source, &peer] {
            let mut receipt_state = OwnedScope::from_receipt(RUN_ID, &receipt)?;
            assert_eq!(receipt_state.phase, Phase::Creating);
            assert!(!share.join(&owned.scope).exists());
            assert!(matches!(
                receipt_state.cleanup(endpoint, &receipt).await.unwrap_err().source,
                CifsError::ClaimOutcomeUnknown
            ));
            assert!(matches!(
                receipt_state.confirm_absence(endpoint).await.unwrap_err().source,
                CifsError::ClaimOutcomeUnknown
            ));
        }
        let late_scope = share.join(&owned.scope);
        fs::create_dir(&late_scope)?;
        assert!(owned.cleanup(&peer, &receipt).await.is_err());
        assert!(late_scope.exists());
        Ok(())
    }

    #[tokio::test]
    async fn cell_cancellation_stops_new_creates_and_keeps_inflight_outcomes_unknown() -> TestResult {
        for during_create in [false, true] {
            let (_temp, _receipt, share, _owned, storage) = fixture().await?;
            let marker = StoragePath::new(".owner-fixture")?;
            fs::create_dir(share.join(marker.as_str()))?;
            let cancel = CancellationToken::new();
            let fault = if during_create {
                Fault::CancelOnCreate(cancel.clone())
            } else {
                Fault::CancelOnMarker(cancel.clone())
            };
            let storage = faulty(&storage, fault)?;
            let cell = cell_path(Role::Source, "cifs_fas2750__local")?;
            let error = prepare_cell(&storage, &marker, &cell, &cancel).await.unwrap_err();
            assert!(matches!(error.source, CifsError::Cancelled));
            assert_eq!(
                error.phase,
                if during_create { "create_cell" } else { "observe_marker" }
            );
            assert_eq!(share.join(MATRIX_DIR).exists(), during_create);
            assert!(!share.join(cell.as_str()).exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn committed_marker_create_with_a_lost_reply_is_reconciled() -> TestResult {
        let (_temp, receipt, share, mut owned, source) = fixture().await?;
        let fault = faulty(&source, Fault::CreateMarkerAck)?;
        let error = owned.claim(fault.namespace()?.as_ref(), &receipt).await.unwrap_err();
        assert_eq!(error.phase, "create_marker");
        let mut reload = OwnedScope::from_receipt(RUN_ID, &receipt)?;
        assert_eq!(reload.phase, Phase::Created);
        reload.cleanup(&source, &receipt).await?;
        assert!(!share.join(&owned.scope).exists());
        Ok(())
    }

    #[tokio::test]
    async fn lost_marker_or_scope_delete_reply_is_recoverable_from_a_second_connection() -> TestResult {
        for (fault, phase) in [
            (Fault::DeleteMarkerAck, "delete_marker"),
            (Fault::DeleteScopeAck, "delete_scope"),
        ] {
            let (_temp, receipt, share, mut owned, source) = fixture().await?;
            owned.claim(source.namespace()?.as_ref(), &receipt).await?;
            let destination = local(&share).await?;
            let (_, marker) = owned.paths()?;
            verify_marker(destination.namespace()?.as_ref(), &marker).await?;
            owned.advance(Phase::Ready, &receipt)?;
            let data = share.join(&owned.scope).join(MATRIX_DIR).join("cell");
            fs::create_dir_all(&data)?;
            fs::write(data.join("payload"), "matrix")?;
            let sentinel = share.join("peer-file");
            fs::write(&sentinel, "peer data")?;
            let error = owned.cleanup(&faulty(&source, fault)?, &receipt).await.unwrap_err();
            assert_eq!(error.phase, phase);
            let mut reload = OwnedScope::from_receipt(RUN_ID, &receipt)?;
            assert_eq!(reload.phase, Phase::Finalizing);
            if phase == "delete_marker" {
                assert!(matches!(
                    reload.confirm_absence(&destination).await.unwrap_err().source,
                    CifsError::ScopeRemaining
                ));
                assert!(share.join(&owned.scope).exists());
            }
            reload.cleanup(&destination, &receipt).await?;
            reload.confirm_absence(&source).await?;
            assert!(!share.join(&owned.scope).exists());
            assert_eq!(fs::read_to_string(sentinel)?, "peer data");
            assert!(receipt.exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn ready_scope_with_missing_or_unreadable_marker_and_finalizing_with_data_fail_closed() -> TestResult {
        let (_temp, receipt, share, mut owned, source) = fixture().await?;
        owned.claim(source.namespace()?.as_ref(), &receipt).await?;
        owned.advance(Phase::Ready, &receipt)?;
        let (_, marker) = owned.paths()?;
        let sentinel = share.join(&owned.scope).join(MATRIX_DIR).join("sentinel");
        fs::create_dir_all(sentinel.parent().unwrap())?;
        fs::write(&sentinel, "peer data")?;
        let denied = faulty(&source, Fault::ObserveMarkerDenied)?;
        assert_eq!(
            owned.cleanup(&denied, &receipt).await.unwrap_err().phase,
            "observe_marker"
        );
        fs::remove_dir(share.join(marker.as_str()))?;
        assert_eq!(
            owned.cleanup(&source, &receipt).await.unwrap_err().phase,
            "check_ownership"
        );
        owned.advance(Phase::Finalizing, &receipt)?;
        assert_eq!(
            owned.cleanup(&source, &receipt).await.unwrap_err().phase,
            "check_ownership"
        );
        assert_eq!(fs::read_to_string(sentinel)?, "peer data");
        Ok(())
    }
}
