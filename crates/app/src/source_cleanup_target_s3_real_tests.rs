//! 已有目标对象的增量跳过项也须通过真实持久化确认，失败时保留 Local 源文件。

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{
    ListRequest, ListingStream, Namespace, NamespaceCapabilities, SourceDescriptor, StorageBuilder, StorageRoleFailure,
};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{SourceCleanupCheck, environment, remove_entry};
use crate::error::{AppError, Result, StorageError};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

struct ArmPersistence {
    inner: Arc<dyn Namespace>,
    control: PathBuf,
    fault: String,
    armed: AtomicBool,
}

#[async_trait]
impl Namespace for ArmPersistence {
    async fn stat(&self, path: &StoragePath, fresh: bool) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path, fresh).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    fn capabilities(&self) -> NamespaceCapabilities {
        self.inner.capabilities()
    }
    async fn persist_observed(
        &self, expected: &SourceDescriptor, cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        // 精确注入在最后的持久化 HEAD，不能让前面的属性校验替代本次验证。
        let first = !self.armed.swap(true, Ordering::SeqCst);
        if first {
            fs::write(&self.control, &self.fault).unwrap_or_else(|error| panic!("fixture fault control: {error}"));
        }
        if first && self.fault == "cancel" {
            let operation = self.inner.persist_observed(expected, cancel.clone());
            tokio::pin!(operation);
            tokio::select! {
                result = &mut operation => result,
                () = sleep(Duration::from_millis(250)) => {
                    cancel.cancel();
                    operation.await
                }
            }
        } else {
            self.inner.persist_observed(expected, cancel).await
        }
    }
}

fn class(error: &StorageRoleFailure) -> FailureClass {
    match error {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    }
}

#[tokio::test]
#[ignore = "requires owned RustFS fixture with LAB_S3_PURGE_TEST_BINARY and TEST_NAME"]
async fn real_target_s3_persistence_gates_unchanged_source_removal() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("payload"), b"verified payload")?;
    let source = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(&environment("LAB_S3_PURGE_URL")?, CreateStorageOptions::default()).await?;
    let control = PathBuf::from(environment("LAB_S3_PURGE_CONTROL")?);
    let fault = environment("LAB_S3_PURGE_FAULT")?;
    let armed = Arc::new(ArmPersistence {
        inner: target.roles.namespace()?,
        control: control.clone(),
        fault: fault.clone(),
        armed: AtomicBool::new(false),
    });
    target.roles = StorageBuilder::new(target.roles.identity().clone(), target.roles.capabilities().clone())
        .namespace(armed.clone())
        .metadata(target.roles.metadata()?)
        .read_source(target.roles.read_source()?)
        .staged_destination(target.roles.staged_destination()?)
        .build()
        .map_err(|error| AppError::ConfigError(format!("owned S3 test adapter invalid: {error}")))?;
    let entry = observe_path(source.storage(), Path::new("payload")).await?;
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new());
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(8),
        remove_entry(&source, &target, &entry, &check, None),
    )
    .await;
    let elapsed = started.elapsed();
    fs::write(&control, "normal")?;
    let result = result.map_err(|_| AppError::ConfigError("S3 target persistence test deadline exceeded".into()))?;
    assert!(
        armed.armed.load(Ordering::SeqCst),
        "must reach the real persistence check"
    );
    println!(
        "application S3 persistence fault={fault} elapsed_ms={} result={result:?}",
        elapsed.as_millis()
    );
    if fault == "normal" {
        result?;
        assert!(!root.path().join("payload").exists());
    } else {
        let cause = match result {
            Err(AppError::StorageError(StorageError::TargetPersistenceUnconfirmed { source, .. })) => source,
            other => panic!("expected a persistence refusal, got {other:?}"),
        };
        let expected = match fault.as_str() {
            "readonly" => FailureClass::PermissionDenied,
            "stall" | "recover_stall" => FailureClass::Timeout,
            "cancel" => FailureClass::Cancelled,
            _ => FailureClass::Connectivity,
        };
        assert_eq!(class(&cause), expected);
        assert_eq!(fs::read(root.path().join("payload"))?, b"verified payload");
        if fault == "cancel" {
            assert!(elapsed < Duration::from_secs(1));
        }
        if matches!(fault.as_str(), "flash" | "recover_stall") {
            remove_entry(&source, &target, &entry, &check, None).await?;
            assert!(!root.path().join("payload").exists());
        }
    }
    Ok(())
}
