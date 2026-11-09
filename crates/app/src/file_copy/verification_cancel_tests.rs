//! 发布后只读校验取消必须保留数据和有类型错误，不能归为需要清理目标的数据失败。

use std::{future::pending, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use data_mover::metadata::{ApplicationFailureKind, MetadataVerificationError};
use data_mover::model::{MetadataCapabilities, MetadataObservations, ObservationPlan, ObservedEntry, StoragePath};
use data_mover::storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure};
use serde::Deserialize;
use task_model::{
    ChangeKind, EntryChange, MetadataTargetVersion, StorageEntryMessage, TransferMode, observation::ObservedItem,
};
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep},
};
use tokio_util::sync::CancellationToken;

use super::{CopyOptions, VersionCopyScope, copy_versions, transfer};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};
use crate::sync::process_metadata_only_entry;
use crate::{
    broadcast::BroadcastForwarder,
    error::{AppError, Result as AppResult, StorageError},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct PendingVerification {
    inner: Arc<dyn Metadata>,
    entered: Arc<Notify>,
}

#[async_trait]
impl Metadata for PendingVerification {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }

    async fn observe(&self, _: &StoragePath, _: ObservationPlan) -> Result<MetadataObservations, StorageRoleFailure> {
        self.entered.notify_one();
        pending().await
    }

    async fn apply(
        &self, _: &StoragePath, _: MetadataMutation, _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("传输通过 stage 应用元数据；发布后不得重复写入")
    }
}

fn stall_verification(target: &mut StorageContext) -> TestResult<Arc<Notify>> {
    let original = target.storage();
    let entered = Arc::new(Notify::new());
    target.roles = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .namespace(original.namespace()?)
        .staged_destination(original.staged_destination()?)
        .metadata(Arc::new(PendingVerification {
            inner: original.metadata()?,
            entered: entered.clone(),
        }))
        .build()?;
    Ok(entered)
}

async fn verify_cancelled_copy(mode: TransferMode, integrity: bool) -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"published payload")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    let entered = stall_verification(&mut target)?;
    let cancel = CancellationToken::new();
    let options = CopyOptions {
        transfer_mode: mode,
        enable_integrity_check: integrity,
        cancel: Some(cancel.clone()),
        ..CopyOptions::default()
    };
    let cancelling = async {
        timeout(Duration::from_secs(5), entered.notified()).await?;
        cancel.cancel();
        TestResult::Ok(())
    };
    let (result, cancelled) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            transfer::copy_current(&source, &target, &entry, &options, None),
            cancelling
        )
    })
    .await?;
    cancelled?;
    let Err(StorageError::PublishedDataMetadata { data, source }) = result else {
        return Err(format!("发布后校验取消丢失数据证据: {result:?}").into());
    };
    assert!(matches!(
        *source,
        StorageError::MetadataVerification(MetadataVerificationError::Cancelled)
    ));
    assert_eq!(data.transferred_bytes, 17);
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"published payload");
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"published payload");
    Ok(())
}

#[tokio::test]
async fn cancelled_published_verification_retains_both_copies_in_every_transfer_mode() -> TestResult {
    for mode in [TransferMode::Direct, TransferMode::Checkpoint] {
        for integrity in [false, true] {
            verify_cancelled_copy(mode, integrity).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_metadata_only_copy_does_not_mutate_the_existing_target() -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"published payload")?;
    std::fs::write(target_root.path().join("file"), b"published payload")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    let before = std::fs::metadata(target_root.path().join("file"))?.modified()?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let broadcaster = BroadcastForwarder::<StorageEntryMessage>::new(8);
    let result = copy_versions(
        [ObservedItem::from(entry)].into_iter(),
        &source,
        &target,
        CopyOptions {
            cancel: Some(cancel),
            ..Default::default()
        },
        "metadata-cancel",
        &broadcaster,
        VersionCopyScope::SelectedVersion {
            change: EntryChange::Changed(ChangeKind::MetadataOnly),
            target_history: Some(false),
            metadata_target: None,
        },
    )
    .await;
    assert!(
        matches!(result, Err(AppError::StorageError(StorageError::MetadataApplication(error)))
        if error.kind() == ApplicationFailureKind::Cancelled)
    );
    assert_eq!(std::fs::metadata(target_root.path().join("file"))?.modified()?, before);
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"published payload");
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"published payload");
    Ok(())
}

struct PendingMetadataOnly {
    inner: Arc<dyn Metadata>,
    entered: Arc<Notify>,
    applying: bool,
}

#[async_trait]
impl Metadata for PendingMetadataOnly {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }

    async fn observe(
        &self, path: &StoragePath, plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if !self.applying {
            self.entered.notify_one();
            return pending().await;
        }
        self.inner.observe(path, plan).await
    }

    async fn apply(
        &self, path: &StoragePath, mutation: MetadataMutation, cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if self.applying {
            self.entered.notify_one();
            cancel.cancelled().await;
        }
        self.inner.apply(path, mutation, cancel).await
    }
}

async fn verify_metadata_only_cancellation(applying: bool, version_workflow: bool) -> TestResult {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"published payload")?;
    std::fs::write(target_root.path().join("file"), b"published payload")?;
    let source =
        Arc::new(create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?);
    let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    let entered = Arc::new(Notify::new());
    let original = target.storage();
    target.roles = StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
        .read_source(original.read_source()?)
        .namespace(original.namespace()?)
        .staged_destination(original.staged_destination()?)
        .metadata(Arc::new(PendingMetadataOnly {
            inner: original.metadata()?,
            entered: entered.clone(),
            applying,
        }))
        .build()?;
    let target = Arc::new(target);
    let cancel = CancellationToken::new();
    let mut broadcaster = BroadcastForwarder::<StorageEntryMessage>::new(8);
    let mut messages = broadcaster.subscribe();
    let copying = async {
        if version_workflow {
            copy_versions(
                [ObservedItem::from(entry.clone())].into_iter(),
                &source,
                &target,
                CopyOptions {
                    cancel: Some(cancel.clone()),
                    ..Default::default()
                },
                "metadata-cancel",
                &broadcaster,
                VersionCopyScope::SelectedVersion {
                    change: EntryChange::Changed(ChangeKind::MetadataOnly),
                    target_history: Some(false),
                    metadata_target: None,
                },
            )
            .await
        } else {
            process_metadata_only_entry(
                &entry,
                source.clone(),
                target.clone(),
                false,
                true,
                &broadcaster,
                Some(cancel.clone()),
            )
            .await
        }
    };
    let cancelling = async {
        timeout(Duration::from_secs(1), entered.notified()).await?;
        cancel.cancel();
        TestResult::Ok(())
    };
    let (result, cancelled) = timeout(Duration::from_secs(2), async { tokio::join!(copying, cancelling) }).await?;
    cancelled?;
    if applying {
        assert!(
            matches!(result, Err(AppError::StorageError(StorageError::MetadataApplication(error)))
            if error.kind() == ApplicationFailureKind::Cancelled)
        );
    } else {
        assert!(matches!(
            result,
            Err(AppError::StorageError(StorageError::MetadataVerification(
                MetadataVerificationError::Cancelled
            )))
        ));
    }
    assert!(
        messages.try_recv().is_err(),
        "cancelled metadata must not publish a success fact"
    );
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"published payload");
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"published payload");
    Ok(())
}

#[tokio::test]
async fn metadata_only_application_and_verification_observe_the_worker_cancellation() -> TestResult {
    for applying in [false, true] {
        for version_workflow in [false, true] {
            verify_metadata_only_cancellation(applying, version_workflow).await?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct RealMetadataCase {
    name: String,
    source: String,
    target: String,
    workflow: String,
    applying: bool,
    target_version: Option<String>,
}

async fn metadata_control(action: &str) -> TestResult<bool> {
    let address = std::env::var("LAB_METADATA_CANCEL_CONTROL")?;
    let mut stream = TcpStream::connect(&address).await?;
    stream
        .write_all(format!("GET /{action} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response.starts_with(b"HTTP/1.0 204"))
}

async fn real_metadata_repair(
    case: &RealMetadataCase, source: Arc<StorageContext>, target: Arc<StorageContext>, entry: Arc<ObservedEntry>,
    cancel: Option<CancellationToken>,
) -> AppResult<()> {
    let broadcaster = BroadcastForwarder::new(8);
    if case.workflow == "ordinary" {
        return process_metadata_only_entry(&entry, source, target, false, true, &broadcaster, cancel).await;
    }
    let binding = case.target_version.clone().map(MetadataTargetVersion::Bound);
    copy_versions(
        [ObservedItem::from(entry)].into_iter(),
        &source,
        &target,
        CopyOptions {
            cancel,
            ..Default::default()
        },
        "real-metadata-cancel",
        &broadcaster,
        VersionCopyScope::SelectedVersion {
            change: EntryChange::Changed(ChangeKind::MetadataOnly),
            target_history: Some(binding.is_some()),
            metadata_target: binding.as_ref(),
        },
    )
    .await
}

#[tokio::test]
#[ignore = "requires --metadata-cancel owned S3 service and held metadata requests"]
async fn real_metadata_only_cancellation_preserves_data_and_target_versions() -> TestResult {
    let cases: Vec<RealMetadataCase> = serde_json::from_str(&std::env::var("LAB_METADATA_CANCEL_CASES")?)?;
    assert_eq!(cases.len(), 6);
    for case in cases {
        let source = Arc::new(create_storage(&case.source, CreateStorageOptions::default()).await?);
        let target = Arc::new(create_storage(&case.target, CreateStorageOptions::default()).await?);
        let entry = Arc::new(observe_path(source.storage(), Path::new("payload")).await?);
        assert!(metadata_control(&format!("arm/{}", case.name)).await?);
        let cancel = CancellationToken::new();
        let work = real_metadata_repair(
            &case,
            source.clone(),
            target.clone(),
            entry.clone(),
            Some(cancel.clone()),
        );
        let cancelling = async {
            timeout(Duration::from_secs(3), async {
                while !metadata_control(&format!("observed/{}", case.name)).await? {
                    sleep(Duration::from_millis(10)).await;
                }
                TestResult::Ok(())
            })
            .await??;
            let started = Instant::now();
            cancel.cancel();
            TestResult::Ok(started)
        };
        let (result, cancelled) = timeout(Duration::from_secs(5), async { tokio::join!(work, cancelling) }).await?;
        let elapsed = cancelled?.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "{} cancellation: {elapsed:?}",
            case.name
        );
        if case.applying {
            assert!(
                matches!(result, Err(AppError::StorageError(StorageError::MetadataApplication(error)))
                if error.kind() == ApplicationFailureKind::Cancelled)
            );
        } else {
            assert!(matches!(
                result,
                Err(AppError::StorageError(StorageError::MetadataVerification(
                    MetadataVerificationError::Cancelled
                )))
            ));
        }
        assert!(metadata_control(&format!("normal/{}", case.name)).await?);
        real_metadata_repair(&case, source, target, entry, None).await?;
        println!(
            "real metadata cancellation case={} applying={} explicit_target_version={} cancellation_ms={} recovered_without_data_replay=true",
            case.name,
            case.applying,
            case.target_version.is_some(),
            elapsed.as_millis()
        );
    }
    Ok(())
}
