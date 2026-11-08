//! 已复制／仅改属性的条目仍须遵守本轮选择的数据校验策略。
use std::{
    env,
    fs::{File, FileTimes},
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use data_mover::integrity::IntegritySide as NativeIntegritySide;
use data_mover::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use data_mover::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageBuilder, StorageRoleFailure};
use futures::stream;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{SourceCleanupCheck, remove_entry};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

#[derive(Clone, Copy)]
enum ReadBehavior {
    Pass,
    Fail(FailureClass),
    Short,
    Stall,
}

struct ReadProbe {
    inner: Arc<dyn ReadSource>,
    behavior: ReadBehavior,
    reads: AtomicUsize,
    entered: Notify,
}

#[async_trait]
impl ReadSource for ReadProbe {
    async fn describe(&self, path: &StoragePath) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.describe(path).await
    }

    async fn read(&self, request: ReadRequest) -> std::result::Result<ByteStream, StorageRoleFailure> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        match self.behavior {
            ReadBehavior::Pass => self.inner.read(request).await,
            ReadBehavior::Fail(class) => Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    request.path,
                    Operation::Read,
                    class,
                    Transience::Permanent,
                    "injected content read refusal",
                )
                .unwrap(),
            )),
            ReadBehavior::Short => Ok(Box::pin(stream::once(async { Ok(Bytes::from_static(b"same")) }))),
            ReadBehavior::Stall => std::future::pending().await,
        }
    }
}

fn probe(storage: &mut StorageContext, behavior: ReadBehavior) -> Result<Arc<ReadProbe>> {
    let reader = Arc::new(ReadProbe {
        inner: storage.roles.read_source()?,
        behavior,
        reads: AtomicUsize::new(0),
        entered: Notify::new(),
    });
    storage.roles = StorageBuilder::new(storage.roles.identity().clone(), storage.roles.capabilities().clone())
        .read_source(reader.clone())
        .namespace(storage.roles.namespace()?)
        .metadata(storage.roles.metadata()?)
        .staged_destination(storage.roles.staged_destination()?)
        .build()
        .unwrap();
    Ok(reader)
}

fn full_check() -> SourceCleanupCheck {
    SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new())
        .with_content_verification(true)
}

#[tokio::test]
async fn full_verification_keeps_source_when_existing_target_has_equal_size_and_mtime_but_wrong_bytes() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"source!")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    let target_file = target_root.path().join("file");
    let before = std::fs::metadata(&target_file)?;
    std::fs::write(&target_file, b"damaged")?;
    File::open(&target_file)?.set_times(FileTimes::new().set_modified(before.modified()?))?;
    assert_eq!(std::fs::metadata(&target_file)?.len(), before.len());
    assert_eq!(std::fs::metadata(&target_file)?.modified()?, before.modified()?);

    let result = remove_entry(&source, &target, &entry, &full_check()).await;
    assert!(
        result.is_err(),
        "full verification must reject corrupted target: {result:?}"
    );
    assert!(matches!(
        result.unwrap_err().storage_error(),
        Some(StorageError::MismatchData(_))
    ));
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"source!");
    assert_eq!(std::fs::read(target_file)?, b"damaged");
    Ok(())
}

#[tokio::test]
async fn quick_verification_reads_no_payload_and_full_verification_reads_both_empty_and_nonempty_files() -> Result<()> {
    for full in [false, true] {
        for payload in [b"".as_slice(), b"payload".as_slice()] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), payload)?;
            let mut source =
                create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let mut target =
                create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
            copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
            let source_reads = probe(&mut source, ReadBehavior::Pass)?;
            let target_reads = probe(&mut target, ReadBehavior::Pass)?;
            let check = full_check().with_content_verification(full);
            remove_entry(&source, &target, &entry, &check).await?;
            assert_eq!(source_reads.reads.load(Ordering::SeqCst), usize::from(full));
            assert_eq!(target_reads.reads.load(Ordering::SeqCst), usize::from(full));
            assert!(!source_root.path().join("file").exists());
            assert_eq!(std::fs::read(target_root.path().join("file"))?, payload);
        }
    }
    Ok(())
}

#[tokio::test]
async fn unreadable_source_or_target_preserves_both_and_attributes_the_original_failure() -> Result<()> {
    for side in [NativeIntegritySide::Source, NativeIntegritySide::Destination] {
        for class in [
            FailureClass::PermissionDenied,
            FailureClass::Connectivity,
            FailureClass::Cancelled,
        ] {
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), b"payload")?;
            let mut source =
                create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let mut target =
                create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
            let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
            copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
            probe(
                if side == NativeIntegritySide::Source {
                    &mut source
                } else {
                    &mut target
                },
                ReadBehavior::Fail(class),
            )?;
            let error = remove_entry(&source, &target, &entry, &full_check()).await.unwrap_err();
            assert_eq!(
                error.storage_error().is_some_and(StorageError::is_cancelled),
                class == FailureClass::Cancelled
            );
            assert!(error.to_string().contains("injected content read refusal"));
            assert!(
                matches!(error, AppError::StorageError(StorageError::SourceCleanupContentUnverified {
                side: failed_side, source: StorageRoleFailure::Entry(failure), ..
            }) if failed_side == side && failure.class() == class)
            );
            assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
            assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
        }
    }
    Ok(())
}

#[tokio::test]
async fn equal_digests_of_two_short_reads_do_not_authorize_source_deletion() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let mut source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    probe(&mut source, ReadBehavior::Short)?;
    probe(&mut target, ReadBehavior::Short)?;
    let error = remove_entry(&source, &target, &entry, &full_check()).await.unwrap_err();
    assert!(matches!(error.storage_error(), Some(StorageError::MismatchData(_))));
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
    Ok(())
}

#[tokio::test]
async fn cancellation_during_content_read_preserves_both_sides() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let source = create_storage(&source_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let mut target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new("file")).await?);
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    let reader = probe(&mut target, ReadBehavior::Stall)?;
    let check = full_check();
    let cancel = async {
        reader.entered.notified().await;
        check.cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(remove_entry(&source, &target, &entry, &check), cancel)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .storage_error()
            .is_some_and(StorageError::is_cancelled)
    );
    assert_eq!(std::fs::read(source_root.path().join("file"))?, b"payload");
    assert_eq!(std::fs::read(target_root.path().join("file"))?, b"payload");
    Ok(())
}

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

#[tokio::test]
#[ignore = "requires run-source-content-local.py owned RustFS fixture and GET-aware relay"]
async fn real_existing_s3_content_verification_gates_source_removal() -> Result<()> {
    let root = PathBuf::from(environment("LAB_CONTENT_SOURCE")?);
    let fault = environment("LAB_CONTENT_FAULT")?;
    let source = create_storage(&root.to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(&environment("LAB_CONTENT_TARGET")?, CreateStorageOptions::default()).await?;
    let entry = observe_path(source.storage(), Path::new("payload")).await?;
    let check = full_check().with_content_verification(fault != "quick");
    let cancel_task = if fault == "cancel" {
        let seen = PathBuf::from(environment("LAB_CONTENT_SEEN")?);
        let cancel = check.cancel.clone();
        Some(tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(3), async {
                while !seen.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
            cancel.cancel();
        }))
    } else {
        None
    };
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(8), remove_entry(&source, &target, &entry, &check))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    if let Some(task) = cancel_task {
        task.await?;
    }
    println!(
        "existing S3 content fault={fault} elapsed_ms={} result={result:?}",
        elapsed.as_millis()
    );
    match fault.as_str() {
        "quick" | "normal" => {
            result?;
            assert!(!root.join("payload").exists());
        }
        "mismatch" => {
            assert!(matches!(
                result.unwrap_err().storage_error(),
                Some(StorageError::MismatchData(_))
            ));
            assert_eq!(std::fs::read(root.join("payload"))?, b"verified payload");
        }
        "cancel" => {
            assert!(
                result
                    .unwrap_err()
                    .storage_error()
                    .is_some_and(StorageError::is_cancelled)
            );
            assert!(elapsed < Duration::from_secs(1));
            assert_eq!(std::fs::read(root.join("payload"))?, b"verified payload");
        }
        _ => {
            let expected = match fault.as_str() {
                "readonly" => FailureClass::PermissionDenied,
                "stall" | "recover_stall" => FailureClass::Timeout,
                _ => FailureClass::Connectivity,
            };
            let failure = match result.unwrap_err() {
                AppError::StorageError(StorageError::SourceCleanupContentUnverified {
                    side: NativeIntegritySide::Destination,
                    source,
                    ..
                }) => source,
                other => panic!("expected attributed content read failure, got {other:?}"),
            };
            let class = match failure {
                StorageRoleFailure::Entry(failure) => failure.class(),
                StorageRoleFailure::Session(failure) => failure.class(),
            };
            assert_eq!(class, expected);
            assert_eq!(std::fs::read(root.join("payload"))?, b"verified payload");
            if matches!(fault.as_str(), "flash" | "recover_stall") {
                std::fs::write(environment("LAB_CONTENT_CONTROL")?, "normal")?;
                remove_entry(&source, &target, &entry, &check).await?;
                assert!(!root.join("payload").exists());
            }
        }
    }
    Ok(())
}
