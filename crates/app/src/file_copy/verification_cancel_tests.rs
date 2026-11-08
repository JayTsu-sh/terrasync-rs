//! 发布后只读校验取消必须保留数据和有类型错误，不能归为需要清理目标的数据失败。

use std::{future::pending, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use data_mover::metadata::MetadataVerificationError;
use data_mover::model::{MetadataCapabilities, MetadataObservations, ObservationPlan, StoragePath};
use data_mover::storage::{Metadata, MetadataMutation, StorageBuilder, StorageRoleFailure};
use task_model::TransferMode;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{CopyOptions, transfer};
use crate::error::StorageError;
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

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
