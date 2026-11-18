//! 真实 NFS 源协议请求与任务元数据计划；目标数据由服务端独立读取。
use std::{env, path::Path, sync::Arc};

use data_mover::model::{
    AclEncoding, FailureClass, MetadataFamily, MetadataObservation, ObservationMode, ObservedEntry, StoragePath,
};
use data_mover::storage::{ListRequest, StorageRoleFailure};
use futures::StreamExt;
use task_model::TransferMode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use super::MigrationPlan;
use super::tests::OBJECT_TARGET;
use crate::error::{AppError, StorageError};
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::{observe_path, observe_path_with_plan};
use crate::storage_factory::{CreateStorageOptions, StorageContext, create_storage};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const PAYLOAD: &[u8] = b"payload survives source ACL acquisition failure";

async fn control(mode: &str) -> Result {
    let mut stream = TcpStream::connect(env::var("LAB_NFS_DIRECT_CONTROL")?).await?;
    stream
        .write_all(format!("GET /{mode} HTTP/1.0\r\nHost: lab\r\n\r\n").as_bytes())
        .await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    assert!(reply.starts_with(b"HTTP/1.0 204"));
    Ok(())
}

async fn deliver(source: &StorageContext, target: &StorageContext, entry: &ObservedEntry) -> crate::error::Result<()> {
    copy_file(
        source,
        target,
        &Arc::new(entry.clone()),
        CopyOptions {
            transfer_mode: TransferMode::Direct,
            ..CopyOptions::default()
        },
        None,
    )
    .await?;
    Ok(())
}

async fn prepare(source: &StorageContext) -> Result {
    let root = tempfile::tempdir()?;
    let local = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    for name in ["plan-omit", "plan-denied", "plan-raw"] {
        std::fs::write(root.path().join(name), PAYLOAD)?;
        let entry = observe_path(local.storage(), Path::new(name)).await?;
        deliver(&local, source, &entry).await?;
    }
    Ok(())
}

async fn omitted(source: &StorageContext, target: &StorageContext) -> Result {
    let source_caps = source.storage().metadata()?.observation_capabilities();
    let plan = MigrationPlan::from_capabilities(source_caps, Some(OBJECT_TARGET), true);
    assert_eq!(plan.observation.acl(), ObservationMode::Omit);
    assert_eq!(plan.observation.xattrs(), ObservationMode::Omit);
    assert!(
        plan.additional_reads
            .iter()
            .all(|read| !read.requested && read.extra_calls == Some(0))
    );
    let namespace = source.storage().namespace()?;
    let mut listing = namespace.list(ListRequest::new(StoragePath::new("")?, CancellationToken::new()));
    let mut pages = 0;
    while let Some(page) = listing.next().await {
        page?;
        pages += 1;
    }
    assert!(pages > 0);
    let entry = observe_path_with_plan(source.storage(), Path::new("plan-omit"), plan.observation).await?;
    assert!(matches!(entry.metadata().acl(), MetadataObservation::NotRequested));
    deliver(source, target, &entry).await?;
    Ok(())
}

async fn denied(source: &StorageContext, target: &StorageContext, plan: &MigrationPlan) -> Result {
    control("acl_denied").await?;
    let entry = observe_path_with_plan(source.storage(), Path::new("plan-denied"), plan.observation).await?;
    assert!(matches!(
        entry.metadata().acl(),
        MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            ..
        }
    ));
    let result = deliver(source, target, &entry).await;
    let Err(AppError::StorageError(StorageError::PublishedDataMetadata { data, source: error })) = result else {
        return Err("source ACL failure must retain published data and its typed cause".into());
    };
    assert_eq!(data.transferred_bytes, PAYLOAD.len() as u64);
    assert!(
        matches!(error.as_ref(), StorageError::SourceMetadataAcquisitionFailed { failures, .. }
        if failures.len() == 1 && failures[0].family == MetadataFamily::Acl && failures[0].class == FailureClass::PermissionDenied)
    );
    assert!(
        source
            .storage()
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await
            .is_err()
    );
    source.storage().namespace()?.stat(entry.path()).await?;
    target.storage().namespace()?.stat(entry.path()).await?;
    let required = plan.observation.with_acl(ObservationMode::Required);
    assert!(
        matches!(source.storage().metadata()?.observe(entry.path(), required).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied)
    );
    control("normal").await?;
    Ok(())
}

async fn raw(source: &StorageContext, target: &StorageContext, plan: &MigrationPlan) -> Result {
    let entry = observe_path_with_plan(source.storage(), Path::new("plan-raw"), plan.observation).await?;
    let acl = entry.metadata().acl().value().ok_or("selected ACL raw value absent")?;
    assert_eq!(acl.encoding(), AclEncoding::NfsV4);
    assert!(acl.access().is_some_and(|bytes| !bytes.is_empty()));
    let restored = ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes(), source.storage().identity())?;
    assert_eq!(restored.metadata().acl(), entry.metadata().acl());
    deliver(source, target, &entry).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires private NFS attribute-mask audit and ACL reply-denial relay"]
async fn real_nfs_metadata_plan_controls_acl_reads_and_preserves_data_on_failure() -> Result {
    control("normal").await?;
    let source = create_storage(&env::var("LAB_NFS_DIRECT_PROXY_URL")?, CreateStorageOptions::default()).await?;
    let target = create_storage(
        &env::var("LAB_METADATA_PLAN_TARGET_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    prepare(&source).await?;
    omitted(&source, &target).await?;
    let plan = MigrationPlan::compile(&source, &target, true).await?;
    assert_eq!(plan.observation.acl(), ObservationMode::BestEffort);
    denied(&source, &target, &plan).await?;
    raw(&source, &target, &plan).await?;
    println!(
        "real NFS metadata plan: omitted ACL has no source ACL query; explicit permission failure retains data/source and blocks purge; raw ACL survives snapshot and copy"
    );
    Ok(())
}
