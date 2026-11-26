//! 原生目标确认与应用删源门槛的真实连接；不冒充数据库／整轮 CLI 验收。
use std::{os::unix::fs::symlink, path::Path, sync::Arc, time::Duration};

use data_mover::model::{EntryKind, FailureClass};
use data_mover::storage::StorageRoleFailure;
use task_model::TransferMode;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{control, environment};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file, metadata::set_entry_metadata, namespace, prepare_directory};
use crate::scan::observe_path;
use crate::source_cleanup::{SourceCleanupCheck, remove_entry};
use crate::storage_factory::{CreateStorageOptions, create_storage};

async fn cancel_at_commit(cancel: CancellationToken) -> Result<Instant> {
    control("wait-fault").await?;
    sleep(Duration::from_millis(250)).await;
    let sent = Instant::now();
    cancel.cancel();
    Ok(sent)
}

async fn case(name: &str, full: bool, fault: Option<&str>) -> Result<()> {
    control("normal").await?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(name), b"guarded payload")?;
    let source = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(
        &environment("LAB_NFS_DIRECT_PROXY_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
    copy_file(
        &source,
        &target,
        &entry,
        CopyOptions {
            transfer_mode: TransferMode::Direct,
            enable_integrity_check: full,
            ..CopyOptions::default()
        },
        None,
    )
    .await?;
    if let Some(fault) = fault {
        control(&format!("commit-{fault}")).await?;
    }
    let cancel = CancellationToken::new();
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), cancel.clone());
    let started = Instant::now();
    let operation = remove_entry(&source, &target, &entry, &check, None);
    let result = if fault == Some("cancel") {
        let (result, sent) = tokio::join!(operation, cancel_at_commit(cancel));
        assert!(sent?.elapsed() < Duration::from_secs(1));
        result
    } else {
        operation.await
    };
    let retained = matches!(fault, Some("disconnect" | "stall" | "cancel"));
    if retained {
        match result {
            Err(AppError::StorageError(StorageError::TargetPersistenceUnconfirmed { source, .. })) => {
                let expected = match fault {
                    Some("disconnect") => FailureClass::Connectivity,
                    Some("stall") => FailureClass::Timeout,
                    _ => FailureClass::Cancelled,
                };
                let class = match &source {
                    StorageRoleFailure::Entry(error) => error.class(),
                    StorageRoleFailure::Session(error) => error.class(),
                };
                assert_eq!(class, expected);
                println!("real application NFS target fault={fault:?} persistence={source:?}");
            }
            outcome => {
                return Err(AppError::ConfigError(format!(
                    "unexpected target persistence outcome: {outcome:?}"
                )));
            }
        }
        assert_eq!(std::fs::read(root.path().join(name))?, b"guarded payload");
    } else {
        result?;
        assert!(!root.path().join(name).exists());
    }
    control("normal").await?;
    println!(
        "real application NFS target name={name} full={full} source_retained={retained} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned target NFS prefix and a COMMIT-aware private relay"]
async fn real_target_nfs_persistence_gates_source_removal() -> Result<()> {
    for full in [false, true] {
        let name = if full { "app-ready-full" } else { "app-ready-quick" };
        timeout(Duration::from_secs(50), Box::pin(case(name, full, None)))
            .await
            .map_err(|_| AppError::ConfigError("NFS target readiness test exceeded its bound".into()))??;
    }
    for fault in ["flash", "disconnect", "stall", "cancel", "lost_ack"] {
        timeout(
            Duration::from_secs(50),
            Box::pin(case(&format!("app-{fault}"), false, Some(fault))),
        )
        .await
        .map_err(|_| AppError::ConfigError("NFS target fault test exceeded its bound".into()))??;
    }
    Ok(())
}

async fn nonfile_case(kind: EntryKind, fault: Option<&str>) -> Result<()> {
    control("normal").await?;
    let root = tempfile::tempdir()?;
    let label = if kind == EntryKind::Directory {
        "directory"
    } else {
        "link"
    };
    let name = format!("app-nonfile-{label}-{}", fault.unwrap_or("ready"));
    let local = root.path().join(&name);
    if kind == EntryKind::Directory {
        std::fs::create_dir(&local)?;
    } else {
        symlink("../opaque/missing", &local)?;
    }
    let source = create_storage(&root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let target = create_storage(
        &environment("LAB_NFS_DIRECT_PROXY_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let entry = observe_path(source.storage(), Path::new(&name)).await?;
    if kind == EntryKind::Directory {
        assert!(prepare_directory(&entry, &target).await?);
    } else {
        namespace::create_symlink(target.storage(), &entry, Path::new("../opaque/missing"), None).await?;
    }
    set_entry_metadata(target.storage(), &entry, None).await?;
    if let Some(fault) = fault {
        control(&format!("commit-{fault}")).await?;
    }
    let cancel = CancellationToken::new();
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), cancel.clone());
    let operation = remove_entry(&source, &target, &entry, &check, None);
    let result = if fault == Some("cancel") {
        let (result, sent) = tokio::join!(operation, cancel_at_commit(cancel));
        assert!(sent?.elapsed() < Duration::from_secs(1));
        result
    } else {
        operation.await
    };
    let retained = matches!(fault, Some("disconnect" | "stall" | "cancel"));
    if retained {
        let expected = match fault {
            Some("disconnect") => FailureClass::Connectivity,
            Some("stall") => FailureClass::Timeout,
            _ => FailureClass::Cancelled,
        };
        match result {
            Err(AppError::StorageError(StorageError::TargetPersistenceUnconfirmed { source, .. })) => {
                let class = match source {
                    StorageRoleFailure::Entry(error) => error.class(),
                    StorageRoleFailure::Session(error) => error.class(),
                };
                assert_eq!(class, expected);
            }
            other => {
                return Err(AppError::ConfigError(format!(
                    "unexpected nonfile readiness: {other:?}"
                )));
            }
        }
        assert!(std::fs::symlink_metadata(&local).is_ok());
        if kind == EntryKind::Symlink {
            assert_eq!(std::fs::read_link(&local)?, Path::new("../opaque/missing"));
        }
    } else {
        result?;
        assert!(std::fs::symlink_metadata(&local).is_err());
    }
    control("normal").await?;
    println!("real application NFS nonfile kind={kind:?} fault={fault:?} source_retained={retained}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned target NFS prefix and a lookup_path-aware private relay"]
async fn real_target_nfs_nonfile_persistence_gates_source_removal() -> Result<()> {
    for kind in [EntryKind::Directory, EntryKind::Symlink] {
        for fault in [
            None,
            Some("flash"),
            Some("disconnect"),
            Some("stall"),
            Some("cancel"),
            Some("lost_ack"),
        ] {
            timeout(Duration::from_secs(40), Box::pin(nonfile_case(kind, fault)))
                .await
                .map_err(|_| AppError::ConfigError("NFS nonfile readiness exceeded its bound".into()))??;
        }
    }
    Ok(())
}
