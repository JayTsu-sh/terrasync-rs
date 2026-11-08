//! 已准备的独占实验室前缀中，独立修改 NFS 服务端权限，核验删源前的真实属性复核。
use std::{
    env,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

use async_trait::async_trait;
use data_mover::model::MetadataFamily;
use data_mover::model::{ObservedEntry, StoragePath, SymlinkTarget};
use data_mover::storage::{
    ListRequest, ListingStream, Namespace, NamespaceReadFailure, PurgeOutcome, SourceDescriptor, StorageBuilder,
    StorageRoleFailure,
};
use tokio::process::Command;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};

use super::{SourceCleanupCheck, remove_entry};
use crate::error::{AppError, Result, StorageError};
use crate::file_copy::{CopyOptions, copy_file};
use crate::scan::observe_path;
use crate::storage_factory::{CreateStorageOptions, create_storage};

fn environment(name: &str) -> Result<String> {
    env::var(name).map_err(|_| AppError::ConfigError(format!("owned {name} required")))
}

async fn remote(script: &str) -> Result<()> {
    let key = env::var("LAB_SSH_KEY").unwrap_or_else(|_| "/home/github-runner/.ssh/terrasync_lab".into());
    let output = Command::new("ssh")
        .args(["-i", &key])
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "root@10.131.9.12"])
        .arg(format!("python3 -c '{}'", script.replace('\'', "'\\''")))
        .output()
        .await?;
    if !output.status.success() {
        return Err(AppError::ConfigError(format!(
            "owned NFS fixture control failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

struct CountPurge {
    inner: Arc<dyn Namespace>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Namespace for CountPurge {
    async fn stat(&self, path: &StoragePath) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.inner.list(request)
    }
    async fn read_link(&self, path: &StoragePath) -> std::result::Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
    async fn purge_observed(
        &self, entry: &ObservedEntry, cancel: CancellationToken,
    ) -> std::result::Result<PurgeOutcome, StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.purge_observed(entry, cancel).await
    }
}

async fn control(mode: &str) -> Result<()> {
    let address = environment("LAB_SOURCE_GUARD_CONTROL")?;
    let mut stream = TcpStream::connect(&address).await?;
    stream
        .write_all(format!("GET /{mode} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    assert!(reply.starts_with(b"HTTP/1.0 204"));
    Ok(())
}

async fn fault_case(fault: &str, quoted_root: &str) -> Result<()> {
    control("normal").await?;
    let mut source = create_storage(
        &environment("LAB_SOURCE_GUARD_NFS_URL")?,
        CreateStorageOptions::default(),
    )
    .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    source.roles = StorageBuilder::new(source.roles.identity().clone(), source.roles.capabilities().clone())
        .namespace(Arc::new(CountPurge {
            inner: source.roles.namespace()?,
            calls: calls.clone(),
        }))
        .metadata(source.roles.metadata()?)
        .read_source(source.roles.read_source()?)
        .staged_destination(source.roles.staged_destination()?)
        .build()
        .unwrap_or_else(|error| panic!("fixture: {error}"));
    let target_root = tempfile::tempdir()?;
    let target = create_storage(&target_root.path().to_string_lossy(), CreateStorageOptions::default()).await?;
    let entry = Arc::new(observe_path(source.storage(), Path::new(fault)).await?);
    copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
    let cancel = CancellationToken::new();
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), cancel.clone());
    control(fault).await?;
    let cancelling = fault == "cancel";
    let watcher = tokio::spawn(async move {
        if cancelling {
            sleep(Duration::from_millis(250)).await;
            cancel.cancel();
        }
    });
    let start = Instant::now();
    let result = timeout(Duration::from_secs(8), remove_entry(&source, &target, &entry, &check)).await;
    let elapsed = start.elapsed();
    control("normal").await?;
    watcher.await?;
    let result =
        result.map_err(|_| AppError::ConfigError("NFS source observation exceeded the test deadline".into()))?;
    eprintln!(
        "real NFS source metadata fault={fault} elapsed_ms={} purge_calls={} result={result:?}",
        elapsed.as_millis(),
        calls.load(Ordering::SeqCst)
    );
    if fault == "flash" {
        result?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    } else {
        assert!(matches!(
            result,
            Err(AppError::StorageError(StorageError::NamespaceRead(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if fault == "cancel" {
            assert!(matches!(
                result,
                Err(AppError::StorageError(StorageError::NamespaceRead(
                    NamespaceReadFailure::Cancelled { .. }
                )))
            ));
            assert!(elapsed < Duration::from_secs(1));
        } else {
            assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_millis(5000));
        }
    }
    drop(source);
    assert_eq!(std::fs::read(target_root.path().join(fault))?, b"payload");
    remote(&format!(
        "from pathlib import Path\np=Path({quoted_root})/{fault:?}\n{}",
        if fault == "flash" {
            "assert not p.exists()"
        } else {
            "assert p.read_bytes()==b'payload'"
        }
    ))
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-source-metadata-guard.py owned NFS relay and prepared prefix"]
async fn real_nfs_source_observation_faults_do_not_attempt_purge_without_facts() -> Result<()> {
    let root = environment("LAB_SOURCE_GUARD_SERVER_ROOT")?;
    if !root.starts_with("/srv/nfs/v4/ci/nightly-source-metadata-guard-") || root.contains("..") {
        return Err(AppError::ConfigError("owned prepared NFS prefix required".into()));
    }
    let quoted_root = serde_json::to_string(&root)?;
    remote(&format!("from pathlib import Path\nimport os\nr=Path({quoted_root})\nfor name in ['flash','disconnect','stall','cancel']:\n p=r/name\n p.write_bytes(b'payload')\n os.chown(p,1000,1000)\n os.chmod(p,0o700)\n")).await?;
    for fault in ["flash", "disconnect", "stall", "cancel"] {
        fault_case(fault, &quoted_root).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires prepare-run.sh, source NFS v4.1 URL and owned fixture root"]
async fn real_nfs_mode_change_after_copy_retains_source_and_unchanged_mode_can_be_deleted() -> Result<()> {
    let source_url = environment("LAB_SOURCE_GUARD_NFS_URL")?;
    let root = environment("LAB_SOURCE_GUARD_SERVER_ROOT")?;
    if !root.starts_with("/srv/nfs/v4/ci/nightly-source-metadata-guard-") || root.contains("..") {
        return Err(AppError::ConfigError(
            "NFS guard test requires its prepared owned prefix".into(),
        ));
    }
    let quoted_root = serde_json::to_string(&root)?;
    remote(&format!("from pathlib import Path\nimport os\nr=Path({quoted_root})\nfor name in ['unchanged','changed']:\n p=r/name\n p.write_bytes(b'payload')\n os.chown(p,1000,1000)\n os.chmod(p,0o700)\n")).await?;
    let source = create_storage(&source_url, CreateStorageOptions::default()).await?;
    let target_root = tempfile::tempdir()?;
    let target = create_storage(
        target_root
            .path()
            .to_str()
            .ok_or_else(|| AppError::ConfigError("fixture path is not UTF-8".into()))?,
        CreateStorageOptions::default(),
    )
    .await?;
    let check = SourceCleanupCheck::new(Duration::from_secs(1), Duration::from_secs(3), CancellationToken::new());
    for (name, changed) in [("unchanged", false), ("changed", true)] {
        let entry = Arc::new(observe_path(source.storage(), Path::new(name)).await?);
        copy_file(&source, &target, &entry, CopyOptions::default(), None).await?;
        if changed {
            remote(&format!("import os\nos.chmod({quoted_root}+'/changed',0o750)")).await?;
        }
        let result = remove_entry(&source, &target, &entry, &check).await;
        if changed {
            assert!(
                matches!(result, Err(AppError::StorageError(StorageError::SourceMetadataChanged { families, .. })) if families == [MetadataFamily::OwnershipMode])
            );
        } else {
            result?;
            remove_entry(&source, &target, &entry, &check).await?;
        }
        assert_eq!(std::fs::read(target_root.path().join(name))?, b"payload");
    }
    remote(&format!("from pathlib import Path\nimport os\nr=Path({quoted_root})\nassert not (r/'unchanged').exists()\np=r/'changed'\nassert p.read_bytes()==b'payload' and os.stat(p).st_mode & 0o777 == 0o750")).await?;
    eprintln!(
        "real NFS v4.1 unchanged_source_deleted=true source_mode_change_retained=true target_data_preserved=true repeated_cleanup_idempotent=true"
    );
    Ok(())
}

#[path = "source_cleanup_target_nfs_real_tests.rs"]
mod target_nfs;
