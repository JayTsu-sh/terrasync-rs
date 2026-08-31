//! One protocol-neutral single-file transfer used by the physical lab matrix.
//!
//! The shell runner supplies an explicitly selected backend profile and an isolated root for
//! each side.  This executable deliberately does not call the legacy URL-dispatching CLI.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use app::local_transfer::{LocalTransferConfig, LocalTransferSink, run_local_transfer};
use async_trait::async_trait;
use data_mover::HdfsConfig;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, ExistingDestinationPolicy, HdfsBackendConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, connect_backend,
};
use data_mover::transfer::{InflightLimits, RecoveryPolicy, TransferFailure, TransferOutcome};
use data_mover::traversal::{TraversalCompletion, TraversalItem, TraversalOutcome, TraversalSession};
use tokio_util::sync::CancellationToken;

const PAYLOAD: &str = "payload.bin";

struct Sink {
    failures: usize,
}

#[async_trait]
impl LocalTransferSink for Sink {
    async fn completed(&mut self, _entry: ObservedEntry, _outcome: TransferOutcome) {}

    async fn entry_failed(&mut self, _failure: data_mover::model::EntryOperationFailure) {
        self.failures += 1;
    }

    async fn transfer_failed(&mut self, _entry: ObservedEntry, _failure: TransferFailure) {
        self.failures += 1;
    }
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    let value = env::var(name)?;
    if value.trim().is_empty() {
        return Err(format!("{name} must not be blank").into());
    }
    Ok(value)
}

fn role_host(role: &str) -> Result<String, Box<dyn Error>> {
    match role {
        "source" => required("LAB_SOURCE_DATA"),
        "destination" => required("LAB_DEST_DATA"),
        _ => Err(format!("unknown endpoint role: {role}").into()),
    }
}

fn profile_kind(profile: &str) -> Result<BackendKind, Box<dyn Error>> {
    match profile {
        "local" => Ok(BackendKind::Local),
        "nfs3" | "nfs40" | "nfs41" => Ok(BackendKind::Nfs),
        "cifs_fas2750" => Ok(BackendKind::Cifs),
        "s3_standard" | "s3_dxn" => Ok(BackendKind::S3),
        "hdfs" => Ok(BackendKind::Hdfs),
        _ => Err(format!("unknown profile: {profile}").into()),
    }
}

fn identity(profile: &str, role: &str, root: &str) -> Result<BackendIdentity, Box<dyn Error>> {
    Ok(BackendIdentity::new(
        profile_kind(profile)?,
        format!("lab/{profile}/{role}/{root}"),
    )?)
}

fn hdfs_client(role: &str) -> Result<HdfsConfig, Box<dyn Error>> {
    let config_dir = env::var("LAB_HDFS_CONFIG_DIR")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let keytab = required("LAB_HDFS_KEYTAB").map(PathBuf::from)?;
    let principal = required("LAB_HDFS_ADMIN_USER")?;
    let cache = required(match role {
        "source" => "LAB_HDFS_SOURCE_CCACHE",
        "destination" => "LAB_HDFS_DESTINATION_CCACHE",
        _ => return Err(format!("unknown endpoint role: {role}").into()),
    })?;
    Ok(HdfsConfig {
        config_dir,
        overrides: HashMap::new(),
        kerberos_credentials: Some(data_mover::HdfsKerberosCredentials {
            principal: Some(principal),
            keytab: Some(keytab),
            cache: Some(cache),
        }),
    })
}

async fn connect(profile: &str, role: &str, root: &str) -> Result<data_mover::storage::Storage, Box<dyn Error>> {
    let backend = identity(profile, role, root)?;
    let config = match profile {
        "local" => BackendConfig::Local(LocalBackendConfig {
            root: PathBuf::from(root),
            identity: backend,
            write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        }),
        "nfs3" | "nfs40" | "nfs41" => BackendConfig::Nfs(NfsBackendConfig {
            url: root.to_owned(),
            identity: backend,
            block_size: None,
            ensure_dir: true,
        }),
        "cifs_fas2750" => BackendConfig::Cifs(CifsBackendConfig {
            server: match role {
                "source" => required("LAB_CIFS_SOURCE_DATA")?,
                "destination" => required("LAB_CIFS_DEST_DATA")?,
                _ => return Err(format!("unknown endpoint role: {role}").into()),
            },
            share: required("LAB_CIFS_SHARE")?,
            root: Some(root.to_owned()),
            username: required("LAB_CIFS_USERNAME")?,
            password: required("LAB_CIFS_PASSWORD")?,
            identity: backend,
        }),
        "s3_standard" => BackendConfig::S3(S3BackendConfig {
            url: format!(
                "s3://{}:{}@{}.{}:9000/{root}",
                required("LAB_S3_ACCESS_KEY")?,
                required("LAB_S3_SECRET_KEY")?,
                required("LAB_S3_BUCKET")?,
                role_host(role)?
            ),
            identity: backend,
            block_size: None,
        }),
        "s3_dxn" => BackendConfig::S3(S3BackendConfig {
            url: format!(
                "s3+dxn://{}:{}@{}.{}/{root}",
                required("LAB_DXN_S3_ACCESS_KEY")?,
                required("LAB_DXN_S3_SECRET_KEY")?,
                required("LAB_DXN_S3_BUCKET")?,
                required("LAB_DXN_S3_ENDPOINT")?.trim_start_matches("http://")
            ),
            identity: backend,
            block_size: None,
        }),
        "hdfs" => BackendConfig::Hdfs(HdfsBackendConfig {
            location: root.to_owned(),
            identity: backend,
            client: hdfs_client(role)?,
            block_size: None,
            ensure_dir: true,
        }),
        _ => return Err(format!("unknown profile: {profile}").into()),
    };
    Ok(connect_backend(config).await?)
}

async fn session(
    source: &BackendIdentity, size: Option<u64>, cancel: CancellationToken,
) -> Result<TraversalSession, Box<dyn Error>> {
    let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).ok_or("non-zero")?, cancel);
    let observed = ObservedEntry::new(
        StoragePath::new(PAYLOAD)?,
        EntryKind::File,
        size,
        None,
        SourceIdentity::new(source.clone(), IdentityStrength::PathScoped, PAYLOAD)?,
    )?;
    tokio::spawn(async move {
        let _ = producer.send(TraversalItem::Entry(Box::new(observed))).await;
        producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 1,
            entry_failures: 0,
        })));
    });
    Ok(session)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 7 {
        return Err("usage: single_process_matrix SOURCE_PROFILE SOURCE_ROLE SOURCE_ROOT DESTINATION_PROFILE DESTINATION_ROLE DESTINATION_ROOT JOB_ID".into());
    }
    let (source_profile, source_role, source_root, destination_profile, destination_role, destination_root, job) =
        (&args[0], &args[1], &args[2], &args[3], &args[4], &args[5], &args[6]);
    let source_id = identity(source_profile, source_role, source_root)?;
    let source = connect(source_profile, source_role, source_root).await?;
    let destination = connect(destination_profile, destination_role, destination_root).await?;
    let size = (source_profile == "local")
        .then(|| std::fs::metadata(PathBuf::from(source_root).join(PAYLOAD)).map(|metadata| metadata.len()))
        .transpose()?;
    let cancel = CancellationToken::new();
    let mut sink = Sink { failures: 0 };
    let report = run_local_transfer(
        session(&source_id, size, cancel.clone()).await?,
        LocalTransferConfig {
            job_identity: job.to_owned(),
            source,
            destination,
            inflight: InflightLimits::new(4, 4 * 1024 * 1024, 1)?,
            max_concurrent_files: NonZeroUsize::new(1).ok_or("non-zero")?,
            existing_destination: ExistingDestinationPolicy::Overwrite,
            recovery_policy: RecoveryPolicy::ResumeOrRestart,
            cancel,
        },
        &mut sink,
    )
    .await?;
    if report.completed_files != 1 || sink.failures != 0 {
        return Err(format!(
            "single-process transfer failed: completed={}, failures={}",
            report.completed_files, sink.failures
        )
        .into());
    }
    Ok(())
}
