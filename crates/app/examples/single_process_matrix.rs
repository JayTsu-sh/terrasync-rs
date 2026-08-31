//! One protocol-neutral single-file transfer used by the physical lab matrix.
//!
//! The shell runner supplies an explicitly selected backend profile and an isolated root for
//! each side.  This executable deliberately does not call the legacy URL-dispatching CLI.

use std::collections::HashMap;
use std::env;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use app::local_transfer::{LocalTransferConfig, LocalTransferSink, run_local_transfer};
use async_trait::async_trait;
use data_mover::HdfsConfig;
use data_mover::error::StorageError;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, ModelValueError, ObservedEntry, SourceIdentity,
    StoragePath,
};
use data_mover::storage::{
    BackendConfig, BackendConnectError, CifsBackendConfig, ExistingDestinationPolicy, HdfsBackendConfig,
    LocalBackendConfig, NfsBackendConfig, S3BackendConfig, connect_backend,
};
use data_mover::transfer::{InflightLimits, RecoveryPolicy, TransferFailure, TransferOutcome, TransferValueError};
use data_mover::traversal::{TraversalCompletion, TraversalItem, TraversalOutcome, TraversalSession};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const PAYLOAD: &str = "payload.bin";

#[derive(Debug, Error)]
enum MatrixError {
    #[error("matrix environment variable {name} must not be blank")]
    MissingConfiguration { name: &'static str },
    #[error("matrix endpoint role must be source or destination")]
    UnknownEndpointRole,
    #[error("matrix profile is not one of the supported lab profiles")]
    UnknownProfile,
    #[error(
        "usage: single_process_matrix SOURCE_PROFILE SOURCE_ROLE SOURCE_ROOT DESTINATION_PROFILE DESTINATION_ROLE DESTINATION_ROOT JOB_ID"
    )]
    Usage,
    #[error("matrix transfer did not complete exactly one file without failures")]
    TransferFailed,
    #[error("non-zero matrix transfer limit is required")]
    InvalidLimit,
    #[error(transparent)]
    Environment(#[from] env::VarError),
    #[error(transparent)]
    Model(#[from] ModelValueError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    BackendConnect(#[from] BackendConnectError),
    #[error(transparent)]
    TransferValue(#[from] TransferValueError),
    #[error(transparent)]
    Application(#[from] app::error::AppError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

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

fn required(name: &'static str) -> Result<String, MatrixError> {
    let value = env::var(name)?;
    if value.trim().is_empty() {
        return Err(MatrixError::MissingConfiguration { name });
    }
    Ok(value)
}

fn role_host(role: &str) -> Result<String, MatrixError> {
    match role {
        "source" => required("LAB_SOURCE_DATA"),
        "destination" => required("LAB_DEST_DATA"),
        _ => Err(MatrixError::UnknownEndpointRole),
    }
}

fn profile_kind(profile: &str) -> Result<BackendKind, MatrixError> {
    match profile {
        "local" => Ok(BackendKind::Local),
        "nfs3" | "nfs40" | "nfs41" => Ok(BackendKind::Nfs),
        "cifs_fas2750" => Ok(BackendKind::Cifs),
        "s3_standard" | "s3_dxn" => Ok(BackendKind::S3),
        "hdfs" => Ok(BackendKind::Hdfs),
        _ => Err(MatrixError::UnknownProfile),
    }
}

fn identity(profile: &str, role: &str, root: &str) -> Result<BackendIdentity, MatrixError> {
    Ok(BackendIdentity::new(
        profile_kind(profile)?,
        format!("lab/{profile}/{role}/{root}"),
    )?)
}

fn hdfs_client(role: &str) -> Result<HdfsConfig, MatrixError> {
    let config_dir = env::var("LAB_HDFS_CONFIG_DIR")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let keytab = required("LAB_HDFS_KEYTAB").map(PathBuf::from)?;
    let principal = required("LAB_HDFS_ADMIN_USER")?;
    let cache = required(match role {
        "source" => "LAB_HDFS_SOURCE_CCACHE",
        "destination" => "LAB_HDFS_DESTINATION_CCACHE",
        _ => return Err(MatrixError::UnknownEndpointRole),
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

async fn connect(profile: &str, role: &str, root: &str) -> Result<data_mover::storage::Storage, MatrixError> {
    let backend = identity(profile, role, root)?;
    let config = match profile {
        "local" => BackendConfig::Local(LocalBackendConfig {
            root: PathBuf::from(root),
            identity: backend,
            write_concurrency: NonZeroUsize::new(2).ok_or(MatrixError::InvalidLimit)?,
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
                _ => return Err(MatrixError::UnknownEndpointRole),
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
        _ => return Err(MatrixError::UnknownProfile),
    };
    Ok(connect_backend(config).await?)
}

async fn session(
    source: &BackendIdentity, size: Option<u64>, cancel: CancellationToken,
) -> Result<TraversalSession, MatrixError> {
    let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).ok_or(MatrixError::InvalidLimit)?, cancel);
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
async fn main() -> Result<(), MatrixError> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 7 {
        return Err(MatrixError::Usage);
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
            max_concurrent_files: NonZeroUsize::new(1).ok_or(MatrixError::InvalidLimit)?,
            existing_destination: ExistingDestinationPolicy::Overwrite,
            recovery_policy: RecoveryPolicy::ResumeOrRestart,
            cancel,
        },
        &mut sink,
    )
    .await?;
    if report.completed_files != 1 || sink.failures != 0 {
        return Err(MatrixError::TransferFailed);
    }
    Ok(())
}
