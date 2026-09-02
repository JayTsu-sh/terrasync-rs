//! One protocol-neutral single-file transfer used by the physical lab matrix.
//!
//! The shell runner supplies an explicitly selected backend profile and an isolated root for
//! each side.  This executable deliberately does not call the legacy URL-dispatching CLI.

use std::collections::HashMap;
use std::env;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use app::local_transfer::{
    EntryRecoveryState, LocalTransferConfig, LocalTransferSink, RecoveryRegistration, run_local_transfer,
};
use async_trait::async_trait;
use data_mover::HdfsConfig;
use data_mover::error::StorageError;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, ModelValueError, ObservedEntry, SourceIdentity,
    StoragePath,
};
use data_mover::storage::{
    BackendConfig, BackendConnectError, CifsBackendConfig, ExistingDestinationPolicy, HdfsBackendConfig,
    LocalBackendConfig, NfsBackendConfig, RecoveryIdentity, S3BackendConfig, connect_backend,
};
use data_mover::transfer::{
    InflightLimits, RecoveryRegistrar, RecoveryRegistrationFailure, Resumability, TransferFailure, TransferOutcome,
    TransferValueError,
};
use data_mover::traversal::{TraversalCompletion, TraversalItem, TraversalOutcome, TraversalSession};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use utils::sanitize_job_id;

const PAYLOAD: &str = "payload.bin";
const RECOVERY_MAGIC: &[u8; 8] = b"TSRCV001";

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

struct FileRecoveryState {
    path: PathBuf,
}

struct FileRecoveryRegistrar {
    path: PathBuf,
    claim: [u8; 32],
}

#[async_trait]
impl RecoveryRegistrar for FileRecoveryRegistrar {
    async fn register(&self, identity: RecoveryIdentity) -> std::result::Result<(), RecoveryRegistrationFailure> {
        persist_recovery_state(&self.path, self.claim, Some(identity.as_bytes()))
            .await
            .map_err(|_| RecoveryRegistrationFailure::unavailable())
    }
}

#[async_trait]
impl EntryRecoveryState for FileRecoveryState {
    async fn open(&self, _entry: &ObservedEntry) -> app::error::Result<RecoveryRegistration> {
        let (claim, identity) = match tokio::fs::read(&self.path).await {
            Ok(bytes) => decode_recovery_state(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let claim = new_recovery_claim(&self.path)?;
                persist_recovery_state(&self.path, claim, None).await?;
                (claim, None)
            }
            Err(error) => return Err(error.into()),
        };
        Ok(RecoveryRegistration::new(
            identity,
            claim,
            Arc::new(FileRecoveryRegistrar {
                path: self.path.clone(),
                claim,
            }),
        ))
    }

    async fn completed(&self, _entry: &ObservedEntry) -> app::error::Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl LocalTransferSink for Sink {
    async fn completed(&mut self, _entry: ObservedEntry, _outcome: TransferOutcome) -> app::error::Result<()> {
        Ok(())
    }

    async fn entry_failed(&mut self, _failure: data_mover::model::EntryOperationFailure) -> app::error::Result<()> {
        self.failures += 1;
        Ok(())
    }

    async fn transfer_failed(&mut self, _entry: ObservedEntry, _failure: TransferFailure) -> app::error::Result<()> {
        self.failures += 1;
        Ok(())
    }
}

fn decode_recovery_state(bytes: &[u8]) -> app::error::Result<([u8; 32], Option<RecoveryIdentity>)> {
    if bytes.len() < 40 || &bytes[..8] != RECOVERY_MAGIC {
        return Err(app::error::AppError::ConfigError(
            "invalid matrix recovery state".to_string(),
        ));
    }
    let claim = bytes[8..40]
        .try_into()
        .map_err(|_| app::error::AppError::ConfigError("invalid matrix recovery claim".to_string()))?;
    let identity = if bytes.len() == 40 {
        None
    } else {
        Some(
            RecoveryIdentity::from_bytes(bytes[40..].to_vec())
                .map_err(|error| app::error::AppError::ConfigError(error.to_string()))?,
        )
    };
    Ok((claim, identity))
}

fn new_recovery_claim(path: &Path) -> std::io::Result<[u8; 32]> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"terrasync/matrix-recovery-claim/v1\0");
    hasher.update(path.as_os_str().as_encoded_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(&now.as_nanos().to_le_bytes());
    Ok(*hasher.finalize().as_bytes())
}

async fn persist_recovery_state(path: &Path, claim: [u8; 32], identity: Option<&[u8]>) -> std::io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(RECOVERY_MAGIC).await?;
    file.write_all(&claim).await?;
    if let Some(identity) = identity {
        file.write_all(identity).await?;
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
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
            read_concurrency: NonZeroUsize::new(2).ok_or(MatrixError::InvalidLimit)?,
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
    let recovery = Arc::new(FileRecoveryState {
        path: env::temp_dir().join(format!("terrasync-{}-{PAYLOAD}.recovery", sanitize_job_id(job))),
    });
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
            resumability: Resumability::Enabled,
            recovery: Some(recovery),
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
