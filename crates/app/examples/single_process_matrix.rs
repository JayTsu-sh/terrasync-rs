//! One protocol-neutral single-file transfer used by the physical lab matrix.
//!
//! The shell runner supplies an explicitly selected backend profile and an isolated root for
//! each side.  This executable deliberately does not call the legacy URL-dispatching CLI.

use std::collections::HashMap;
use std::env;
use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use app::error::StorageError;
use app::local_transfer::{LocalTransferConfig, LocalTransferSink, run_local_transfer};
use async_trait::async_trait;
use data_mover::model::{FailureClass, ModelValueError, ObservedEntry, StoragePath};
use data_mover::storage::HdfsConfig;
use data_mover::storage::{
    BackendConfig, BackendConnectError, HdfsBackendConfig, LocalBackendConfig, NfsBackendConfig, S3BackendConfig,
    StorageRoleFailure, connect_backend,
};
use data_mover::transfer::{TransferFailure, TransferOutcome, TransferPolicy, TransferSide, TransferValueError};
use data_mover::traversal::{TraversalCompletion, TraversalItem, TraversalOutcome, TraversalSession};
use thiserror::Error;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

#[path = "single_process_matrix/cifs.rs"]
mod cifs;
#[path = "single_process_matrix/diagnostic.rs"]
mod diagnostic;

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
        "usage: single_process_matrix SOURCE_PROFILE SOURCE_ROLE SOURCE_ROOT DESTINATION_PROFILE DESTINATION_ROLE DESTINATION_ROOT JOB_ID, --delete-hdfs-run HDFS_BASE RUN_ID, or a CIFS run command with RUN_ID ROLE"
    )]
    Usage,
    #[error("matrix transfer did not complete exactly one file without failures")]
    TransferFailed,
    #[error("matrix transfer cancelled")]
    Cancelled,
    #[error(transparent)]
    Cifs(#[from] cifs::CifsFailure),
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
    #[error(transparent)]
    Role(#[from] StorageRoleFailure),
    #[error(transparent)]
    Capability(#[from] data_mover::storage::CapabilityUnavailable),
}

struct Sink {
    failures: usize,
    source_role: String,
    destination_role: String,
    source_server: Option<&'static str>,
    destination_server: Option<&'static str>,
}

#[async_trait]
impl LocalTransferSink for Sink {
    async fn completed(&mut self, _entry: ObservedEntry, outcome: TransferOutcome) -> app::error::Result<()> {
        println!(
            "{}",
            serde_json::json!({
                "route": format!("{:?}", outcome.route),
                "transferred_bytes": outcome.transferred_bytes,
                "reused_bytes": outcome.reused_bytes,
                "blake3": outcome.blake3.map(|digest| blake3::Hash::from_bytes(digest).to_hex().to_string()),
                "recovery": format!("{:?}", outcome.recovery),
            })
        );
        Ok(())
    }

    async fn entry_failed(&mut self, failure: data_mover::model::EntryOperationFailure) -> app::error::Result<()> {
        diagnostic::emit(
            "observe_source",
            &self.source_role,
            self.source_server,
            diagnostic::Cause::entry(&failure),
            false,
        );
        self.failures += 1;
        Ok(())
    }

    async fn transfer_failed(&mut self, _entry: ObservedEntry, failure: TransferFailure) -> app::error::Result<()> {
        let (role, server) = match failure.side() {
            TransferSide::Source => (self.source_role.as_str(), self.source_server),
            TransferSide::Destination => (self.destination_role.as_str(), self.destination_server),
            TransferSide::Orchestration => ("orchestration", None),
        };
        let cause = failure
            .source()
            .and_then(|error| error.downcast_ref::<StorageRoleFailure>())
            .map_or_else(diagnostic::Cause::unclassified, diagnostic::Cause::role);
        let outcome = if failure.published_data().is_some() {
            "published"
        } else if failure.final_destination_changed() {
            "changed_or_unknown"
        } else {
            "not_indicated"
        };
        diagnostic::record(&format!("{:?}", failure.phase()), role, server, cause, outcome);
        if let Some(error) = failure.cleanup_failure() {
            diagnostic::emit(
                "cleanup_stage",
                &self.destination_role,
                self.destination_server,
                diagnostic::Cause::role(error),
                true,
            );
        }
        self.failures += 1;
        Ok(())
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
        kerberos_credentials: Some(data_mover::storage::HdfsKerberosCredentials {
            principal: Some(principal),
            keytab: Some(keytab),
            cache: Some(cache),
        }),
    })
}

async fn connect(profile: &str, role: &str, root: &str) -> Result<data_mover::storage::Storage, MatrixError> {
    let config = match profile {
        "local" => BackendConfig::Local(LocalBackendConfig {
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            root: PathBuf::from(root),
            read_concurrency: NonZeroUsize::new(2).ok_or(MatrixError::InvalidLimit)?,
            write_concurrency: NonZeroUsize::new(2).ok_or(MatrixError::InvalidLimit)?,
        }),
        "nfs3" | "nfs40" | "nfs41" => BackendConfig::Nfs(NfsBackendConfig {
            url: root.to_owned(),
            block_size: None,
            ensure_dir: true,
            transfer_concurrency: None,
        }),
        "cifs_fas2750" => return Ok(cifs::connect_cell(role, root).await?),
        "s3_standard" => BackendConfig::S3(S3BackendConfig {
            url: format!(
                "s3://{}:{}@{}.{}:9000/{root}",
                required("LAB_S3_ACCESS_KEY")?,
                required("LAB_S3_SECRET_KEY")?,
                required("LAB_S3_BUCKET")?,
                role_host(role)?
            ),
            block_size: None,
            single_put_threshold: None,
        }),
        "s3_dxn" => BackendConfig::S3(S3BackendConfig {
            url: format!(
                "s3://{}:{}@{}.{}/{root}?compat=dxn",
                required("LAB_DXN_S3_ACCESS_KEY")?,
                required("LAB_DXN_S3_SECRET_KEY")?,
                required("LAB_DXN_S3_BUCKET")?,
                required("LAB_DXN_S3_ENDPOINT")?.trim_start_matches("http://")
            ),
            block_size: None,
            single_put_threshold: None,
        }),
        "hdfs" => BackendConfig::Hdfs(HdfsBackendConfig {
            location: root.to_owned(),
            client: hdfs_client(role)?,
            block_size: None,
            ensure_dir: true,
        }),
        _ => return Err(MatrixError::UnknownProfile),
    };
    connect_backend(config).await.map_err(|error| {
        diagnostic::emit("connect", role, None, diagnostic::Cause::connect(&error, false), false);
        error.into()
    })
}

async fn session(
    source: &data_mover::storage::Storage, cancel: CancellationToken,
) -> Result<TraversalSession, MatrixError> {
    let (producer, session) = TraversalSession::bounded(NonZeroUsize::new(1).ok_or(MatrixError::InvalidLimit)?, cancel);
    let descriptor = source.read_source()?.describe(&StoragePath::new(PAYLOAD)?).await?;
    let observed = ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity,
    )?;
    tokio::spawn(async move {
        let _ = producer.send(TraversalItem::Entry(Box::new(observed))).await;
        producer.finish(Ok(TraversalOutcome::Completed(TraversalCompletion::new({
            let mut totals = data_mover::traversal::ScanTotals::default();
            totals.emitted = 1;
            totals
        }))));
    });
    Ok(session)
}

fn hdfs_cleanup_target(
    root: &str, run_id: &str, configured_base: &str, configured_location: &str,
) -> Result<(url::Url, StoragePath), MatrixError> {
    let suffix = run_id
        .strip_prefix("nightly-")
        .or_else(|| run_id.strip_prefix("release-"))
        .ok_or(MatrixError::Usage)?;
    let hash = suffix
        .strip_suffix("-matrix")
        .or_else(|| suffix.strip_suffix("-e2e"))
        .ok_or(MatrixError::Usage)?;
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err(MatrixError::Usage);
    }
    let mut location = url::Url::parse(root).map_err(|_| MatrixError::Usage)?;
    let configured = url::Url::parse(configured_location).map_err(|_| MatrixError::Usage)?;
    if location.scheme() != "hdfs"
        || configured.scheme() != "hdfs"
        || !location
            .host_str()
            .zip(configured.host_str())
            .is_some_and(|(actual, expected)| actual == expected)
        || location.port() != configured.port()
        || location.password().is_some()
        || location.query().is_some()
        || location.fragment().is_some()
        || location.path() != configured_base
    {
        return Err(MatrixError::Usage);
    }
    let base = location.path().trim_matches('/');
    if base.is_empty()
        || base.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || !part.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        })
    {
        return Err(MatrixError::Usage);
    }
    let path = StoragePath::new(format!("{base}/{run_id}"))?;
    location.set_path("/");
    Ok((location, path))
}

#[cfg(test)]
mod cleanup_scope_tests {
    use super::hdfs_cleanup_target;

    #[test]
    fn permits_only_hashed_scopes_under_the_configured_base_and_nameservice() {
        let scope = format!("nightly-{}-matrix", "a".repeat(64));
        let base = "/tmp/terrasync-nightly/ci";
        let location = "hdfs://hdfscluster/";
        let root = "hdfs://hdfscluster/tmp/terrasync-nightly/ci";
        assert!(hdfs_cleanup_target(root, &scope, base, location).is_ok());
        assert!(hdfs_cleanup_target(root, &scope, base, "hdfs://HdfsCluster/").is_err());
        assert!(
            hdfs_cleanup_target(
                "hdfs://HdfsCluster/tmp/terrasync-nightly/ci",
                &scope,
                base,
                "hdfs://HdfsCluster/"
            )
            .is_ok()
        );
        assert!(hdfs_cleanup_target(root, "nightly-important", base, location).is_err());
        assert!(hdfs_cleanup_target(root, &format!("nightly-{}-other", "a".repeat(64)), base, location).is_err());
        assert!(hdfs_cleanup_target("hdfs://hdfscluster/production", &scope, base, location).is_err());
        assert!(hdfs_cleanup_target("hdfs://other/tmp/terrasync-nightly/ci", &scope, base, location).is_err());
        assert!(hdfs_cleanup_target(root, &format!("nightly-{}-matrix", "A".repeat(64)), base, location).is_err());
    }
}

#[tokio::main]
async fn main() -> Result<(), MatrixError> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if let [command, run_id, role] = args.as_slice()
        && let Some(action) = cifs::Action::parse(command)
    {
        return Ok(action.run(run_id, role).await?);
    }
    if let [command, root, run_id] = args.as_slice() {
        if command == "--delete-hdfs-run" {
            let (location, path) = hdfs_cleanup_target(
                root,
                run_id,
                &required("LAB_HDFS_BASE_PATH")?,
                &required("LAB_HDFS_LOCATION")?,
            )?;
            let storage = match connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
                location: location.to_string(),
                client: hdfs_client("destination")?,
                block_size: None,
                ensure_dir: false,
            }))
            .await
            {
                Ok(storage) => storage,
                Err(error) => {
                    if let Some(failure) = error.session_failure() {
                        eprintln!("HDFS_CLEANUP_FAILURE={:?}", failure.class());
                    } else {
                        eprintln!("HDFS_CLEANUP_FAILURE=connection_unknown");
                    }
                    return Err(error.into());
                }
            };
            match storage.namespace()?.delete_subtree(&path, false).await {
                Ok(()) => return Ok(()),
                Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
                    return Ok(());
                }
                Err(error) => {
                    let class = match &error {
                        StorageRoleFailure::Entry(failure) => failure.class(),
                        StorageRoleFailure::Session(failure) => failure.class(),
                    };
                    eprintln!("HDFS_CLEANUP_FAILURE={class:?}");
                    return Err(error.into());
                }
            }
        }
    }
    if args.len() != 7 {
        return Err(MatrixError::Usage);
    }
    let (source_profile, source_role, source_root, destination_profile, destination_role, destination_root, job) =
        (&args[0], &args[1], &args[2], &args[3], &args[4], &args[5], &args[6]);
    let cancel = CancellationToken::new();
    #[cfg(unix)]
    {
        let mut terminate = signal(SignalKind::terminate())?;
        let cancel = cancel.clone();
        tokio::spawn(async move {
            terminate.recv().await;
            cancel.cancel();
        });
    }
    let source = connect(source_profile, source_role, source_root).await?;
    if cancel.is_cancelled() {
        return Err(MatrixError::Cancelled);
    }
    let destination = connect(destination_profile, destination_role, destination_root).await?;
    if cancel.is_cancelled() {
        return Err(MatrixError::Cancelled);
    }
    let mut sink = Sink {
        failures: 0,
        source_role: source_role.clone(),
        destination_role: destination_role.clone(),
        source_server: cifs::diagnostic_server(source_profile, source_role),
        destination_server: cifs::diagnostic_server(destination_profile, destination_role),
    };
    let traversal = session(&source, cancel.clone()).await.inspect_err(|error| {
        if let MatrixError::Role(error) = error {
            diagnostic::emit(
                "observe_source",
                source_role,
                sink.source_server,
                diagnostic::Cause::role(error),
                false,
            );
        }
    })?;
    let report = run_local_transfer(
        traversal,
        LocalTransferConfig {
            job_identity: job.to_owned(),
            source,
            destination,

            max_concurrent_files: NonZeroUsize::new(1).ok_or(MatrixError::InvalidLimit)?,

            transfer_policy: TransferPolicy::Checkpointed,
            cancel: cancel.clone(),
        },
        &mut sink,
    )
    .await?;
    if report.cancelled || cancel.is_cancelled() {
        return Err(MatrixError::Cancelled);
    }
    if report.completed_files != 1 || sink.failures != 0 {
        return Err(MatrixError::TransferFailed);
    }
    Ok(())
}
