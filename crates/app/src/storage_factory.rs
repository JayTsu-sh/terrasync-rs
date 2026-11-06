use crate::error::storage::{Result as StorageResult, StorageError, operation};
use data_mover::TransferConcurrency;
use data_mover::model::BackendKind;
use data_mover::model::*;
use data_mover::storage::{
    self, BackendConfig as NativeBackendConfig, CifsBackendConfig, HdfsBackendConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, Storage,
};
use data_mover::storage::{HdfsConfig, HdfsKerberosCredentials};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
};
use tokio::fs::create_dir_all;
use utils::app_config::{AppConfig, HdfsClientConfig, StorageConfig};

use crate::error::{AppError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageRole {
    Source,
    Destination,
}

fn configured_hdfs(config: &HdfsClientConfig) -> HdfsConfig {
    HdfsConfig {
        config_dir: config.config_dir.clone(),
        overrides: config.overrides.clone(),
        kerberos_credentials: config.kerberos.as_ref().map(|kerberos| HdfsKerberosCredentials {
            principal: kerberos.principal.clone(),
            keytab: kerberos.keytab.clone(),
            cache: kerberos.cache.clone(),
        }),
    }
}

fn create_storage_options(
    storage_type: BackendKind, block_size: Option<u64>, ensure_dir: bool, role: StorageRole, storage: &StorageConfig,
) -> Result<CreateStorageOptions> {
    let hdfs = if matches!(storage_type, BackendKind::Hdfs) {
        let hdfs = match role {
            StorageRole::Source => storage.source.hdfs.as_ref(),
            StorageRole::Destination => storage.destination.hdfs.as_ref(),
        };
        Some(configured_hdfs(hdfs.ok_or_else(|| {
            AppError::ConfigError(format!("HDFS {role:?} role configuration is required"))
        })?))
    } else {
        None
    };
    Ok(CreateStorageOptions {
        block_size,
        ensure_dir,
        hdfs,
    })
}

pub async fn create_storage_for_role(
    path: &str, block_size: Option<u64>, ensure_dir: bool, role: StorageRole,
) -> Result<StorageContext> {
    let storage_type = backend_kind_from_location(path);
    let storage = if matches!(storage_type, BackendKind::Hdfs) {
        AppConfig::fetch()?.storage
    } else {
        StorageConfig::default()
    };
    Ok(create_storage(
        path,
        create_storage_options(storage_type, block_size, ensure_dir, role, &storage)?,
    )
    .await?)
}

/// 统一存储工厂使用的类型化选项。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateStorageOptions {
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
    pub hdfs: Option<HdfsConfig>,
}

impl CreateStorageOptions {
    #[must_use]
    pub const fn new(block_size: Option<u64>, ensure_dir: bool) -> Self {
        Self {
            block_size,
            ensure_dir,
            hdfs: None,
        }
    }
}

pub fn backend_kind_from_location(path: &str) -> BackendKind {
    if path.starts_with("nfs://") || path.starts_with("nfs4://") {
        BackendKind::Nfs
    } else if path.starts_with("s3") && path.contains("://") {
        BackendKind::S3
    } else if path.starts_with("smb://") {
        BackendKind::Cifs
    } else if path.starts_with("hdfs://") {
        BackendKind::Hdfs
    } else {
        BackendKind::Local
    }
}
#[derive(Clone)]
pub struct StorageContext {
    pub(crate) roles: Storage,
    pub(crate) block_size: u64,
    pub(crate) reconnect_config: Option<std::sync::Arc<NativeBackendConfig>>,
}
pub async fn create_storage(path: &str, options: CreateStorageOptions) -> StorageResult<StorageContext> {
    let kind = backend_kind_from_location(path);
    let block_size = options.block_size.unwrap_or(4 * 1024 * 1024);
    if block_size == 0 {
        return Err(StorageError::ConfigError("block size must be positive".into()));
    }
    let config = match kind {
        BackendKind::Local => {
            let root = PathBuf::from(path);
            if options.ensure_dir {
                create_dir_all(&root).await?;
            }
            let concurrency = TransferConcurrency::from_env(BackendKind::Local, TransferConcurrency::defaults(4, 2))
                .map_err(StorageError::from)?;
            NativeBackendConfig::Local(LocalBackendConfig {
                root,
                rename_context: None,
                read_concurrency: NonZeroUsize::new(concurrency.read())
                    .ok_or_else(|| operation("zero read concurrency"))?,
                write_concurrency: NonZeroUsize::new(concurrency.write())
                    .ok_or_else(|| operation("zero write concurrency"))?,
                read_chunk_bytes: options.block_size.and_then(|v| NonZeroUsize::new(v as usize)),
                write_chunk_bytes: options.block_size.and_then(|v| NonZeroUsize::new(v as usize)),
            })
        }
        BackendKind::Nfs => NativeBackendConfig::Nfs(NfsBackendConfig {
            url: path.into(),
            block_size: options.block_size,
            ensure_dir: options.ensure_dir,
            transfer_concurrency: None,
        }),
        BackendKind::S3 => NativeBackendConfig::S3(S3BackendConfig {
            url: path.into(),
            block_size: options.block_size,
            single_put_threshold: None,
        }),
        BackendKind::Cifs => {
            let mut config = CifsBackendConfig::from_url(path).map_err(StorageError::from)?;
            config.ensure_dir = options.ensure_dir;
            NativeBackendConfig::Cifs(config)
        }
        BackendKind::Hdfs => {
            let client = options
                .hdfs
                .ok_or_else(|| StorageError::ConfigError("HDFS requires explicit client configuration".into()))?;
            NativeBackendConfig::Hdfs(HdfsBackendConfig {
                location: path.into(),
                client,
                block_size: options.block_size,
                ensure_dir: options.ensure_dir,
            })
        }
    };
    let reconnect_config = (kind != BackendKind::Local).then(|| std::sync::Arc::new(config.clone()));
    let roles = storage::connect_backend(config).await.map_err(StorageError::from)?;
    Ok(StorageContext {
        roles,
        block_size,
        reconnect_config,
    })
}
pub(crate) fn path(value: &Path) -> StorageResult<StoragePath> {
    if value.as_os_str().is_empty() {
        Ok(StoragePath::root())
    } else {
        StoragePath::new(
            value
                .to_str()
                .ok_or_else(|| StorageError::InvalidPath("non-UTF-8 storage path".into()))?,
        )
        .map_err(Into::into)
    }
}
pub(crate) fn timestamp(value: i64) -> StorageTimestamp {
    StorageTimestamp::new(i128::from(value), TimePrecision::Nanoseconds)
        .unwrap_or_else(|_| unreachable!("nanoseconds are always aligned"))
}

impl StorageContext {
    pub fn storage(&self) -> &Storage {
        &self.roles
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }
}

pub fn canonicalize_path(value: &str) -> std::io::Result<String> {
    if backend_kind_from_location(value) == BackendKind::Local {
        Ok(soft_canonicalize::soft_canonicalize(value)?
            .to_string_lossy()
            .into_owned())
    } else {
        Ok(value.into())
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use data_mover::model::BackendKind;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use utils::app_config::{HdfsClientConfig, HdfsKerberosConfig, StorageConfig, StorageRoleConfig};

    use super::{StorageRole, create_storage_options};

    #[test]
    fn hdfs_options_select_credentials_for_the_requested_role() {
        let storage = StorageConfig {
            source: StorageRoleConfig {
                hdfs: Some(HdfsClientConfig {
                    config_dir: Some(PathBuf::from("/hadoop/source")),
                    overrides: HashMap::from([("dfs.nameservices".to_string(), "source-ha".to_string())]),
                    kerberos: Some(HdfsKerberosConfig {
                        principal: Some("source@EXAMPLE.COM".to_string()),
                        keytab: Some(PathBuf::from("/secret/source.keytab")),
                        cache: None,
                    }),
                }),
            },
            destination: StorageRoleConfig {
                hdfs: Some(HdfsClientConfig {
                    config_dir: Some(PathBuf::from("/hadoop/destination")),
                    ..HdfsClientConfig::default()
                }),
            },
        };

        let source = create_storage_options(BackendKind::Hdfs, None, false, StorageRole::Source, &storage).unwrap();
        let destination =
            create_storage_options(BackendKind::Hdfs, None, true, StorageRole::Destination, &storage).unwrap();

        let source = source.hdfs.unwrap();
        let destination = destination.hdfs.unwrap();
        assert_eq!(source.config_dir, Some(PathBuf::from("/hadoop/source")));
        assert_eq!(destination.config_dir, Some(PathBuf::from("/hadoop/destination")));
        assert_eq!(
            source.kerberos_credentials.unwrap().principal.as_deref(),
            Some("source@EXAMPLE.COM")
        );
    }

    #[test]
    fn non_hdfs_options_do_not_receive_hdfs_credentials() {
        let storage = StorageConfig {
            source: StorageRoleConfig {
                hdfs: Some(HdfsClientConfig {
                    config_dir: Some(PathBuf::from("/hadoop/source")),
                    ..HdfsClientConfig::default()
                }),
            },
            ..StorageConfig::default()
        };

        let options = create_storage_options(BackendKind::Nfs, None, false, StorageRole::Source, &storage).unwrap();
        assert!(options.hdfs.is_none());
    }

    #[test]
    fn hdfs_options_reject_missing_role_configuration_without_ambient_fallback() {
        let error = create_storage_options(
            BackendKind::Hdfs,
            None,
            false,
            StorageRole::Source,
            &StorageConfig::default(),
        )
        .unwrap_err()
        .to_string();

        assert_eq!(error, "Configuration error: HDFS Source role configuration is required");
    }
}
