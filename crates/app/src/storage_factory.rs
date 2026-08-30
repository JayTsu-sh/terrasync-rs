use data_mover::{
    BackendConfig, CreateStorageOptions, HdfsConfig, HdfsKerberosCredentials, StorageEnum, StorageType,
    detect_storage_type,
};
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
    storage_type: StorageType, block_size: Option<u64>, ensure_dir: bool, role: StorageRole, storage: &StorageConfig,
) -> Result<CreateStorageOptions> {
    let backend = if matches!(storage_type, StorageType::Hdfs) {
        let hdfs = match role {
            StorageRole::Source => storage.source.hdfs.as_ref(),
            StorageRole::Destination => storage.destination.hdfs.as_ref(),
        };
        BackendConfig::Hdfs(configured_hdfs(hdfs.ok_or_else(|| {
            AppError::ConfigError(format!("HDFS {role:?} role configuration is required"))
        })?))
    } else {
        BackendConfig::Default
    };
    Ok(CreateStorageOptions {
        block_size,
        ensure_dir,
        backend,
    })
}

pub async fn create_storage_for_role(
    path: &str, block_size: Option<u64>, ensure_dir: bool, role: StorageRole,
) -> Result<StorageEnum> {
    let storage_type = detect_storage_type(path);
    let storage = if matches!(storage_type, StorageType::Hdfs) {
        AppConfig::fetch()?.storage
    } else {
        StorageConfig::default()
    };
    Ok(data_mover::create_storage(
        path,
        create_storage_options(storage_type, block_size, ensure_dir, role, &storage)?,
    )
    .await?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use data_mover::{BackendConfig, StorageType};
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

        let source = create_storage_options(StorageType::Hdfs, None, false, StorageRole::Source, &storage).unwrap();
        let destination =
            create_storage_options(StorageType::Hdfs, None, true, StorageRole::Destination, &storage).unwrap();

        let BackendConfig::Hdfs(source) = source.backend else {
            panic!("source HDFS config must be selected");
        };
        let BackendConfig::Hdfs(destination) = destination.backend else {
            panic!("destination HDFS config must be selected");
        };
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

        let options = create_storage_options(StorageType::Nfs, None, false, StorageRole::Source, &storage).unwrap();
        assert_eq!(options.backend, BackendConfig::Default);
    }

    #[test]
    fn hdfs_options_reject_missing_role_configuration_without_ambient_fallback() {
        let error = create_storage_options(
            StorageType::Hdfs,
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
