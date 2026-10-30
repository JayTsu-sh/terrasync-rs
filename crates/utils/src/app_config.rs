//! 应用配置管理模块
//!
//! 该模块负责应用程序的配置管理，包括：
//! 1. 配置结构的定义
//! 2. 配置文件的加载和解析
//! 3. 配置的访问和更新
//! 4. 配置验证

// 标准库
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

// 外部crate
use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};

// 内部模块
use crate::crypto::CryptoUtil;
use crate::error::{Result, UtilsError};

// 全局配置存储：OnceLock 保证只初始化一次，RwLock 支持 override_with 写入
static CONFIG: OnceLock<RwLock<AppConfig>> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
    pub max_size: u64,
    pub max_backups: u32,
    pub level: String,
    #[serde(default)]
    pub enable_json: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScanConfig {
    pub concurrency: usize,
    pub include_tags: bool,
    #[serde(default)]
    pub depth: Option<u32>,
    #[serde(default)]
    pub r#match: Option<String>,
    #[serde(default)]
    pub exclude: Option<String>,
}

// 定义一致性校验类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum ChecksumType {
    MD5,
    SHA256,
    CRC32,
    CRC64,
}

impl fmt::Display for ChecksumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChecksumType::MD5 => write!(f, "MD5"),
            ChecksumType::SHA256 => write!(f, "SHA256"),
            ChecksumType::CRC32 => write!(f, "CRC32"),
            ChecksumType::CRC64 => write!(f, "CRC64"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SyncConfig {
    pub is_source_reserved: bool,
    pub concurrency: usize,
    #[serde(default)]
    pub enable_integrity_check: Option<bool>,
    #[serde(default)]
    pub enable_acl: Option<bool>,
    #[serde(default)]
    pub qos: Option<String>,
    #[serde(default)]
    pub peak_qos_rate: Option<f32>,
    #[serde(default)]
    pub block_size: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntegrityCheckConfig {
    pub concurrency: usize,
    #[serde(default)]
    pub mtime_precision: IntegrityMtimePrecision,
    #[serde(default)]
    pub mtime_tolerance_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityMtimePrecision {
    Exact,
    #[default]
    Auto,
}

impl Default for IntegrityCheckConfig {
    fn default() -> Self {
        Self {
            concurrency: 8,
            mtime_precision: IntegrityMtimePrecision::Auto,
            mtime_tolerance_ms: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteConfig {
    pub concurrency: usize,
}

impl Default for DeleteConfig {
    fn default() -> Self {
        Self { concurrency: 8 }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClickhouseConfig {
    pub dsn: String,
    pub dial_timeout: u32,
    pub read_timeout: u32,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

impl fmt::Debug for ClickhouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseConfig")
            .field("dsn", &"[REDACTED]")
            .field("dial_timeout", &self.dial_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub enabled: bool,
    pub r#type: String,
    pub batch_size: u32,
    pub clickhouse: ClickhouseConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LicenseConfig {
    pub path: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HdfsKerberosConfig {
    #[serde(default)]
    pub principal: Option<String>,
    #[serde(default)]
    pub keytab: Option<PathBuf>,
    #[serde(default)]
    pub cache: Option<String>,
}

impl fmt::Debug for HdfsKerberosConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HdfsKerberosConfig")
            .field("principal", &self.principal)
            .field("keytab", &self.keytab.as_ref().map(|_| "<redacted>"))
            .field("cache", &self.cache.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HdfsClientConfig {
    #[serde(default)]
    pub config_dir: Option<PathBuf>,
    #[serde(default)]
    pub overrides: HashMap<String, String>,
    #[serde(default)]
    pub kerberos: Option<HdfsKerberosConfig>,
}

impl fmt::Debug for HdfsClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let override_keys = self.overrides.keys().collect::<Vec<_>>();
        f.debug_struct("HdfsClientConfig")
            .field("config_dir", &self.config_dir)
            .field("override_keys", &override_keys)
            .field("kerberos", &self.kerberos)
            .finish()
    }
}

impl HdfsClientConfig {
    fn validate(&self, field: &str) -> Result<()> {
        if self.config_dir.as_ref().is_some_and(|path| path.as_os_str().is_empty()) {
            return Err(UtilsError::ConfigConversionError(format!(
                "{field}.config_dir 不能为空"
            )));
        }
        if self
            .overrides
            .iter()
            .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
        {
            return Err(UtilsError::ConfigConversionError(format!(
                "{field}.overrides 不能包含空键或空值"
            )));
        }
        if let Some(kerberos) = &self.kerberos {
            if kerberos.principal.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(UtilsError::ConfigConversionError(format!(
                    "{field}.kerberos.principal 不能为空"
                )));
            }
            if kerberos.keytab.as_ref().is_some_and(|path| path.as_os_str().is_empty()) {
                return Err(UtilsError::ConfigConversionError(format!(
                    "{field}.kerberos.keytab 不能为空"
                )));
            }
            if kerberos.cache.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(UtilsError::ConfigConversionError(format!(
                    "{field}.kerberos.cache 不能为空"
                )));
            }
            if kerberos.keytab.is_none() && kerberos.cache.is_none() {
                return Err(UtilsError::ConfigConversionError(format!(
                    "{field}.kerberos 必须配置 keytab 或 cache"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageRoleConfig {
    #[serde(default)]
    pub hdfs: Option<HdfsClientConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub source: StorageRoleConfig,
    #[serde(default)]
    pub destination: StorageRoleConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub log: LogConfig,
    pub scan: ScanConfig,
    pub sync: SyncConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub integrity_check: IntegrityCheckConfig,
    #[serde(default)]
    pub delete: DeleteConfig,
    #[serde(default = "default_license_config")]
    pub license: LicenseConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

fn default_license_config() -> LicenseConfig {
    LicenseConfig {
        path: "license.json".to_string(),
    }
}

impl AppConfig {
    /// 一步初始化配置：`default_config`(最低优先级) → `config_file`(中优先级) → build → 存入 `OnceLock`
    ///
    /// 调用后通过 `override_with()` 设置 CLI 参数覆盖（最高优先级），
    /// 最终通过 `fetch()` 获取配置快照。
    pub fn init(default_config: Option<&str>, config_file: Option<&Path>) -> Result<()> {
        let mut builder = Config::builder();

        // 1. 默认配置（最低优先级）
        if let Some(config_contents) = default_config {
            builder = builder.add_source(File::from_str(config_contents, FileFormat::Toml));
        }

        // 2. 用户配置文件（中优先级，覆盖默认值）
        if let Some(config_file_path) = config_file {
            let path_str = config_file_path
                .to_str()
                .ok_or_else(|| UtilsError::ConfigConversionError("Invalid config file path encoding".to_string()))?;
            builder = builder.add_source(File::with_name(path_str));
        }

        let config = builder.build()?;
        let app_config: AppConfig = config.try_into()?;

        // 校验配置值合理性
        app_config.validate()?;

        CONFIG
            .set(RwLock::new(app_config))
            .map_err(|_| UtilsError::ConfigConversionError("Config already initialized".to_string()))?;

        Ok(())
    }

    /// CLI 参数覆盖配置值（最高优先级）
    ///
    /// 接受闭包直接修改 `AppConfig` 字段，类型安全。
    /// 示例：`AppConfig::override_with(|c| c.log.level = "debug".to_string())?;`
    pub fn override_with<F>(mutator: F) -> Result<()>
    where
        F: FnOnce(&mut AppConfig),
    {
        let lock = CONFIG
            .get()
            .ok_or_else(|| UtilsError::ConfigConversionError("Config not initialized".to_string()))?;
        let mut w = lock.write()?;
        mutator(&mut w);
        Ok(())
    }

    /// 获取配置快照（clone `AppConfig`）
    ///
    /// 相比旧实现（每次 clone `ConfigBuilder` + build），开销大幅降低。
    pub fn fetch() -> Result<AppConfig> {
        let lock = CONFIG
            .get()
            .ok_or_else(|| UtilsError::ConfigConversionError("Config not initialized".to_string()))?;
        let r = lock.read()?;
        Ok(r.clone())
    }

    /// 校验配置值的合理性，在 `init()` 中 build 后调用
    pub fn validate(&self) -> Result<()> {
        let check_concurrency = |val: usize, name: &str| -> Result<()> {
            if val == 0 {
                Err(UtilsError::ConfigConversionError(format!("{name} 必须 > 0")))
            } else {
                Ok(())
            }
        };
        check_concurrency(self.scan.concurrency, "scan.concurrency")?;
        check_concurrency(self.sync.concurrency, "sync.concurrency")?;
        check_concurrency(self.integrity_check.concurrency, "integrity_check.concurrency")?;
        check_concurrency(self.delete.concurrency, "delete.concurrency")?;

        // sync 额外校验
        if let Some(rate) = self.sync.peak_qos_rate
            && rate <= 0.0
        {
            return Err(UtilsError::ConfigConversionError(
                "sync.peak_qos_rate 必须 > 0".to_string(),
            ));
        }

        // database 配置校验
        if self.database.enabled {
            if self.database.batch_size == 0 {
                return Err(UtilsError::ConfigConversionError(
                    "database.batch_size 必须 > 0".to_string(),
                ));
            }
            if self.database.clickhouse.dsn.is_empty() {
                return Err(UtilsError::ConfigConversionError(
                    "database.clickhouse.dsn 不能为空".to_string(),
                ));
            }
        }

        // log 配置校验
        if self.log.max_size == 0 {
            return Err(UtilsError::ConfigConversionError("log.max_size 必须 > 0".to_string()));
        }

        if let Some(hdfs) = &self.storage.source.hdfs {
            hdfs.validate("storage.source.hdfs")?;
        }
        if let Some(hdfs) = &self.storage.destination.hdfs {
            hdfs.validate("storage.destination.hdfs")?;
        }

        Ok(())
    }
}

// Coerce Config into AppConfig

impl TryFrom<Config> for AppConfig {
    type Error = UtilsError;

    fn try_from(config: Config) -> Result<Self> {
        // 获取原始配置
        let mut app_config = AppConfig {
            log: config.get::<LogConfig>("log")?,
            scan: config.get::<ScanConfig>("scan")?,
            sync: config.get::<SyncConfig>("sync")?,
            database: config.get::<DatabaseConfig>("database")?,
            integrity_check: config
                .get::<IntegrityCheckConfig>("integrity_check")
                .unwrap_or_default(),
            delete: config.get::<DeleteConfig>("delete").unwrap_or_default(),
            license: config
                .get::<LicenseConfig>("license")
                .unwrap_or_else(|_| default_license_config()),
            storage: config.get::<StorageConfig>("storage").unwrap_or_default(),
        };

        // 解密ClickHouse密码
        if let Some(password) = &mut app_config.database.clickhouse.password {
            *password = CryptoUtil::decrypt_password(password)?;
        }

        Ok(app_config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const BASE_CONFIG: &str = r#"
[log]
max_size = 10
max_backups = 2
level = "info"

[scan]
concurrency = 4
include_tags = false

[sync]
is_source_reserved = true
concurrency = 4

[database]
enabled = false
type = "clickhouse"
batch_size = 100

[database.clickhouse]
dsn = "http://localhost:8123"
dial_timeout = 10
read_timeout = 30
database = "default"
username = "default"
"#;

    #[test]
    fn storage_hdfs_config_is_role_scoped_and_redacted() {
        let source_secret = "/secrets/source.keytab";
        let destination_secret = "FILE:/secrets/destination.ccache";
        let contents = format!(
            "{BASE_CONFIG}\n[storage.source.hdfs]\nconfig_dir = \"/etc/hadoop/source\"\n\
             [storage.source.hdfs.kerberos]\nkeytab = \"{source_secret}\"\n\
             [storage.destination.hdfs]\nconfig_dir = \"/etc/hadoop/destination\"\n\
             [storage.destination.hdfs.kerberos]\ncache = \"{destination_secret}\"\n"
        );
        let raw = Config::builder()
            .add_source(File::from_str(&contents, FileFormat::Toml))
            .build()
            .unwrap();
        let config = AppConfig::try_from(raw).unwrap();

        assert_eq!(
            config.storage.source.hdfs.as_ref().unwrap().config_dir.as_deref(),
            Some(Path::new("/etc/hadoop/source"))
        );
        assert_eq!(
            config.storage.destination.hdfs.as_ref().unwrap().config_dir.as_deref(),
            Some(Path::new("/etc/hadoop/destination"))
        );
        let debug = format!("{:?}", config.storage);
        assert!(!debug.contains(source_secret));
        assert!(!debug.contains(destination_secret));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn storage_config_defaults_to_no_backend_credentials() {
        let raw = Config::builder()
            .add_source(File::from_str(BASE_CONFIG, FileFormat::Toml))
            .build()
            .unwrap();
        let config = AppConfig::try_from(raw).unwrap();

        assert!(config.storage.source.hdfs.is_none());
        assert!(config.storage.destination.hdfs.is_none());
    }

    #[test]
    fn clickhouse_debug_redacts_password() {
        let config = ClickhouseConfig {
            dsn: "http://dsn-user:dsn-secret@clickhouse.internal".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: "terrasync".to_string(),
            username: "writer".to_string(),
            password: Some("decrypted-secret".to_string()),
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("decrypted-secret"));
        assert!(!debug.contains("dsn-secret"));
    }

    #[test]
    fn hdfs_kerberos_requires_a_non_empty_credential_source() {
        let contents =
            format!("{BASE_CONFIG}\n[storage.source.hdfs.kerberos]\nprincipal = \"hdfs/user@EXAMPLE.COM\"\n");
        let raw = Config::builder()
            .add_source(File::from_str(&contents, FileFormat::Toml))
            .build()
            .unwrap();
        let config = AppConfig::try_from(raw).unwrap();

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("storage.source.hdfs.kerberos"));
        assert!(!error.contains("hdfs/user@EXAMPLE.COM"));
    }

    #[test]
    fn hdfs_validation_never_echoes_secret_values() {
        let secret_keytab = "/private/a-very-secret.keytab";
        let contents = format!(
            "{BASE_CONFIG}\n[storage.destination.hdfs]\nconfig_dir = \"/etc/hadoop\"\n\
             [storage.destination.hdfs.overrides]\nfs_defaultFS = \"secret-namenode.internal\"\n\
             [storage.destination.hdfs.kerberos]\nprincipal = \"\"\nkeytab = \"{secret_keytab}\"\n"
        );
        let raw = Config::builder()
            .add_source(File::from_str(&contents, FileFormat::Toml))
            .build()
            .unwrap();
        let config = AppConfig::try_from(raw).unwrap();

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("storage.destination.hdfs.kerberos.principal"));
        assert!(!error.contains(secret_keytab));
        assert!(!error.contains("secret-namenode.internal"));
    }
}
