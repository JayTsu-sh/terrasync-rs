//! 应用配置管理模块
//!
//! 该模块负责应用程序的配置管理，包括：
//! 1. 配置结构的定义
//! 2. 配置文件的加载和解析
//! 3. 配置的访问和更新
//! 4. 配置验证

// 标准库
use std::fmt;
use std::path::Path;
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClickhouseConfig {
    pub dsn: String,
    pub dial_timeout: u32,
    pub read_timeout: u32,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DuckDBSQLiteConfig {
    pub in_memory: bool,
    pub pool_size: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub enabled: bool,
    pub r#type: String,
    pub batch_size: u32,
    pub clickhouse: ClickhouseConfig,
    pub duckdb: DuckDBSQLiteConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LicenseConfig {
    pub path: String,
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
        };

        // 解密ClickHouse密码
        if let Some(password) = &mut app_config.database.clickhouse.password {
            *password = CryptoUtil::decrypt_password(password)?;
        }

        Ok(app_config)
    }
}
