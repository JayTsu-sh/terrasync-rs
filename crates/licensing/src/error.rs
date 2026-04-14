// 标准库
// 无

// 外部 crate
use thiserror::Error;

// 内部模块
// 无

/// License 模块专用错误枚举
#[derive(Error, Debug)]
pub enum LicenseError {
    /// License 文件未找到
    #[error("License file not found: {0}")]
    FileNotFound(String),

    /// License 文件格式无效
    #[error("Invalid license format: {0}")]
    InvalidFormat(String),

    /// Ed25519 签名验证失败（对外模糊化）
    #[error("License verification failed")]
    SignatureInvalid,

    /// License 已过期（对外模糊化）
    #[error("License verification failed")]
    Expired { expires_at: String },

    /// 激活窗口期已过（明确报出，合法用户需要此信息联系管理员）
    #[error("Activation window expired (issued {issued_at}, window {window_days} days)")]
    ActivationWindowExpired { issued_at: String, window_days: u32 },

    /// 机器指纹不匹配（对外模糊化）
    #[error("License verification failed")]
    MachineBindingMismatch,

    /// 已达最大绑定机器数（对外模糊化）
    #[error("License verification failed")]
    MaxMachinesReached { max: u32 },

    /// HMAC 绑定完整性校验失败（对外模糊化）
    #[error("License verification failed")]
    BindingHmacInvalid,

    /// 时间回拨检测（对外模糊化）
    #[error("License verification failed")]
    ClockRegression,

    /// 哨兵文件与 license 文件不一致（对外模糊化）
    #[error("License verification failed")]
    LicenseFileRestored {
        sentinel_clock: String,
        license_clock: String,
    },

    /// 哨兵指纹不匹配（对外模糊化）
    #[error("License verification failed")]
    SentinelMismatch,

    /// 机器指纹采集失败
    #[error("Failed to collect machine fingerprint: {0}")]
    FingerprintError(String),

    /// 功能未授权
    #[error("Feature not licensed: {feature}")]
    FeatureNotLicensed { feature: String },

    /// License 未激活
    #[error("License not activated. Run 'terrasync activate --license <path>' first")]
    NotActivated,

    /// IO 错误
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// JSON 序列化/反序列化错误
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// 加密操作错误
    #[error("Crypto error: {0}")]
    CryptoError(String),
}

/// License 模块结果类型别名
pub type Result<T> = std::result::Result<T, LicenseError>;
