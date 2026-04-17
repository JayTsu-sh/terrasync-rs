// 标准库
// 无

// 外部 crate
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// 内部模块
// 无

/// 默认激活窗口期（天）
fn default_activation_window() -> u32 {
    7
}

/// 默认最大绑定机器数
fn default_max_machines() -> u32 {
    1
}

/// License 文件顶层结构
///
/// 包含被签名保护的 payload、Ed25519 签名，以及可选的激活绑定信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseFile {
    /// 授权内容（被 Ed25519 签名保护）
    pub payload: Payload,
    /// Ed25519 签名（Base64 编码）
    pub signature: String,
    /// 机器激活绑定信息（激活前为 None）
    pub activation: Option<Activation>,
    /// 单调递增 license 时钟（持久化），用于防止时间回拨攻击
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license_clock: Option<DateTime<Utc>>,
}

/// 授权内容（不可篡改）
///
/// payload 的 JSON 序列化结果被 Ed25519 签名保护，
/// 任何修改都会导致签名验证失败。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    /// 唯一 License ID (UUID v4)
    pub license_id: String,
    /// 被授权人姓名
    pub licensee: String,
    /// 签发时间
    pub issued_at: DateTime<Utc>,
    /// 过期时间（None = 永久有效）
    pub expires_at: Option<DateTime<Utc>>,
    /// 激活窗口期（天），签发后必须在此期限内完成激活
    #[serde(default = "default_activation_window")]
    pub activation_window_days: u32,
    /// 最大绑定机器数（1 = 单机，N > 1 = 多机，0 = 不限制）
    #[serde(default = "default_max_machines")]
    pub max_machines: u32,
}

/// 激活绑定信息
///
/// 记录所有已绑定的机器，以及保护完整性的 HMAC 值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activation {
    /// 已绑定的机器列表
    pub machines: Vec<MachineBinding>,
    /// HMAC-SHA256 绑定值（hex 编码），保护整个 machines 列表的完整性
    pub binding_hmac: String,
}

/// 单台机器的绑定记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineBinding {
    /// 机器指纹（SHA-256 hex）
    pub machine_fingerprint: String,
    /// 该机器的激活时间
    pub activated_at: DateTime<Utc>,
    /// 该机器最后一次成功验证时间（单调递增）
    pub last_verified: DateTime<Utc>,
}

/// 哨兵文件数据（字段名缩短，避免文件内容出现明文关键词）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelData {
    /// `license_id` 的 SHA-256 前 16 位（不存明文 `license_id`）
    pub id: String,
    /// 机器指纹
    pub fp: String,
    /// 激活时间
    pub ts: DateTime<Utc>,
    /// license 时钟值
    pub ck: DateTime<Utc>,
    /// HMAC 校验
    pub hm: String,
}
