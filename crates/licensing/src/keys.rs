// 标准库
// 无

// 外部 crate
// 无

// 内部模块
// 无

/// Ed25519 公钥（Base64 编码），编译嵌入 terrasync 二进制
///
/// 由 `license-gen keygen` 命令生成，需要手动更新此常量。
/// 对应的私钥保存在安全位置，仅管理员持有。
pub const EMBEDDED_PUBLIC_KEY: &str = "PLACEHOLDER_PUBLIC_KEY_BASE64";

/// HMAC 绑定密钥种子
///
/// 与 Ed25519 签名组合，通过 HKDF 派生实际的 HMAC 密钥。
/// 由 `license-gen keygen` 命令生成，需要手动更新此常量。
pub const HMAC_BINDING_SECRET: &[u8; 32] = b"PLACEHOLDER_HMAC_SECRET_32BYTES!";
