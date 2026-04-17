//! License 授权模块
//!
//! 提供 terrasync 的离线 license 验证、激活和生成功能。
//!
//! ## 架构
//! - 生成工具（`license-gen`）使用 Ed25519 私钥签发 license 文件
//! - terrasync 使用编译嵌入的公钥验证签名
//! - 首次激活时采集机器指纹并通过 HMAC 绑定
//! - 后续运行时验证签名 + 指纹 + 时间

// 标准库
use std::sync::OnceLock;

// 外部 crate
// 无

// 公共模块
pub mod activate;
pub mod clock;
pub mod crypto_utils;
pub mod error;
pub mod fingerprint;
pub mod generate;
pub mod keys;
pub mod prelude;
pub mod sentinel;
pub mod types;
pub mod verify;

/// 全局 License 状态（启动验证后设置，后续 `quick_verify` 读取）
static GLOBAL_LICENSE: OnceLock<types::LicenseFile> = OnceLock::new();

/// 设置全局 License（启动验证后调用一次）
pub fn set_global_license(license: types::LicenseFile) -> error::Result<()> {
    GLOBAL_LICENSE
        .set(license)
        .map_err(|_| error::LicenseError::CryptoError("Global license already initialized".to_string()))
}

/// 获取全局 License 引用
pub fn get_global_license() -> error::Result<&'static types::LicenseFile> {
    GLOBAL_LICENSE.get().ok_or(error::LicenseError::NotActivated)
}
