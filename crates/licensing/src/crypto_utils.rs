// 标准库
// 无

// 外部 crate
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, VerifyingKey};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

// 内部模块
use crate::error::{LicenseError, Result};
use crate::keys::{EMBEDDED_PUBLIC_KEY, HMAC_BINDING_SECRET};
use crate::types::{MachineBinding, SentinelData};

type HmacSha256 = Hmac<Sha256>;

/// 使用内嵌公钥验证 Ed25519 签名
pub fn verify_signature(payload_json: &str, signature_base64: &str) -> Result<()> {
    let pub_key_bytes = BASE64
        .decode(EMBEDDED_PUBLIC_KEY)
        .map_err(|e| LicenseError::CryptoError(format!("Failed to decode public key: {e}")))?;

    let verifying_key = VerifyingKey::from_bytes(
        pub_key_bytes
            .as_slice()
            .try_into()
            .map_err(|_| LicenseError::CryptoError("Invalid public key length".to_string()))?,
    )
    .map_err(|e| LicenseError::CryptoError(format!("Invalid public key: {e}")))?;

    let sig_bytes = BASE64
        .decode(signature_base64)
        .map_err(|e| LicenseError::CryptoError(format!("Failed to decode signature: {e}")))?;

    let signature = Signature::from_bytes(
        sig_bytes
            .as_slice()
            .try_into()
            .map_err(|_| LicenseError::CryptoError("Invalid signature length".to_string()))?,
    );

    verifying_key
        .verify_strict(payload_json.as_bytes(), &signature)
        .map_err(|_| LicenseError::SignatureInvalid)
}

/// 计算机器列表的 binding HMAC
///
/// HMAC 密钥通过 HKDF 从 HMAC_BINDING_SECRET 和 Ed25519 签名联合派生。
/// 消息为所有机器绑定信息的序列化。
pub fn compute_binding_hmac(
    signature: &str, machines: &[MachineBinding], license_clock: Option<DateTime<Utc>>,
) -> Result<String> {
    let derived_key = derive_hmac_key(signature)?;

    let message = build_hmac_message(machines, license_clock);

    let mut mac = HmacSha256::new_from_slice(&derived_key)
        .map_err(|e| LicenseError::CryptoError(format!("HMAC init failed: {e}")))?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

/// 使用 HKDF 从 HMAC_BINDING_SECRET + signature 派生 HMAC 密钥
fn derive_hmac_key(signature: &str) -> Result<[u8; 32]> {
    let hk = Hkdf::<Sha256>::new(Some(signature.as_bytes()), HMAC_BINDING_SECRET);
    let mut okm = [0u8; 32];
    hk.expand(b"binding-v1", &mut okm)
        .map_err(|e| LicenseError::CryptoError(format!("HKDF expand failed: {e}")))?;
    Ok(okm)
}

/// 将机器绑定列表序列化为 HMAC 消息
fn build_hmac_message(machines: &[MachineBinding], license_clock: Option<DateTime<Utc>>) -> String {
    let mut parts: Vec<String> = machines
        .iter()
        .map(|m| {
            format!(
                "{}:{}:{}",
                m.machine_fingerprint,
                m.activated_at.to_rfc3339(),
                m.last_verified.to_rfc3339()
            )
        })
        .collect();

    // 将时钟值纳入 HMAC 保护范围
    if let Some(clock) = license_clock {
        parts.push(format!("clock:{}", clock.to_rfc3339()));
    }

    parts.join("|")
}

/// 计算 SHA-256 并返回 hex 字符串
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// 计算哨兵 HMAC
pub fn compute_sentinel_hmac(license_id: &str, fingerprint: &str, activated_at: &DateTime<Utc>) -> Result<String> {
    let message = format!("{}:{}:{}", license_id, fingerprint, activated_at.to_rfc3339());
    let mut mac = HmacSha256::new_from_slice(HMAC_BINDING_SECRET)
        .map_err(|e| LicenseError::CryptoError(format!("HMAC init failed: {e}")))?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

/// 验证哨兵 HMAC
pub fn verify_sentinel_hmac(sentinel: &SentinelData) -> Result<()> {
    let expected = compute_sentinel_hmac(&sentinel.id, &sentinel.fp, &sentinel.ts)?;
    if expected != sentinel.hm {
        return Err(LicenseError::BindingHmacInvalid);
    }
    Ok(())
}
