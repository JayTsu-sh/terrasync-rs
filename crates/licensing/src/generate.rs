// 标准库
use std::fs;
use std::path::Path;

// 外部 crate
#[cfg(feature = "generate")]
use base64::Engine;
#[cfg(feature = "generate")]
use base64::engine::general_purpose::STANDARD as BASE64;
#[cfg(feature = "generate")]
use chrono::{Duration, Utc};
#[cfg(feature = "generate")]
use ed25519_dalek::{Signer, SigningKey};
#[cfg(feature = "generate")]
use rand::RngCore;
#[cfg(feature = "generate")]
use rand::rngs::OsRng;

// 内部模块
use crate::error::{LicenseError, Result};
use crate::types::LicenseFile;
#[cfg(feature = "generate")]
use crate::types::Payload;

/// 生成 Ed25519 密钥对
///
/// 返回 (`private_key_bytes`, `public_key_bytes`)
#[cfg(feature = "generate")]
pub fn generate_keypair() -> ([u8; 32], [u8; 32]) {
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    (signing_key.to_bytes(), verifying_key.to_bytes())
}

/// 生成 32 字节随机 HMAC secret
#[cfg(feature = "generate")]
pub fn generate_hmac_secret() -> [u8; 32] {
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    secret
}

/// 保存密钥对到文件，并输出 Rust 代码片段
#[cfg(feature = "generate")]
pub fn save_keypair(output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    let (private_key, public_key) = generate_keypair();
    let hmac_secret = generate_hmac_secret();

    // 保存私钥
    let private_key_path = output_dir.join("private.key");
    fs::write(&private_key_path, private_key)?;
    println!("Private key saved to: {}", private_key_path.display());

    // 保存公钥
    let public_key_path = output_dir.join("public.key");
    fs::write(&public_key_path, public_key)?;
    println!("Public key saved to: {}", public_key_path.display());

    // 保存 HMAC secret
    let secret_path = output_dir.join("hmac_secret.key");
    fs::write(&secret_path, hmac_secret)?;
    println!("HMAC secret saved to: {}", secret_path.display());

    // 输出 Rust 代码片段
    println!("\n=== 复制以下内容到 licensing/src/keys.rs ===\n");
    println!(
        "pub const EMBEDDED_PUBLIC_KEY: &str = \"{}\";",
        BASE64.encode(public_key)
    );
    println!("pub const HMAC_BINDING_SECRET: &[u8; 32] = &{hmac_secret:?};");
    println!("\n=== End ===");

    Ok(())
}

/// 生成签名的 license 文件
#[cfg(feature = "generate")]
pub fn generate_license(
    private_key_path: &Path, licensee: &str, days: Option<u32>, max_machines: u32, activation_window_days: u32,
    output_path: &Path,
) -> Result<()> {
    // 读取私钥
    let private_key_bytes: [u8; 32] = fs::read(private_key_path)
        .map_err(|e| LicenseError::CryptoError(format!("Failed to read private key: {e}")))?
        .try_into()
        .map_err(|_| LicenseError::CryptoError("Invalid private key length (expected 32 bytes)".to_string()))?;

    let signing_key = SigningKey::from_bytes(&private_key_bytes);

    // 构建 payload
    let now = Utc::now();
    let payload = Payload {
        license_id: uuid::Uuid::new_v4().to_string(),
        licensee: licensee.to_string(),
        issued_at: now,
        expires_at: days.map(|d| now + Duration::days(i64::from(d))),
        activation_window_days,
        max_machines,
    };

    // 序列化并签名
    let payload_json = serde_json::to_string(&payload).map_err(|e| LicenseError::InvalidFormat(e.to_string()))?;
    let signature = signing_key.sign(payload_json.as_bytes());

    let license = LicenseFile {
        payload,
        signature: BASE64.encode(signature.to_bytes()),
        activation: None,
        license_clock: None,
    };

    // 写出
    let content = serde_json::to_string_pretty(&license)?;
    fs::write(output_path, content)?;

    println!("License generated: {}", output_path.display());
    println!("  Licensee: {licensee}");
    if let Some(d) = days {
        println!("  Valid for: {d} days");
        println!(
            "  Expires: {}",
            license.payload.expires_at.map_or("N/A".to_string(), |e| e.to_rfc3339())
        );
    } else {
        println!("  Valid for: permanent");
    }
    println!(
        "  Max machines: {}",
        if max_machines == 0 {
            "unlimited".to_string()
        } else {
            max_machines.to_string()
        }
    );
    println!("  Activation window: {activation_window_days} days");

    Ok(())
}

/// 显示 license 文件信息
pub fn show_license_info(license_path: &Path) -> Result<()> {
    let content = fs::read_to_string(license_path)?;
    let license: LicenseFile =
        serde_json::from_str(&content).map_err(|e| LicenseError::InvalidFormat(e.to_string()))?;

    println!("=== License Information ===");
    println!("  License ID: {}", license.payload.license_id);
    println!("  Licensee: {}", license.payload.licensee);
    println!("  Issued at: {}", license.payload.issued_at.to_rfc3339());
    println!(
        "  Expires at: {}",
        license
            .payload
            .expires_at
            .map_or("permanent".to_string(), |e| e.to_rfc3339())
    );
    println!(
        "  Max machines: {}",
        if license.payload.max_machines == 0 {
            "unlimited".to_string()
        } else {
            license.payload.max_machines.to_string()
        }
    );
    println!("  Activation window: {} days", license.payload.activation_window_days);

    match &license.activation {
        None => {
            println!("  Status: NOT ACTIVATED");
        }
        Some(activation) => {
            println!("  Status: ACTIVATED ({} machine(s))", activation.machines.len());
            for (i, m) in activation.machines.iter().enumerate() {
                println!("  Machine #{}: ", i + 1);
                println!("    Fingerprint: {}", m.machine_fingerprint);
                println!("    Activated at: {}", m.activated_at.to_rfc3339());
                println!("    Last verified: {}", m.last_verified.to_rfc3339());
            }
        }
    }

    Ok(())
}
