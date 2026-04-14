// 标准库
use std::path::PathBuf;

// 外部 crate
use chrono::{DateTime, Utc};
use tracing::warn;

// 内部模块
use crate::crypto_utils::{sha256_hex, verify_sentinel_hmac};
use crate::error::{LicenseError, Result};
use crate::types::SentinelData;

/// 获取哨兵文件路径
///
/// 路径伪装为系统运行时文件，不包含 "terrasync" / "license" / "seal" 等关键词。
/// 文件名由 license_id 的 SHA-256 前 16 字符构成，无法从文件名反推 license。
pub fn sentinel_path(license_id: &str) -> PathBuf {
    let hash = sha256_hex(license_id.as_bytes());
    let filename = format!(".rt_{}.dat", &hash[..16]);

    #[cfg(target_os = "linux")]
    let base = PathBuf::from("/var/lib/.cache/runtime.d");

    #[cfg(target_os = "windows")]
    let base = {
        let program_data = std::env::var("ProgramData").unwrap_or_else(|_| r"C:\ProgramData".to_string());
        PathBuf::from(program_data).join(r"Microsoft\DeviceSync\runtime.d")
    };

    base.join(filename)
}

/// 读取哨兵（不存在返回 None）
pub fn read_sentinel(license_id: &str) -> Result<Option<SentinelData>> {
    let path = sentinel_path(license_id);
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)?;
    let data: SentinelData = serde_json::from_str(&content).map_err(|e| LicenseError::InvalidFormat(e.to_string()))?;
    Ok(Some(data))
}

/// 写入/更新哨兵
pub fn write_sentinel(data: &SentinelData) -> Result<()> {
    let path = sentinel_path(&data.id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string(data)?;
    // 原子写
    let tmp = path.with_extension("dat.tmp");
    std::fs::write(&tmp, &content)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// 验证哨兵一致性
pub fn verify_sentinel(
    sentinel: &SentinelData, current_fingerprint: &str, license_clock: Option<DateTime<Utc>>,
) -> Result<()> {
    // 1. HMAC 完整性验证
    verify_sentinel_hmac(sentinel)?;

    // 2. 指纹匹配检查
    if sentinel.fp != current_fingerprint {
        warn!("License verification failed");
        return Err(LicenseError::SentinelMismatch);
    }

    // 3. 时钟一致性检查：哨兵 clock 不应大于 license 文件 clock
    if let Some(file_clock) = license_clock {
        if sentinel.ck > file_clock {
            warn!("License verification failed");
            return Err(LicenseError::LicenseFileRestored {
                sentinel_clock: sentinel.ck.to_rfc3339(),
                license_clock: file_clock.to_rfc3339(),
            });
        }
    }

    Ok(())
}
