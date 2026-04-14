// 标准库
use std::path::Path;

// 外部 crate
use chrono::Utc;
use tracing::{info, warn};

// 内部模块
use crate::clock::LicenseClock;
use crate::crypto_utils::{compute_binding_hmac, sha256_hex, verify_signature};
use crate::error::{LicenseError, Result};
use crate::fingerprint::collect_fingerprint;
use crate::sentinel::{read_sentinel, verify_sentinel, write_sentinel};
use crate::types::{LicenseFile, SentinelData};

/// 验证已激活的 license（启动时调用）
///
/// 验证签名 → 初始化时钟 → 检查绑定完整性 → 检查有效期 → 检查指纹 → 哨兵检查 → 更新时钟 → 写回
pub fn verify_license(license_path: &Path) -> Result<LicenseFile> {
    let mut license = read_license(license_path)?;
    let payload_json = serialize_payload(&license)?;

    // 1. Ed25519 签名验证
    verify_signature(&payload_json, &license.signature)?;

    // 2. 初始化 LicenseClock
    let mut clock = match license.license_clock {
        Some(persisted) => {
            warn!("License verification failed");
            LicenseClock::from_persisted(persisted)?
        }
        None => LicenseClock::initialize(),
    };

    // 3. 不限机器的 license：仅检查有效期 + 更新时钟
    if license.payload.max_machines == 0 {
        check_expiry_with_clock(&license, &clock)?;
        license.license_clock = Some(clock.now());
        write_license_atomic(license_path, &license)?;
        update_sentinel_clock(&license, None)?;
        return Ok(license);
    }

    // 4. 必须已激活
    if license.activation.is_none() {
        return Err(LicenseError::NotActivated);
    }

    // 5. HMAC 验证（兼容新旧格式）
    verify_binding_hmac_compat(&license)?;

    // 6. 有效期检查（使用时钟值）
    check_expiry_with_clock(&license, &clock)?;

    // 7. 采集当前机器指纹
    let current_fp = collect_fingerprint()?;

    // 8. 查找机器
    {
        let activation = license.activation.as_ref().ok_or(LicenseError::NotActivated)?;
        if !activation.machines.iter().any(|m| m.machine_fingerprint == current_fp) {
            warn!("License verification failed");
            return Err(LicenseError::MachineBindingMismatch);
        }
    }

    // 9. 哨兵检查
    if let Some(sentinel) = read_sentinel(&license.payload.license_id)? {
        verify_sentinel(&sentinel, &current_fp, license.license_clock)?;
    }

    // 10. 推进时钟 + 更新 last_verified
    let clock_now = clock.advance()?;
    let activation = license.activation.as_mut().ok_or(LicenseError::NotActivated)?;
    let machine = activation
        .machines
        .iter_mut()
        .find(|m| m.machine_fingerprint == current_fp)
        .ok_or(LicenseError::MachineBindingMismatch)?;
    machine.last_verified = clock_now;
    license.license_clock = Some(clock_now);

    // 11. 重算 binding_hmac（含 clock）
    activation.binding_hmac = compute_binding_hmac(&license.signature, &activation.machines, license.license_clock)?;

    // 12. 原子写回 license 文件
    write_license_atomic(license_path, &license)?;

    // 13. 更新哨兵 clock
    update_sentinel_clock(&license, Some(&current_fp))?;

    info!(
        "License verified: licensee={}, expires={:?}",
        license.payload.licensee, license.payload.expires_at
    );

    Ok(license)
}

/// 轻量级验证（不写文件，用于关键操作前的二次校验）
///
/// 仅检查：签名有效、时钟未回拨、未过期、指纹匹配、HMAC 有效
pub fn quick_verify(license: &LicenseFile) -> Result<()> {
    let payload_json = serialize_payload(license)?;
    verify_signature(&payload_json, &license.signature)?;

    // 使用时钟检查过期（如果有 license_clock）
    match license.license_clock {
        Some(persisted) => {
            let clock = LicenseClock::from_persisted(persisted)?;
            check_expiry_with_clock(license, &clock)?;
        }
        None => check_expiry(license)?,
    }

    if license.payload.max_machines == 0 {
        return Ok(());
    }

    // HMAC 验证（兼容新旧格式）
    verify_binding_hmac_compat(license)?;

    let current_fp = collect_fingerprint()?;
    let activation = license.activation.as_ref().ok_or(LicenseError::NotActivated)?;
    if !activation.machines.iter().any(|m| m.machine_fingerprint == current_fp) {
        return Err(LicenseError::MachineBindingMismatch);
    }

    Ok(())
}

/// 使用 license 时钟检查有效期
fn check_expiry_with_clock(license: &LicenseFile, clock: &LicenseClock) -> Result<()> {
    if let Some(expires_at) = license.payload.expires_at {
        if clock.now() > expires_at {
            warn!("License verification failed");
            return Err(LicenseError::Expired {
                expires_at: expires_at.to_rfc3339(),
            });
        }
    }
    Ok(())
}

/// 检查有效期（旧格式回退，使用系统时钟）
fn check_expiry(license: &LicenseFile) -> Result<()> {
    if let Some(expires_at) = license.payload.expires_at {
        if Utc::now() > expires_at {
            return Err(LicenseError::Expired {
                expires_at: expires_at.to_rfc3339(),
            });
        }
    }
    Ok(())
}

/// HMAC 验证（兼容新旧格式）
///
/// 先尝试新格式（含 license_clock），失败回退旧格式（不含 clock），
/// 处理升级迁移场景。
fn verify_binding_hmac_compat(license: &LicenseFile) -> Result<()> {
    let activation = license.activation.as_ref().ok_or(LicenseError::NotActivated)?;

    // 先尝试新格式（含 license_clock）
    let new_hmac = compute_binding_hmac(&license.signature, &activation.machines, license.license_clock)?;
    if new_hmac == activation.binding_hmac {
        return Ok(());
    }

    // 回退：尝试旧格式（不含 license_clock）
    let legacy_hmac = compute_binding_hmac(&license.signature, &activation.machines, None)?;
    if legacy_hmac == activation.binding_hmac {
        info!("Legacy HMAC format detected, will upgrade on write-back");
        return Ok(());
    }

    warn!("License verification failed");
    Err(LicenseError::BindingHmacInvalid)
}

/// 更新哨兵文件的 license_clock
fn update_sentinel_clock(license: &LicenseFile, fingerprint: Option<&str>) -> Result<()> {
    let license_id = &license.payload.license_id;
    if let Some(sentinel) = read_sentinel(license_id)? {
        // 更新现有哨兵的 clock
        let mut updated = sentinel;
        if let Some(clock) = license.license_clock {
            updated.ck = clock;
        }
        write_sentinel(&updated)?;
    } else if let Some(fp) = fingerprint {
        // 迁移场景：首次运行时无哨兵，创建一个
        if let Some(clock) = license.license_clock {
            let sentinel_id = sha256_hex(license_id.as_bytes())[..16].to_string();
            let hm = crate::crypto_utils::compute_sentinel_hmac(license_id, fp, &clock)?;
            let sentinel = SentinelData {
                id: sentinel_id,
                fp: fp.to_string(),
                ts: clock,
                ck: clock,
                hm,
            };
            write_sentinel(&sentinel)?;
        }
    }
    Ok(())
}

/// 读取并解析 license 文件
pub fn read_license(path: &Path) -> Result<LicenseFile> {
    if !path.exists() {
        return Err(LicenseError::FileNotFound(path.display().to_string()));
    }
    let content = std::fs::read_to_string(path)?;
    let license: LicenseFile =
        serde_json::from_str(&content).map_err(|e| LicenseError::InvalidFormat(e.to_string()))?;
    Ok(license)
}

/// 序列化 payload 为确定性 JSON（用于签名验证）
pub fn serialize_payload(license: &LicenseFile) -> Result<String> {
    serde_json::to_string(&license.payload).map_err(|e| LicenseError::InvalidFormat(e.to_string()))
}

/// 原子写回 license 文件（临时文件 + rename）
pub fn write_license_atomic(path: &Path, license: &LicenseFile) -> Result<()> {
    let content = serde_json::to_string_pretty(license)?;

    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &content)?;
    std::fs::rename(&tmp_path, path)?;

    Ok(())
}
