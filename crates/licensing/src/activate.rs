// 标准库
use std::path::Path;

// 外部 crate
use chrono::{Duration, Utc};
use tracing::info;

// 内部模块
use crate::crypto_utils::{compute_binding_hmac, compute_sentinel_hmac, sha256_hex, verify_signature};
use crate::error::{LicenseError, Result};
use crate::fingerprint::collect_fingerprint;
use crate::sentinel::{read_sentinel, write_sentinel};
use crate::types::{Activation, MachineBinding, SentinelData};
use crate::verify::{read_license, serialize_payload, write_license_atomic};

/// 激活 license（显式 activate 命令调用）
///
/// 流程：
/// 1. 读取并验签
/// 2. 检查激活窗口期
/// 3. 采集机器指纹
/// 4. 哨兵检查（防止重复激活）
/// 5. 如果已有 activation，检查是否可以追加新机器
/// 6. 写入 binding + 哨兵 并写回文件
pub fn activate_license(license_path: &Path) -> Result<()> {
    let mut license = read_license(license_path)?;
    let payload_json = serialize_payload(&license)?;

    // 1. Ed25519 签名验证
    verify_signature(&payload_json, &license.signature)?;

    // 2. 不限机器数的 license 无需激活
    if license.payload.max_machines == 0 {
        info!("License does not require machine binding (max_machines=0)");
        return Ok(());
    }

    // 3. 检查激活窗口期
    let now = Utc::now();
    let window = Duration::days(i64::from(license.payload.activation_window_days));
    if now - license.payload.issued_at > window {
        return Err(LicenseError::ActivationWindowExpired {
            issued_at: license.payload.issued_at.to_rfc3339(),
            window_days: license.payload.activation_window_days,
        });
    }

    // 4. 采集当前机器指纹
    let current_fp = collect_fingerprint()?;

    // 5. 哨兵检查：此机器是否已激活此 license
    let sentinel_id = sha256_hex(license.payload.license_id.as_bytes())[..16].to_string();
    if let Some(sentinel) = read_sentinel(&license.payload.license_id)?
        && sentinel.fp == current_fp
        && sentinel.id == sentinel_id
    {
        info!("This machine is already activated (sentinel exists)");
        return Ok(());
    }

    match &mut license.activation {
        None => {
            // 首次激活：创建新的 Activation
            let binding = MachineBinding {
                machine_fingerprint: current_fp.clone(),
                activated_at: now,
                last_verified: now,
            };

            let machines = vec![binding];

            // 初始化 license 时钟
            license.license_clock = Some(now);

            let binding_hmac = compute_binding_hmac(&license.signature, &machines, license.license_clock)?;

            license.activation = Some(Activation { machines, binding_hmac });

            info!(
                "License activated: licensee={}, machine_fingerprint={}",
                license.payload.licensee, current_fp
            );
        }
        Some(activation) => {
            // 已有激活记录：验证 HMAC 完整性（兼容新旧格式）
            let new_hmac = compute_binding_hmac(&license.signature, &activation.machines, license.license_clock)?;
            let legacy_hmac = compute_binding_hmac(&license.signature, &activation.machines, None)?;
            if new_hmac != activation.binding_hmac && legacy_hmac != activation.binding_hmac {
                return Err(LicenseError::BindingHmacInvalid);
            }

            // 检查本机是否已绑定
            if activation.machines.iter().any(|m| m.machine_fingerprint == current_fp) {
                info!("This machine is already activated");
                return Ok(());
            }

            // 检查是否达到最大机器数
            if activation.machines.len() >= license.payload.max_machines as usize {
                return Err(LicenseError::MaxMachinesReached {
                    max: license.payload.max_machines,
                });
            }

            // 追加新机器
            activation.machines.push(MachineBinding {
                machine_fingerprint: current_fp.clone(),
                activated_at: now,
                last_verified: now,
            });

            // 初始化/更新 license 时钟
            if license.license_clock.is_none() {
                license.license_clock = Some(now);
            }

            // 重算 HMAC（含 clock）
            activation.binding_hmac =
                compute_binding_hmac(&license.signature, &activation.machines, license.license_clock)?;

            info!(
                "Additional machine activated: licensee={}, machine_fingerprint={}, total_machines={}",
                license.payload.licensee,
                current_fp,
                activation.machines.len()
            );
        }
    }

    // 6. 原子写回 license 文件
    write_license_atomic(license_path, &license)?;

    // 7. 写入哨兵文件
    // 使用 sentinel_id（license_id 的 SHA-256 前16位）而非明文 license_id 计算 HMAC，
    // 与 verify_sentinel_hmac 保持一致（验证方只能访问 sentinel.id，不能访问原始 license_id）
    let seal_hmac = compute_sentinel_hmac(&sentinel_id, &current_fp, &now)?;
    let sentinel = SentinelData {
        id: sentinel_id,
        fp: current_fp,
        ts: now,
        ck: license.license_clock.unwrap_or(now),
        hm: seal_hmac,
    };
    write_sentinel(&sentinel)?;

    Ok(())
}
