// 标准库
// 无

// 外部 crate
use sha2::{Digest, Sha256};

// 内部模块
use crate::error::{LicenseError, Result};

/// 采集当前机器的指纹
///
/// Linux 下组合三个源：
/// - `/etc/machine-id` (systemd 安装时生成)
/// - `/sys/class/dmi/id/product_uuid` (SMBIOS UUID)
/// - 首个物理网卡 MAC 地址
///
/// 三者拼接后 SHA-256 哈希，返回 hex 字符串。
#[cfg(target_os = "linux")]
pub fn collect_fingerprint() -> Result<String> {
    let machine_id = read_trimmed("/etc/machine-id")
        .map_err(|e| LicenseError::FingerprintError(format!("Failed to read /etc/machine-id: {e}")))?;

    let product_uuid = read_trimmed("/sys/class/dmi/id/product_uuid").unwrap_or_else(|_| "no-product-uuid".to_string());

    let mac = get_first_physical_mac()
        .map_err(|e| LicenseError::FingerprintError(format!("Failed to get MAC address: {e}")))?;

    let disk_serial = get_first_disk_serial_linux().unwrap_or_else(|_| "no-disk-serial".to_string());

    let combined = format!("{machine_id}:{product_uuid}:{mac}:{disk_serial}");
    Ok(sha256_hex(combined.as_bytes()))
}

/// Windows 下采集机器指纹
///
/// 组合：
/// - `HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid`
/// - 首个物理网卡 MAC 地址
/// - 首个磁盘序列号
#[cfg(target_os = "windows")]
pub fn collect_fingerprint() -> Result<String> {
    let machine_guid = read_windows_machine_guid()
        .map_err(|e| LicenseError::FingerprintError(format!("Failed to read MachineGuid: {e}")))?;

    let mac = get_first_physical_mac_windows().unwrap_or_else(|_| "no-mac-address".to_string());

    let disk_serial = get_first_disk_serial_windows().unwrap_or_else(|_| "no-disk-serial".to_string());

    let combined = format!("{machine_guid}:{mac}:{disk_serial}");
    Ok(sha256_hex(combined.as_bytes()))
}

/// 计算 SHA-256 哈希并返回 hex 字符串
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// 读取文件内容并去除首尾空白
#[cfg(target_os = "linux")]
fn read_trimmed(path: &str) -> std::io::Result<String> {
    let content = std::fs::read_to_string(path)?;
    Ok(content.trim().to_string())
}

/// 获取首个物理网卡的 MAC 地址（Linux）
///
/// 跳过回环接口和常见虚拟接口（docker0, veth*, virbr*, br-*）
#[cfg(target_os = "linux")]
fn get_first_physical_mac() -> std::io::Result<String> {
    let net_dir = "/sys/class/net";
    let mut entries: Vec<_> = std::fs::read_dir(net_dir)?
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| !is_virtual_interface(name))
        .collect();

    // 按名称排序，保证一致性
    entries.sort();

    for iface in &entries {
        let addr_path = format!("{net_dir}/{iface}/address");
        if let Ok(mac) = std::fs::read_to_string(&addr_path) {
            let mac = mac.trim();
            // 跳过全零 MAC（未启用的接口）
            if !mac.is_empty() && mac != "00:00:00:00:00:00" {
                return Ok(mac.to_string());
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No physical network interface found",
    ))
}

/// 判断网络接口是否为虚拟接口
#[cfg(target_os = "linux")]
fn is_virtual_interface(name: &str) -> bool {
    let virtual_prefixes = ["lo", "docker", "veth", "virbr", "br-", "vnet", "tun", "tap"];
    virtual_prefixes.iter().any(|prefix| name.starts_with(prefix))
}

/// 读取 Windows MachineGuid 注册表项
#[cfg(target_os = "windows")]
fn read_windows_machine_guid() -> std::io::Result<String> {
    use std::process::Command;

    let output = Command::new("reg")
        .args(["query", r"HKLM\SOFTWARE\Microsoft\Cryptography", "/v", "MachineGuid"])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    // 输出格式: "    MachineGuid    REG_SZ    xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    for line in stdout.lines() {
        if line.contains("MachineGuid") {
            if let Some(guid) = line.split_whitespace().last() {
                return Ok(guid.to_string());
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "MachineGuid not found in registry",
    ))
}

/// 获取首个物理网卡的 MAC 地址（Windows）
#[cfg(target_os = "windows")]
fn get_first_physical_mac_windows() -> std::io::Result<String> {
    use std::process::Command;

    let output = Command::new("getmac").args(["/fo", "csv", "/nh", "/v"]).output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        // CSV 格式: "连接名","网络适配器","物理地址","传输名称"
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() >= 3 {
            let mac = fields[2].trim_matches('"').trim();
            if !mac.is_empty() && mac != "N/A" && mac != "已断开连接" {
                return Ok(mac.replace('-', ":").to_lowercase());
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No physical network interface found",
    ))
}

/// 获取首个磁盘序列号（Linux）
///
/// 大多数 hypervisor 克隆 VM 时会为虚拟磁盘生成新的序列号。
#[cfg(target_os = "linux")]
fn get_first_disk_serial_linux() -> std::io::Result<String> {
    let block_dir = "/sys/block";
    let mut disks: Vec<_> = std::fs::read_dir(block_dir)?
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        // 仅物理磁盘（sd*, vd*, nvme*），排除 loop/ram/dm 等
        .filter(|name| {
            name.starts_with("sd") || name.starts_with("vd") || name.starts_with("nvme") || name.starts_with("hd")
        })
        .collect();

    disks.sort();

    for disk in &disks {
        let serial_path = format!("{block_dir}/{disk}/serial");
        if let Ok(serial) = std::fs::read_to_string(&serial_path) {
            let serial = serial.trim();
            if !serial.is_empty() {
                return Ok(serial.to_string());
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No disk serial number found",
    ))
}

/// 获取首个磁盘序列号（Windows）
#[cfg(target_os = "windows")]
fn get_first_disk_serial_windows() -> std::io::Result<String> {
    use std::process::Command;

    let output = Command::new("wmic")
        .args(["diskdrive", "get", "serialnumber"])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().skip(1) {
        let serial = line.trim();
        if !serial.is_empty() {
            return Ok(serial.to_string());
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No disk serial number found",
    ))
}
