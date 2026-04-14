//! CLI命令执行模块
//!
//! 该模块实现了应用程序的各种命令执行逻辑，包括：
//! 1. 扫描命令
//! 2. 同步命令
//! 3. 配置命令
//! 4. Windows ACE命令（仅Windows平台）
//! 5. 辅助函数
//! 6. 参数校验函数

// 标准库
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

// 外部crate
use app::config::SyncJobConfig;
use app::orchestrator::SyncOrchestrator;
use app::prelude::{ScanType, incremental_sync, integrity_check, parse_bandwidth_string, rm, scan, sync};
use storage_v2::create_storage_for_dest;
use tracing::info;
use transport::quic;
use utils::app_config::AppConfig;
#[cfg(target_os = "windows")]
use {
    app::prelude::{copy_aces, list_security_info},
    utils::logger,
};

// 内部模块
use crate::error::{CliError, Result};

// ==================== 参数校验函数 ====================

/// 验证qos参数格式
/// 支持格式：数字 + 任意空格 + 单位（大小写不敏感）
/// 单位支持：gib/s, gib, gb/s, gb, mib/s, mib, mb/s, mb, kib/s, kib, kb/s, kb, b/s, b
pub fn validate_qos_format(s: &str) -> Result<String> {
    match parse_bandwidth_string(s) {
        Ok(_) => Ok(s.to_string()),
        Err(_) => Err(CliError::InvalidParameter(
            "无效的qos格式。支持格式如'1GiB/s'或'200MiB/s'，单位支持：GiB/s, MiB/s".to_string(),
        )),
    }
}

/// 校验并 trim 存储路径（本地 / nfs:// / s3:// / s3+http:// / s3+https:// / s3+hcp://）
/// 参考 storage_v2/src/storage_enum.rs 和 storage_v2/src/s3.rs 中的协议解析逻辑
pub fn validate_storage_path(s: &str) -> Result<String> {
    let s = s.trim();
    if s.is_empty() {
        return Err(CliError::InvalidParameter("路径不能为空".to_string()));
    }
    Ok(s.to_string())
}

/// 校验 block_size 格式（复用 parse_bandwidth_string 单位解析）
/// 提前了 app/sync.rs 中 parse_size() 的格式校验部分
pub fn validate_block_size(s: &str) -> Result<String> {
    match parse_bandwidth_string(s) {
        Ok(_) => Ok(s.to_string()),
        Err(_) => Err(CliError::InvalidParameter(
            "无效的 block_size 格式。支持格式如 '4MiB'、'16MiB'".to_string(),
        )),
    }
}

/// 校验 peak_qos_rate 范围（必须 > 0）
pub fn validate_peak_qos_rate(s: &str) -> Result<f32> {
    let val: f32 = s
        .parse()
        .map_err(|_| CliError::InvalidParameter("peak_qos_rate 必须为正浮点数".to_string()))?;
    if val <= 0.0 {
        return Err(CliError::InvalidParameter("peak_qos_rate 必须 > 0".to_string()));
    }
    Ok(val)
}

/// 校验 iops 范围（必须 > 0）
pub fn validate_iops(s: &str) -> Result<u32> {
    let val: u32 = s
        .parse()
        .map_err(|_| CliError::InvalidParameter("iops 必须为正整数".to_string()))?;
    if val == 0 {
        return Err(CliError::InvalidParameter("iops 必须 > 0".to_string()));
    }
    Ok(val)
}

// ==================== 辅助函数 ====================

/// 准备job目录（job_id 已由 cli_match 中的 resolve_job_id 确定）
fn prepare_job(job_type: &str, job_id: &str) -> Result<(String, bool)> {
    let jobs_dir = "jobs";
    fs::create_dir_all(jobs_dir)?;

    // 构建job目录路径
    let job_dir = format!("{}/{}_{}", jobs_dir, job_type, job_id);
    let job_path_exists = Path::new(&job_dir).exists();

    // 如果是全量操作，创建目录
    if !job_path_exists {
        fs::create_dir_all(&job_dir)?;
        info!("Created job directory for full {}: {}", job_type, job_dir);
    }

    Ok((job_dir, job_path_exists))
}

// ==================== 命令处理函数 ====================

/// 处理扫描命令
pub async fn scan_cmd(
    job_id: &str, path: &str, depth: u32, r#match: &Option<String>, exclude: &Option<String>, raw_command_line: String,
) -> Result<()> {
    let config = AppConfig::fetch()?;

    // CLI 参数优先，配置文件值作为 fallback
    let effective_depth = if depth != 0 {
        depth
    } else {
        config.scan.depth.unwrap_or(0)
    };
    let effective_match = r#match.as_ref().or(config.scan.r#match.as_ref()).cloned();
    let effective_exclude = exclude.as_ref().or(config.scan.exclude.as_ref()).cloned();

    let (job_dir, job_path_exists) = prepare_job("scan", job_id)?;

    // 确定扫描类型
    let scan_type = if job_path_exists {
        ScanType::Incremental
    } else {
        ScanType::Full
    };
    scan(
        job_id.to_string(),
        job_dir,
        scan_type,
        path,
        effective_depth,
        &effective_match,
        &effective_exclude,
        raw_command_line,
        None,
        None,
    )
    .await?;

    Ok(())
}

/// 处理复制命令
pub async fn sync_cmd(
    job_id: &str, src_path: &Option<String>, dest_path: &Option<String>, enable_integrity_check: bool,
    enable_acl: bool, r#match: &Option<String>, exclude: &Option<String>, qos: &Option<String>, peak_qos_rate: f32,
    iops: Option<u32>, block_size: &Option<String>, file_list: &Option<String>, packaged: bool,
    package_depth: Option<usize>, remote: &Option<String>, tls_server_cert: &Option<String>, raw_command_line: String,
) -> Result<()> {
    let config = AppConfig::fetch()?;

    // CLI 参数优先，配置文件值作为 fallback
    let effective_enable_integrity_check = if enable_integrity_check {
        true
    } else {
        config.sync.enable_integrity_check.unwrap_or(false)
    };
    let effective_enable_acl = if enable_acl {
        true
    } else {
        config.sync.enable_acl.unwrap_or(false)
    };
    let effective_match = r#match.as_ref().or(config.scan.r#match.as_ref()).cloned();
    let effective_exclude = exclude.as_ref().or(config.scan.exclude.as_ref()).cloned();
    let effective_qos = qos.as_ref().or(config.sync.qos.as_ref()).cloned();
    let effective_peak_qos_rate = if peak_qos_rate != 2.0 {
        peak_qos_rate
    } else {
        config.sync.peak_qos_rate.unwrap_or(2.0)
    };
    let effective_block_size = block_size.as_ref().or(config.sync.block_size.as_ref()).cloned();

    // 双进程模式提示
    if let Some(addr) = remote {
        info!("Remote mode enabled, connecting to Receiver at {}", addr);
    }

    let (job_dir, job_path_exists) = prepare_job("replicate", job_id)?;

    // 确定同步类型
    let scan_type = if job_path_exists {
        ScanType::Incremental
    } else {
        ScanType::Full
    };

    let src_path_str = src_path
        .as_ref()
        .ok_or_else(|| CliError::InvalidParameter("src_path is required".to_string()))?
        .as_str();
    let dest_path_str = dest_path
        .as_ref()
        .ok_or_else(|| CliError::InvalidParameter("dest_path is required".to_string()))?
        .as_str();

    // 双进程模式：通过 QUIC 连接远端 Receiver
    if let Some(remote_addr) = remote {
        // 加载服务端证书（防 MITM）
        let tls_cert_bytes: Option<Vec<u8>> = match tls_server_cert {
            Some(path) => match std::fs::read(path) {
                Ok(bytes) => {
                    info!("[sync] Loaded TLS server cert from {}", path);
                    Some(bytes)
                }
                Err(e) => {
                    return Err(CliError::InvalidParameter(format!(
                        "Failed to read TLS server cert '{}': {}",
                        path, e
                    )));
                }
            },
            None => None,
        };

        let config = SyncJobConfig {
            job_id: job_id.to_string(),
            job_dir,
            src_path: src_path_str.to_string(),
            dest_path: dest_path_str.to_string(),
            enable_integrity_check: effective_enable_integrity_check,
            enable_acl: effective_enable_acl,
            scan_type,
            r#match: effective_match,
            exclude: effective_exclude,
            qos: effective_qos,
            peak_qos_rate: effective_peak_qos_rate,
            block_size: effective_block_size,
            file_list: file_list.clone(),
            iops,
            packaged,
            package_depth: package_depth.unwrap_or(0),
            raw_command_line,
            progress_callback_url: None,
        };
        return SyncOrchestrator::new_remote(config, remote_addr, tls_cert_bytes)
            .run()
            .await
            .map_err(CliError::AppError);
    }

    match scan_type {
        ScanType::Incremental => {
            info!("Starting incremental replicate");
            incremental_sync(
                job_id.to_string(),
                job_dir,
                src_path_str,
                dest_path_str,
                effective_enable_integrity_check,
                effective_enable_acl,
                scan_type,
                &effective_match,
                &effective_exclude,
                &effective_qos,
                effective_peak_qos_rate,
                &effective_block_size,
                iops,
                packaged,
                package_depth.unwrap_or(0),
                raw_command_line,
                None,
            )
            .await?;
        }
        ScanType::Full => {
            sync(
                job_id.to_string(),
                job_dir,
                src_path_str,
                dest_path_str,
                effective_enable_integrity_check,
                effective_enable_acl,
                scan_type,
                &effective_match,
                &effective_exclude,
                &effective_qos,
                effective_peak_qos_rate,
                &effective_block_size,
                file_list,
                iops,
                packaged,
                package_depth.unwrap_or(0),
                raw_command_line,
                None,
            )
            .await?;
        }
    }

    Ok(())
}

/// Show the configuration file
pub fn config() -> Result<()> {
    let config = AppConfig::fetch()?;
    println!("{:#?}", config);

    Ok(())
}

#[cfg(target_os = "windows")]
/// 处理ACE list命令
pub async fn ace_list_cmd(
    job_id: &str, path: &str, owner: &Option<String>, depth: u32, r#match: &Option<String>, exclude: &Option<String>,
    include_inherited: bool, raw_command_line: String,
) -> Result<()> {
    let (_, _) = prepare_job("scan_ace", job_id)?;
    let log_path = logger::get_current_app_log_path();

    println!("Command: {}", raw_command_line);
    println!("Job ID: {}", job_id);
    println!("Log Path: {}", log_path);

    match list_security_info(job_id, path, owner, depth, r#match, exclude, include_inherited).await {
        Ok(_) => Ok(()),
        Err(e) => Err(crate::error::CliError::ExecutionError(format!(
            "No security info available for path: {} - Error: {}",
            path, e
        ))),
    }
}

#[cfg(target_os = "windows")]
/// 处理ACE copy命令
pub async fn ace_copy_cmd(
    job_id: &str, source_path: &str, target_path: &str, depth: u32, r#match: &Option<String>, exclude: &Option<String>,
    raw_command_line: String,
) -> Result<()> {
    let (_, _) = prepare_job("copy_ace", job_id)?;
    let log_path = logger::get_current_app_log_path();

    println!("Command: {}", raw_command_line);
    println!("Job ID: {}", job_id);
    println!("Log Path: {}", log_path);

    match copy_aces(job_id, source_path, target_path, depth, r#match, exclude).await {
        Ok(_) => Ok(()),
        Err(e) => Err(crate::error::CliError::ExecutionError(format!(
            "Failed to copy ACEs from {} to {} - Error: {}",
            source_path, target_path, e
        ))),
    }
}

/// 处理删除命令
pub async fn rm_cmd(path: &str) -> Result<()> {
    info!("执行删除命令，路径: {}", path);

    rm(path).await?;

    Ok(())
}

/// 处理完整性检查命令
pub async fn integrity_check_cmd(
    job_id: &str, src_path: &str, dest_path: &str, quick: bool, auto_fix: bool, raw_command_line: String,
) -> Result<()> {
    let (job_dir, _) = prepare_job("integrity_check", job_id)?;

    info!(
        "执行完整性检查命令，源路径: {}, 目标路径: {}, 快速模式: {}, 自动修复: {}",
        src_path, dest_path, quick, auto_fix
    );

    integrity_check(
        job_id.to_string(),
        job_dir,
        src_path,
        dest_path,
        quick,
        auto_fix,
        raw_command_line,
        None,
    )
    .await?;

    Ok(())
}

/// 启动 Web GUI 管理界面
#[cfg(feature = "gui")]
pub async fn gui_cmd(host: &str, port: u16) -> Result<()> {
    info!("Starting Web GUI on {}:{}", host, port);
    web::start_web_server(host, port).await?;
    Ok(())
}

/// 启动 Receiver daemon（双进程模式目标端）
///
/// 监听 QUIC 连接，接收 Sender 的分页文件列表，比较后请求传输。
/// 生成的 TLS 证书写入 `tls_cert_out`，Sender 侧通过 `--tls-server-cert` 加载以防 MITM。
pub async fn serve_cmd(listen: &str, dest_path: &str, tls_cert_out: &str) -> Result<()> {
    info!("Starting Receiver daemon on {}, dest: {}", listen, dest_path);

    // 1. 解析监听地址
    let listen_addr: SocketAddr = listen
        .parse()
        .map_err(|e| CliError::InvalidParameter(format!("Invalid listen address: {}", e)))?;

    // 3. 创建目标端 StorageEnum
    let dest_storage = Arc::new(create_storage_for_dest(dest_path, None).await?);
    info!("Created destination storage for: {}", dest_path);

    // 4. 接受 QUIC 连接，获取服务端证书 DER
    let (receiver_transport, _endpoint, cert_der) = quic::accept(listen_addr).await?;
    info!("Accepted QUIC connection");

    // 5. 将服务端 TLS 证书写入文件（供 Sender 侧 --tls-server-cert 使用）
    if let Err(e) = std::fs::write(tls_cert_out, &cert_der) {
        tracing::warn!(
            "无法写入 TLS 证书到 '{}': {}。Sender 侧将无法验证服务端身份。",
            tls_cert_out,
            e
        );
    } else {
        info!(
            "Server TLS cert written to '{}'. Pass this to Sender: --tls-server-cert {}",
            tls_cert_out, tls_cert_out
        );
    }

    // 6. 启动双进程 receiver task
    app::receiver::receiver_task_remote(&receiver_transport, dest_storage)
        .await
        .map_err(CliError::AppError)?;

    info!("Receiver daemon completed");
    Ok(())
}
