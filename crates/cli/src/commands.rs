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
use app::scan::ScanRequest;
use std::fs;
#[cfg(feature = "remote")]
use std::net::SocketAddr;
#[cfg(feature = "remote")]
use std::path::Path;
use task_model::{SourceCleanupPolicy, TransferMode};
use tokio::signal::ctrl_c;
use tokio_util::sync::CancellationToken;

// 外部crate
use app::config::SyncJobConfig;
use app::prelude::{integrity_check, parse_bandwidth_string, rm, scan, sync};
#[cfg(feature = "remote")]
use app::remote_sync::{send, serve};
use app::sync::incremental_copy_changes;
use data_mover::redact_storage_url;
use tracing::info;
use utils::app_config::AppConfig;
#[cfg(target_os = "windows")]
use {
    app::prelude::{copy_aces, list_security_info},
    utils::logger,
};

// 内部模块
use crate::error::{CliError, Result};

pub async fn copy_changes_cmd(job_id: &str, execution_id: &str, offset: u64, limit: u32) -> Result<()> {
    let page = incremental_copy_changes(job_id, execution_id, offset, limit)
        .await?
        .ok_or_else(|| CliError::InvalidParameter("incremental copy execution not found for this job".into()))?;
    println!(
        "Execution {} | {:?} | target cleanup {:?} | archive coverage {:?}",
        page.execution_id, page.phase, page.target_cleanup, page.archive_coverage
    );
    if let Some(range) = page.target_preserved_range {
        println!(
            "Target preserved scope={:?} source=unverified reason={:?}",
            range.scope, range.reason
        );
    }
    for count in page.counts {
        println!("{} {}", count.kind, count.count);
    }
    for change in page.changes {
        if let Some(previous_path) = change.previous_path {
            println!("{} {:?} -> {:?}", change.kind, previous_path, change.path);
        } else {
            print!("{} {:?}", change.kind, change.path);
            if let Some(source) = change.source_presence {
                print!(" source={source}");
            }
            if let Some(target) = change.target_outcome {
                print!(" target={target}");
            }
            if let Some(reason) = change.reason {
                print!(" {reason:?}");
            }
            println!();
        }
    }
    if let Some(next) = page.next_offset {
        println!("Next page --offset {next}");
    }
    Ok(())
}

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

/// 校验并 trim 存储路径（本地 / nfs:// / smb:// / hdfs:// / s3:// / s3+https://）
/// 位置分类由 app 的存储工厂负责；协议 URL 的解析由 data-mover backend 负责
pub fn validate_storage_path(s: &str) -> Result<String> {
    let s = s.trim();
    if s.is_empty() {
        return Err(CliError::InvalidParameter("路径不能为空".to_string()));
    }
    Ok(s.to_string())
}

/// 校验 `block_size` 格式（复用 `parse_bandwidth_string` 单位解析）
/// 提前了 app/sync.rs 中 `parse_size()` 的格式校验部分
pub fn validate_block_size(s: &str) -> Result<String> {
    match parse_bandwidth_string(s) {
        Ok(_) => Ok(s.to_string()),
        Err(_) => Err(CliError::InvalidParameter(
            "无效的 block_size 格式。支持格式如 '4MiB'、'16MiB'".to_string(),
        )),
    }
}

/// 校验 `delta_size_threshold` 格式（同 `validate_block_size`，复用 `parse_bandwidth_string`
/// 单位解析）
#[cfg(feature = "remote")]
pub fn validate_delta_size_threshold(s: &str) -> Result<String> {
    match parse_bandwidth_string(s) {
        Ok(_) => Ok(s.to_string()),
        Err(_) => Err(CliError::InvalidParameter(
            "无效的 delta_size_threshold 格式。支持格式如 '512MiB'、'1GiB'".to_string(),
        )),
    }
}

/// 校验 `peak_qos_rate` 范围（必须 > 0）
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

/// 准备job目录（`job_id` 已由 `cli_match` 中的 `resolve_job_id` 确定）
///
/// 返回 `(job_dir, pre_existing)`，其中 `pre_existing` 为 `true` 表示目录在
/// 本次 `create_dir_all` 之前已经存在，用作 app 层判定 ScanType 的
/// 文件系统 fallback 信号（数据库不可达时使用）。
#[allow(clippy::similar_names)]
fn prepare_job(job_type: &str, job_id: &str) -> Result<(String, bool)> {
    let jobs_dir = "jobs";
    fs::create_dir_all(jobs_dir)?;

    let job_dir = format!("{jobs_dir}/{job_type}_{job_id}");
    let pre_existing = fs::metadata(&job_dir).is_ok();
    fs::create_dir_all(&job_dir)?;

    Ok((job_dir, pre_existing))
}

// ==================== 命令处理函数 ====================

/// 处理扫描命令
pub async fn scan_cmd(
    job_id: &str, path: &str, depth: u32, r#match: &Option<String>, exclude: &Option<String>, resume: bool,
    raw_command_line: String,
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

    if resume && !config.database.enabled {
        return Err(CliError::InvalidParameter("扫描 --resume 需要启用 ClickHouse".into()));
    }
    let (job_dir, _) = prepare_job("scan", job_id)?;
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        if ctrl_c().await.is_ok() {
            signal_cancel.cancel();
        }
    });
    let result = scan(ScanRequest {
        resume,
        job_id,
        job_dir: &job_dir,
        path,
        depth: effective_depth,
        include: &effective_match,
        exclude: &effective_exclude,
        command: raw_command_line,
        cancel,
        callback_url: None,
        concurrency: None,
    })
    .await;
    signal.abort();
    result.map_err(Into::into)
}

/// 处理复制命令
#[allow(clippy::too_many_arguments)]
pub async fn sync_cmd(
    job_id: &str, src_path: &Option<String>, dest_path: &Option<String>, enable_integrity_check: bool,
    enable_acl: bool, r#match: &Option<String>, exclude: &Option<String>, qos: &Option<String>, peak_qos_rate: f32,
    iops: Option<u32>, block_size: &Option<String>, #[cfg(feature = "remote")] delta_size_threshold: &Option<String>,
    file_list: &Option<String>, packaged: bool, package_depth: Option<usize>,
    #[cfg(feature = "remote")] remote: &Option<String>, #[cfg(feature = "remote")] tls_server_cert: &Option<String>,
    #[cfg(feature = "remote")] token: &Option<String>, raw_command_line: String, resume: bool,
    #[cfg(feature = "remote")] delete_target: bool,
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
    let effective_peak_qos_rate = if (peak_qos_rate - 2.0_f32).abs() > f32::EPSILON {
        peak_qos_rate
    } else {
        config.sync.peak_qos_rate.unwrap_or(2.0)
    };
    let effective_block_size = block_size.as_ref().or(config.sync.block_size.as_ref()).cloned();

    // 双进程模式提示
    #[cfg(feature = "remote")]
    if let Some(addr) = remote {
        info!("Remote mode enabled, connecting to Receiver at {}", addr);
    }

    let (job_dir, job_dir_pre_existing) = prepare_job("replicate", job_id)?;

    let src_path_str = src_path
        .as_ref()
        .ok_or_else(|| CliError::InvalidParameter("src_path is required".to_string()))?
        .as_str();
    let dest_path_str = dest_path
        .as_ref()
        .ok_or_else(|| CliError::InvalidParameter("dest_path is required".to_string()))?
        .as_str();

    let config = SyncJobConfig {
        job_id: job_id.to_string(),
        job_dir,
        job_dir_pre_existing,
        src_path: src_path_str.to_string(),
        dest_path: dest_path_str.to_string(),
        source_cleanup: SourceCleanupPolicy::from_reserved(config.sync.is_source_reserved),
        enable_integrity_check: effective_enable_integrity_check,
        enable_acl: effective_enable_acl,
        r#match: effective_match,
        exclude: effective_exclude,
        qos: effective_qos,
        peak_qos_rate: effective_peak_qos_rate,
        block_size: effective_block_size,
        #[cfg(feature = "remote")]
        delta_size_threshold: delta_size_threshold.clone(),
        #[cfg(not(feature = "remote"))]
        delta_size_threshold: None,
        file_list: file_list.clone(),
        iops,
        packaged,
        package_depth: package_depth.unwrap_or(0),
        raw_command_line,
        progress_callback_url: None,
        transfer_mode: TransferMode::from_resume(resume),
        #[cfg(feature = "remote")]
        delete_target,
        #[cfg(not(feature = "remote"))]
        delete_target: false,
        concurrency: None,
    };

    // 双进程模式：通过 QUIC 连接远端 Receiver
    #[cfg(feature = "remote")]
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
                        "Failed to read TLS server cert '{path}': {e}"
                    )));
                }
            },
            None => None,
        };

        return send(&config, remote_addr, tls_cert_bytes.as_deref(), token.as_deref())
            .await
            .map_err(CliError::AppError);
    }

    // ScanType 由 app 层自动判定（查数据库 base 表，fallback 文件系统）
    sync(config).await?;

    Ok(())
}

/// Show the configuration file
pub fn config() -> Result<()> {
    let config = AppConfig::fetch()?;
    println!("{config:#?}");

    Ok(())
}

#[cfg(target_os = "windows")]
/// 处理ACE list命令
pub async fn ace_list_cmd(
    job_id: &str, path: &str, owner: &Option<String>, depth: u32, r#match: &Option<String>, exclude: &Option<String>,
    include_inherited: bool, raw_command_line: String,
) -> Result<()> {
    let (_job_dir, _pre_existing) = prepare_job("scan_ace", job_id)?;
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
    let (_job_dir, _pre_existing) = prepare_job("copy_ace", job_id)?;
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
    info!("执行删除命令，路径: {}", redact_storage_url(path));

    rm(path).await?;

    Ok(())
}

/// 处理完整性检查命令
pub async fn integrity_check_cmd(
    job_id: &str, src_path: &str, dest_path: &str, quick: bool, auto_fix: bool, raw_command_line: String,
) -> Result<()> {
    let (job_dir, _pre_existing) = prepare_job("integrity_check", job_id)?;

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
#[cfg(feature = "remote")]
pub async fn serve_cmd(listen: &str, dest_path: &str, tls_cert_out: &str, token: &Option<String>) -> Result<()> {
    info!(
        "Starting Receiver daemon on {}, dest: {}",
        listen,
        redact_storage_url(dest_path)
    );

    // 1. 解析监听地址
    let listen_addr: SocketAddr = listen
        .parse()
        .map_err(|e| CliError::InvalidParameter(format!("Invalid listen address: {e}")))?;

    serve(listen_addr, dest_path, Path::new(tls_cert_out), token.as_deref()).await?;
    Ok(())
}
