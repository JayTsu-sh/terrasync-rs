#![recursion_limit = "256"]

//! CLI命令行接口模块
//!
//! 该模块定义了应用程序的命令行接口，包括：
//! 1. 命令行参数解析
//! 2. 子命令定义
//! 3. 命令执行逻辑
//! 4. 辅助函数

// 标准库
#[cfg(feature = "license")]
use std::path::Path;
use std::path::PathBuf;

// 外部crate
use chrono::Local;
use clap::Parser;
use tracing::info;
use utils::app_config::AppConfig;
use utils::sanitize_job_id;
use utils::types::LogLevel;

// 内部模块
pub mod commands;
mod commands_enum;
pub mod error;

/// 应用程序版本号
const APP_VERSION: &str = "3.0.0";

/// 重新导出命令枚举类型
pub use commands_enum::*;

/// 公共API的 `prelude` 模块
///
/// 用户可以通过 `use cli::prelude::*` 来导入最常用的类型，
/// 简化应用开发过程中的导入语句
pub mod prelude {
    /// 作业ID转换函数
    pub use utils::sanitize_job_id;

    /// ACE子命令枚举
    #[cfg(target_os = "windows")]
    pub use super::AceCommands;
    /// 命令行参数解析结构体
    pub use super::Cli;
    /// 子命令枚举
    pub use super::Commands;
    /// 命令行入口函数
    pub use super::cli_match;
    /// 命令执行函数
    pub use super::commands::*;
    /// 错误处理相关类型
    pub use super::error::{CliError, Result};
}

#[derive(Parser, Debug)]
#[command(name = "rust-terrasync")]
#[command(about = "A Rust-based terrasync application", long_about = None)]
#[command(disable_version_flag = true)]
#[command(after_help = "CONFIGURATION PRIORITY:\n  \
    CLI arguments > config file (-c) > built-in defaults\n\n\
    Parameters marked with 'Also configurable in config file' can be set in a TOML config file.\n\
    Example: terrasync -c my_config.toml sync src dest")]
pub struct Cli {
    /// Path to custom config file (TOML format).
    /// Settings in this file override defaults, and CLI arguments override both.
    #[arg(short, long, value_name = "FILE", global = true)]
    pub config: Option<PathBuf>,

    /// Set the logging level (trace, debug, info, warn, error).
    /// Also configurable in config file under [log] level
    #[arg(
        name = "log_level",
        short = 'l',
        long = "log-level",
        value_name = "LOG_LEVEL",
        global = true
    )]
    pub log_level: Option<LogLevel>,

    /// Path to license file (overrides config file setting)
    #[cfg(feature = "license")]
    #[arg(long, value_name = "FILE", global = true)]
    pub license: Option<PathBuf>,

    /// Enable JSON structured logging (outputs to app.json.log).
    /// Also configurable in config file under `[log]` `enable_json`
    #[arg(long, global = true)]
    pub json: bool,

    /// Print version information
    #[arg(short, long, global = true)]
    pub version: bool,

    /// Subcommands
    #[clap(subcommand)]
    command: Commands,
}

/// 从已解析的命令中提取或自动生成 `job_id`
///
/// - 有 `--id` 参数：sanitize 后返回
/// - 无 `--id` 但为 job 类命令：自动生成时间戳 ID
/// - 非 job 类命令（Config, Serve, Rm, Gui）：返回 None
fn resolve_job_id(command: &Commands) -> Option<String> {
    let raw_id = match command {
        Commands::Scan { id, .. } | Commands::Sync { id, .. } | Commands::IntegrityCheck { id, .. } => id.as_deref(),
        #[cfg(target_os = "windows")]
        Commands::Ace { ace_command } => match ace_command {
            AceCommands::List { id, .. } | AceCommands::Copy { id, .. } => id.as_deref(),
        },
        _ => return None,
    };
    Some(raw_id.map_or_else(|| format!("{}", Local::now().format("%Y%m%d_%H%M%S")), sanitize_job_id))
}

/// 从 `resolved_job_id` 中获取 `job_id` 引用，若为 None 则返回错误
fn require_job_id<'a>(job_id: Option<&'a str>, cmd: &str) -> error::Result<&'a str> {
    job_id.ok_or_else(|| error::CliError::InvalidParameter(format!("{cmd} requires job_id")))
}

/// 命令行入口点，处理命令行参数并执行相应命令
///
/// 该函数是CLI模块的主入口点，负责：
/// 1. 解析命令行参数
/// 2. 初始化配置
/// 3. 设置日志记录
/// 4. 根据命令执行相应的逻辑
///
/// # 返回值
/// - 成功时返回Ok(())
/// - 失败时返回包含错误信息的Result
pub async fn cli_match() -> error::Result<()> {
    // 进程级 TLS crypto provider — 必须先于任何 RustlsClientConfig::builder() 调用：
    // data-mover 的 s3 (s3+https 自签证书) 和 transport 的 quic 都依赖 process-wide
    // default provider。重复 install 第二次起返回 Err 并被忽略，单一真理源在此。
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args: Vec<String> = std::env::args().collect();
    // 检查版本参数（需要在 Cli::parse 前处理，避免缺少子命令时报错）
    for arg in &args {
        if arg == "-v" || arg == "--version" {
            println!("version: {APP_VERSION}");
            std::process::exit(0);
        }
    }

    let cli = Cli::parse();

    // 处理版本参数
    if cli.version {
        println!("version: {APP_VERSION}");
        std::process::exit(0);
    }

    // 将原始参数组合成单个字符串
    let raw_command_line = std::env::args_os()
        .map(|arg| {
            arg.into_string()
                .unwrap_or_else(|os_str| os_str.to_string_lossy().into_owned())
        })
        .collect::<Vec<String>>()
        .join(" ");

    // Initialize Configuration: default_config → -c config_file → build
    let config_contents = include_str!("resources/default_config.toml");
    AppConfig::init(Some(config_contents), cli.config.as_deref())?;

    // CLI 参数覆盖（最高优先级）
    if let Some(log_level) = &cli.log_level {
        AppConfig::override_with(|c| c.log.level = log_level.to_string())?;
    }
    if cli.json {
        AppConfig::override_with(|c| c.log.enable_json = true)?;
    }

    let resolved_job_id = resolve_job_id(&cli.command);
    utils::logger::setup_logging(resolved_job_id.as_deref())?;

    // ===== License 验证 =====
    #[cfg(feature = "license")]
    {
        let license_path = resolve_license_path(cli.license.as_deref())?;
        match &cli.command {
            // activate 命令：执行激活，然后返回
            Commands::Activate => {
                licensing::activate::activate_license(&license_path)?;
                println!("License activated successfully.");
                return Ok(());
            }
            // config 命令：不需要 license 验证
            Commands::Config => {}
            // 其他命令：验证 license
            _ => {
                let license = licensing::verify::verify_license(&license_path)?;
                licensing::set_global_license(license)?;
            }
        }
    }
    // ========================

    println!(" 🚀 Terrasync {APP_VERSION} | (c) 2025 LenovoNetapp, Inc. \n");

    info!(" 🚀 Terrasync {} | (c) 2025 LenovoNetapp, Inc.", APP_VERSION);

    // Execute the subcommand（job 类命令使用 resolved_job_id）
    let job_id_ref = resolved_job_id.as_deref();
    match &cli.command {
        Commands::Scan {
            depth,
            path,
            r#match,
            exclude,
            ..
        } => {
            commands::scan_cmd(
                require_job_id(job_id_ref, "scan")?,
                path,
                *depth,
                r#match,
                exclude,
                raw_command_line,
            )
            .await?;
        }
        Commands::Sync {
            src_path,
            dest_path,
            enable_integrity_check,
            enable_acl,
            r#match,
            exclude,
            qos,
            peak_qos_rate,
            iops,
            block_size,
            file_list,
            packaged,
            package_depth,
            remote,
            tls_server_cert,
            token,
            no_resume,
            ..
        } => {
            commands::sync_cmd(
                require_job_id(job_id_ref, "sync")?,
                &Some(src_path.clone()),
                &Some(dest_path.clone()),
                *enable_integrity_check,
                *enable_acl,
                r#match,
                exclude,
                qos,
                *peak_qos_rate,
                *iops,
                block_size,
                file_list,
                *packaged,
                *package_depth,
                remote,
                tls_server_cert,
                token,
                raw_command_line,
                *no_resume,
            )
            .await?;
        }
        Commands::Serve {
            listen,
            dest_path,
            tls_cert_out,
            token,
        } => {
            commands::serve_cmd(listen, dest_path, tls_cert_out, token).await?;
        }
        Commands::Config => commands::config()?,
        #[cfg(feature = "license")]
        Commands::Activate => {} // 已在上方处理
        Commands::Rm { path } => commands::rm_cmd(path).await?,
        Commands::IntegrityCheck {
            src_path,
            dest_path,
            quick,
            auto_fix,
            ..
        } => {
            commands::integrity_check_cmd(
                require_job_id(job_id_ref, "integrity_check")?,
                src_path,
                dest_path,
                *quick,
                *auto_fix,
                raw_command_line,
            )
            .await?;
        }
        #[cfg(feature = "gui")]
        Commands::Gui { host, port } => commands::gui_cmd(host, *port).await?,
        #[cfg(target_os = "windows")]
        Commands::Ace { ace_command } => match ace_command {
            AceCommands::List {
                path,
                owner,
                depth,
                r#match,
                exclude,
                include_inherited,
                ..
            } => {
                commands::ace_list_cmd(
                    require_job_id(job_id_ref, "ace_list")?,
                    path,
                    owner,
                    *depth,
                    r#match,
                    exclude,
                    *include_inherited,
                    raw_command_line,
                )
                .await?
            }
            #[cfg(target_os = "windows")]
            AceCommands::Copy {
                source_path,
                target_path,
                depth,
                r#match,
                exclude,
                ..
            } => {
                commands::ace_copy_cmd(
                    require_job_id(job_id_ref, "ace_copy")?,
                    source_path,
                    target_path,
                    *depth,
                    r#match,
                    exclude,
                    raw_command_line,
                )
                .await?
            }
        },
    }
    Ok(())
}

#[cfg(feature = "license")]
/// 解析 license 文件路径（三级 fallback）
///
/// 1. CLI 参数 `--license <path>`（最高优先）
/// 2. 配置文件 `[license] path`
/// 3. 可执行文件同目录的 `license.json`
fn resolve_license_path(cli_license: Option<&Path>) -> error::Result<PathBuf> {
    // 1. CLI 参数
    if let Some(path) = cli_license {
        return Ok(path.to_path_buf());
    }

    // 2. 配置文件
    if let Ok(config) = AppConfig::fetch() {
        let config_path = PathBuf::from(&config.license.path);
        if config_path.exists() {
            return Ok(config_path);
        }
    }

    // 3. 可执行文件同目录
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let default_path = exe_dir.join("license.json");
            if default_path.exists() {
                return Ok(default_path);
            }
        }
    }

    // 4. 当前工作目录
    let cwd_path = PathBuf::from("license.json");
    if cwd_path.exists() {
        return Ok(cwd_path);
    }

    Err(error::CliError::LicenseError(
        licensing::error::LicenseError::FileNotFound(
            "license.json not found (checked --license, config, exe dir, cwd)".to_string(),
        ),
    ))
}
