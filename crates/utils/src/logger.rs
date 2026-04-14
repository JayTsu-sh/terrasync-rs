//! 日志记录模块
//!
//! 该模块负责应用程序的日志记录功能，包括：
//! 1. 日志系统的初始化
//! 2. 日志级别配置
//! 3. 日志输出到文件（支持按大小和时间轮转）
//! 4. 与tracing库的集成
//! 5. tokio任务追踪支持（span）
//! 6. 日志格式配置（compact格式，含文件名和行号）

// 标准库
use std::env::{current_dir, current_exe, var};
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::OnceLock;

// 外部crate
use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::{EnvFilter, Layer, Registry};

// 内部模块
use crate::app_config::AppConfig;
use crate::error::{Result, UtilsError};

// 存储当前的日志文件路径（只设置一次，之后只读）
static CURRENT_APP_LOG_PATH: OnceLock<String> = OnceLock::new();

// 存储 NonBlocking writer 的 guard，防止 drop 导致日志丢失
static LOG_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();

// 存储 app_layer 的 reload handle，用于运行时动态更新日志级别
static APP_FILTER_HANDLE: OnceLock<Handle<EnvFilter, Registry>> = OnceLock::new();

/// 设置全局日志记录系统
pub fn setup_logging(job_id: Option<&str>) -> Result<()> {
    // 已初始化则跳过（OnceLock 保证只设置一次，避免重复创建 file appender）
    if LOG_GUARDS.get().is_some() {
        return Ok(());
    }

    // 防御性校验：确保 job_id 不含路径穿越字符
    if let Some(id) = job_id {
        if id.contains('/') || id.contains('\\') || id.contains("..") {
            return Err(UtilsError::Other(format!(
                "job_id contains unsafe path characters: {}",
                id
            )));
        }
    }

    // 初始化log到tracing的桥接（外部crate如rdkafka、aws-sdk-s3使用log crate）
    if let Err(e) = LogTracer::init() {
        if e.to_string() != "logger already registered" {
            return Err(UtilsError::LoggerError(e));
        }
    }

    // 获取日志级别
    let log_level = get_log_level_from_config()?;

    // 获取轮转配置
    let (max_size_bytes, max_backups) = get_rotation_config();

    // 获取日志文件路径
    let (log_file_path, error_log_file_path) = get_log_file_path(job_id)?;

    // 创建日志目录
    let log_dir = log_file_path
        .parent()
        .ok_or_else(|| UtilsError::Other("Failed to get log file parent directory".to_string()))?;
    create_dir_all(log_dir)?;

    // 存储 app.log 路径
    let app_log_full_path = log_file_path
        .to_str()
        .ok_or_else(|| UtilsError::Other("Failed to convert log path to string".to_string()))?
        .to_string();
    let _ = CURRENT_APP_LOG_PATH.set(app_log_full_path);

    // 创建 app.log 的 rolling file writer
    let app_roller = BasicRollingFileAppender::new(
        &log_file_path,
        RollingConditionBasic::new().daily().max_size(max_size_bytes),
        max_backups,
    )
    .map_err(|e| UtilsError::Other(format!("Failed to create app log roller: {}", e)))?;

    // 创建 error.log 目录
    let error_log_dir = error_log_file_path
        .parent()
        .ok_or_else(|| UtilsError::Other("Failed to get error log file parent directory".to_string()))?;
    create_dir_all(error_log_dir)?;

    // 创建 error.log 的 rolling file writer
    let error_roller = BasicRollingFileAppender::new(
        &error_log_file_path,
        RollingConditionBasic::new().daily().max_size(max_size_bytes),
        max_backups,
    )
    .map_err(|e| UtilsError::Other(format!("Failed to create error log roller: {}", e)))?;

    // 包装为非阻塞 writer
    let (app_non_blocking, app_guard) = tracing_appender::non_blocking(app_roller);
    let (error_non_blocking, error_guard) = tracing_appender::non_blocking(error_roller);

    // 收集所有 guard 防止 drop
    let mut guards = vec![app_guard, error_guard];

    // 为每个 layer 创建独立的 EnvFilter（EnvFilter 不实现 Clone）
    let app_filter = EnvFilter::builder()
        .with_default_directive(log_level.into())
        .from_env_lossy();

    // 将 app_filter 包装为 reload::Layer，支持运行时动态更新日志级别
    let (reloadable_filter, app_filter_handle) = tracing_subscriber::reload::Layer::new(app_filter);
    let _ = APP_FILTER_HANDLE.set(app_filter_handle);

    let error_filter = EnvFilter::new("error");

    // 创建 app.log 的 Layer（使用可重载的过滤器）
    let app_layer = tracing_subscriber::fmt::layer()
        .with_writer(app_non_blocking)
        .with_ansi(false)
        .with_line_number(true)
        .with_file(true)
        .with_target(true)
        .compact()
        .with_filter(reloadable_filter);

    // 创建 error.log 的 Layer
    let error_layer = tracing_subscriber::fmt::layer()
        .with_writer(error_non_blocking)
        .with_ansi(false)
        .with_line_number(true)
        .with_file(true)
        .with_target(true)
        .compact()
        .with_filter(error_filter);

    // JSON layer（可选，启用后输出结构化日志到 app.json.log）
    let is_json_enabled = AppConfig::fetch().ok().is_some_and(|c| c.log.enable_json);

    let json_layer = if is_json_enabled {
        let json_log_path = log_file_path
            .parent()
            .ok_or_else(|| UtilsError::Other("Failed to get log file parent directory".to_string()))?
            .join("app.json.log");

        let json_roller = BasicRollingFileAppender::new(
            &json_log_path,
            RollingConditionBasic::new().daily().max_size(max_size_bytes),
            max_backups,
        )
        .map_err(|e| UtilsError::Other(format!("Failed to create JSON log roller: {}", e)))?;

        let (json_non_blocking, json_guard) = tracing_appender::non_blocking(json_roller);
        guards.push(json_guard);

        let json_filter = EnvFilter::builder()
            .with_default_directive(log_level.into())
            .from_env_lossy();

        Some(
            tracing_subscriber::fmt::layer()
                .with_writer(json_non_blocking)
                .with_ansi(false)
                .json()
                .with_span_list(true)
                .with_current_span(true)
                .with_filter(json_filter),
        )
    } else {
        None
    };

    // 存储 guard 防止 drop
    let _ = LOG_GUARDS.set(guards);

    // 使用 Registry 初始化
    if let Err(e) = Registry::default()
        .with(app_layer)
        .with(error_layer)
        .with(json_layer)
        .try_init()
    {
        let error_msg = e.to_string();
        if !(error_msg.contains("Global logger already set")
            || error_msg.contains("attempted to set a logger after the logging system was already initialized"))
        {
            return Err(UtilsError::Other(e.to_string()));
        }
    }

    Ok(())
}

/// 获取轮转配置（max_size_bytes, max_backups）
fn get_rotation_config() -> (u64, usize) {
    if let Ok(app_config) = AppConfig::fetch() {
        let max_size = app_config.log.max_size * 1024 * 1024; // MB -> bytes
        let max_backups = app_config.log.max_backups as usize;
        return (max_size, max_backups);
    }
    // 默认: 100MB, 10个备份
    (100 * 1024 * 1024, 10)
}

/// 从配置中获取日志级别
fn get_log_level_from_config() -> Result<tracing::Level> {
    if let Ok(app_config) = AppConfig::fetch() {
        return parse_log_level(&app_config.log.level);
    }
    Ok(tracing::Level::INFO)
}

/// 解析字符串为日志级别
fn parse_log_level(level: &str) -> Result<tracing::Level> {
    match level.to_lowercase().as_str() {
        "debug" => Ok(tracing::Level::DEBUG),
        "info" => Ok(tracing::Level::INFO),
        "warn" | "warning" => Ok(tracing::Level::WARN),
        "error" => Ok(tracing::Level::ERROR),
        "trace" => Ok(tracing::Level::TRACE),
        _ => Err(UtilsError::InvalidLogLevel(
            "Invalid log level. Only 'trace', 'debug', 'info', 'warn', 'error' are allowed.".to_string(),
        )),
    }
}

/// 获取日志文件路径
fn get_log_file_path(job_id: Option<&str>) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
    let base_dir = if let Ok(manifest_dir) = var("CARGO_MANIFEST_DIR") {
        let project_root = Path::new(&manifest_dir)
            .parent()
            .unwrap_or_else(|| Path::new(&manifest_dir));
        project_root.to_path_buf()
    } else {
        let current_exe = current_exe()?;
        let mut exe_dir = current_exe;
        exe_dir.pop();
        if !exe_dir.exists() {
            exe_dir = current_dir()?;
        }
        exe_dir
    };

    let log_dir = match job_id {
        Some(id) => base_dir.join("logs").join(id),
        None => base_dir.join("logs"),
    };
    Ok((log_dir.join("app.log"), log_dir.join("error.log")))
}

/// 运行时动态更新日志级别
///
/// 通过 `tracing_subscriber::reload` 机制，在不重启进程的情况下切换日志级别。
/// 仅影响 app.log 的过滤器，error.log 始终只记录 ERROR 级别。
pub fn reload_log_level(level: &str) -> Result<()> {
    let parsed_level = parse_log_level(level)?;
    tracing::info!("Changing log level to: {level}");
    let new_filter = EnvFilter::new(parsed_level.to_string());
    if let Some(handle) = APP_FILTER_HANDLE.get() {
        handle
            .reload(new_filter)
            .map_err(|e| UtilsError::Other(format!("Failed to reload log level: {e}")))?;
    }
    Ok(())
}

/// 获取当前的应用日志文件完整路径
pub fn get_current_app_log_path() -> String {
    CURRENT_APP_LOG_PATH
        .get()
        .cloned()
        .unwrap_or_else(|| "Logger not enabled".to_string())
}

/// 测试专用的日志初始化函数
///
/// 使用 `std::sync::Once` 保证多测试并行运行时只初始化一次。
/// 输出到 stderr（配合 cargo test --nocapture），包含文件名和行号。
pub fn init_test_logging() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::Level::DEBUG.into())
                    .from_env_lossy(),
            )
            .with_test_writer()
            .with_file(true)
            .with_line_number(true)
            .with_target(true)
            .compact()
            .try_init()
            .ok();
    });
}
