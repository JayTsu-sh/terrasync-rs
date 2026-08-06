// 标准库
// 无

// 外部crate
use data_mover::filter::{FilterExpression, parse_filter_expression};
use db::config::DatabaseConfig;
use tracing::{debug, info};
use utils::app_config::AppConfig;

// 内部模块
use crate::error::Result;
use crate::scan::ScanType;

#[derive(Debug, Clone, PartialEq)]
pub enum JobType {
    Scan,
    IncrementalScan, // 增量扫描
    Copy,            // 全量拷贝
    IncrementalCopy, // 增量拷贝
    IntegrityCheck,  // 完整性检查
}

impl JobType {
    /// 获取任务类型对应的完成消息前缀
    pub fn to_operation_name(&self) -> &'static str {
        match self {
            JobType::Scan => "Scan",
            JobType::IncrementalScan => "Incremental Scan",
            JobType::Copy => "Copy",
            JobType::IncrementalCopy => "Incremental Copy",
            JobType::IntegrityCheck => "Integrity Check",
        }
    }

    /// 获取进度回调 payload 中使用的名称
    pub fn to_callback_name(&self) -> &'static str {
        match self {
            JobType::Scan => "scan",
            JobType::IncrementalScan => "incremental_scan",
            JobType::Copy => "copy",
            JobType::IncrementalCopy => "incremental_copy",
            JobType::IntegrityCheck => "integrity_check",
        }
    }
}

/// 扫描配置结构体 - 内部使用的完整配置
#[derive(Debug, Clone)]
pub struct ScanConfig {
    pub job_id: String,
    pub path: String,
    pub depth: u32,
    pub scan_type: ScanType,
    pub concurrency: usize,
    pub r#match: String,
    pub exclude: String,
    pub match_expressions: Option<FilterExpression>,
    pub exclude_expressions: Option<FilterExpression>,
    pub include_tags: bool,
    pub file_list: Option<String>,
    pub packaged: bool,
    pub package_depth: usize,
}

#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub raw_command_line: String,
}

/// 消费配置结构体 - 内部使用的完整配置
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub job_id: String,
    pub job_type: JobType,
    pub job_dir: String,
    pub db_config: DatabaseConfig,
    pub console_config: ConsoleConfig,
    /// 进度回调 URL（由环境变量 `TERRASYNC_PROGRESS_CALLBACK_URL` 拼接而成）
    /// 当 web 层启动任务时设置，CLI 模式下为 None
    pub progress_callback_url: Option<String>,
}

/// Sync/IncrementalSync 的统一任务配置
///
/// 整合 `sync()/incremental_sync()` 的所有参数，供 `SyncOrchestrator` 使用。
/// `scan_type` 由 `SyncOrchestrator::run()` 自动判定（查数据库 base 表，fallback 文件系统）。
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // 配置结构按用途分组的开关集合，重构为枚举会失去可读性
pub struct SyncJobConfig {
    pub job_id: String,
    pub job_dir: String,
    /// `job_dir` 是否在本次调用前已经存在（由调用方 snapshot 注入）
    /// 用于 app 层判定 `ScanType` 的文件系统 fallback（数据库不可达时）
    pub job_dir_pre_existing: bool,
    pub src_path: String,
    pub dest_path: String,
    pub enable_integrity_check: bool,
    pub enable_acl: bool,
    pub r#match: Option<String>,
    pub exclude: Option<String>,
    pub qos: Option<String>,
    pub peak_qos_rate: f32,
    pub block_size: Option<String>,
    /// delta 传输 size 门槛（例如 "512MiB"），仅 `--remote` 双进程模式生效：超过该大小的
    /// 文件即使数据不匹配也降级为全量传输，为 `None` 时 Receiver 侧使用默认值 512MiB
    /// （见 issue #54 阶段 0）
    pub delta_size_threshold: Option<String>,
    pub file_list: Option<String>,
    pub iops: Option<u32>,
    pub packaged: bool,
    pub package_depth: usize,
    pub raw_command_line: String,
    /// 进度回调 URL（web 层注入，CLI 为 None）
    pub progress_callback_url: Option<String>,
    /// 关闭字节级断点续传（强制整体复制大文件）
    pub no_resume: bool,
    /// `--delete-target`：仅 `--remote` 双进程模式生效，控制是否清理目标端多余文件
    /// （目标端存在但源端已不存在的条目）。本地模式的孤儿清理由 DB diff 驱动、
    /// 无条件执行，不受此字段影响（两套机制并存但互不干扰）。
    pub delete_target: bool,
}

/// 初始化扫描配置
#[allow(clippy::too_many_arguments)]
pub fn initialize_scan_config(
    job_id: &str, path: &str, depth: u32, scan_type: ScanType, r#match: &Option<String>, exclude: &Option<String>,
    concurrency: usize, include_tags: bool, file_list: &Option<String>, packaged: bool, package_depth: usize,
) -> Result<ScanConfig> {
    let scan_config = ScanConfig {
        job_id: job_id.to_string(),
        path: path.to_string(),
        depth,
        scan_type,
        concurrency,
        r#match: r#match.clone().unwrap_or_default(),
        exclude: exclude.clone().unwrap_or_default(),
        match_expressions: r#match.as_deref().and_then(|expr| parse_filter_expression(expr).ok()),
        exclude_expressions: exclude.as_deref().and_then(|expr| parse_filter_expression(expr).ok()),
        include_tags,
        file_list: file_list.clone(),
        packaged,
        package_depth,
    };
    debug!("Created scan configuration: {:?}", scan_config);
    Ok(scan_config)
}

/// 初始化消费者配置
///
/// `progress_callback_url`：完整的 HTTP 回调 URL（web 层传入，CLI 传 `None`）
pub fn initialize_consumer_config(
    job_id: &str, job_dir: &str, job_type: JobType, raw_command_line: String, app_config: &AppConfig,
    progress_callback_url: Option<String>,
) -> Result<ConsumerConfig> {
    let db_config = db::config::DatabaseConfig::from_app_config(&app_config.database);

    let consumer_config = ConsumerConfig {
        job_id: job_id.to_string(),
        job_type,
        job_dir: job_dir.to_string(),
        db_config,
        console_config: ConsoleConfig { raw_command_line },
        progress_callback_url,
    };
    info!("Created consumer configuration: {:?}", consumer_config);
    Ok(consumer_config)
}
