use data_mover::model::ObservationPlan;
use task_model::metadata::MetadataScope;
use task_model::{SourceCleanupPolicy, TransferMode};
// 标准库
// 无

// 外部crate
use data_mover::filter::{FilterExpression, parse_filter_expression};
use data_mover::redact_storage_url;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use utils::app_config::AppConfig;
use utils::app_config::DatabaseConfig;

// 内部模块
use crate::callback::CallbackEndpoint;
use crate::error::Result;
use crate::scan::ScanType;

/// Redacts credentials from storage-URL arguments embedded in a recorded command line.
#[must_use]
pub fn redact_command_line(command: &str) -> String {
    command
        .split_whitespace()
        .map(redact_storage_url)
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// 迁移任务固定本轮过滤时间；独立扫描未指定时保持既有实时语义。
    pub filter_time_anchor: Option<i64>,
    pub include_tags: bool,
    pub observation_plan: Option<ObservationPlan>,
    pub metadata_scope: MetadataScope,
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
    pub progress_callback_url: Option<CallbackEndpoint>,
}

/// Sync/IncrementalSync 的统一任务配置
///
/// 本地与远端迁移任务的执行参数。
/// `scan_type` 由任务层依据持久化代次策略判定；数据库故障直接上报。
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // 配置结构按用途分组的开关集合，重构为枚举会失去可读性
pub struct SyncJobConfig {
    pub job_id: String,
    pub job_dir: String,
    /// `job_dir` 是否在本次调用前已经存在（由调用方 snapshot 注入）
    /// 不作为迁移模式或数据库故障的替代依据。
    pub job_dir_pre_existing: bool,
    pub src_path: String,
    pub dest_path: String,
    /// CLI／Web 在接受任务时固定，后续代次记录和源清理共用此值。
    pub source_cleanup: SourceCleanupPolicy,
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
    pub progress_callback_url: Option<CallbackEndpoint>,
    /// 文件写入模式，由 CLI 参数优先级显式确定。
    pub transfer_mode: TransferMode,
    /// `--delete-target`：仅 `--remote` 双进程模式生效，控制是否清理目标端多余文件
    /// （目标端存在但源端已不存在的条目）。本地模式的孤儿清理由 DB diff 驱动、
    /// 须经策略及源端存在性确认，不受此远程开关影响。
    pub delete_target: bool,
    pub concurrency: Option<usize>,
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
        match_expressions: r#match.as_deref().map(parse_filter_expression).transpose()?,
        exclude_expressions: exclude.as_deref().map(parse_filter_expression).transpose()?,
        filter_time_anchor: None,
        include_tags,
        observation_plan: None,
        metadata_scope: MetadataScope::default(),
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
    progress_callback_url: Option<CallbackEndpoint>,
) -> Result<ConsumerConfig> {
    let db_config = app_config.database.clone();

    let consumer_config = ConsumerConfig {
        job_id: job_id.to_string(),
        job_type,
        job_dir: job_dir.to_string(),
        db_config,
        console_config: ConsoleConfig {
            raw_command_line: redact_command_line(&raw_command_line),
        },
        progress_callback_url,
    };
    info!("Created consumer configuration: {:?}", consumer_config);
    Ok(consumer_config)
}

#[cfg(test)]
mod redaction_tests {
    use super::*;
    use crate::error::AppError;
    use std::mem::discriminant;

    #[test]
    fn invalid_filters_return_the_typed_error_instead_of_admitting_every_entry() {
        for (expression, is_exclude) in [("invalid_field == 1", false), ("size > invalid", true)] {
            let expected = match parse_filter_expression(expression) {
                Err(error) => error,
                Ok(_) => panic!("fixture must be rejected by the native parser"),
            };
            let (include, exclude) = if is_exclude {
                (None, Some(expression.into()))
            } else {
                (Some(expression.into()), None)
            };
            let result = initialize_scan_config(
                "invalid-filter",
                "unused",
                0,
                ScanType::Full,
                &include,
                &exclude,
                1,
                false,
                &None,
                false,
                0,
            );
            match result {
                Err(AppError::ScanFilter(actual)) => {
                    assert_eq!(discriminant(&actual), discriminant(&expected));
                    assert_eq!(actual.to_string(), expected.to_string());
                }
                other => panic!("invalid filter must fail closed: {other:?}"),
            }
        }
    }

    #[test]
    fn absent_and_valid_filters_keep_their_distinct_selection() -> Result<()> {
        for include in [None, Some("modified < 7d".into())] {
            let config = initialize_scan_config(
                "valid-filter",
                "unused",
                0,
                ScanType::Full,
                &include,
                &None,
                1,
                false,
                &None,
                false,
                0,
            )?;
            assert_eq!(config.match_expressions.is_some(), include.is_some());
            assert!(config.exclude_expressions.is_none());
        }
        Ok(())
    }

    #[test]
    fn command_line_redacts_storage_credentials_and_preserves_other_arguments() {
        let command = "terrasync sync /src s3://access:secret@bucket.host:9000/prefix --delete-target";
        let redacted = redact_command_line(command);

        assert_eq!(
            redacted,
            "terrasync sync /src s3://***:***@bucket.host:9000/prefix --delete-target"
        );
        assert!(!redacted.contains("access"));
        assert!(!redacted.contains("secret"));
    }

    #[test]
    fn command_line_preserves_urls_without_userinfo_and_local_paths() {
        let command = "terrasync sync /src nfs://server:2049/export:/prefix";
        assert_eq!(redact_command_line(command), command);
    }

    #[test]
    fn command_line_redacts_username_even_without_password() {
        let redacted = redact_command_line("terrasync sync /src s3://access@bucket.host/prefix");
        assert_eq!(redacted, "terrasync sync /src s3://***:***@bucket.host/prefix");
        assert!(!redacted.contains("access"));
    }
}
