use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use utils::app_config::AppConfig;

use crate::domain::endpoint::Endpoint;
use crate::domain::execution::{ExecutionType, TaskExecution};
use crate::domain::path::MigrationPath;
use crate::domain::repository::{
    ConfigRepository, EndpointRepository, ExecutionRepository, PathRepository, TaskRepository,
};
use crate::domain::task::{MigrationTask, TaskConfig, TaskStatus, TaskType};
use crate::error::{Result, WebError};
use crate::infrastructure::clickhouse_client;
use crate::infrastructure::db::sanitize_job_id;

/// 进度回调基础 URL（web 层在绑定端口后设置，CLI 模式不存在此值）
static PROGRESS_CALLBACK_BASE_URL: OnceLock<String> = OnceLock::new();

/// 由 `server.rs` 在 TcpListener 绑定成功后调用一次
pub fn set_progress_callback_base_url(url: String) {
    PROGRESS_CALLBACK_BASE_URL.get_or_init(|| url);
}

/// 构造指定任务的完整回调 URL
fn progress_callback_url_for(task_id: &str) -> Option<String> {
    PROGRESS_CALLBACK_BASE_URL
        .get()
        .map(|base| format!("{}/tasks/{}/progress", base.trim_end_matches('/'), task_id))
}

/// 最大并发任务数。app::scan/sync 内部使用了非 Send 的 dyn Consumer，
/// 每个任务需要独立线程+独立 runtime，因此必须限制并发数以控制资源消耗。
const MAX_CONCURRENT_TASKS: usize = 8;

/// 任务执行桥接层 — 将 MigrationTask 转换为 app::scan/sync 调用
pub struct TaskRunner {
    task_repo: Arc<dyn TaskRepository>,
    endpoint_repo: Arc<dyn EndpointRepository>,
    path_repo: Arc<dyn PathRepository>,
    execution_repo: Arc<dyn ExecutionRepository>,
    config_repo: Arc<dyn ConfigRepository>,
    running_tasks: DashMap<String, CancellationToken>,
    concurrency_limit: Arc<Semaphore>,
}

impl TaskRunner {
    pub fn new(
        task_repo: Arc<dyn TaskRepository>, endpoint_repo: Arc<dyn EndpointRepository>,
        path_repo: Arc<dyn PathRepository>, execution_repo: Arc<dyn ExecutionRepository>,
        config_repo: Arc<dyn ConfigRepository>,
    ) -> Self {
        Self {
            task_repo,
            endpoint_repo,
            path_repo,
            execution_repo,
            config_repo,
            running_tasks: DashMap::new(),
            concurrency_limit: Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS)),
        }
    }

    /// 启动任务执行
    pub async fn start_task(&self, task: &MigrationTask) -> Result<String> {
        if !task.status.can_start() {
            return Err(WebError::InvalidTaskState {
                status: task.status.to_string(),
                operation: "start".to_string(),
            });
        }

        // 检查并发限制：在创建执行记录前获取许可
        let permit = self.concurrency_limit.clone().try_acquire_owned().map_err(|_| {
            WebError::ValidationError(format!(
                "已达最大并发任务数 ({MAX_CONCURRENT_TASKS})，请等待其他任务完成后重试"
            ))
        })?;

        // 查找端点和路径信息
        let source_endpoint = self.get_endpoint(&task.source_endpoint_id).await?;
        let source_path = match &task.source_path_id {
            Some(id) => Some(self.get_path(id).await?),
            None => None,
        };

        let dest_info = if let (Some(dep_id), Some(dp_id)) = (&task.dest_endpoint_id, &task.dest_path_id) {
            let ep = self.get_endpoint(dep_id).await?;
            let path = self.get_path(dp_id).await?;
            Some((ep, path))
        } else {
            None
        };

        // 判断是全量还是增量（根据 job 目录是否存在）
        let job_id = sanitize_job_id(&task.id);
        let job_dir = format!("jobs/{job_id}");
        let execution_type = if std::path::Path::new(&job_dir).exists() {
            ExecutionType::Incremental
        } else {
            ExecutionType::Full
        };

        // 创建执行记录
        let execution = TaskExecution {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task.id.clone(),
            execution_type,
            status: TaskStatus::Running,
            started_at: chrono::Utc::now(),
            finished_at: None,
            stats: None,
            error_message: None,
        };
        self.execution_repo.save(&execution).await?;

        // 更新任务状态
        self.task_repo
            .update_status(&task.id, TaskStatus::Running, None)
            .await?;

        // 创建取消令牌
        let cancel_token = CancellationToken::new();
        self.running_tasks.insert(task.id.clone(), cancel_token.clone());

        // 在后台 spawn 任务执行
        let task_id = task.id.clone();
        let execution_id = execution.id.clone();
        let task_type = task.task_type.clone();
        let task_config = task.config.clone();

        let source_url = source_endpoint.to_storage_url(source_path.as_ref().map(|p| p.sub_path.as_str()));
        let dest_url = dest_info.as_ref().map(|(ep, p)| ep.to_storage_url(Some(&p.sub_path)));

        let task_repo = self.task_repo.clone();
        let execution_repo = self.execution_repo.clone();
        let config_repo = self.config_repo.clone();
        let running_tasks = self.running_tasks.clone();

        // app::scan/sync 内部使用了非 Send 的 dyn Consumer，
        // 无法直接 tokio::spawn。在独立线程+独立 runtime 中执行。
        std::thread::spawn(move || {
            // permit 在线程结束时自动 drop，释放并发许可
            let _permit = permit;
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build();
            let rt = match rt {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to create runtime for task {task_id}: {e}");
                    running_tasks.remove(&task_id);
                    return;
                }
            };

            rt.block_on(async move {
                let result = run_migration_task(
                    &task_id,
                    &job_id,
                    &job_dir,
                    task_type,
                    &task_config,
                    &source_url,
                    dest_url.as_deref(),
                    cancel_token,
                    config_repo,
                )
                .await;

                let (final_status, error_msg) = match result {
                    Ok(()) => (TaskStatus::Completed, None),
                    Err(e) => {
                        error!("Task {task_id} failed: {e}");
                        (TaskStatus::Failed, Some(e.to_string()))
                    }
                };

                if let Err(e) = task_repo
                    .update_status(&task_id, final_status.clone(), error_msg.as_deref())
                    .await
                {
                    error!("Failed to update task status: {e}");
                }

                // stats 由 is_final 回调写入，此处传 None，COALESCE 保留已有数据
                if let Err(e) = execution_repo
                    .update_status(&execution_id, final_status, None, error_msg.as_deref())
                    .await
                {
                    error!("Failed to update execution status: {e}");
                }

                running_tasks.remove(&task_id);
            });
        });

        Ok(execution.id)
    }

    /// 取消正在运行的任务
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        if let Some((_, token)) = self.running_tasks.remove(task_id) {
            token.cancel();
            info!("Task {task_id} cancellation requested");
            Ok(())
        } else {
            Err(WebError::InvalidTaskState {
                status: "not running".to_string(),
                operation: "cancel".to_string(),
            })
        }
    }

    /// 清理任务关联的数据（ClickHouse 表 + job 目录）
    pub async fn cleanup_task_data(&self, task_id: &str) {
        let job_id = sanitize_job_id(task_id);
        let job_dir = format!("jobs/{job_id}");

        match std::fs::remove_dir_all(&job_dir) {
            Ok(()) => info!("[TaskRunner] Removed job dir for deleted task: {}", job_dir),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => error!("[TaskRunner] Failed to remove job dir {}: {e}", job_dir),
        }

        drop_old_scan_tables(&job_id).await;
    }

    /// 检查任务是否正在运行
    pub fn is_running(&self, task_id: &str) -> bool {
        self.running_tasks.contains_key(task_id)
    }

    async fn get_endpoint(&self, id: &str) -> Result<Endpoint> {
        self.endpoint_repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| WebError::EndpointNotFound(id.to_string()))
    }

    async fn get_path(&self, id: &str) -> Result<MigrationPath> {
        self.path_repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| WebError::PathNotFound(id.to_string()))
    }
}

/// 执行具体的迁移任务（调用 app 层）；统计数据由 is_final 回调写入 SQLite
async fn run_migration_task(
    task_id: &str, job_id: &str, job_dir: &str, task_type: TaskType, task_config: &TaskConfig, source_url: &str,
    dest_url: Option<&str>, cancel_token: CancellationToken, config_repo: Arc<dyn ConfigRepository>,
) -> Result<()> {
    // 从 SQLite 读取 ClickHouse 配置，覆盖 AppConfig 全局配置并校验连通性
    let ch_cfg = apply_db_config_from_sqlite(config_repo.as_ref())
        .await?
        .ok_or_else(|| WebError::ConnectionTestFailed("ClickHouse 未配置".to_string()))?;
    clickhouse_client::test_connectivity(&ch_cfg.dsn, &ch_cfg.username, &ch_cfg.password, &ch_cfg.database)
        .await
        .map_err(|_| WebError::ConnectionTestFailed("请先在设置中检查 ClickHouse 数据库配置".to_string()))?;

    let callback_url = progress_callback_url_for(task_id);

    match task_type {
        TaskType::Scan => {
            // 纯扫描：强制全量，清理旧数据
            match std::fs::remove_dir_all(job_dir) {
                Ok(()) => info!("[TaskRunner] Removed old job dir for fresh full scan: {}", job_dir),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => error!("[TaskRunner] Failed to remove job dir {}: {e}", job_dir),
            }
            drop_old_scan_tables(job_id).await;

            app::scan::scan(
                job_id.to_string(),
                job_dir.to_string(),
                app::scan::ScanType::Full,
                source_url,
                task_config.depth,
                &task_config.match_expr,
                &task_config.exclude_expr,
                format!("gui:scan {source_url}"),
                Some(cancel_token.clone()),
                callback_url,
            )
            .await?;
            Ok(())
        }
        TaskType::Sync => {
            let dest = dest_url
                .ok_or_else(|| WebError::ValidationError("Sync task requires destination endpoint".to_string()))?;

            let scan_type = if std::path::Path::new(job_dir).exists() {
                app::scan::ScanType::Incremental
            } else {
                app::scan::ScanType::Full
            };

            app::sync::sync(
                job_id.to_string(),
                job_dir.to_string(),
                source_url,
                dest,
                task_config.enable_integrity_check,
                task_config.enable_acl,
                scan_type,
                &task_config.match_expr,
                &task_config.exclude_expr,
                &task_config.qos,
                task_config.peak_qos_rate,
                &task_config.block_size,
                &None,
                task_config.iops,
                false,
                0,
                format!("gui:sync {source_url} {dest}"),
                callback_url,
            )
            .await?;
            Ok(())
        }
        TaskType::IntegrityCheck => {
            let dest = dest_url.ok_or_else(|| {
                WebError::ValidationError("Integrity check requires destination endpoint".to_string())
            })?;

            app::sync::integrity_check(
                job_id.to_string(),
                job_dir.to_string(),
                source_url,
                dest,
                false,
                false,
                format!("gui:integrity_check {source_url} {dest}"),
                callback_url,
            )
            .await?;
            Ok(())
        }
    }
}

/// 清理旧的 ClickHouse 扫描表，确保全量扫描从干净状态开始
async fn drop_old_scan_tables(job_id: &str) {
    let app_config = match AppConfig::fetch() {
        Ok(c) => c,
        Err(_) => return,
    };

    if !app_config.database.enabled {
        return;
    }

    let dsn = app_config.database.clickhouse.dsn.trim_end_matches('/');
    let username = &app_config.database.clickhouse.username;
    let password = app_config.database.clickhouse.password.as_deref().unwrap_or("");
    let database = &app_config.database.clickhouse.database;

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    for table in &[
        format!("base_{job_id}"),
        format!("scan_state_{job_id}"),
        format!("incremental_{job_id}"),
    ] {
        let query = format!("DROP TABLE IF EXISTS `{table}`");
        let url = format!("{dsn}/");
        let result = client
            .post(&url)
            .basic_auth(username, Some(password))
            .header("X-ClickHouse-Database", database)
            .body(query)
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                info!("[TaskRunner] Dropped ClickHouse table: {}", table);
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                error!("[TaskRunner] Failed to drop table {}: {}", table, body);
            }
            Err(e) => {
                error!("[TaskRunner] Failed to drop table {}: {}", table, e);
            }
        }
    }
}

/// 从 SQLite 读取的 ClickHouse 配置
pub(crate) struct ChConfig {
    pub(crate) dsn: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) batch_size: u32,
}

/// 从 SQLite system_config 表一次性读取所有 ClickHouse 配置
async fn load_ch_config(config_repo: &dyn ConfigRepository) -> Result<Option<ChConfig>> {
    let all = config_repo.get_all().await?;
    let get = |key: &str| -> Option<String> { all.iter().find(|(k, _)| k == key).map(|(_, v)| strip_json_quotes(v)) };

    let host = get("clickhouse_host").filter(|h| !h.is_empty());
    let Some(host) = host else {
        return Ok(None);
    };

    let port_str = get("clickhouse_port").unwrap_or_else(|| "8123".to_string());
    let port: u16 = port_str
        .parse()
        .map_err(|_| WebError::ValidationError(format!("clickhouse_port 必须是有效端口号: {port_str}")))?;
    let username = get("clickhouse_user").unwrap_or_else(|| "default".to_string());
    let password = get("clickhouse_password").unwrap_or_default();
    let database = get("clickhouse_database").unwrap_or_else(|| "default".to_string());
    let batch_size = get("clickhouse_batch_size")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(800000);

    // 校验 host 合法性：仅允许合法主机名或 IP 地址
    if host.contains('/')
        || host.contains('?')
        || host.contains('#')
        || host.contains('@')
        || host.contains('\n')
        || host.contains('\r')
        || host.contains('\0')
    {
        return Err(WebError::ValidationError(format!(
            "clickhouse_host 包含非法字符: {host}"
        )));
    }
    // 校验 database 和 username 不含 HTTP header 注入字符
    if database.contains('\n') || database.contains('\r') || database.contains('\0') {
        return Err(WebError::ValidationError(format!(
            "clickhouse_database 包含非法字符: {database}"
        )));
    }
    if username.contains('\n') || username.contains('\r') || username.contains('\0') {
        return Err(WebError::ValidationError(format!(
            "clickhouse_user 包含非法字符: {username}"
        )));
    }
    let dsn = format!("http://{host}:{port}");
    Ok(Some(ChConfig {
        dsn,
        username,
        password,
        database,
        batch_size,
    }))
}

/// 从 SQLite 读取 ClickHouse 配置并覆盖 AppConfig 全局配置，返回读取到的配置
///
/// 在以下时机调用：
/// - 服务启动时（加载已保存的配置）
/// - 用户通过 API 修改 ClickHouse 配置后
/// - 任务执行前（确保使用最新配置）
pub(crate) async fn apply_db_config_from_sqlite(config_repo: &dyn ConfigRepository) -> Result<Option<ChConfig>> {
    let cfg = match load_ch_config(config_repo).await? {
        Some(c) => c,
        None => return Ok(None),
    };

    AppConfig::override_with(|c| {
        c.database.enabled = true;
        c.database.r#type = "clickhouse".to_string();
        c.database.batch_size = cfg.batch_size;
        c.database.clickhouse.dsn = cfg.dsn.clone();
        c.database.clickhouse.username = cfg.username.clone();
        c.database.clickhouse.password = Some(cfg.password.clone()).filter(|s| !s.is_empty());
        c.database.clickhouse.database = cfg.database.clone();
    })?;

    debug!(
        "[TaskRunner] Applied ClickHouse config: dsn={}, db={}",
        cfg.dsn, cfg.database
    );

    Ok(Some(cfg))
}

/// SQLite 中 value_json 存的是 JSON 值，字符串带引号（如 `"http://..."`），需要去掉
fn strip_json_quotes(s: &str) -> String {
    serde_json::from_str::<String>(s).unwrap_or_else(|_| {
        tracing::warn!(
            "[ConfigRepo] Config value is not a JSON string, using raw (len={})",
            s.len()
        );
        s.to_string()
    })
}
