use std::sync::{Arc, OnceLock};
use storage_adapter::{StorageError, TransferMode};

use app::error::AppError;
use app::scan::{ScanRequest, scan};
use app::scan_resume::{clear_scan, scan_execution_is_incremental};
use dashmap::{DashMap, mapref::entry::Entry};
use std::time::Duration;
use tokio::select;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::domain::endpoint::Endpoint;
use crate::domain::execution::{ExecutionType, TaskExecution};
use crate::domain::path::MigrationPath;
use crate::domain::repository::{
    ConfigRepository, EndpointRepository, ExecutionRepository, PathRepository, TaskRepository,
};
use crate::domain::storage_failure::StorageFailureDetail;
use crate::domain::task::{MigrationTask, TaskConfig, TaskStatus, TaskType};
use crate::error::{Result, WebError};
use crate::infrastructure::clickhouse_client;
use crate::infrastructure::clickhouse_config::apply_db_config_from_sqlite;
use crate::infrastructure::db::sanitize_job_id;

/// 进度回调基础 URL（web 层在绑定端口后设置，CLI 模式不存在此值）
static PROGRESS_CALLBACK_BASE_URL: OnceLock<String> = OnceLock::new();

/// 由 `server.rs` 在 `TcpListener` 绑定成功后调用一次
pub fn set_progress_callback_base_url(url: String) {
    PROGRESS_CALLBACK_BASE_URL.get_or_init(|| url);
}

/// 构造指定任务的完整回调 URL
fn progress_callback_url_for(task_id: &str, execution_id: &str) -> Option<String> {
    PROGRESS_CALLBACK_BASE_URL.get().map(|base| {
        format!(
            "{}/tasks/{}/progress?execution_id={}",
            base.trim_end_matches('/'),
            task_id,
            execution_id
        )
    })
}

/// 最大并发任务数。`app::scan`/`sync` 内部使用了非 `Send` 的 `dyn Consumer`，
/// 每个任务需要独立线程+独立 runtime，因此必须限制并发数以控制资源消耗。
const MAX_CONCURRENT_TASKS: usize = 8;

/// 任务执行桥接层 — 将 `MigrationTask` 转换为 `app::scan`/`sync` 调用
pub struct TaskRunner {
    task_repo: Arc<dyn TaskRepository>,
    endpoint_repo: Arc<dyn EndpointRepository>,
    path_repo: Arc<dyn PathRepository>,
    execution_repo: Arc<dyn ExecutionRepository>,
    config_repo: Arc<dyn ConfigRepository>,
    running_tasks: Arc<DashMap<String, CancellationToken>>,
    concurrency_limit: Arc<Semaphore>,
}

struct RunningTaskGuard {
    task_id: String,
    tasks: Arc<DashMap<String, CancellationToken>>,
}
impl Drop for RunningTaskGuard {
    fn drop(&mut self) {
        self.tasks.remove(&self.task_id);
    }
}

fn task_outcome(result: &Result<()>, cancel: &CancellationToken) -> (TaskStatus, Option<String>) {
    if cancel.is_cancelled() {
        return (TaskStatus::Cancelled, None);
    }
    match result {
        Ok(()) => (TaskStatus::Completed, None),
        Err(error) => (TaskStatus::Failed, Some(error.to_string())),
    }
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
            running_tasks: Arc::new(DashMap::new()),
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

        let cancel_token = CancellationToken::new();
        match self.running_tasks.entry(task.id.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(cancel_token.clone());
            }
            Entry::Occupied(_) => {
                return Err(WebError::InvalidTaskState {
                    status: "running or saving checkpoint".into(),
                    operation: "start".into(),
                });
            }
        }
        let task_guard = RunningTaskGuard {
            task_id: task.id.clone(),
            tasks: self.running_tasks.clone(),
        };

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

        // 扫描从检查点读取本轮类型；迁移任务沿用原来的目录判定。
        let job_id = sanitize_job_id(&task.id);
        let job_dir = format!("jobs/{job_id}");
        let incremental = if task.task_type == TaskType::Scan {
            apply_db_config_from_sqlite(self.config_repo.as_ref()).await?;
            !task.config.reset_scan_on_start && scan_execution_is_incremental(&job_id).await?
        } else {
            std::path::Path::new(&job_dir).exists()
        };
        let execution_type = if incremental {
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
            storage_error: None,
        };
        self.execution_repo.save(&execution).await?;

        // 更新任务状态
        self.task_repo
            .update_status(&task.id, TaskStatus::Running, None, None)
            .await?;

        // 在后台 spawn 任务执行
        let task_id = task.id.clone();
        let execution_id = execution.id.clone();
        let task_type = task.task_type.clone();
        let task_config = task.config.clone();
        let mut task_for_run = task.clone();

        let source_url = source_endpoint.to_storage_url(source_path.as_ref().map(|p| p.sub_path.as_str()));
        let dest_url = dest_info.as_ref().map(|(ep, p)| ep.to_storage_url(Some(&p.sub_path)));

        let task_repo = self.task_repo.clone();
        let execution_repo = self.execution_repo.clone();
        let config_repo = self.config_repo.clone();

        // app::scan/sync 内部使用了非 Send 的 dyn Consumer，
        // 无法直接 tokio::spawn。在独立线程+独立 runtime 中执行。
        std::thread::spawn(move || {
            // permit 在线程结束时自动 drop，释放并发许可
            let _permit = permit;
            let _task_guard = task_guard;
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build();
            let rt = match rt {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to create runtime for task {task_id}: {e}");
                    return;
                }
            };

            rt.block_on(async move {
                let result = async {
                    if task_for_run.task_type == TaskType::Scan && task_for_run.config.reset_scan_on_start {
                        apply_db_config_from_sqlite(config_repo.as_ref()).await?;
                        drop_scan_tables(&job_id).await?;
                        task_for_run.config.reset_scan_on_start = false;
                        task_repo.update(&task_for_run).await?;
                    }
                    run_migration_task(
                        &task_id,
                        &execution_id,
                        &job_id,
                        &job_dir,
                        task_type,
                        &task_config,
                        &source_url,
                        dest_url.as_deref(),
                        cancel_token.child_token(),
                        config_repo,
                    )
                    .await
                }
                .await;

                let (final_status, error_msg) = task_outcome(&result, &cancel_token);
                let storage_error = if final_status == TaskStatus::Failed {
                    match &result {
                        Err(WebError::AppError(AppError::StorageError(StorageError::RemoteScan(failure)))) => {
                            Some(StorageFailureDetail::from(failure.as_ref()))
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                if let Some(message) = &error_msg {
                    error!("Task {task_id} failed: {message}");
                }

                if let Err(e) = task_repo
                    .update_status(
                        &task_id,
                        final_status.clone(),
                        error_msg.as_deref(),
                        storage_error.as_ref(),
                    )
                    .await
                {
                    error!("Failed to update task status: {e}");
                }

                // stats 由 is_final 回调写入，此处传 None，COALESCE 保留已有数据
                if let Err(e) = execution_repo
                    .update_status(
                        &execution_id,
                        final_status,
                        None,
                        error_msg.as_deref(),
                        storage_error.as_ref(),
                    )
                    .await
                {
                    error!("Failed to update execution status: {e}");
                }
            });
        });

        Ok(execution.id)
    }

    /// 取消正在运行的任务
    pub fn cancel_task(&self, task_id: &str) -> Result<()> {
        if let Some(token) = self.running_tasks.get(task_id) {
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

    pub async fn pause_all_and_wait(&self) {
        for token in self.running_tasks.iter() {
            token.value().cancel();
        }
        if timeout(Duration::from_secs(60), async {
            while !self.running_tasks.is_empty() {
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_err()
        {
            error!("Task shutdown timed out; persisted scan checkpoints remain resumable");
        }
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

/// 执行具体的迁移任务（调用 app 层）；统计数据由 `is_final` 回调写入 `SQLite`
#[allow(clippy::too_many_arguments)]
async fn run_migration_task(
    task_id: &str, execution_id: &str, job_id: &str, job_dir: &str, task_type: TaskType, task_config: &TaskConfig,
    source_url: &str, dest_url: Option<&str>, cancel_token: CancellationToken, config_repo: Arc<dyn ConfigRepository>,
) -> Result<()> {
    // 从 SQLite 读取 ClickHouse 配置，覆盖 AppConfig 全局配置并校验连通性
    let ch_cfg = select! {
        biased;
        _=cancel_token.cancelled()=>return Err(AppError::ObservationScanCancelled.into()),
        config=apply_db_config_from_sqlite(config_repo.as_ref())=>config?,
    }
    .ok_or_else(|| WebError::ConnectionTestFailed("ClickHouse 未配置".to_string()))?;
    select! {
        biased;
        _=cancel_token.cancelled()=>return Err(AppError::ObservationScanCancelled.into()),
        result=clickhouse_client::test_connectivity(&ch_cfg.connection.dsn, &ch_cfg.connection.username, ch_cfg.connection.password.as_deref().unwrap_or(""), &ch_cfg.connection.database)=>
            result.map_err(|_| WebError::ConnectionTestFailed("请先在设置中检查 ClickHouse 数据库配置".to_string()))?,
    }

    let callback_url = progress_callback_url_for(task_id, execution_id);

    match task_type {
        TaskType::Scan => {
            scan(ScanRequest {
                job_id,
                job_dir,
                path: source_url,
                depth: task_config.depth,
                include: &task_config.match_expr,
                exclude: &task_config.exclude_expr,
                command: format!("gui:scan {source_url}"),
                cancel: cancel_token,
                callback_url,
                concurrency: task_config.concurrency,
            })
            .await?;
            Ok(())
        }
        TaskType::Sync => {
            let dest = dest_url
                .ok_or_else(|| WebError::ValidationError("Sync task requires destination endpoint".to_string()))?;

            // 在调用 app 层之前 snapshot job_dir 预存在状态，用作 DB 不可达时的 fallback 信号
            let job_dir_pre_existing = std::path::Path::new(job_dir).exists();

            // ScanType 由 app 层自动判定（查数据库 base 表，fallback 文件系统快照）
            let config = app::config::SyncJobConfig {
                job_id: job_id.to_string(),
                job_dir: job_dir.to_string(),
                job_dir_pre_existing,
                src_path: source_url.to_string(),
                dest_path: dest.to_string(),
                enable_integrity_check: task_config.enable_integrity_check,
                enable_acl: task_config.enable_acl,
                r#match: task_config.match_expr.clone(),
                exclude: task_config.exclude_expr.clone(),
                qos: task_config.qos.clone(),
                peak_qos_rate: task_config.peak_qos_rate,
                block_size: task_config.block_size.clone(),
                // web 层是否暴露该参数为后续可选项，本次先给默认值保证编译通过（同 delete_target）
                delta_size_threshold: None,
                file_list: None,
                iops: task_config.iops,
                packaged: false,
                package_depth: 0,
                raw_command_line: format!("gui:sync {source_url} {dest}"),
                progress_callback_url: callback_url,
                transfer_mode: TransferMode::Checkpoint,
                // web 层是否暴露该参数为后续可选项，本次先给默认值保证编译通过
                delete_target: false,
                concurrency: task_config.concurrency,
            };
            app::sync::sync(config).await?;
            Ok(())
        }
        TaskType::IntegrityCheck => {
            let dest = dest_url.ok_or_else(|| {
                WebError::ValidationError("Integrity check requires destination endpoint".to_string())
            })?;

            app::integrity_check::integrity_check_with_concurrency(
                job_id.to_string(),
                job_dir.to_string(),
                source_url,
                dest,
                false,
                false,
                format!("gui:integrity_check {source_url} {dest}"),
                callback_url,
                task_config.concurrency,
            )
            .await?;
            Ok(())
        }
    }
}

/// 清理旧的 `ClickHouse` 扫描表，确保全量扫描从干净状态开始
async fn drop_old_scan_tables(job_id: &str) {
    if let Err(error) = drop_scan_tables(job_id).await {
        error!(%error, "Failed to remove scan data");
    }
}

async fn drop_scan_tables(job_id: &str) -> Result<()> {
    clear_scan(job_id).await?;
    Ok(())
}

#[cfg(test)]
mod resume_tests {
    use super::*;

    #[test]
    fn user_pause_cannot_be_reported_as_completed_or_failed() {
        let token = CancellationToken::new();
        token.cancel();
        assert_eq!(task_outcome(&Ok(()), &token).0, TaskStatus::Cancelled);
        let failure = Err(WebError::ValidationError("scan paused".into()));
        assert_eq!(task_outcome(&failure, &token).0, TaskStatus::Cancelled);
        assert_eq!(task_outcome(&failure, &CancellationToken::new()).0, TaskStatus::Failed);
    }

    #[test]
    fn active_slot_is_shared_until_the_worker_finishes_draining() {
        let tasks = Arc::new(DashMap::new());
        let token = CancellationToken::new();
        tasks.insert("scan".into(), token.clone());
        let guard = RunningTaskGuard {
            task_id: "scan".into(),
            tasks: tasks.clone(),
        };
        token.cancel();
        assert!(tasks.contains_key("scan"));
        drop(guard);
        assert!(!tasks.contains_key("scan"));
    }
}
