use std::sync::Arc;

use app::consumer::ProgressReport;
use app::consumer::stats::ProgressDetail;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::domain::execution::{ExecutionStats, LogLevel, TaskExecution, TaskLog};
use crate::domain::progress::TaskProgress;
use crate::domain::repository::{
    EndpointRepository, ExecutionRepository, PathRepository, ProgressRepository, TaskRepository,
};
use crate::domain::task::{MigrationTask, TaskConfig, TaskFilter, TaskStatus, TaskType};
use crate::error::{Result, WebError};
use crate::infrastructure::task_runner::TaskRunner;

pub struct TaskService {
    task_repo: Arc<dyn TaskRepository>,
    execution_repo: Arc<dyn ExecutionRepository>,
    endpoint_repo: Arc<dyn EndpointRepository>,
    path_repo: Arc<dyn PathRepository>,
    progress_repo: Arc<dyn ProgressRepository>,
    task_runner: Arc<TaskRunner>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTaskRequest {
    pub name: Option<String>,
    pub source_endpoint_id: Option<String>,
    pub source_path_id: Option<String>,
    pub config: Option<TaskConfig>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTaskRequest {
    pub name: String,
    pub task_type: TaskType,
    pub source_endpoint_id: String,
    pub source_path_id: Option<String>,
    pub dest_endpoint_id: Option<String>,
    pub dest_path_id: Option<String>,
    #[serde(default)]
    pub config: TaskConfig,
}

impl TaskService {
    pub fn new(
        task_repo: Arc<dyn TaskRepository>, execution_repo: Arc<dyn ExecutionRepository>,
        endpoint_repo: Arc<dyn EndpointRepository>, path_repo: Arc<dyn PathRepository>,
        progress_repo: Arc<dyn ProgressRepository>, task_runner: Arc<TaskRunner>,
    ) -> Self {
        Self {
            task_repo,
            execution_repo,
            endpoint_repo,
            path_repo,
            progress_repo,
            task_runner,
        }
    }

    pub async fn create_task(&self, req: CreateTaskRequest) -> Result<MigrationTask> {
        if req.name.trim().is_empty() {
            return Err(WebError::ValidationError("Task name cannot be empty".to_string()));
        }

        // 校验源端点存在
        self.endpoint_repo
            .find_by_id(&req.source_endpoint_id)
            .await?
            .ok_or_else(|| WebError::EndpointNotFound(req.source_endpoint_id.clone()))?;
        // 仅在提供了 path_id 时校验路径存在
        if let Some(ref path_id) = req.source_path_id {
            self.path_repo
                .find_by_id(path_id)
                .await?
                .ok_or_else(|| WebError::PathNotFound(path_id.clone()))?;
        }

        // Sync 和 IntegrityCheck 需要目标端点
        if req.task_type != TaskType::Scan {
            let dest_ep_id = req.dest_endpoint_id.as_ref().ok_or_else(|| {
                WebError::ValidationError("Destination endpoint is required for sync/integrity_check".to_string())
            })?;
            let dest_path_id = req.dest_path_id.as_ref().ok_or_else(|| {
                WebError::ValidationError("Destination path is required for sync/integrity_check".to_string())
            })?;
            self.endpoint_repo
                .find_by_id(dest_ep_id)
                .await?
                .ok_or_else(|| WebError::EndpointNotFound(dest_ep_id.clone()))?;
            self.path_repo
                .find_by_id(dest_path_id)
                .await?
                .ok_or_else(|| WebError::PathNotFound(dest_path_id.clone()))?;
        }

        let task = MigrationTask {
            id: uuid::Uuid::new_v4().to_string(),
            name: req.name,
            task_type: req.task_type,
            source_endpoint_id: req.source_endpoint_id,
            source_path_id: req.source_path_id,
            dest_endpoint_id: req.dest_endpoint_id,
            dest_path_id: req.dest_path_id,
            config: req.config,
            status: TaskStatus::Pending,
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            error_message: None,
        };

        self.task_repo.save(&task).await?;
        Ok(task)
    }

    pub async fn update_task(&self, id: &str, req: UpdateTaskRequest) -> Result<MigrationTask> {
        let mut task = self.get_task(id).await?;

        if matches!(task.status, TaskStatus::Running) {
            return Err(WebError::InvalidTaskState {
                status: task.status.to_string(),
                operation: "update".to_string(),
            });
        }

        if let Some(name) = req.name {
            if name.trim().is_empty() {
                return Err(WebError::ValidationError("Task name cannot be empty".to_string()));
            }
            task.name = name;
        }

        if let Some(source_endpoint_id) = req.source_endpoint_id {
            self.endpoint_repo
                .find_by_id(&source_endpoint_id)
                .await?
                .ok_or_else(|| WebError::EndpointNotFound(source_endpoint_id.clone()))?;
            task.source_endpoint_id = source_endpoint_id;
        }

        if let Some(source_path_id) = req.source_path_id {
            if source_path_id.is_empty() {
                task.source_path_id = None;
            } else {
                self.path_repo
                    .find_by_id(&source_path_id)
                    .await?
                    .ok_or_else(|| WebError::PathNotFound(source_path_id.clone()))?;
                task.source_path_id = Some(source_path_id);
            }
        }

        if let Some(config) = req.config {
            task.config = config;
        }

        self.task_repo.update(&task).await?;
        Ok(task)
    }

    pub async fn start_task(&self, id: &str) -> Result<String> {
        let task = self.get_task(id).await?;
        self.task_runner.start_task(&task).await
    }

    pub async fn cancel_task(&self, id: &str) -> Result<()> {
        self.task_runner.cancel_task(id)?;
        self.task_repo.update_status(id, TaskStatus::Cancelled, None).await
    }

    pub async fn get_task(&self, id: &str) -> Result<MigrationTask> {
        self.task_repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| WebError::TaskNotFound(id.to_string()))
    }

    pub async fn list_tasks(&self, filter: TaskFilter) -> Result<Vec<MigrationTask>> {
        self.task_repo.find_all(&filter).await
    }

    pub async fn delete_task(&self, id: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        if !task.status.can_delete() {
            return Err(WebError::InvalidTaskState {
                status: task.status.to_string(),
                operation: "delete".to_string(),
            });
        }
        // 清理关联的 ClickHouse 表和 job 目录
        self.task_runner.cleanup_task_data(id).await;
        // 清理关联的执行记录和日志
        self.execution_repo.delete_by_task_id(id).await?;
        self.task_repo.delete(id).await
    }

    pub async fn get_executions(&self, task_id: &str) -> Result<Vec<TaskExecution>> {
        self.execution_repo.find_by_task_id(task_id).await
    }

    pub async fn get_execution(&self, id: &str) -> Result<TaskExecution> {
        self.execution_repo
            .find_by_id(id)
            .await?
            .ok_or_else(|| WebError::ExecutionNotFound(id.to_string()))
    }

    pub async fn get_execution_logs(
        &self, execution_id: &str, level: Option<LogLevel>, offset: u32, limit: u32,
    ) -> Result<Vec<TaskLog>> {
        self.execution_repo.find_logs(execution_id, level, offset, limit).await
    }

    pub async fn delete_execution(&self, id: &str) -> Result<()> {
        let execution = self.get_execution(id).await?;
        if !execution.status.can_delete() {
            return Err(WebError::InvalidTaskState {
                status: execution.status.to_string(),
                operation: "delete execution".to_string(),
            });
        }
        self.execution_repo.delete_by_id(id).await
    }

    /// 接收来自 app 层的进度回调，保存到 SQLite；最终回调时同步更新执行记录统计
    pub async fn update_progress(&self, task_id: &str, report: ProgressReport) -> Result<()> {
        // 最终回调时校验任务存在且处于 Running 状态；周期回调跳过校验以减少 SQLite 查询
        if report.is_final {
            let task = self
                .task_repo
                .find_by_id(task_id)
                .await?
                .ok_or_else(|| WebError::TaskNotFound(task_id.to_string()))?;
            if !matches!(task.status, TaskStatus::Running) {
                return Err(WebError::InvalidTaskState {
                    status: task.status.to_string(),
                    operation: "update progress".to_string(),
                });
            }
        }

        let report_json = serde_json::to_string(&report)?;

        let progress = TaskProgress {
            task_id: task_id.to_string(),
            report_json,
            is_final: report.is_final,
            updated_at: chrono::Utc::now(),
        };

        self.progress_repo.upsert(&progress).await?;

        // 最终回调时将统计数据持久化到执行记录，保证"查看报告"能显示统计和图表
        if report.is_final {
            info!("[Progress] Received final callback for task {task_id}, persisting stats to execution");
            let executions = self.execution_repo.find_by_task_id(task_id).await?;
            let status_summary: Vec<_> = executions.iter().map(|e| format!("{}={}", e.id, e.status)).collect();
            if let Some(exec) = executions.into_iter().find(|e| matches!(e.status, TaskStatus::Running)) {
                let stats = execution_stats_from_report(&report);
                debug!(
                    "[Progress] Writing stats to execution {}: files={}, bytes={}, duration={:.1}s",
                    exec.id, stats.scanned_files, stats.total_bytes, stats.duration_secs
                );
                // status 传 Running 保持不变，COALESCE 确保 finished_at/error_message 不被清除
                self.execution_repo
                    .update_status(&exec.id, TaskStatus::Running, Some(&stats), None)
                    .await?;
            } else {
                warn!(
                    "[Progress] Final callback for task {task_id}: no running execution found! \
                    Execution statuses: {status_summary:?}"
                );
            }
        }

        Ok(())
    }

    /// 从 `SQLite` 读取任务进度
    pub async fn get_progress(&self, task_id: &str) -> Result<Option<TaskProgress>> {
        self.progress_repo.find_by_task_id(task_id).await
    }
}

/// 从 `ProgressReport` 提取 `ExecutionStats`，供写入执行记录
fn execution_stats_from_report(report: &ProgressReport) -> ExecutionStats {
    match &report.detail {
        ProgressDetail::Full {
            scanned_files,
            scanned_dirs,
            total_bytes,
            transferred_bytes,
            error_count,
        } => ExecutionStats {
            scanned_files: *scanned_files,
            scanned_dirs: *scanned_dirs,
            total_bytes: *total_bytes,
            transferred_bytes: *transferred_bytes,
            error_count: *error_count,
            duration_secs: report.elapsed_secs,
            avg_speed_bytes_per_sec: report.speed_bytes_per_sec,
        },
        ProgressDetail::Incremental {
            scanned_files,
            scanned_dirs,
            scanned_size,
            transferred_bytes,
            error_count,
            ..
        } => ExecutionStats {
            scanned_files: *scanned_files,
            scanned_dirs: *scanned_dirs,
            total_bytes: *scanned_size,
            transferred_bytes: *transferred_bytes,
            error_count: *error_count,
            duration_secs: report.elapsed_secs,
            avg_speed_bytes_per_sec: report.speed_bytes_per_sec,
        },
    }
}
