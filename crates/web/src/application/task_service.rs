use std::sync::Arc;

use app::consumer::ProgressReport;
use app::consumer::stats::ProgressDetail;
use app::consumer::stats::diagnostics::{CallbackAck, safe_diagnostic_text};
use serde::Deserialize;
use tracing::info;

use crate::domain::execution::{ExecutionStats, LogLevel, TaskExecution, TaskLog};
use crate::domain::progress::{DiagnosticPage, ProgressSubmission, TaskProgress};
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
            storage_error: None,
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

        let old = task.clone();
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
            if task.source_endpoint_id != source_endpoint_id {
                task.source_path_id = None;
            }
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

        if let Some(mut config) = req.config {
            validate_task_config(&config)?;
            config.reset_scan_on_start = old.config.reset_scan_on_start;
            task.config = config;
        }
        if task.task_type == TaskType::Scan
            && (old.source_endpoint_id != task.source_endpoint_id
                || old.source_path_id != task.source_path_id
                || old.config.scan_resume != task.config.scan_resume
                || old.config.depth != task.config.depth
                || old.config.match_expr != task.config.match_expr
                || old.config.exclude_expr != task.config.exclude_expr)
        {
            task.config.reset_scan_on_start = true;
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
        Ok(())
    }

    pub async fn pause_all_and_wait(&self) {
        self.task_runner.pause_all_and_wait().await;
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

    /// 明细、快照和最终执行统计原子提交，只有提交成功才能确认批次。
    pub async fn update_progress(
        &self, task_id: &str, mut report: ProgressReport, execution_id: Option<&str>,
    ) -> Result<CallbackAck> {
        if let Some(batch) = &mut report.diagnostics {
            for event in &mut batch.events {
                event.sanitize();
            }
        }
        if let Some(runtime) = &mut report.scan_runtime {
            runtime.current_error = runtime.current_error.as_deref().map(safe_diagnostic_text);
        }
        let progress = TaskProgress {
            task_id: task_id.to_string(),
            report_json: serde_json::to_string(&report)?,
            is_final: report.is_final,
            updated_at: chrono::Utc::now(),
        };
        let stats = report.is_final.then(|| execution_stats_from_report(&report));
        let receipt = self
            .progress_repo
            .receive(ProgressSubmission {
                progress: &progress,
                identity: report.callback_identity.as_ref(),
                diagnostics: report.diagnostics.as_ref(),
                execution_id,
                final_stats: stats.as_ref(),
            })
            .await?;
        if receipt.accepted && report.is_final {
            info!("[Progress] Committed final callback for task {task_id}");
        }
        Ok(receipt.ack)
    }

    /// 从 `SQLite` 读取任务进度
    pub async fn get_progress(&self, task_id: &str) -> Result<Option<TaskProgress>> {
        self.progress_repo.find_by_task_id(task_id).await
    }

    /// 读取已确认明细，不删除当前错误状态或累计数字。
    pub async fn get_diagnostics(&self, task_id: &str, offset: u32, limit: u32) -> Result<DiagnosticPage> {
        self.get_task(task_id).await?;
        self.progress_repo.diagnostics(task_id, offset, limit).await
    }
}

fn validate_task_config(config: &TaskConfig) -> Result<()> {
    if config.concurrency.is_some_and(|value| !(1..=128).contains(&value)) {
        return Err(WebError::ValidationError("并发度必须为 1 到 128".into()));
    }
    Ok(())
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
            scan: report.scan.clone(),
            scan_runtime: report.scan_runtime.clone(),
        },
        ProgressDetail::Incremental {
            scanned_files,
            scanned_symlinks,
            scanned_dirs,
            scanned_size,
            transferred_bytes,
            error_count,
            ..
        } => ExecutionStats {
            scanned_files: scanned_files.saturating_add(*scanned_symlinks),
            scanned_dirs: *scanned_dirs,
            total_bytes: *scanned_size,
            transferred_bytes: *transferred_bytes,
            error_count: *error_count,
            duration_secs: report.elapsed_secs,
            avg_speed_bytes_per_sec: report.speed_bytes_per_sec,
            scan: report.scan.clone(),
            scan_runtime: report.scan_runtime.clone(),
        },
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    use crate::api::state::AppState;
    use crate::application::endpoint_service::CreateEndpointRequest;
    use crate::domain::endpoint::{EndpointConfig, EndpointType};
    use sqlx::sqlite::SqlitePoolOptions;

    #[test]
    fn execution_summary_includes_incremental_symlinks() -> Result<()> {
        let report: ProgressReport = serde_json::from_value(serde_json::json!({
            "job_id":"links", "job_type":"IncrementalScan", "elapsed_secs":1.0,
            "speed_bytes_per_sec":0.0, "is_final":true,
            "detail":{"kind":"Incremental", "scanned_files":10, "scanned_dirs":2,
                "scanned_symlinks":4, "scanned_size":0, "transferred_bytes":0,
                "error_count":0, "new_count":0, "new_size":0, "changed_count":0,
                "changed_size":0, "deleted_count":0, "deleted_size":0,
                "renamed_count":0, "renamed_size":0}
        }))?;
        let stats = execution_stats_from_report(&report);
        assert_eq!(stats.scanned_files, 14);
        assert_eq!(stats.scanned_dirs, 2);
        Ok(())
    }

    #[tokio::test]
    async fn edited_config_survives_reload_and_only_semantic_changes_restart_scan() -> Result<()> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await?;
        let app = AppState::new(pool.clone());
        let ep = app
            .endpoint_service
            .create_endpoint(CreateEndpointRequest {
                name: "config source".into(),
                endpoint_type: EndpointType::Local,
                config: EndpointConfig::Local {
                    path: "/tmp/config-test".into(),
                },
            })
            .await?;
        let task = app
            .task_service
            .create_task(CreateTaskRequest {
                name: "config scan".into(),
                task_type: TaskType::Scan,
                source_endpoint_id: ep.id,
                source_path_id: None,
                dest_endpoint_id: None,
                dest_path_id: None,
                config: TaskConfig {
                    concurrency: Some(8),
                    depth: 1,
                    ..Default::default()
                },
            })
            .await?;
        let update = |config| UpdateTaskRequest {
            name: None,
            source_endpoint_id: None,
            source_path_id: None,
            config: Some(config),
        };
        let changed = app
            .task_service
            .update_task(
                &task.id,
                update(TaskConfig {
                    concurrency: Some(32),
                    ..task.config.clone()
                }),
            )
            .await?;
        assert!(
            !changed.config.reset_scan_on_start,
            "tuning concurrency must retain resume checkpoint"
        );
        let fresh = AppState::new(pool);
        let loaded = fresh.task_service.get_task(&task.id).await?;
        assert_eq!(loaded.config.concurrency, Some(32));
        assert_eq!(loaded.config.depth, 1);
        let changed = fresh
            .task_service
            .update_task(
                &task.id,
                update(TaskConfig {
                    depth: 2,
                    ..loaded.config.clone()
                }),
            )
            .await?;
        assert!(
            changed.config.reset_scan_on_start,
            "new scan scope must use a fresh baseline"
        );
        assert!(
            fresh
                .task_service
                .update_task(
                    &task.id,
                    update(TaskConfig {
                        concurrency: Some(0),
                        ..changed.config
                    })
                )
                .await
                .is_err()
        );
        assert_eq!(
            fresh.task_service.get_task(&task.id).await?.config.concurrency,
            Some(32)
        );
        Ok(())
    }
}
