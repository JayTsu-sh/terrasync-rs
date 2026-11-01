use std::sync::Arc;

use app::consumer::stats::report::ProgressReport;

use crate::domain::analysis::{AnalysisRepository, AnalysisTreeQuery, StorageAnalysis};
use crate::domain::endpoint::EndpointConfig;
use crate::domain::repository::{
    EndpointRepository, ExecutionRepository, PathRepository, ProgressRepository, TaskRepository,
};
use crate::domain::task::{TaskStatus, TaskType};
use crate::error::{Result, WebError};

pub struct StorageAnalysisService {
    analysis_repo: Arc<dyn AnalysisRepository>,
    task_repo: Arc<dyn TaskRepository>,
    path_repo: Arc<dyn PathRepository>,
    endpoint_repo: Arc<dyn EndpointRepository>,
    progress_repo: Arc<dyn ProgressRepository>,
    execution_repo: Arc<dyn ExecutionRepository>,
}

impl StorageAnalysisService {
    pub fn new(
        analysis_repo: Arc<dyn AnalysisRepository>, task_repo: Arc<dyn TaskRepository>,
        path_repo: Arc<dyn PathRepository>, endpoint_repo: Arc<dyn EndpointRepository>,
        progress_repo: Arc<dyn ProgressRepository>, execution_repo: Arc<dyn ExecutionRepository>,
    ) -> Self {
        Self {
            analysis_repo,
            task_repo,
            path_repo,
            endpoint_repo,
            progress_repo,
            execution_repo,
        }
    }

    pub async fn get_tree(&self, task_id: &str, query: &AnalysisTreeQuery) -> Result<StorageAnalysis> {
        let (path, limit) = query.validate()?;
        let task = self
            .task_repo
            .find_by_id(task_id)
            .await?
            .ok_or_else(|| WebError::TaskNotFound(task_id.to_string()))?;
        if task.task_type != TaskType::Scan || task.status == TaskStatus::Pending || task.config.reset_scan_on_start {
            return Err(WebError::AnalysisUnavailable(
                "请先执行扫描任务，再查看目录数据分析".to_string(),
            ));
        }
        let endpoint = self
            .endpoint_repo
            .find_by_id(&task.source_endpoint_id)
            .await?
            .ok_or_else(|| WebError::EndpointNotFound(task.source_endpoint_id.clone()))?;
        let executions = self.execution_repo.find_by_task_id(task_id).await?;
        // 失败重启会推进 task.started_at；必须以真实成功执行绑定旧基线的源配置。
        // 首次未完成扫描则保守绑定最早一次执行，避免重试替旧的部分数据改名。
        let source_execution = executions
            .iter()
            .filter(|execution| execution.status == TaskStatus::Completed)
            .max_by_key(|execution| execution.started_at)
            .or_else(|| executions.iter().min_by_key(|execution| execution.started_at));
        if source_execution.is_none_or(|execution| endpoint.updated_at > execution.started_at) {
            return Err(WebError::AnalysisUnavailable(
                "扫描源配置已变更或无法确认，请重新完成扫描后查看目录数据分析".to_string(),
            ));
        }
        // 展示扫描范围，不返回含密码的 storage URL。
        let base = match &endpoint.config {
            EndpointConfig::Local { path } => path.clone(),
            EndpointConfig::Nfs {
                export_path, prefix, ..
            } => join_path(export_path, prefix.as_deref()),
            EndpointConfig::S3 { bucket, prefix, .. } => join_path(bucket, prefix.as_deref()),
            EndpointConfig::Cifs { share, prefix, .. } => join_path(share, prefix.as_deref()),
        };
        let sub_path = if let Some(path_id) = &task.source_path_id {
            let selected = self
                .path_repo
                .find_by_id(path_id)
                .await?
                .ok_or_else(|| WebError::PathNotFound(path_id.clone()))?;
            if selected.endpoint_id != endpoint.id {
                return Err(WebError::AnalysisUnavailable(
                    "扫描子路径不属于当前源端点，请检查任务配置".to_string(),
                ));
            }
            Some(selected.sub_path)
        } else {
            None
        };
        let source_path = join_path(&base, sub_path.as_deref());
        let children = self.analysis_repo.children(task_id, &path, query.metric, limit).await?;
        let current = self
            .task_repo
            .find_by_id(task_id)
            .await?
            .ok_or_else(|| WebError::TaskNotFound(task_id.to_string()))?;
        if current.started_at != task.started_at || current.status != task.status || current.config.reset_scan_on_start
        {
            return Err(WebError::AnalysisUnavailable(
                "扫描结果正在更新，请刷新后重试".to_string(),
            ));
        }
        if self
            .endpoint_repo
            .find_by_id(&endpoint.id)
            .await?
            .is_none_or(|current| current.updated_at != endpoint.updated_at)
        {
            return Err(WebError::AnalysisUnavailable(
                "扫描源配置正在更新，请刷新后重试".to_string(),
            ));
        }
        let total_bytes = children.iter().map(|child| child.bytes).sum();
        let file_count = children.iter().map(|child| child.file_count).sum();
        let directory_count = children.iter().map(|child| child.directory_count).sum();
        let progress = self.progress_repo.find_by_task_id(task_id).await?;
        let report = progress
            .as_ref()
            .and_then(|p| serde_json::from_str::<ProgressReport>(&p.report_json).ok());
        let final_stats = report
            .as_ref()
            .filter(|r| r.is_final)
            .and_then(|r| r.final_stats.as_ref());
        // 完成回调不保证数据库每批写入成功；在根视图额外核对文件条目数。
        let is_partial = task.status != TaskStatus::Completed
            || final_stats
                .is_none_or(|s| s.errors.total() > 0 || (path.is_empty() && file_count != s.regular_files as u64));
        Ok(StorageAnalysis {
            task_id: task.id,
            path,
            source_path,
            scanned_at: task.finished_at.map(|t| t.to_rfc3339()),
            is_partial,
            total_bytes,
            file_count,
            directory_count,
            children,
        })
    }
}

fn join_path(base: &str, suffix: Option<&str>) -> String {
    suffix.filter(|s| !s.is_empty()).map_or_else(
        || base.to_string(),
        |suffix| format!("{}/{}", base.trim_end_matches('/'), suffix.trim_start_matches('/')),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::StorageAnalysisService;
    use crate::api::state::AppState;
    use crate::application::task_service::UpdateTaskRequest;
    use crate::domain::analysis::{AnalysisMetric, AnalysisNode, AnalysisRepository, AnalysisTreeQuery};
    use crate::error::{Result, WebError};
    use crate::infrastructure::endpoint_repo::SqliteEndpointRepo;
    use crate::infrastructure::execution_repo::SqliteExecutionRepo;
    use crate::infrastructure::path_repo::SqlitePathRepo;
    use crate::infrastructure::progress_repo::SqliteProgressRepo;
    use crate::infrastructure::task_repo::SqliteTaskRepo;

    struct FixtureAnalysis;

    #[async_trait]
    impl AnalysisRepository for FixtureAnalysis {
        async fn children(&self, _: &str, _: &str, _: AnalysisMetric, _: u16) -> Result<Vec<AnalysisNode>> {
            Ok(vec![AnalysisNode {
                path: "data".into(),
                name: "data".into(),
                kind: "directory".into(),
                bytes: 123,
                file_count: 2,
                directory_count: 1,
                modified_at: None,
            }])
        }
    }

    async fn fixture(status: &str, task_type: &str, expected_files: u64) -> StorageAnalysisService {
        fixture_with_pool(status, task_type, expected_files).await.0
    }

    async fn fixture_with_pool(
        status: &str, task_type: &str, expected_files: u64,
    ) -> (StorageAnalysisService, sqlx::SqlitePool) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO endpoints (id,name,endpoint_type,config_json,created_at,updated_at) VALUES ('endpoint','S3','s3',?,'2026-10-31T00:00:00Z','2026-10-31T00:00:00Z')")
            .bind(r#"{"type":"s3","access_key":"private-key","secret_key":"private-secret","bucket":"archive","host":"example.com","port":9000,"prefix":"team","protocol":"http"}"#)
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO migration_tasks (id,name,task_type,source_endpoint_id,config_json,status,created_at) VALUES ('task','scan',?,'endpoint','{}',?,'2026-10-31T00:00:00Z')")
            .bind(task_type).bind(status).execute(&pool).await.unwrap();
        if status != "pending" {
            sqlx::query("INSERT INTO task_executions (id,task_id,execution_type,status,started_at) VALUES ('original','task','full',?,'2026-10-31T01:00:00Z')")
                .bind(status).execute(&pool).await.unwrap();
        }
        let report = serde_json::json!({
            "job_id":"task", "job_type":"scan", "elapsed_secs":1, "speed_bytes_per_sec":0,
            "detail":{"kind":"Full","scanned_files":expected_files,"scanned_dirs":1,"total_bytes":123,"transferred_bytes":0,"error_count":0},
            "is_final":true,
            "final_stats":{"dirs":1,"dir_size":0,"regular_files":expected_files,"regular_file_size":123,"symlinks":0,"symlink_size":0,
                "errors":{"scan":0,"copy":0,"copy_acl":0,"copy_xattr":0,"delete":0,"rename":0,"symlink_op":0,"pack":0,"integrity_check":0}}
        });
        sqlx::query("INSERT INTO task_progress (task_id,report_json,is_final,updated_at) VALUES ('task',?,1,'2026-10-31T00:00:00Z')")
            .bind(report.to_string())
            .execute(&pool)
            .await
            .unwrap();
        let service = StorageAnalysisService::new(
            Arc::new(FixtureAnalysis),
            Arc::new(SqliteTaskRepo::new(pool.clone())),
            Arc::new(SqlitePathRepo::new(pool.clone())),
            Arc::new(SqliteEndpointRepo::new(pool.clone())),
            Arc::new(SqliteProgressRepo::new(pool.clone())),
            Arc::new(SqliteExecutionRepo::new(pool.clone())),
        );
        (service, pool)
    }

    #[tokio::test]
    async fn semantic_source_edits_hide_old_analysis_until_rerun() {
        let (service, pool) = fixture_with_pool("completed", "scan", 2).await;
        sqlx::query("INSERT INTO migration_paths (id,endpoint_id,sub_path,created_at) VALUES ('new-path','endpoint','other-team','2026-11-01T00:00:00Z')")
            .execute(&pool).await.unwrap();
        let app = AppState::new(pool);
        let task = app
            .task_service
            .update_task(
                "task",
                UpdateTaskRequest {
                    name: None,
                    source_endpoint_id: None,
                    source_path_id: Some("new-path".into()),
                    config: None,
                },
            )
            .await
            .unwrap();
        assert!(task.config.reset_scan_on_start);
        assert!(matches!(
            service.get_tree("task", &AnalysisTreeQuery::default()).await,
            Err(WebError::AnalysisUnavailable(_))
        ));
    }

    #[tokio::test]
    async fn endpoint_edit_is_not_validated_by_a_failed_restart() {
        let (service, pool) = fixture_with_pool("completed", "scan", 2).await;
        sqlx::query("UPDATE endpoints SET config_json=replace(config_json,'archive','different-bucket'), updated_at='2026-10-31T02:00:00Z' WHERE id='endpoint'")
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE migration_tasks SET status='failed', started_at='2026-10-31T03:00:00Z' WHERE id='task'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_executions (id,task_id,execution_type,status,started_at) VALUES ('retry','task','incremental','failed','2026-10-31T03:00:00Z')")
            .execute(&pool).await.unwrap();
        assert!(matches!(
            service.get_tree("task", &AnalysisTreeQuery::default()).await,
            Err(WebError::AnalysisUnavailable(_))
        ));
        // 真正完成新一轮后，原有执行历史无需迁移即可证明新配置已被扫描。
        sqlx::query("UPDATE task_executions SET status='completed' WHERE id='retry'")
            .execute(&pool)
            .await
            .unwrap();
        let result = service.get_tree("task", &AnalysisTreeQuery::default()).await.unwrap();
        assert_eq!(result.source_path, "different-bucket/team");
    }

    #[tokio::test]
    async fn uncompleted_retry_keeps_analysis_of_unchanged_completed_source() {
        let (service, pool) = fixture_with_pool("completed", "scan", 2).await;
        sqlx::query("INSERT INTO task_executions (id,task_id,execution_type,status,started_at) VALUES ('retry','task','incremental','failed','2026-10-31T03:00:00Z')")
            .execute(&pool).await.unwrap();
        assert_eq!(
            service
                .get_tree("task", &AnalysisTreeQuery::default())
                .await
                .unwrap()
                .source_path,
            "archive/team"
        );
    }

    #[tokio::test]
    async fn first_partial_scan_cannot_be_relabeled_by_an_endpoint_edit_and_retry() {
        let (service, pool) = fixture_with_pool("failed", "scan", 2).await;
        sqlx::query("UPDATE endpoints SET updated_at='2026-10-31T02:00:00Z' WHERE id='endpoint'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_executions (id,task_id,execution_type,status,started_at) VALUES ('retry','task','full','failed','2026-10-31T03:00:00Z')")
            .execute(&pool).await.unwrap();
        assert!(matches!(
            service.get_tree("task", &AnalysisTreeQuery::default()).await,
            Err(WebError::AnalysisUnavailable(_))
        ));
    }

    #[tokio::test]
    async fn missing_selected_path_does_not_relabel_the_scan_as_endpoint_root() {
        let (service, pool) = fixture_with_pool("completed", "scan", 2).await;
        // 模拟从旧数据库导入的悬空路径引用。
        sqlx::query("PRAGMA foreign_keys=OFF").execute(&pool).await.unwrap();
        sqlx::query("UPDATE migration_tasks SET source_path_id='missing-path' WHERE id='task'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            service.get_tree("task", &AnalysisTreeQuery::default()).await,
            Err(WebError::PathNotFound(_))
        ));
    }

    #[tokio::test]
    async fn analysis_uses_real_task_scope_without_exposing_storage_credentials() {
        let service = fixture("completed", "scan", 2).await;
        let result = service.get_tree("task", &AnalysisTreeQuery::default()).await.unwrap();
        assert_eq!(result.source_path, "archive/team");
        assert_eq!(result.total_bytes, 123);
        assert_eq!(result.file_count, 2);
        assert!(!result.is_partial);
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("private-key"));
        assert!(!json.contains("private-secret"));
    }

    #[tokio::test]
    async fn does_not_describe_running_or_missing_entries_as_complete() {
        let query = AnalysisTreeQuery::default();
        assert!(
            fixture("running", "scan", 2)
                .await
                .get_tree("task", &query)
                .await
                .unwrap()
                .is_partial
        );
        assert!(
            fixture("completed", "scan", 3)
                .await
                .get_tree("task", &query)
                .await
                .unwrap()
                .is_partial
        );
        assert!(matches!(
            fixture("pending", "scan", 2).await.get_tree("task", &query).await,
            Err(WebError::AnalysisUnavailable(_))
        ));
        assert!(matches!(
            fixture("completed", "sync", 2).await.get_tree("task", &query).await,
            Err(WebError::AnalysisUnavailable(_))
        ));
        assert!(matches!(
            fixture("completed", "scan", 2).await.get_tree("missing", &query).await,
            Err(WebError::TaskNotFound(_))
        ));
    }
}
