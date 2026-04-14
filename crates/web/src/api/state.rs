use std::sync::Arc;

use sqlx::SqlitePool;

use crate::application::analytics_service::AnalyticsService;
use crate::application::config_service::ConfigService;
use crate::application::endpoint_service::EndpointService;
use crate::application::path_service::PathService;
use crate::application::storage_browser_service::StorageBrowserService;
use crate::application::task_service::TaskService;
use crate::infrastructure::config_repo::SqliteConfigRepo;
use crate::infrastructure::endpoint_repo::SqliteEndpointRepo;
use crate::infrastructure::execution_repo::SqliteExecutionRepo;
use crate::infrastructure::path_repo::SqlitePathRepo;
use crate::infrastructure::progress_repo::SqliteProgressRepo;
use crate::infrastructure::task_repo::SqliteTaskRepo;
use crate::infrastructure::task_runner::TaskRunner;

/// Axum 应用共享状态
#[derive(Clone)]
pub struct AppState {
    pub endpoint_service: Arc<EndpointService>,
    pub path_service: Arc<PathService>,
    pub task_service: Arc<TaskService>,
    pub analytics_service: Arc<AnalyticsService>,
    pub config_service: Arc<ConfigService>,
    pub storage_browser_service: Arc<StorageBrowserService>,
}

impl AppState {
    pub async fn new(pool: SqlitePool) -> Self {
        let endpoint_repo = Arc::new(SqliteEndpointRepo::new(pool.clone()));
        let path_repo = Arc::new(SqlitePathRepo::new(pool.clone()));
        let task_repo = Arc::new(SqliteTaskRepo::new(pool.clone()));
        let execution_repo = Arc::new(SqliteExecutionRepo::new(pool.clone()));
        let progress_repo = Arc::new(SqliteProgressRepo::new(pool.clone()));
        let config_repo = Arc::new(SqliteConfigRepo::new(pool.clone()));

        let task_runner = Arc::new(TaskRunner::new(
            task_repo.clone(),
            endpoint_repo.clone(),
            path_repo.clone(),
            execution_repo.clone(),
            config_repo.clone(),
        ));

        let endpoint_service = Arc::new(EndpointService::new(endpoint_repo.clone(), task_repo.clone()));
        let path_service = Arc::new(PathService::new(path_repo.clone(), endpoint_repo.clone()));
        let task_service = Arc::new(TaskService::new(
            task_repo,
            execution_repo,
            endpoint_repo,
            path_repo,
            progress_repo.clone(),
            task_runner,
        ));
        let analytics_service = Arc::new(AnalyticsService::new(progress_repo));
        let config_service = Arc::new(ConfigService::new(config_repo));
        let storage_browser_service = Arc::new(StorageBrowserService::new());

        Self {
            endpoint_service,
            path_service,
            task_service,
            analytics_service,
            config_service,
            storage_browser_service,
        }
    }
}
