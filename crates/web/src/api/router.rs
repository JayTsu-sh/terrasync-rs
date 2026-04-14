use axum::Router;
use axum::http::{HeaderValue, Method, header};
use axum::routing::{delete, get, post, put};
use tower_http::cors::{AllowHeaders, AllowOrigin, CorsLayer};

use crate::api::state::AppState;
use crate::api::{
    analytics_handler, config_handler, endpoint_handler, filter_handler, path_handler, static_files, task_handler,
    ws_handler,
};

/// 构建完整的 Axum Router
pub fn build_router(state: AppState, port: u16) -> Router {
    let api = Router::new()
        // 端点管理
        .route("/endpoints", get(endpoint_handler::list_endpoints))
        .route("/endpoints", post(endpoint_handler::create_endpoint))
        .route("/endpoints/{id}", get(endpoint_handler::get_endpoint))
        .route("/endpoints/{id}", put(endpoint_handler::update_endpoint))
        .route("/endpoints/{id}", delete(endpoint_handler::delete_endpoint))
        .route("/endpoints/{id}/test", post(endpoint_handler::test_connection))
        // 本地文件系统目录浏览（路径自动补全）
        .route("/fs/list-dirs", post(endpoint_handler::list_dirs))
        // NFS 导出查询
        .route("/nfs/exports", post(endpoint_handler::list_nfs_exports))
        // S3 桶查询
        .route("/s3/buckets", post(endpoint_handler::list_s3_buckets))
        // 路径管理
        .route("/endpoints/{id}/paths", get(path_handler::list_paths))
        .route("/endpoints/{id}/paths", post(path_handler::create_path))
        .route("/paths/{id}", delete(path_handler::delete_path))
        // 任务管理
        .route("/tasks", get(task_handler::list_tasks))
        .route("/tasks", post(task_handler::create_task))
        .route("/tasks/{id}", get(task_handler::get_task))
        .route("/tasks/{id}", put(task_handler::update_task))
        .route("/tasks/{id}", delete(task_handler::delete_task))
        .route("/tasks/{id}/start", post(task_handler::start_task))
        .route("/tasks/{id}/cancel", post(task_handler::cancel_task))
        .route(
            "/tasks/{id}/progress",
            get(task_handler::get_progress).post(task_handler::update_progress),
        )
        // 任务分析
        .route("/tasks/{id}/analytics", get(analytics_handler::get_analytics))
        // 执行历史
        .route("/tasks/{id}/executions", get(task_handler::list_executions))
        .route("/executions/{id}", get(task_handler::get_execution))
        .route("/executions/{id}", delete(task_handler::delete_execution))
        .route("/executions/{id}/logs", get(task_handler::get_execution_logs))
        // 过滤条件元数据
        .route("/filter-fields", get(filter_handler::get_filter_fields))
        // 系统配置
        .route("/config", get(config_handler::get_config))
        .route("/config", put(config_handler::update_config))
        .route("/config/test-clickhouse", post(config_handler::test_clickhouse))
        // WebSocket
        .route("/ws", get(ws_handler::ws_handler));

    let mut origins = vec![format!("http://127.0.0.1:{port}"), format!("http://localhost:{port}")];
    // 开发模式：前端 dev server 常用端口
    #[cfg(debug_assertions)]
    {
        origins.push("http://localhost:5173".to_string());
        origins.push("http://127.0.0.1:5173".to_string());
    }
    let allowed_origins: Vec<HeaderValue> = origins
        .into_iter()
        .filter_map(|s| HeaderValue::from_str(&s).ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(allowed_origins))
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(AllowHeaders::list([header::CONTENT_TYPE, header::AUTHORIZATION]));

    Router::new()
        .nest("/api/v1", api)
        .fallback(static_files::static_handler)
        .layer(cors)
        .with_state(state)
}
