use std::net::SocketAddr;

use tracing::info;

use crate::error::{Result, WebError};

/// 启动 Web GUI 服务器
///
/// 服务器会监听 Ctrl+C 信号进行优雅关闭。
pub async fn start_web_server(host: &str, port: u16) -> Result<()> {
    // 1. 初始化 SQLite 数据库
    let db_pool = crate::infrastructure::db::init_database().await?;

    // 2. 构建应用状态
    let app_state = crate::api::state::AppState::new(db_pool).await;

    // 3. 从 SQLite 加载已保存的配置并应用
    app_state.config_service.apply_saved_config().await;

    // 4. 构建路由
    let app = crate::api::router::build_router(app_state, port);

    // 5. 绑定地址并启动服务
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| WebError::ValidationError(format!("Invalid address: {e}")))?;

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(WebError::IoError)?;

    // 通知 task_runner 进度回调基础 URL（仅 web 层持有，不污染 app crate）
    crate::infrastructure::task_runner::set_progress_callback_base_url(format!("http://127.0.0.1:{port}/api/v1"));

    info!("TerraSync Web GUI listening on http://{}", addr);
    info!("Press Ctrl+C to stop the server");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(WebError::IoError)?;

    info!("Web server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("Ctrl+C received, shutting down...");
}
