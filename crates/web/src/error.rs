use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

/// Web crate 专用错误枚举
#[derive(Error, Debug)]
pub enum WebError {
    /// App 层错误
    #[error("{0}")]
    AppError(#[from] app::error::AppError),

    /// 工具错误
    #[error("{0}")]
    UtilsError(#[from] utils::error::UtilsError),

    /// SQLite 数据库错误
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// 数据库迁移错误
    #[error("Migration error: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    /// IO 错误
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    /// JSON 序列化/反序列化错误
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    /// 端点未找到
    #[error("Endpoint not found: {0}")]
    EndpointNotFound(String),

    /// 路径未找到
    #[error("Path not found: {0}")]
    PathNotFound(String),

    /// 任务未找到
    #[error("Task not found: {0}")]
    TaskNotFound(String),

    /// 执行记录未找到
    #[error("Execution not found: {0}")]
    ExecutionNotFound(String),

    /// 请求参数校验失败
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// 任务状态不允许该操作
    #[error("Task is {status}, cannot {operation}")]
    InvalidTaskState { status: String, operation: String },

    /// 端点正在被运行中的任务使用
    #[error("Endpoint is in use by running task: {0}")]
    EndpointInUseByRunningTask(String),

    /// 端点连接测试失败
    #[error("Connection test failed: {0}")]
    ConnectionTestFailed(String),

    /// WebSocket 错误
    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    /// 名称已存在
    #[error("{entity} with name '{name}' already exists")]
    DuplicateName { entity: String, name: String },

    /// Storage 层错误（如 NFS 查询失败）
    #[error("Storage error: {0}")]
    StorageError(#[from] storage_v2::error::StorageError),

    /// 请求体 JSON 反序列化失败
    #[error("Invalid request body: {0}")]
    JsonRejectionError(String),
}

impl From<JsonRejection> for WebError {
    fn from(rejection: JsonRejection) -> Self {
        WebError::JsonRejectionError(rejection.body_text())
    }
}

pub type Result<T> = std::result::Result<T, WebError>;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let status = match &self {
            WebError::EndpointNotFound(_)
            | WebError::PathNotFound(_)
            | WebError::TaskNotFound(_)
            | WebError::ExecutionNotFound(_) => StatusCode::NOT_FOUND,

            WebError::ValidationError(_) | WebError::InvalidTaskState { .. } | WebError::JsonRejectionError(_) => {
                StatusCode::BAD_REQUEST
            }

            WebError::EndpointInUseByRunningTask(_) | WebError::DuplicateName { .. } => StatusCode::CONFLICT,

            WebError::ConnectionTestFailed(_) => StatusCode::BAD_GATEWAY,

            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = ErrorResponse {
            error: self.to_string(),
        };

        (status, Json(body)).into_response()
    }
}
