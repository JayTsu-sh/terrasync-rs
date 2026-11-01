use app::error::AppError;
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use data_mover::model::FailureClass;
use data_mover::probes::ProbeError;
use data_mover::storage::StorageRoleFailure;
use serde::Serialize;
use storage_adapter::error::StorageError;
use thiserror::Error;
use tracing::error;

/// Web crate 专用错误枚举
#[derive(Error, Debug)]
pub enum WebError {
    /// App 层错误
    #[error("{0}")]
    AppError(#[from] AppError),

    /// 工具错误
    #[error("{0}")]
    UtilsError(#[from] utils::error::UtilsError),

    /// `SQLite` 数据库错误
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// 数据库迁移错误
    #[error("Migration error: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    /// IO 错误
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("GUI 数据库已被其他进程占用，无法启动第二个后端")]
    DatabaseLease(#[from] std::fs::TryLockError),

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

    /// 最新扫描明细当前不可用于分析。
    #[error("{0}")]
    AnalysisUnavailable(String),

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

    /// 端点 backend 构造失败，保留类型化错误链供服务端诊断。
    #[error("Endpoint connection setup failed")]
    EndpointAppConnectionFailed(#[source] AppError),

    /// 端点存储连通性检查失败，保留类型化错误链供服务端诊断。
    #[error("Endpoint storage connection failed")]
    EndpointStorageConnectionFailed(#[source] StorageError),

    /// WebSocket 错误
    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    /// 名称已存在
    #[error("{entity} with name '{name}' already exists")]
    DuplicateName { entity: String, name: String },

    /// Storage 层错误（如 NFS 查询失败）
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

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

// 只返回类型化分类对应的提示，不将底层 URL、认证信息或任意诊断文本写入响应。
fn connection_failure_class(error: &StorageError) -> Option<FailureClass> {
    let role = match error {
        StorageError::Role(role) => Some(role),
        StorageError::Probe(probe) => match probe.error.as_ref() {
            ProbeError::Role(role) => Some(role),
            _ => None,
        },
        StorageError::BackendConnect(error) => return error.session_failure().map(|failure| failure.class()),
        _ => None,
    };
    role.map(|role| match role {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    })
}

fn connection_failure_message(class: Option<FailureClass>) -> &'static str {
    match class {
        Some(FailureClass::PermissionDenied) => "连接测试失败：没有访问权限，请检查共享目录权限和用户身份",
        Some(FailureClass::Authentication) => "连接测试失败：认证失败，请检查账号和凭据",
        Some(FailureClass::Connectivity) => "连接测试失败：无法连接存储服务器，请检查网络、地址和端口",
        Some(FailureClass::NotFound) => "连接测试失败：共享路径不存在，请检查共享路径和前缀配置",
        Some(FailureClass::InvalidInput) => "连接测试失败：存储探测参数无效，请检查路径配置",
        Some(FailureClass::Unsupported) => "连接测试失败：存储服务不支持当前探测操作",
        Some(FailureClass::Throttled) => "连接测试失败：存储服务正在限流，请稍后重试",
        Some(FailureClass::Cancelled) => "连接测试已取消，请重试",
        Some(FailureClass::Protocol | FailureClass::Corruption) => "连接测试失败：存储服务响应异常，请检查协议配置",
        _ => "连接测试失败：请检查服务器地址、共享路径和认证配置",
    }
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

            WebError::EndpointInUseByRunningTask(_)
            | WebError::DuplicateName { .. }
            | WebError::AnalysisUnavailable(_) => StatusCode::CONFLICT,

            WebError::ConnectionTestFailed(_)
            | WebError::EndpointAppConnectionFailed(_)
            | WebError::EndpointStorageConnectionFailed(_) => StatusCode::BAD_GATEWAY,

            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let connection_error = match &self {
            WebError::EndpointStorageConnectionFailed(error)
            | WebError::EndpointAppConnectionFailed(AppError::StorageError(error)) => {
                Some(connection_failure_class(error))
            }
            WebError::EndpointAppConnectionFailed(_) => Some(None),
            _ => None,
        };
        // 可预期的连接失败提供安全提示；其他 5xx 仍隐藏内部错误。
        let error_msg = if let Some(class) = connection_error {
            error!(failure_class = ?class, status = %status, "Endpoint connection test failed");
            connection_failure_message(class).to_string()
        } else if status.is_server_error() {
            error!(error = %self, status = %status, "Internal server error");
            "Internal server error".to_string()
        } else {
            self.to_string()
        };

        let body = ErrorResponse { error: error_msg };

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use data_mover::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
    use data_mover::probes::{ProbeError, ProbeFailure};
    use data_mover::storage::StorageRoleFailure;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[tokio::test]
    async fn connection_test_returns_safe_actionable_failure() -> TestResult {
        let failure = EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Namespace,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "smb://user:do-not-expose@server/private",
        )?;
        let probe = ProbeFailure::from(ProbeError::Role(StorageRoleFailure::Entry(failure)));
        let response = WebError::EndpointStorageConnectionFailed(StorageError::Probe(probe)).into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 4096).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        let message = value["error"].as_str().unwrap_or_default();
        assert!(message.contains("权限"), "{message}");
        assert!(!message.contains("do-not-expose"));
        Ok(())
    }

    #[tokio::test]
    async fn connection_setup_error_does_not_expose_credentials() -> TestResult {
        let error = StorageError::ConfigError("s3://key:do-not-expose@host/bucket".into());
        let response = WebError::EndpointAppConnectionFailed(AppError::StorageError(error)).into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 4096).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        let message = value["error"].as_str().unwrap_or_default();
        assert!(message.contains("配置"), "{message}");
        assert!(!message.contains("do-not-expose"));
        Ok(())
    }

    #[tokio::test]
    async fn unexpected_server_errors_remain_private() -> TestResult {
        let response = WebError::IoError(std::io::Error::other("private internal detail")).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), 4096).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["error"], "Internal server error");
        Ok(())
    }
}
