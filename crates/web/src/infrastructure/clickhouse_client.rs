use tracing::debug;

use crate::error::{Result, WebError};

/// 创建预配置的 reqwest Client（带超时）
fn build_http_client(timeout_secs: u64) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| WebError::ConnectionTestFailed(format!("创建 HTTP 客户端失败: {e}")))
}

/// 测试 ClickHouse 连通性（`SELECT 1`）
pub async fn test_connectivity(dsn: &str, username: &str, password: &str, database: &str) -> Result<()> {
    let dsn = dsn.trim_end_matches('/');
    debug!("Testing ClickHouse connectivity: {}", dsn);

    let client = build_http_client(5)?;

    let query_url = format!("{}/?query=SELECT+1", dsn);
    let resp = client
        .get(&query_url)
        .basic_auth(username, Some(password))
        .header("X-ClickHouse-Database", database)
        .send()
        .await
        .map_err(|e| WebError::ConnectionTestFailed(format!("无法连接到 ClickHouse: {e}")))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(WebError::ConnectionTestFailed(format!("连接失败: {body}")));
    }

    Ok(())
}
