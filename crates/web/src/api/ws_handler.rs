use axum::extract::WebSocketUpgrade;
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tracing::warn;

/// WebSocket 进度推送端点
///
/// 当前为占位实现 — 保持连接并定期发送心跳。
/// 后续将集成 `TaskRunner` 的进度通道，推送 `ProgressEvent`。
pub async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let ping = serde_json::json!({ "type": "ping" });
                if let Ok(text) = serde_json::to_string(&ping)
                    && sender.send(Message::Text(text.into())).await.is_err()
                {
                    break;
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
    warn!("WebSocket client disconnected");
}
