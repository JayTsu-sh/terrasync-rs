use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

/// 嵌入前端构建产物（web-ui/dist）
#[derive(Embed)]
#[folder = "../../web-ui/dist"]
#[prefix = ""]
struct Asset;

/// 静态文件服务 handler（SPA fallback）
pub async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // 尝试精确匹配文件
    if let Some(file) = Asset::get(path) {
        return serve_file(path, &file);
    }

    // SPA fallback: 非 API 路径都返回 index.html
    if let Some(file) = Asset::get("index.html") {
        return serve_file("index.html", &file);
    }

    not_found_response()
}

fn serve_file(path: &str, file: &rust_embed::EmbeddedFile) -> Response<Body> {
    let mime = mime_guess::from_path(path).first_or_octet_stream();

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .body(Body::from(file.data.clone()))
        .unwrap_or_else(|_| not_found_response())
}

fn not_found_response() -> Response<Body> {
    let mut res = Response::new(Body::from("Not Found"));
    *res.status_mut() = StatusCode::NOT_FOUND;
    res
}
