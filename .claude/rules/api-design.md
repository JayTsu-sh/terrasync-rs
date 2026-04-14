---
description: API design patterns for web/ crate (axum handlers)
paths:
  - "web/src/api/**/*"
  - "web/src/application/**/*"
---

# API Design Rules（Rust / axum）

## Handler 结构

每个 handler 遵循此模式：

```rust
pub async fn do_something(
    State(state): State<AppState>,
    Json(req): Json<SomeRequest>,  // 或 Path / Query
) -> Result<Json<T>> {
    // 1. 输入已由 axum extractor 反序列化验证
    // 2. 调用 service 层（绝不内联业务逻辑）
    let result = state.some_service.do_thing(req).await?;
    // 3. 返回结果
    Ok(Json(result))
}
```

- Handler 不包含业务逻辑，只做 HTTP ↔ Service 适配
- Handler 超过 20 行说明逻辑放错了地方
- 错误通过 `?` 传播，由全局错误处理转换为 HTTP 响应

## 四层 DDD 分层

```
web/src/
  api/           # Handler + Router（只做适配）
  application/   # Service 层（编排业务流程）
  domain/        # 实体 + 值对象（纯业务规则）
  infrastructure/# Repo + DB + 外部集成
```

依赖方向：`api → application → domain ← infrastructure`

## 错误处理

- 使用 `web::error::WebError`（thiserror 枚举）
- `impl IntoResponse for WebError` 统一转换为 JSON 错误响应
- 输入校验失败用 `JsonRejection` 捕获并转为 400

## 查询参数

列表接口通过 `Query<XxxQuery>` 接收筛选/分页参数，字段类型用 `Option<T>` + `Default`。
