# Codebase — Workspace 详细结构

## Crate 入口文件

| Crate | 关键入口 | 说明 |
|-------|---------|------|
| `src/` (root) | `src/main.rs` | 只调 `cli::cli_match()`，无业务逻辑 |
| `cli/` | `src/commands.rs` | 各子命令实现（scan/sync/ace/web/license） |
| `app/` | `src/scan.rs`, `src/sync.rs`, `src/dir_walker.rs` | 核心业务逻辑 |
| `app/` | `src/consumer/manager.rs` | ConsumerManager，注册/驱动所有 consumer |
| `data-mover` | 外部 git dep | StorageEnum、NASEntry、S3Entry、filter、qos |
| `db/` | `src/traits.rs` | Database trait（ClickHouse + DuckDB 双实现）|
| `transport/` | `src/lib.rs`, `src/message.rs` | SenderMsg/ReceiverMsg 协议定义 |
| `sync-delta/` | `src/lib.rs` | chunk 级 rolling checksum + delta 算法 |
| `utils/` | `src/config.rs` | AppConfig（全局配置单例）|
| `web/` | `src/api/`, `src/application/`, `src/domain/`, `src/infrastructure/` | DDD 四层 |

## Cargo Features

| Feature | 默认 | 说明 |
|---------|------|------|
| `basic` | ✓ | 始终开启 |
| `duckdb` | ✗ | DuckDB 数据库支持（`db/duckdb.rs`）|
| `license` | ✗ | 离线 license 验证与激活 |
| `gui` | ✗ | GUI 模式 |
| `profiling` | ✗ | `console-subscriber`（tokio-console）|

常用组合：
```bash
cargo build --features duckdb          # 开发调试
cargo build --all-features             # 全功能验证
cargo build --release                  # 生产构建（基础功能）
```

## Workspace Clippy Lints

`Cargo.toml` workspace 级别，所有 crate 继承：

```toml
[workspace.lints.clippy]
pedantic = "warn"          # 大量额外检查（已豁免 module_name_repetitions 等）
unwrap_used = "deny"       # 编译期强制 — 不得 .unwrap()
expect_used = "deny"       # 编译期强制 — 不得 .expect()
dbg_macro = "warn"
todo = "warn"
unimplemented = "warn"

[workspace.lints.rust]
unsafe_code = "warn"       # NFS FFI 处可局部 #[allow] 豁免
```

豁免写法（仅在合理场景使用）：
```rust
#[allow(clippy::unwrap_used)]   // 测试代码
#[allow(unsafe_code)]           // NFS FFI 绑定
```

## 代码风格

`rustfmt.toml` 配置：
```toml
max_width = 120
edition = "2024"
imports_granularity = "Module"
group_imports = "StdExternalCrate"
```

所有 crate 暴露 `pub mod prelude` 做常用类型再导出。

## 前端技术栈与约束

技术栈：`Vue 3 <script setup>` + `Naive UI` + `Tailwind CSS` + `Pinia` + `Vue Router`

目录：`web-ui/src/`

**约束（强制）：**
- 优先复用 `web-ui/src/components/` 已有组件，**禁止**重复造轮子
- **禁止**引入新图标包，项目统一使用 `@vicons/ionicons5`
- 设计稿使用 Pencil MCP，实现时追求 1:1 视觉保真
- 尽量依赖 Naive UI 默认样式，减少 Tailwind 手动覆盖

**构建：**
```bash
cd web-ui && npm install && npm run build
# 或通过 Makefile
make frontend   # 仅构建前端
make backend    # 仅构建后端
make all        # 前端 + 后端
```

前端构建产物由 `web/build.rs` 在后端构建时自动嵌入（可通过 `SKIP_FRONTEND_BUILD=1` 跳过）。
