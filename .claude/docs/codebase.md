# Codebase — Workspace 详细结构

## Crate 入口文件

| Crate | 关键入口 | 说明 |
|-------|---------|------|
| 根包 | `src/main.rs` | CLI 入口 |
| `crates/cli/` | `src/commands.rs` | 子命令解析与应用调用 |
| `crates/app/` | `src/scan.rs`, `src/sync.rs`, `src/dir_walker.rs` | 产品任务与扫描／复制编排 |
| `crates/app/` | `src/consumer/manager.rs` | 数据库与统计消费者的生命周期 |
| `data-mover` | 根 Cargo.toml 中固定 revision 的 git 依赖 | 原生观察、typed storage roles、遍历、传输、元数据及恢复 |
| `crates/task-model/` | `src/lib.rs`, `src/observation.rs` | 无 I/O 的共享观察视图、任务事件和传输模式 |
| `crates/db/` | `src/catalogue/`, `src/scan/`, `src/execution/`, `src/schema/` | 唯一 ClickHouse 实现；Single／Cluster 布局在内部处理 |
| `crates/transport/` | `src/message.rs`, `src/directory.rs`, `src/chunk.rs` | 进程内／QUIC 消息、NDX、快照 wire 编码和块所有权 |
| `crates/storage-adapter/` | `src/connected.rs`, `src/scanning.rs`, `src/streaming.rs` | 重构中尚待归位的产品配置、观察投影与存储调用；db 和 transport 已不依赖它 |
| `crates/sync-delta/` | `src/lib.rs` | 块级签名与 delta 算法 |
| `crates/utils/` | `src/app_config.rs` | 应用配置及公共工具 |
| `crates/web/` | `src/api/`, `src/application/`, `src/domain/`, `src/infrastructure/` | Web 入口与应用契约 |

## Cargo Features

| Feature | 默认 | 说明 |
|---------|------|------|
| `basic` | ✓ | 基础 CLI |
| `license` | ✗ | 离线 license 验证与激活 |
| `gui` | ✗ | Web GUI |
| `profiling` | ✗ | `console-subscriber`（tokio-console）|
| `lab-fault-injection` | ✗ | 实验室受控故障验证入口 |

```bash
cargo check --workspace --all-targets --locked
cargo build --all-features
cargo build --release --locked
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

公开入口以各 crate 的 `lib.rs` 为准；避免为删除的模型保留兼容再导出。

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
