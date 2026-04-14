# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Before You Write Any Code

Every time. No exceptions.

1. **Grep first.** Search for existing patterns before creating anything. If a convention exists, follow it.
2. **Blast radius.** What depends on what you're changing? Check imports, tests, consumers. Unknown blast radius = not ready to code.
3. **Ask, don't assume.** Ambiguous request? Ask ONE clarifying question. Don't guess, don't ask five questions. One, then move.
4. **Smallest change.** Solve what was asked. No bonus refactors. No unrequested features. Scope creep is a bug.
5. **Verification plan.** How will you prove this works? Answer this before writing code.

## Build & Development Commands

```bash
# Standard build
cargo build                          # debug, basic feature only
cargo build --release                # release build
cargo build --features duckdb        # with DuckDB support
cargo build --all-features           # all features

# Cross-compile for Linux (uses cargo-zigbuild)
make release                         # x86_64-unknown-linux-musl release
make debug                           # x86_64-unknown-linux-musl debug

# Run
cargo run -- scan --id my_scan /path/to/dir
cargo run --features duckdb -- scan --id my_scan /path/to/dir

# Tests
cargo test --workspace --no-fail-fast           # all workspace tests
cargo test -p app test_scan                     # single test in a crate
cargo test -p app -- test_name --nocapture      # with output

# Code quality
cargo fmt                            # format (see rustfmt.toml)
cargo check                          # type-check without building
```

## Workspace Architecture

Cargo workspace，crate 职责：

| Crate          | Role                                                                     |
| -------------- | ------------------------------------------------------------------------ |
| `src/` (root)  | Entry point only — calls `cli::cli_match()`                              |
| `cli/`         | Argument parsing (clap), routes commands to `app`                        |
| `app/`         | Core business logic: scan, sync, dir\_walker, consumers, ACE             |
| `storage_v2/`  | Storage abstraction: NASEntry, S3Entry, StorageV2Enum, filter, qos       |
| `db/`          | Database layer: ClickHouse (always), DuckDB (feature-gated)              |
| `transport/`   | 传输层抽象：InProcess / QUIC，Sender↔Receiver 消息协议                   |
| `sync-delta/`  | chunk 级增量算法（rsync 风格）：rolling checksum, block signature, delta  |
| `licensing/`   | 离线 license 验证、激活、生成（Ed25519 签名 + 机器指纹绑定）             |
| `kafka/`       | Kafka producer/consumer for distributed sync mode                        |
| `utils/`       | Shared utilities: AppConfig (config.rs), logger, crypto, types           |
| `web/`         | Web API (axum + SQLite), DDD 四层: api/application/domain/infrastructure |

### Data Flow

```
CLI args → cli::cli_match()
         → app::scan / app::sync / app::ace
         → app::dir_walker (walks storage via storage_v2::StorageV2Enum)
         → transport (InProcess/QUIC) ←→ app::receiver (双进程模式)
         → sync-delta (增量同步时的 chunk 匹配)
         → app::consumer (ConsumerManager → DatabaseConsumer, StatisticConsumer, KafkaConsumer)
         → db::Database trait (ClickHouse or DuckDB backend)
```

### Key Types

- **`storage_v2::{NASEntry, S3Entry}`** — 主力 entry 类型（NAS/S3 分离，类型精确）
- **`storage_v2::storage_enum::StorageV2Enum`** — 存储枚举 dispatch（Local/NFS/S3/CIFS）
- **`transport::message::{SenderMsg, ReceiverMsg}`** — Sender↔Receiver 传输协议消息
- **`sync_delta::DeltaToken`** — 增量差异描述（BlockRef / Literal）
- **`db::traits::Database`** — abstraction over ClickHouse / DuckDB backends
- **`app::scan::ScanType`** — `Full` or `Incremental` (determined by job directory existence)

### Features

- **`basic`** (default) — always on
- **`duckdb`** — enables DuckDB database support via `db/duckdb.rs`
- **`license`** — enables license verification and activation
- **`gui`** — enables GUI mode
- **`profiling`** — enables `console-subscriber` for tokio-console

### Incremental Scan/Sync

Operations become incremental automatically when a `jobs/<job_id>/` directory already exists. Deleting that directory resets to full scan/sync. Database tables are namespaced by job ID (e.g., `base_<job_id>`, `incremental_<job_id>`).

### Storage URL Formats

- Local: `/path/to/dir` or `C:\path\to\dir`
- NFS v3: `nfs://server:port/export/path:/prefix?uid=1000&gid=1000`
- S3: `s3://access_key:secret_key@bucket.host:port/prefix` or `s3+https://...`
- SMB/CIFS: `smb://user:password@host[:port]/share[/sub/path]`（域用户 `\` 需编码为 `%5C`）

### Code Style

- `rustfmt.toml`: `max_width = 120`, `edition = "2024"`, `imports_granularity = "Module"`, `group_imports = "StdExternalCrate"`
- All crates expose a `pub mod prelude` for common re-exports

### use 语句规范（强制）

**约束 1 — 位置：** 所有 `use` 语句必须集中在文件顶部，禁止在函数体、`impl` 块或任何嵌套作用域内声明 `use`。

- 唯一例外：`#[cfg(test)]` / `tests/` 块内可在块内部使用 `use`

**约束 2 — 路径深度：** 在函数体/表达式中直接书写的路径最多两层（`A::B`）。超过两层的类型必须通过顶部 `use` 导入后使用短名称。禁止在代码中直接书写 `crate::xxx` 形式的路径。

```rust
// ❌ 禁止 — 三层路径 / crate:: 直接出现在代码中
let entry = storage_v2::common::NASEntry::new();
fn foo() -> crate::error::Result<()> { ... }

// ✅ 正确 — 通过顶部 use 导入
use storage_v2::common::NASEntry;
use crate::error::Result;
let entry = NASEntry::new();
```

> 例外：`std::io::Error`、`std::fmt::Display` 等标准库广为人知的路径，在不影响可读性时可酌情保留。

### 重构规范（强制）

**重构只改结构，不改语义。重构前后的可观测行为必须完全一致。**

- 允许：提取函数、重命名变量、调整模块结构、消除重复、改善类型签名
- 禁止：在重构的同时修改逻辑、添加新功能、改变错误处理路径、调整并发行为

**验证方式：** 重构完成后，所有现有测试必须 pass，且不得修改测试本身来迁就重构。

```rust
// ❌ 重构时偷偷改了逻辑（原来出错时跳过，现在改成返回错误）
// before
if let Err(e) = do_thing() { error!("{}", e); }
// after（声称是"重构"，实为改变语义）
do_thing()?;

// ✅ 保留原有语义
if let Err(e) = do_thing() { error!("{}", e); }
```

如果在重构过程中发现原有逻辑有问题，应**单独提交**修复，不与重构混在一起。

### 错误处理规范（强制）

#### 1. 禁止 `.unwrap()` / `.expect()`

**严禁在任何生产代码中使用** **`.unwrap()`** **和** **`.expect()`。**

已通过 workspace clippy lint 强制执行（`Cargo.toml` 中 `unwrap_used = "deny"`, `expect_used = "deny"`），编译期自动拒绝。

```rust
// ❌ 禁止（编译不过）
let val = some_option.unwrap();

// ✅ 用 ? / if let / ok_or / unwrap_or_default
let val = some_result?;
let val = some_option.ok_or(MyError::NotFound)?;
let val = some_option.unwrap_or_default();
```

**唯一例外：** `#[cfg(test)]` 块或 `tests/` 目录下的测试代码（通过 `#[allow(clippy::unwrap_used)]`）。

***

#### 2. 错误类型：每个 crate 必须用 `thiserror` 定义专属 Error 枚举

**每个 crate 都必须有** **`src/error.rs`，定义该 crate 专属的错误枚举，不得使用** **`Box<dyn Error>`** **或** **`String`** **作为跨边界错误类型。**

##### 标准文件结构

```
<crate>/src/
  error.rs      # XxxError 枚举 + pub type Result<T>
  lib.rs        # pub mod error; pub use error::{XxxError, Result};
```

##### 枚举模板

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum XxxError {
    // 包装下游 crate 错误：用 #[from] 实现自动转换
    #[error("Storage error: {0}")]
    StorageError(#[from] storage_v2::error::StorageError),

    // 包装标准库错误
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    // 本 crate 特有的语义变种：用具体描述，不用泛化的 String
    #[error("Scan failed for job {job_id}: {reason}")]
    ScanFailed { job_id: String, reason: String },

    #[error("Entry not found: {0}")]
    EntryNotFound(String),
}

pub type Result<T> = std::result::Result<T, XxxError>;
```

错误传播方向：`utils → storage/storage_v2 → db / kafka → app → cli / web`

上层 crate 通过 `#[from]` 自动包装下层错误，**不得**用 `.to_string()` 丢失类型信息后再包进 `String` 变种。

##### 跨层转换规则

```rust
// ❌ 丢失类型信息
AppError::ScanError(storage_err.to_string())

// ✅ 保留类型，用 #[from] 或 impl From<>
#[error("Storage error: {0}")]
StorageError(#[from] storage_v2::error::StorageError),
// 然后直接 storage_op()?  — 自动转换

// ✅ 需要附加上下文时，用 map_err 包装语义变种
storage_op().map_err(|e| AppError::CopyFailed { path: path.clone(), source: e })?;
```

##### 变种命名规范

- **具体操作**：`ScanFailed`、`CopyFailed`、`ConnectionError` — 优先
- **包装下游**：`StorageError(#[from] ...)`、`DatabaseError(#[from] ...)` — 用于透传
- **禁止**：裸 `Error(String)` 作为兜底变种（只允许在 `utils` 层兜底，其余 crate 必须具名）

## High-Performance IO / Async Patterns（硬性规则）

> 详细模式、代码模板和诊断流程见 skill: `rust-high-perf-io` / `rust-perf-review` / `rust-async-debugging`
> 并发问题分析见 agent: `@async-analyzer`

- 大文件传输用 `Bytes`/`BytesMut`，**禁止** `Vec<u8>` clone
- 共享计数用 `AtomicUsize`，**禁止** `Mutex<u64>`；并发 map 用 `DashMap`，**禁止** `RwLock<HashMap>`
- 热点缓存用 `moka::sync::Cache`，**禁止** `Arc<Mutex<HashMap>>`
- CPU 密集任务用 `spawn_blocking` / `rayon::spawn`，**禁止**在 async fn 中直接计算
- DB 写入必须批量 `batch_insert`，**禁止**循环逐条 insert
- 限速用 `QosManager` (TokenBucket)，**禁止** `tokio::time::sleep` 粗粒度限速
- channel 背压靠 `bounded()` 自然限流，**禁止** unbounded channel
- 存储层优先 `storage_v2`，枚举 dispatch > `Box<dyn Storage>`
- `StorageEnum` 接口尽可能复用，避免重复类似的接口
- 热路径**禁止** `format!` / `to_string()`，应复用 buffer（`write!(&mut buf, ...)`  + `buf.clear()`）
- 已知大小的集合必须 `Vec::with_capacity(n)`，**禁止**空 `Vec` + 循环 push 导致多次 realloc
- 字符串 Key 的 HashMap 用 `SmolStr` + `FxHashMap`，**禁止** `HashMap<String, V>` 在热路径频繁 insert
- 迭代器链保持惰性，**禁止**不必要的中间 `.collect()` 再 `.into_iter()`

***

## Workspace Clippy Lints

`Cargo.toml` 中已配置 workspace 级 lint（所有 crate 继承）：

- `clippy::pedantic = warn`（已豁免 `module_name_repetitions` 等噪声规则）
- `clippy::unwrap_used = deny` / `clippy::expect_used = deny` — 编译期强制
- `clippy::dbg_macro = warn` / `clippy::todo = warn` / `clippy::unimplemented = warn`
- `unsafe_code = warn`（NFS FFI 处可局部 `#[allow]` 豁免）

***

## 前端设计约束

- 前端技术栈：Vue 3 `<script setup>` + Naive UI + Tailwind CSS + Pinia + Vue Router
- 优先复用 `web-ui/src/components/` 已有组件，**禁止**重复造轮子
- **禁止**引入新图标包（项目统一使用 `@vicons/ionicons5`）
- 设计稿使用 Pencil MCP，实现时追求 **1:1 视觉保真**
- 尽量依赖 Naive UI 默认样式，减少 Tailwind 手动覆盖

## Approach

1. 行动前先思考，写代码前先阅读现有文件
2. 输出简洁，推理彻底
3. 优先编辑而不是重写整个文件
4. 不要重复阅读已经读过的文件
5. 在宣布完成前测试你的代码
6. 不要有奉承的开场白或结束语
7. 保持解决方案简单直接
8. 用户指令始终覆盖此文件
9. 注释用中文，技术标识符保持英文

