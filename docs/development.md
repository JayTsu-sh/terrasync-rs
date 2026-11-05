# 开发规范与场景索引

AI 工作流程以根目录 [AGENTS.md](../AGENTS.md) 为准，产品约束以 [agent.md](../agent.md) 为准。本页集中编码规范和技术资料路由。现有 .claude 目录中的文档和校验脚本可作为普通文件使用，不依赖 Claude 自动加载或专属 agent。

## 按场景查阅

| 场景 | 依据 |
| --- | --- |
| 理解当前 workspace、crate 和依赖版本 | 根 Cargo.toml、crates/、[代码结构资料](../.claude/docs/codebase.md)；代码与构建配置为当前事实 |
| 扫描、同步、故障恢复和 checkpoint | [架构资料](../.claude/docs/architecture.md)、[项目约定](../agent.md)、crates/app/、crates/storage-adapter/ 与 crates/db/ 的当前实现及测试 |
| 外部依赖与真实环境测试 | [E2E 资料](../.claude/docs/e2e-testing.md)、[实验室约束](../tests/lab/README.md) |
| 存储驱动或环境配置 | [服务资料](../.claude/docs/services-and-storage.md)；实际能力、URL 解析和参数以当前依赖 API 与 CLI --help 为准 |
| Web UI | [代码结构资料](../.claude/docs/codebase.md) 前端章节及 web-ui/package.json |
| commit / PR、产物存放 | [提交规范](../.claude/docs/conventions.md) |
| 构建、测试与工具链 | Cargo.toml、Makefile、[CI](../.github/workflows/ci.yml)；运行与影响范围对应的步骤 |

旧架构名、历史接口和旧 CLI 示例须对照当前实现核验后使用。注释使用中文，技术标识符保持英文。

# Rust 编码规范

> 修改 Rust、错误传播或进行相应 review 前，按 AGENTS.md 显式阅读本节。工具链与 lint 配置以 Cargo.toml 和 CI 为准。

## use 语句规范（强制）

**规则 1 — 位置：** 所有 `use` 语句必须集中在文件顶部，禁止在函数体、`impl` 块或任何嵌套作用域内声明 `use`。

唯一例外：`#[cfg(test)]` / `tests/` 块内可在块内部使用 `use`。

**规则 2 — 路径深度：** 函数体/表达式中直接书写的路径最多两层（`A::B`）。超过两层的类型必须通过顶部 `use` 导入后使用短名称。禁止在代码中直接书写 `crate::xxx` 形式的路径。

```rust
// ❌ 禁止
let entry = data_mover::NASEntry::new();
fn foo() -> crate::error::Result<()> { ... }

// ✅ 正确
use data_mover::NASEntry;
use crate::error::Result;
let entry = NASEntry::new();
```

例外：`std::io::Error`、`std::fmt::Display` 等广为人知的标准库路径，不影响可读性时可酌情保留。

## 重构规范（强制）

**重构只改结构，不改语义。重构前后的可观测行为必须完全一致。**

允许：提取函数、重命名变量、调整模块结构、消除重复、改善类型签名。

禁止：在重构的同时修改逻辑、添加新功能、改变错误处理路径、调整并发行为。

验证：重构完成后，所有现有测试必须 pass，不得修改测试本身来迁就重构。

```rust
// ❌ 重构时偷偷改了语义（原来出错跳过，现在改成返回错误）
// before: if let Err(e) = do_thing() { error!("{}", e); }
// after:  do_thing()?;   ← 语义已变，不是重构

// ✅ 保留原有语义
if let Err(e) = do_thing() { error!("{}", e); }
```

发现原有逻辑有问题时，**单独提交**修复，不与重构混在一起。

## 错误处理规范（强制）

### 禁止 `.unwrap()` / `.expect()`

生产代码禁止使用；测试按仓库现有 lint 允许范围处理。

```rust
// ❌ 禁止（编译不过）
let val = some_option.unwrap();

// ✅ 替代方案
let val = some_result?;
let val = some_option.ok_or(MyError::NotFound)?;
let val = some_option.unwrap_or_default();
```

唯一例外：`#[cfg(test)]` 块或 `tests/` 目录（通过 `#[allow(clippy::unwrap_used)]`）。

### 每个 crate 必须用 thiserror 定义专属 Error 枚举

**标准文件结构：**
```
<crate>/src/
  error.rs      # XxxError 枚举 + pub type Result<T>
  lib.rs        # pub mod error; pub use error::{XxxError, Result};
```

**枚举模板：**
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum XxxError {
    #[error("Storage error: {0}")]
    StorageError(#[from] data_mover::error::StorageError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Scan failed for job {job_id}: {reason}")]
    ScanFailed { job_id: String, reason: String },
}

pub type Result<T> = std::result::Result<T, XxxError>;
```

**错误边界：** 各层包装实际依赖的有类型错误并保留 source；依赖方向以 Cargo.toml 与当前模块边界为准。

上层通过 `#[from]` 自动包装下层错误，**不得**用 `.to_string()` 丢失类型后包进 `String` 变种。

```rust
// ❌ 丢失类型信息
AppError::ScanError(storage_err.to_string())

// ✅ 保留类型
storage_op()?   // 通过 #[from] 自动转换

// ✅ 需附加上下文时
storage_op().map_err(|e| AppError::CopyFailed { path: path.clone(), source: e })?;
```

**变种命名：**
- 具体操作（优先）：`ScanFailed`、`CopyFailed`、`ConnectionError`
- 包装下游（透传）：`StorageError(#[from] ...)`、`DatabaseError(#[from] ...)`
- 禁止：裸 `Error(String)` 兜底变种（`utils` 层除外，其余 crate 必须具名）
