# Rust 编码规范（始终加载）

> 这些规则从 CLAUDE.md 提升而来，每次会话自动加载。违反将在 code review 中打回。

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

严禁在生产代码中使用。已通过 workspace clippy lint 编译期强制拒绝。

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

**错误传播方向：** `utils → data-mover → db / kafka → app → cli / web`

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
