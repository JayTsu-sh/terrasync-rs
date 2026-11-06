# 开发规范与场景索引

AI 工作流程以根目录 [AGENTS.md](../AGENTS.md) 为准，产品约束按本页与专项文档执行。本页集中编码规范和技术资料路由。现有 .claude 目录中的文档和校验脚本可作为普通文件使用，不依赖 Claude 自动加载或专属 agent。


## 产品职责与变更验收

本项目组合存储库、ClickHouse、CLI／Web 和任务通知。产品状态与多项外部副作用的一致性由本层负责，存储协议的修复回到 data-mover 或相应 client，避免在 app、CLI、Web 各自复制 backend 分支与恢复循环。接口应把复杂度集中在所属模块；增加抽象或配置前说明实际变化点和使用方。

| 修改范围 | 设计约束与必须验证的结果 |
| --- | --- |
| 任务创建、执行、取消与终态 | CLI／Web 复用任务用例及状态契约；故障、背压和取消后收束后台操作，独立验证最终状态与最终通知，不以 HTTP 返回或进程退出代替业务成功 |
| 扫描与续扫 | 非续扫全量／增量使用 unordered；首次计划续扫也显式启用 --resume，全量／增量续扫使用 blocks。生产 checkpoint 至少间隔 60 秒，并在满足间隔后等完整目录／子目录块完成；大平铺目录不能仅因计时到而提前确认。功能测试可缩短间隔，性能验收保持至少 60 秒 |
| 持久化与统计 | 对齐已确认事实、checkpoint 与统计快照；恢复从确认点继续，累计数包含 checkpoint，重放实际去重，不能估算补偿或再次累加。过滤统计区分扫描／过滤／保留及目录、regular 文件、软链接等类型；目录剪枝未访问项不计数 |
| ClickHouse 与提交 | Single／Cluster 共用上层语义，表布局、分片／副本及提交差异集中 db 模块；扫描及复制／同步元数据都按影响范围分别验证两种部署。确认丢失、旧执行延迟写入与清理必须靠执行身份和事实对账处理，不能假定超时即未执行 |
| 性能、内存与并发 | 队列、批次、预取和在途工作有界，避免按总 entry 数常驻保留明细。全量／全量续扫损失目标不超过 5%，增量／增量续扫不超过 10%；分别匹配 unordered／blocks 的不落库基线，不跨模式比较；增量覆盖无变更与约 1% 增删改。报告峰值 RSS 和持续增长；约 200 MiB 是优化目标，不能牺牲正确性或已确认性能要求来换取 |
| 续扫性能和整理 | 续扫净吞吐按对应 blocks 基线验收；另报中断前后总运行耗时及回退／重扫成本，排除人为暂停等待。应用主动触发的基线整理计入总耗时，不能隐藏数据库整理、重放或收尾成本 |

这是当前开发与验收目标，不代表现有实现已经全部达标。存储／服务连通性的故障矩阵与通知策略统一见 [故障约束](external-dependency-failures.md)；本页不重复定义超时与重试规则。

## 按场景查阅

| 场景 | 依据 |
| --- | --- |
| 理解当前 workspace、crate 和依赖版本 | 根 Cargo.toml、crates/、[代码结构资料](../.claude/docs/codebase.md)；代码与构建配置为当前事实 |
| 扫描、同步、故障恢复和 checkpoint | [架构资料](../.claude/docs/architecture.md)、crates/app/ 的当前任务与扫描测试、crates/app/scan/、crates/app/file_copy/ 与 crates/db/ 的当前实现及测试 |
| 外部依赖与真实环境测试 | [E2E 资料](../.claude/docs/e2e-testing.md)、[实验室约束](../tests/lab/README.md) |
| 存储驱动或环境配置 | [服务资料](../.claude/docs/services-and-storage.md)；实际能力、URL 解析和参数以当前依赖 API 与 CLI --help 为准 |
| Web API／前端 | [Web／前端规范](web-development.md)、web-ui/package.json |
| commit / PR、产物存放 | [提交规范](conventions.md) |
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
let map = std::collections::HashMap::<String, usize>::new();
fn foo() -> crate::error::Result<()> { ... }

// ✅ 正确
use std::collections::HashMap;
use crate::error::Result;
let map = HashMap::<String, usize>::new();
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

## 首次开发阶段：不要求兼容性

本项目目前处于首次开发阶段。所有设计、实现、优化、重构和 code review 均以当前最合理的架构和实现为准，不要求任何向后兼容。

数据库表结构、已有数据、持久化 checkpoint 和统计格式，以及内部与外部接口、函数签名、CLI、配置和 callback 协议，均可直接调整。不为旧版本或旧实现增加兼容层、数据迁移、回填、双读写、兼容分支或弃用过渡。

接口或格式调整时，同步更新所有当前调用方、配置示例、测试和文档；当前版本的功能正确性、断点续扫、精确计数、重试幂等、数据一致性及性能和内存目标仍须验证。

## 迁移行为测试资料

迁移策略切换、失败后源端变化及同 job ID 的代表场景整理在 [迁移代次测试用例](../tests/scenarios/migration-generations.md)。该文件是待实现／执行的测试规格，不能据此宣称功能已覆盖或通过。

## 提交粒度

完成一个可独立验证且语义完整的小功能或修复，通过适用检查后及时 commit。结构重构与行为变化分开；相互依赖才能成立的跨文件修改作为一个原子提交，避免积累大量无关改动，也不按文件数或机械频率制造半成品提交。推送与合入分别遵守当前 review 和检查要求。
