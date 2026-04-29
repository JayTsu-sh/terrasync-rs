# 提交与协作规范

## Commit 消息格式

```
<type>: <中文描述>

<可选正文（中文）>
```

### Type 列表

| Type | 用途 |
|------|------|
| `feat` | 新功能 |
| `fix` | Bug 修复 |
| `refactor` | 重构（不改功能，不改 bug） |
| `docs` | 文档 |
| `test` | 测试 |
| `chore` | 构建/依赖/工具 |
| `perf` | 性能优化 |
| `ci` | CI/CD |

### 规范细节

- 描述用**中文**，简洁说明做了什么（不超过 50 字）
- 正文解释**为什么**，而非重复"做了什么"（代码本身说明了 what）
- Scope 可选：`fix(incremental-sync): 正确处理非叶目录 rename`
- 不使用 `Co-Authored-By:` 归属（全局配置已禁用）

### 示例

```
fix(incremental-sync): 正确处理非叶目录 rename 和跨父目录 move 场景

非叶目录 rename 时需要递归更新子树所有条目的父路径，
原实现只处理了直接子条目，导致深层子目录路径错误。
```

```
refactor: 将 detect_change_kind 提升为 ChangeKind::from_entry_diff 公共方法

原实现散落在多处，统一后便于测试和复用。
```

## 产物归位规则

| 产物类型 | 放哪里 |
|---------|-------|
| 测试日志、报告 | `$TMPDIR/terrasync-harness-results/` |
| 临时构建产物 | `target/`（已 gitignore）|
| 设计文档（入库） | `docs/specs/<date>-<name>-design.md` |
| 实施计划（不入库） | `.claude/plans/<date>-<name>.md` |
| skill 本机配置（不入库） | `.claude/skills/*/. env` |

**禁止**将测试产物、日志、临时文件提交入库。

## PR 格式

```markdown
## 变更说明

- 一句话描述核心变更

## 背景

为什么需要这个变更（问题是什么）

## 测试方案

- [ ] cargo test --workspace 通过
- [ ] 相关 e2e skill 验证通过（列出具体 skill 名）
- [ ] cargo clippy 无新 warn/error
```

## Spec 文档格式（`docs/specs/`）

```markdown
# <功能名> 设计

**日期：** YYYY-MM-DD  
**状态：** Draft / Approved / Implemented

## 问题

## 方案

## 接口设计

## 影响范围

## 验证方式
```

## .gitignore 规范

以下路径已 gitignore，不要手动添加入库：
- `.claude/plans/`
- `.claude/skills/*/.env`（保留 `.env.example`）
- `target/`
- `jobs/`（增量运行目录，运行时生成）
