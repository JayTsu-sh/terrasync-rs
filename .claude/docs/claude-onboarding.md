# Claude 协作入门

本文件解释 terrasync-rs 如何使用 Harness Engineering 范式让 Claude 成为可靠的协作者。

## 知识分层体系

```
CLAUDE.md              — 场景导航 + 每次会话必须知道的铁律（约 100 行）
.claude/docs/          — 按需加载的深度知识（本目录）
.claude/rules/         — 自动加载的约束规则
.claude/memory/        — 跨会话积累的学习
```

### rules/ 加载模式

规则默认始终加载（无 `paths:` frontmatter）：
```
.claude/rules/rust-patterns.md      — Rust 编码规范
.claude/rules/api-design.md         — Web API 设计规范
```

E2E 环境不再由 Claude skills 管理；统一使用 `tests/lab/` 和 Nightly workflow。
非敏感拓扑在 `tests/lab/common.sh`，凭据由 self-hosted runner 注入。

## Skill 体系

每个 skill 是一个**双模式能力包**：Claude 可交互执行，也可程序化自动运行。

**目录结构（标准模板）：**
```
.claude/skills/<skill-name>/
├── SKILL.md           # Claude 的操作手册（<500 行）
├── .env.example       # 配置契约（入库，定义"需要什么"）
├── .env               # 本机真值（不入库，gitignore）
└── scripts/
    └── run.py         # 自动化入口（实现 run(env) 接口）
```

**SKILL.md 必须包含：**
- `description:` — 触发词（Claude 根据此决定何时调用）
- `trigger:` — 具体触发短语列表
- 环境准备步骤
- 执行步骤（编号，清晰）
- 验证步骤（预期输出）
- 清理步骤

新的 E2E 场景应加入 `tests/lab/run-e2e.sh`，并通过 Nightly workflow 验证。

## Memory 提升梯

```
会话内纠错（Claude 理解错了）
    ↓
memory/corrections.jsonl    — 每次被纠错立即 append（不要批量）
    ↓（同一模式被纠错 ≥2 次）
memory/learned-rules.md     — 自动提升，Claude 复杂任务开始前读
    ↓（/evolve 审查通过 + 多 session 验证稳定）
.claude/rules/rust-patterns.md 或其他 rules/ 文件
    ↓（rules/ 中遵守 10+ session 无违反）
CLAUDE.md                   — 最终归宿（极少到达这里）
```

提升判断标准：
- corrections.jsonl → learned-rules.md：同模式纠错 ≥2 次（自动）
- learned-rules.md → rules/：`/evolve` 审查 + 多 session 稳定验证（手动）
- rules/ → CLAUDE.md：确实是每次会话都需要的铁律（极少）

## SDD 工作流（何时写 Spec vs Plan）

```
docs/specs/<date>-<name>-design.md  — 设计文档（入库，团队资产）
.claude/plans/<date>-<name>.md      — 实施计划（不入库，用完即弃）
```

**需要写 Spec 的场景：**
- 新增存储协议支持（新 crate 或大改 data-mover 接口）
- 跨多个 crate 的架构重构
- 引入新的数据库 schema 或重大改动

**只需要 Plan 的场景：**
- Bug fix（单 crate 内）
- 单文件优化
- 新增或调整单个 E2E 场景

**判断标准：** "改完后需要向其他人解释为什么这样设计吗？"如果是，写 Spec。

## 常见任务速查

| 任务 | 读哪里 | 怎么做 |
|------|-------|-------|
| 改 scan 逻辑 | `architecture.md` | 读数据流 + 增量状态机 |
| 跑 E2E 测试 | `e2e-testing.md` | 触发 Nightly workflow |
| 改存储驱动 | `services-and-storage.md` | 了解 StorageEnum dispatch 方式 |
| 提交代码 | `conventions.md` | 遵循 conventional commits + 中文 |
| 发现 Rust 违规 | `rules/rust-patterns.md` | 直接查规则细节 |
