# .claude/docs/ — 知识库

按需加载，不要全读。根据任务找到对应文件。

| 文件 | 内容摘要 | 何时读 |
|------|---------|-------|
| `codebase.md` | Workspace 详细结构、crate 入口文件、前端约束、Clippy lints、Cargo features | 深入理解代码 / 改前端 |
| `architecture.md` | 数据流详解、增量状态机、高性能 IO/Async 规则、设计决策 | 改 scan/sync/transport 逻辑 |
| `e2e-testing.md` | Nightly lab 测试矩阵、覆盖范围和运行入口 | 跑或修改 E2E 测试 |
| `services-and-storage.md` | 存储驱动（NFS/S3/CIFS）、URL 格式详解、测试环境 IP/端口 | 改存储驱动 / 配置环境 |
| [提交规范](../../docs/conventions.md) | Commit、PR 和产物存放 | 提交代码 / 写 PR |
