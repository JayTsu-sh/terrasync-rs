# Web 与前端开发规范

修改 Web API、应用用例、前端状态或界面前阅读本页；通用约束及架构审查见 [AGENTS.md](../AGENTS.md) 和 [架构 review](architecture-review.md)。

## Rust Web API

- handler 负责 HTTP 输入／输出与 application service 适配，业务流程和状态决策归 application，领域规则归 domain，数据库与外部访问归 infrastructure；按依赖契约维护职责，避免在 handler 内实现迁移策略。
- 使用当前 WebError 及统一 IntoResponse 处理错误，保留类型与 source；输入格式、业务参数和权限分别在所属层验证，不能以反序列化成功代表业务合法。
- 查询、分页等使用当前类型化 extractor／请求契约；CLI 与 Web 共用任务语义，故障行为按 [连通性约束](external-dependency-failures.md) 处理，任务交互按对应实际用例验证。

## 前端职责

- 使用当前 Vue Composition API／TypeScript 技术栈；依赖与构建命令以 web-ui/package.json 为准。
- API 调用集中 api 层；composable 管理业务状态和操作，view 负责组装、消息、dialog 与用户确认。composable 抛出可识别错误或返回结构化确认结果，避免直接调用 UI 消息／对话框 API。
- 保留当前 ValidationError 与 API 错误的区分，明确加载、确认、失败和取消状态；防止重复提交、冲突弹窗和不适用字段验证，错误反馈保留准确任务结果。
- 使用现有 PageHeader、BackLink、ScanAnalysis 等共享组件；新增抽象先确认实际重复职责，避免跨 view 复制业务规则。

## UI 一致性

- 组件外观沿用 App.vue 的 themeOverrides 和 Tailwind 设计值；颜色、字体、圆角和尺寸以当前主题为准，避免逐处硬编码或覆盖 Naive UI 默认样式。
- 宽度等布局用既有 class 约定；复用已有表单分组、required 标记、密码可见性、placeholder 和只读详情／编辑弹窗模式。连接状态同时提供文字和视觉提示。
- 表格沿用当前分页、搜索和列语义；图标使用已采用的图标包，新组件先核对主题和已有组件。

## 验证

- 行为变更先明确预期并增加适用回归，再实现／重构；纯排版按影响范围验证构建与界面一致性，避免只复刻实现的测试。
- 按 package.json 执行适用的类型检查、vitest、构建和 Playwright 场景。UI 测试复用既有 API mock／fixture；真实服务连通性与任务集成验收另行执行，mock 不能替代链路证据。
- 覆盖类型／状态／表单互斥、保存中重复提交、错误和确认流程；涉及跨页面或任务状态的变更验证实际用户路径。
