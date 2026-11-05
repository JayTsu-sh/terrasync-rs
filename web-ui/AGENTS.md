# 前端开发约束

继承根目录 [AGENTS.md](../AGENTS.md)。修改 API 调用、composable、view、主题或前端测试前，必须阅读 [Web／前端规范](../docs/web-development.md)。

- API、业务状态与 UI 反馈各归所属层，复用既有组件和主题，保持任务与错误语义一致。
- 按行为变化执行类型、回归和交互验证；UI mock 用于界面测试，不能替代真实服务连通性验收。
- 架构 review 检查业务逻辑是否跨 view／composable 重复、状态和确认流程是否分散，合入前解决本次引入的结构问题。
