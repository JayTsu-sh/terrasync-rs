# 提交与协作规范

- commit 使用 `<type>(<scope>): <中文描述>`，scope 可选；type 为 feat、fix、refactor、perf、docs、test、chore 或 ci，描述简洁，正文解释原因。
- 结构重构与行为修复分开提交，不添加 Co-Authored-By 归属行。
- PR 说明具体问题、最终行为、职责归属、验证及未测范围；按 [架构 review](architecture-review.md) 审查最终提交，不能以笼统“已测试”代替证据。
- 设计文档记录当前方案、接口／状态契约、影响和验收方式；临时计划与测试日志留在运行结果目录，凭据和本机配置不提交。
- 测试产物使用既有临时目录／忽略范围，构建产物归 target；本轮外的源文件、数据库及无关存储对象保持原状。
