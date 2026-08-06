# E2E 测试

可重复的 E2E 测试由 GitHub Actions 的 `Nightly lab` 工作流和
`tests/lab/` 脚本维护，不再使用本地 Claude skills。

Nightly 包含：

- Local、NFSv3、NFSv4.1、S3 的 4×4 同步矩阵
- 每个方向的 SHA-256、quick integrity 和 full integrity 验证
- 各后端增量同步和本地过滤场景
- 独立的双进程 QUIC E2E
- 每次运行独立的 ClickHouse 数据库及自动清理

手动执行应通过 GitHub Actions 的 `workflow_dispatch` 触发。实验室拓扑、
必要环境变量、健康检查和清理约束见 `tests/lab/README.md`。

当前物理实验室没有 SMB/CIFS 服务，因此 Nightly 不声明 CIFS 物理覆盖；
CIFS 仍由单元和集成测试覆盖。增加 Samba 源/目标服务后，再把它加入矩阵。
