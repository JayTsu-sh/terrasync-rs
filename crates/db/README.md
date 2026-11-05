# ClickHouse 持久化

本 crate 负责扫描与复制／同步目录的持久化，仅支持 ClickHouse。

- 扫描入口为 `scan_store::ScanStore`，内部封装单机与集群部署差异。
- 复制／同步目录通过 `DatabaseFactory::new_database` 创建，构造时检查未完成扫描，避免读取未发布的 working 数据。
- 单机扫描按作业建表；集群扫描使用按 `job_id` 隔离的共享表。
- 集群复制／同步目录使用独立的作业目录布局，不能与集群扫描共享表混用。
- 数据库请求超时不代表服务端未执行；恢复、提交和清理必须核验持久事实。

连接与部署配置使用 `ClickHouseConfig`。构造入口不提供数据库类型注册或插件机制。

开发及验收依据：

- [开发规范](../../docs/development.md)
- [外部依赖故障约束](../../docs/external-dependency-failures.md)
- [架构审查](../../docs/architecture-review.md)

真实环境测试见 [实验室说明](../../tests/lab/README.md)。
