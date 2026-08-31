# Observation snapshot 数据库设计

**日期：** 2026-08-31  
**状态：** Implemented

## 问题

terrasync 不能从协议字段重建扫描观察，也不能在读取数据库后重新访问后端补事实。旧表中的
`file_handle` 等协议字段不再是新架构的持久身份接口。

## 方案

ClickHouse 继续使用单张扫描大表。表中同时保存：

- 可查询的中立投影：`identity_key`、`relative_path`、`backend_kind`、`entry_kind`、
  `size`、`modified_unix_nanos` 和 `current_state`；
- data-mover 生成的 `entry_snapshot` opaque bytes。

`identity_key` 是增量 identity join 的唯一新接口。需要完整观察时，terrasync 把 snapshot
原样交还 data-mover codec，并校验解码结果与同一行的查询投影一致。terrasync 不解析 backend
facts，也没有后端 refetch fallback。

## 兼容边界

不迁移历史 ClickHouse schema 或历史行。启用新 projection 前重建对应 job 表；代码不维护双
schema、旧 snapshot codec 或 `file_handle` 到 identity 的转换适配器。

## 验证方式

- snapshot 无损 round-trip；
- 损坏 snapshot 返回 typed codec error；
- snapshot 与查询列不一致时 fast fail；
- identity join SQL 只引用 `identity_key`；
- 真实 ClickHouse 上的新扫描大表可创建并重复初始化。
