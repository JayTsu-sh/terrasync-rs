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

同一张物理大表还保存 append-only recovery 状态行（`row_kind = 1`）。状态行包含
`recovery_attempt_order`、`recovery_attempt_id`、32-byte claim、事件顺序以及 data-mover 的
opaque `recovery_identity`。观察查询统一限定 `row_kind = 0`；发布新 observation snapshot 前，
现有 recovery 状态行会复制到 staging 表后再执行 `EXCHANGE TABLES`，因此扫描不会清除进行中
的恢复状态。

attempt 的 `(order, id)` 是总序：调用方在显式开始替代 attempt 时递增 order，同 order 的竞争
者由 id 确定性决胜。读取按 `(attempt order, attempt id, event order)` 取最大值，因此旧 attempt
晚到的登记和完成事件不能覆盖新 owner。相同 attempt 的相同 identity 重复登记直接 ACK；不同
identity fast fail。completed 是同 attempt 的终态事件，清空可复用 identity，且同 attempt 不能
再次启动 payload。

terrasync 通过 `RecoveryProvider` 惰性打开 recovery 状态。只有 data-mover planner 已确认该传输
可产生可复用 checkpoint 时才访问 ClickHouse；单 streaming chunk 和 atomic native copy 不打开
attempt，也不写 recovery/completed 事件。

## 兼容边界

不迁移历史 ClickHouse schema 或历史行。启用新 projection 前重建对应 job 表；代码不维护双
schema、旧 snapshot codec 或 `file_handle` 到 identity 的转换适配器。

## 验证方式

- snapshot 无损 round-trip；
- 损坏 snapshot 返回 typed codec error；
- snapshot 与查询列不一致时 fast fail；
- identity join SQL 只引用 `identity_key`；
- 同 attempt 重开返回同 claim 与 identity；
- 新 attempt 继承未完成 identity，但旧 registrar 不能覆盖它；
- completed 清空 identity，并阻止同 attempt 重开；
- 单 chunk 传输不打开 recovery store；
- 真实 ClickHouse 上的新扫描大表可创建并重复初始化。
