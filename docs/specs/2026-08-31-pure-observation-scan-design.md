# Pure observation scan projection 设计

**日期：** 2026-08-31  
**状态：** Implemented

## 问题

扫描流中的局部成功不能证明完整快照；若直接发布部分观察，取消或失败会造成错误的删除判断。
产品层需要在完整遍历证据与全部批次持久化成功后统一发布扫描结果。

## 方案

以 bounded traversal、批量 staging 写入和原子 snapshot 发布组成独立的纯观察扫描流程。

## 数据流

`project_pure_scan` 直接消费 data-mover 的 bounded `TraversalSession`。每个成功的
`ObservedEntry` 被编码成 #140 定义的同表 query projection 与 opaque snapshot；entry failure
只计入失败统计，不伪造成 entry，也不会终止对剩余 stream 的排空。

## 接口设计

`project_pure_scan` 接收 `TraversalSession`、批次大小及 `ObservationScanSink`。
sink 封装 generation 开始、批次写入、完整发布与中止操作；扫描结果返回观察、失败及删除统计。

## 完整性和发布

扫描开始时创建与 base 表结构完全一致的 staging 表。批次只写 staging。只有同时满足以下条件
才执行发布：

- traversal 返回 `Completed`；
- completion 中的成功数与实际投影数一致；
- entry failure 数为零；
- 所有数据库批次写入成功。

发布使用 ClickHouse `EXCHANGE TABLES` 原子替换整个 snapshot。交换前通过 persisted
`identity_key` 计算删除数量；因此删除只是完整 snapshot 发布的结果，不会在扫描过程中逐条执行。
取消、entry failure、terminal failure 或写入失败均丢弃 staging，并且不提交 scan generation。

snapshot 交换与 generation 提交由同一个数据库发布操作持有。generation 写入失败时立即反向
`EXCHANGE TABLES` 恢复旧 snapshot，由数据库发布操作自身丢弃 staging 并重置 working generation。
交换成功后的旧表删除属于可重试清理：清理失败不会把已经提交的扫描误报为未发布，下一轮 begin
会先清理该 retired table。若反向交换本身也失败，则返回明确的 transaction failure，要求运维介入，
不会静默声称回滚成功。
`EXCHANGE` 自身若返回无法确认的结果，会进入 ambiguous recovery gate：禁止下一轮 begin 和
destructive abort，保留 base 与 rollback table，直到外部核对表身份后处理。

## 边界

该模块不解析 backend facts，不按协议名称分支，也不通过 storage refetch 重建 entry。URL 到新
data-mover storage facade 的最终入口切换不属于 projection；旧扫描编排的删除在后续历史路径清理票
中移除。

## 影响范围

涉及 `app::observation_scan` 与 `db::ClickHouseDatabase` 的 observation snapshot 生命周期。
存储协议及 snapshot codec 仍由 data-mover 实现；既有扫描入口的清理边界见上节。

## 验证方式

- bounded batch 与成功统计；
- 公开 bounded `TraversalSession` 到真实 ClickHouse sink 的完整发布与删除；
- entry failure、取消和 completion count mismatch 阻止发布；
- generation commit 失败会在数据库发布操作内反向交换并清理 staging；
- 真实 ClickHouse 覆盖 base/temp snapshot 写入、identity JOIN、atomic exchange 和删除结果；
- opaque snapshot 重建仍由 data-mover codec 独占。
