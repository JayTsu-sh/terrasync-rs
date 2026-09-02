# Remote advertisement 设计

**日期：** 2026-09-01
**状态：** Implemented

## 边界

data-mover 只提供 bounded `TraversalSession`、中立 `ObservedEntry` 和 opaque snapshot codec。
terrasync 拥有远端分页、session-local NDX、wire failure projection、QUIC FileList stream 和
terminal protocol。NDX 不写数据库或 checkpoint，也不能跨 negotiated session 复用。

## 发送侧

`advertise_remote` 按 traversal 顺序消费 entry 与 entry failure：

- entry 只编码为 data-mover 所有的 opaque snapshot；wire 不包含 `EntryEnum` 或 backend facts 字段；
- 每个 session 从 `SessionNdx(0)` 重新编号，页大小有明确上限；
- entry failure 先刷新之前的页，再作为独立事件发送，因此不会丢失顺序；
- 每次发送都 await bounded transport，慢 Receiver 会把背压传回 traversal；
- traversal 的 Completed、Cancelled、backend-session failure 和 runtime failure 都映射为唯一 terminal；
- Completed 的 entry/failure 计数必须与实际 projection 一致。

## 接收侧

`RemoteAdvertisementReceiver::accept` 是增量 validator。它验证 page sequence、连续 NDX、页
上限、snapshot codec 和 terminal 计数。每页验证后立即返回 `AdvertisementReceipt::Page`，自身
只保留页号、下一个 NDX 和计数，不累计完整 FileList，也不访问源端或目标端 storage。

`receive` 仅是测试和小型调用方的汇总便利函数；#145 的双进程 transfer session 应逐事件调用
`accept`，边收页边做目标端比较和请求生成。

## Wire

`SenderMsg::Advertisement` 走 QUIC FileList stream。协议版本为 v6，最低兼容版本同为 v6；不
提供 v5 双解码或旧 `DirPageResult` 到新 snapshot 页的兼容转换。旧 remote 编排路径将在 #152
移除，新 expert transfer 的消费接线属于 #145。

## 验证

- opaque snapshot round-trip 且无 backend refetch；
- session-local NDX 与 page sequence 连续；
- entry failure、取消、session failure 和计数冲突有独立终态；
- 损坏 snapshot、乱序页、缺失 terminal 和伪造完成计数 fast fail；
- capacity=1 的 in-process transport 能阻塞发送者；
- advertisement page 与 terminal 通过真实 QUIC FileList stream 往返。
