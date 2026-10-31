# sync 写入模式与原地 delta 设计

**日期：** 2026-10-31

**状态：** Implemented

## 问题

原先按后端或文件大小隐式选择策略，双进程固定 Atomic，无法表达用户所选写入模式。
已有 20GiB 文件仅改变 10MiB 时，Atomic delta 虽然节省网络传输，仍需重建整个目标文件。
默认 512MiB 门槛也会让这种大文件退回全量传输。

## 方案

CLI 优先级为 `--resume` → Checkpointed，否则 `--in-place` → AtomicReplace，否则 Direct。
全量、增量、版本化 S3 队列及 rename 后的数据重拷贝都携带同一策略。不支持时明确报错，
不自动切换模式。旧 `--no-resume` 只保留解析兼容，与 `--resume` 互斥。

双进程 Unix Local 的 Direct 使用上游 `DirectPatchSession`：接收阶段保持 basis 不变，
消费所有引用并计算完整摘要，仅暂存与目标不同的区间；确认源端证据并复核 basis 后才应用补丁。
同位置 Match 不重写；移动或重叠引用在改写前已全部消费。临时输入、文件句柄、校验、元数据
及发布都由 data-mover 持有。输入失败后会话不可再提交，Unix 暂存创建后立即 unlink。

原地补丁需要独立的 `supports_sparse_patch` / `patch_direct` 能力，不能复用先截断文件的普通
positioned writer。当前仅 Unix Local 支持；其他远端目标应显式使用 Atomic/Checkpoint。
Direct 默认不受 512MiB 门槛限制，但保留用户显式设置的门槛。Atomic/Checkpoint 保持旧门槛。

## 接口设计

- `TransferMode::from_flags` 确定优先级，映射到 data-mover 的三种 `TransferPolicy`。
- `SyncJobConfig`、worker 恢复上下文及 `SessionConfig` 传递模式；wire v10 拒绝旧端混用。
- `StreamChunk` 的同位置提示只由 Receiver 重建器在读取 basis 后产生，不信任外部跳过指令。
- 连续 Match 合并为最多 2MiB 的读取；重建器使用实际 basis 长度，支持增长和缩短。
- Matcher 对重复内容优先引用同位置，字面量分片最多 1MiB；rolling checksum 按5552字节批量
  取模，保持原值并降低签名 CPU 开销。
- HDFS 的应用进度记录仅在 Checkpoint 模式启用，Direct/Atomic 不生成恢复记录。

## 影响范围

涉及 CLI、app 编排/全量/增量/重建流程、storage-adapter、transport 会话和实验室脚本。
上游 data-mover PR #173 已先经独立审查和 CI 合入 main，依赖固定到 `4781406`。
Web 任务继续显式使用 Checkpoint；tar API 仅支持 Atomic，其他选择明确拒绝。

Direct 不提供事务回滚和外部进程并发写隔离：接收中断不改 basis，应用补丁途中失败可能留下
部分更新，重试必须重新生成签名。Checkpoint 的目标前缀可复用，但当前 wire 重跑仍重放数据/
delta token 来计算完整摘要，不能宣称网络字节级续传。签名与完整校验仍可能读取整文件。

## 验证方式

覆盖 CLI 四种参数组合及顺序、原 inode/硬链接语义、能力拒绝、块移动与重叠引用、增长缩短、
不完整输入、失败锁定、basis 并发变化以及 Checkpoint 前缀复用；回归全量与 delta 的 redo。

使用真实 CLI、ClickHouse、版本化 S3 和 NFS 验证全量/增量模式贯穿。
`tests/lab/verify-direct-delta.py` 用真实 QUIC 检验20GiB/10MiB场景：完整 SHA-256 一致、inode
保持、目标文件仅写10MiB；输入暂存另约10MiB。fixture 为稀疏文件，时间仅为当前机器的实验样本，
不当作物理磁盘性能保证。测试日志及 JSON 报告位于仓库外。
