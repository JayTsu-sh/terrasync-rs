# Architecture — 数据流与设计决策

## 完整数据流

```
CLI args
  └→ cli::cli_match()
       └→ app::scan / app::sync / app::ace / app::integrity_check
            └→ app::dir_walker
                 └→ data_mover::StorageEnum  (枚举 dispatch: Local / NFS / S3 / CIFS)
                      └→ NASEntry / S3Entry  (各协议 entry 类型)
            └→ transport::InProcess / QUIC
                 └→ app::receiver (双进程模式下的接收方)
            └→ sync-delta  (增量同步时的 chunk 级匹配)
            └→ app::consumer::ConsumerManager
                 ├→ DatabaseConsumer  → db::Database (ClickHouse / DuckDB)
                 ├→ StatisticConsumer (内存统计，进度广播)
                 └→ KafkaConsumer     → kafka crate (分布式模式)
```

## 增量 Scan/Sync 状态机

```
首次运行（无 jobs/<job_id>/ 目录）
  → ScanType::Full
  → 写入 base_<job_id> 表（全量快照）
  → 创建 jobs/<job_id>/ 目录

再次运行（jobs/<job_id>/ 存在）
  → ScanType::Incremental
  → 读取 base_<job_id> 做 diff
  → 写入 incremental_<job_id> 表（变更记录）
  → 计算 ChangeKind：Added / Modified / Deleted / Renamed / Moved

重置：删除 jobs/<job_id>/ → 下次回到 Full
```

- DB 表按 job_id 命名空间隔离：`base_<job_id>`、`state_<job_id>`、`incremental_<job_id>`
- Renamed/Moved 场景通过 `ChangeKind::from_entry_diff()` 公共方法判断
- 跨父目录 move（非叶目录）由 orchestrator 特殊处理，确保子树一致性

## 高性能 IO / Async 规则（硬性约束）

> 违反以下规则会引发 clippy warn/deny，或在 code review 中被打回。

| 场景 | 正确做法 | 禁止做法 |
|------|---------|---------|
| 大文件传输 | `Bytes` / `BytesMut` | `Vec<u8>` clone |
| 共享计数 | `AtomicUsize` | `Mutex<u64>` |
| 并发 map | `DashMap` | `RwLock<HashMap>` |
| 热点缓存 | `moka::sync::Cache` | `Arc<Mutex<HashMap>>` |
| CPU 密集 | `spawn_blocking` / `rayon::spawn` | async fn 内直接计算 |
| DB 写入 | `batch_insert` | 循环逐条 insert |
| 限速 | `QosManager` (TokenBucket) | `tokio::time::sleep` 粗粒度 |
| channel 背压 | `bounded()` 自然限流 | unbounded channel |
| 存储层 | `StorageEnum` 枚举 dispatch | `Box<dyn Storage>` |
| 热路径字符串 | `write!(&mut buf, ...)` + `buf.clear()` | `format!` / `to_string()` |
| 集合初始化 | `Vec::with_capacity(n)` | 空 Vec + 循环 push |
| 字符串 Key Map | `SmolStr` + `FxHashMap` | `HashMap<String, V>` 热路径 |
| 迭代器 | 保持惰性链 | 不必要的中间 `.collect()` |

## 设计决策

**为什么用 StorageEnum 而非 Box\<dyn Storage\>**  
枚举 dispatch 零虚表开销，编译期可知所有变体，便于穷举匹配和特化优化。热路径（dir_walker 每次迭代都调用）收益显著。

**为什么 transport 有 InProcess 和 QUIC 两种**  
单机场景用 InProcess（零拷贝 channel），跨机场景用 QUIC（低延迟 UDP 传输）。两者实现同一个消息协议（`SenderMsg` / `ReceiverMsg`），上层透明。

**为什么增量用 jobs/ 目录而非 DB flag**  
目录存在与否即为状态，无需 DB 查询。删除目录 = 原子重置。多 job 并发时各自隔离，互不干扰。
