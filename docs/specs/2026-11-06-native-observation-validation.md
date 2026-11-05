# 原生观察重构：阶段验证记录

本记录区分已完成的阶段验证与最终验收。当前工作分支尚未合入；本页不代表整个数据库重构已完成。

## 已提交的观察绑定

- TerraSync `d361ce0` 将普通、checkpoint、Direct 和 HDFS 恢复复制入口绑定到共享 `Arc<ObservedEntry>`。
- data-mover `8dce4f1` 在目标准备前核验所选观察，拒绝源变化、无效索引和删除标记索引；请求 clone 不复制历史。
- data-mover `18bcb24` 提供逐目录索引需要的根对象诊断策略。默认独立扫描仍报告不能作为子条目交付的根对象；父页已处理根对象的目录索引可以只枚举子项。首次枚举与 checkpoint 重列共用规则，策略进入恢复指纹。
- 两个上游阶段均已执行完整 `make ci`。恢复重列回归先复现 Blocks 恢复产生额外 Unsupported，再修复通过。

## 目录对象与复制事实

此阶段增加目录 marker 的原生比较摘要，不解码 backend opaque facts，也不为比较重复编码 snapshot。摘要贯通 Single／Cluster 的写入、查询和候选缓存。

逻辑目录与其真实零字节对象分别承担统计和传输职责。NDX 仍关联原观察，数据与 metadata 借用 marker leaf；目标发布完成后才确认。虚拟目录不合成对象。远端目标索引区分同路径普通文件、逻辑目录和尾斜线对象，索引键借用共享观察中的路径。

marker 有到无的转换产生显式删除意图，不能仅由当前目录没有 marker 推断删除。未确认的删除保留旧基线；恢复从去重后的逻辑基线重建意图。新增虚拟目录保留目标无关对象。删除机制只操作目录自身零字节对象，保留后代；不支持目录对象的目标不执行该操作。

复制所需 tags 来自实际目标能力，不受扫描展示开关控制。远端握手独立传递该能力，协议版本为 12。源端有效 observation plan 与发布准入共用能力判断；所需事实读取失败不能降级成 Omit 后成功复制。

## 实际环境结果

候选二进制 SHA-256：`a93c6c9e1e6e2b2d5907a6e0a9ca9a0661b3b1f39545a21d1f3bbe863e49de2d`。构建基于 `d361ce0` 加阶段工作树；精确构建摘要保存在 `/tmp/terrasync-marker-tags-build-source-v3.json`。测试报告中的工作树摘要包含随后补强的测试，不作为二进制源码身份。

- 真实 S3、ClickHouse Single／Cluster 和两个真实 QUIC 进程：17 场景通过。覆盖首次复制、再次执行、虚拟目录新增 marker、标签变化、marker 消失、后代保留、目标无关 marker 保护、Local→S3，以及 `scan.include_tags=false` 的必需 tags 复制。
- 证据：`/work/terrasync-db-refactor-evidence/release-db-refactor-marker-tags-v3-20261105-171043/report.json`。实际 run id 应以对应报告为准；执行入口是 `/tmp/terrasync-db-refactor-marker-tags-live.py`，prepare／cleanup 日志在 `/tmp/terrasync-marker-tags-v3-lab-*.log`。
- 真实 catalogue 适用场景：Single 10／10，Cluster 12／12。新增用例验证 marker 差异等待 ACK、删除失败后恢复仍保留意图；限制测试表后台合并并断言存在多条物理行，防止后台合并掩盖旧事实选择错误。记录见 `/tmp/terrasync-db-refactor-real-marker-tags-v4-{single,cluster}-catalogue.{log,json}`。
- 上述成功 S3 测试报告中，数据库、bucket、版本、上传状态及私有配置清理均通过。失败的早期诊断不计为通过。

## 远端源观察校验

远端 full／delta 内容读取改用共享 native expert source session，在 FileBegin 或 delta tokens 前核验所选观察。源读取与摘要由同一个 session 完成；错误不发送 EOF。普通对象、目录 marker 和历史版本共享该机制，原始观察通过 Arc 共享。

真实零字节替换注入先复现失败：S3 HEAD 已读取 LastModified，但 descriptor 没有交付这项事实，替换后仍被复制。data-mover `04f0854` 补齐 descriptor 的修改时间，并按双方声明的较粗时间精度比较；精度以内不能区分的变化不宣称可检测。上游完整 `make ci` 通过，新增精度与替换回归先失败后通过。

修复候选 SHA-256：`9c7ae27860a6561bdc71584630aa6e4212b2beced0b7cf20bec401315f72c709`；精确源码记录在 `/tmp/terrasync-source-mtime-build-source.json`。真实 QUIC 两例通过：扫描后替换零字节普通对象与目录 marker，均报告一个条目错误，目标未发布，也没有内容 GET。CLI 按既有条目报告语义退出，不能用退出码代替任务结果。单元回归另断言没有 FileBegin 和成功 ACK。

证据：`/work/terrasync-db-refactor-evidence/release-db-refactor-zero-source-v3-20261105-184316/report.json`；独立 bucket 和私有配置已清理。root pinned workspace check 与 app／db／transport／storage-adapter 测试通过。

## 合入前仍须完成

普通文件 metadata-only 存在已有的标签应用与错误确认缺口，需要独立修复；零字节准入修复不替代该项。

还需完成数据库内部职责拆分、移除无用 DTO／原型及旧版本计数、任务契约和 transport 的归属整理，以及按设计删除 storage-adapter。最终候选必须重跑对应 CI、真实 HDFS 和受影响故障链路，并完成全量／增量／续扫性能与峰值 RSS 对照。先前 HDFS 通过的二进制不能作为最终候选证据。
