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

同一 SHA 的正常回归继续通过：目录 marker／标签 17 例与版本历史 14 例，覆盖 Single／Cluster、atomic／checkpoint、QUIC、null 版本及部分历史失败。证据分别位于 `/work/terrasync-db-refactor-evidence/release-db-refactor-source-mtime-marker-tags-20261105-184345/report.json` 和 `/work/terrasync-db-refactor-evidence/release-db-refactor-source-mtime-ordered-20261105-184453/report.json`；独立库与 bucket 清理通过。

该 SHA 已上传真实 Kerberos HDFS 节点，重新通过 7 个场景：预载、HDFS→Local、HDFS→HDFS、两处完整性检查，以及 QUIC→Local／HDFS。Local 目标 SHA 与 0640 权限通过，catalogue 保留 mode 416 且 uid／gid 为 null。证据在 `/work/terrasync-db-refactor-evidence/release-db-refactor-source-mtime-hdfs-20261105-184945/report.json`，独立 HDFS 路径与数据库清理通过。此结果替代旧二进制的 HDFS 阶段证据，后续代码变化仍须重测。

该阶段完整 Rust 检查通过：fmt、workspace check／Clippy、workspace 测试与串行双进程 E2E 16 例。日志前缀为 `/tmp/terrasync-source-mtime-`。

## 仅元数据更新的确认

普通文件 metadata-only 已复现标签未应用却确认成功的问题。修复让已发布目标和 staged stream 共用已捕获 metadata 计划；协议写入仍由 native role 负责。标签替换与清空都实际应用，失败返回保留 role 原因的类型错误，Receiver 不增加成功计数或发送分类确认。未知时间不合成 epoch；历史版本仍走完整复制，当前路径 metadata API 不声称支持源目标历史版本映射。

复审另发现复制计划隐含普通文件假设，会剥离目录的 setgid。data-mover `4717f1c` 让复制计划显式接收 EntryKind，目录保留组继承位，普通文件保持安全的 set-id 规则。原生计划与真实 Local role 应用回归先失败后通过，上游完整 `make ci` 通过；Unix 权限测试显式限制 Unix 平台。

最终阶段候选 SHA-256：`825ef2e16332b299d567f2831787e172a539cea2e4571cb08a69c0328ebb07eb`。源码记录：`/tmp/terrasync-directory-metadata-build-source.json`。fmt、pinned workspace check／Clippy、workspace 测试与串行双进程 E2E 16 例通过；本轮 app 207、storage-adapter 22 个测试通过，忽略项不计通过。

- 真实 S3 + QUIC 3 例通过：标签替换、清空、受控 403 拒绝。源目标来自实际相同列表时间和大小的对象，未伪造扫描事实。每例只有一次标签 PUT，没有内容 PUT，目标字节与版本数保持不变；拒绝记录一个条目错误。修复前候选同一场景没有执行标签写入，不能确认完成。最终报告：`/work/terrasync-db-refactor-evidence/release-db-refactor-metadata-final-metadata-only-20261105-191122/report.json`。
- 同候选版本历史 14 例与零字节替换 2 例通过。报告分别为 `/work/terrasync-db-refactor-evidence/release-db-refactor-metadata-final-ordered-20261105-191240/report.json` 和 `/work/terrasync-db-refactor-evidence/release-db-refactor-metadata-final-zero-source-20261105-191352/report.json`；包括部分历史失败不假确认、源替换不发布，以及 null 版本顺序。
- 同候选目录 marker／标签 17 例通过：`/work/terrasync-db-refactor-evidence/release-db-refactor-metadata-final-marker-tags-20261105-191134/report.json`。
- 同候选真实 Kerberos HDFS 7 例通过，增加含子文件的 sticky 目录，Local 目标保留文件 0640 与目录 01775，catalogue 对应 mode 416／1021、uid／gid 均为 null。报告：`/work/terrasync-db-refactor-evidence/release-db-refactor-metadata-final-hdfs-sticky-20261105-191253/report.json`。

上述成功报告的独立数据库、bucket、HDFS 路径和私有配置均已清理。

曾错误地使用 HDFS setgid 目录作为真实验收，早期失败不作为产品回归复现或修复证据。[HDFS 3.4.2 明确不支持 setuid／setgid，目录支持 sticky bit](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html)。setgid 的验证证据是原生计划回归和真实 Local role 的目录应用测试。

## 独立权限事实的增量比较

原生扫描持久化 mode／uid／gid 时使用独立权限事实，比较却仅读取完整 ownership 组合。真实 HDFS 的未变化第二轮 unordered 扫描产生了两条错误的 `metadata_changed`。修复让比较与行投影复用 ObservedView 的同一组 getter，保留未知值与真实零值的区别；所选历史版本不读取容器权限，无新增 I/O 或历史复制。

命名主体、单侧数值身份、UID／GID／mode 为零、未知 mode 及真实 mode 变化的本地测试先复现失败后通过。完整 Rust 检查、workspace 测试与串行双进程 E2E 16 例通过，日志前缀 `/tmp/terrasync-independent-permissions-`。

候选 SHA-256：`91ab7a59e584166fb013656504159d12fb9c77da032b987262a72d9a8fd94ea8`；源码记录 `/tmp/terrasync-independent-permissions-build-source.json`。

- 真实 HDFS + ClickHouse Single：11 例通过。unordered 与 blocks 各两次扫描，第二轮 metadata_changed 累计数均为 0→0；保留其余 HDFS／QUIC 复制与完整性场景，文件 0640、目录 01775 和未知 uid／gid 正确。报告 `/work/terrasync-db-refactor-evidence/release-db-refactor-independent-permission-single-20261105-192506/report.json`，独立资源清理通过。
- 同候选真实 HDFS + ClickHouse Cluster（2 分片 × 3 副本）11 例通过。按 job_id 从六副本共享事件表去重检查，unordered／blocks 均为 0→0；复制、完整性、权限与目录结果同时通过。报告 `/work/terrasync-db-refactor-evidence/release-db-refactor-independent-permission-cluster-v2-20261105-192702/report.json`；独立数据库、HDFS 路径和临时认证配置已清理。早期测试查询误用了单机的按 job 分表名，404 不作为产品失败或通过证据。
- 修复前真实失败报告 `/work/terrasync-db-refactor-evidence/release-db-refactor-independent-permission-red-20261105-191640/report.json` 记录 unordered 的累计 metadata_changed 为 0→2，资源清理通过。

## 数据库结构整理

SQL 列定义和有效投影已集中到 schema／columns，JOIN 分类构造集中到 catalogue／comparison；搬迁前后有效 SQL 文本和字段顺序机械比对一致。未接入任务执行流的 observation snapshot 独立原型及 EXCHANGE 生命周期已删除；只有该原型会设置的歧义标志和对应错误一并移除，实际 NativeScan／copy generation 恢复保持原样。

原型的损坏 snapshot、错误 backend、路径不匹配校验已迁到实际使用的记录解码入口。真实 ClickHouse 验证通过：身份替换不产生重复路径；当前协议下更新提交、空代次删除检测及最终目录为空。旧专属 EXCHANGE 测试随接口删除，历史设计已标明被替代。该阶段完整 Rust 检查及双进程 16 例通过，日志前缀 `/tmp/terrasync-prototype-removal-`；后续最终分支仍须复查依赖固定后的整体结果。

失效的 version_count 已从 SQL、Single／Cluster 行 DTO、比较摘要和批内计数中删除，移除了对应分组 map 和路径分配。逐版本视图、原生历史顺序、null／删除标记与实际逐版本 ACK 保留。测试直接断言选择的行数、版本 ID 和数据。

字段清理阶段 fmt／workspace check／Clippy、workspace 测试及双进程 16 例通过；app 201、db 106 个本地测试通过，忽略项另计。真实 Single／Cluster catalogue、Single 集成测试及两种部署的切点测试均通过且独立数据库清理成功，报告前缀 `/tmp/terrasync-db-refactor-real-version-count-v1-`。Cluster 17 个真实扫描场景亦已通过（总耗时约 402 秒），包括大规模身份查询的有界 SQL 校验；独立数据库已清理。二进制和源摘要记录在 `/tmp/terrasync-version-count-cluster-test-binary.json`。这项功能校验不替代相同模式下的吞吐／峰值 RSS 性能验收。

## 私有行编码与 manifest 写入

SQL 行 DTO 已收回私有 schema／records；traits 只描述任务操作，删除了从未携带实际查询结果的 QueryResult。tar manifest 接口直接接收共享原生观察，数据库逐条借用路径、扩展名和版本 ID；应用层不再构造持久化行 Vec，只有 tags 的 SQL JSON 仍需编码。

应用层保留 Arc 共享、逐版本统计及选择 ID 的断言，逐版本 SQL 编码断言迁到私有 schema；manifest 黄金测试核对全部字段与借用。Source snapshot 解码与 insert／end 错误传播保持原样，未改变未知写结果协议。

该阶段 workspace check／Clippy／测试及双进程 E2E 16 例通过；app 201、db 108 个本地测试通过，忽略项另计。真实 catalogue Single 10／10、Cluster 11／11、Single 集成 44／44 通过且测试数据库清理成功。记录前缀 `/tmp/terrasync-db-refactor-real-private-row-v2-`，Rust 日志前缀 `/tmp/terrasync-private-row-dto-`；首次 Clippy 的测试 lint 问题已修复，最终通过日志为 `clippy-v2.log`。

## 快照读取投影与表生命周期

流式观察回读只查询 relative_path、backend_kind、backend_id 和 entry_snapshot，避免读取随后丢弃的摘要列。完整观察仍由原生 snapshot 恢复；宽行与窄行共用 backend、codec 和路径校验，保留历史目录对象路径规则。窄行消费自身，将 backend ID 移入 native 身份构造；校验函数仅借用该身份。原过滤、FINAL、读取重试、取消与发送流程保持不变。

建表、排序键校验、表清理等九个方法集中到 catalogue／tables，SQL 与方法体机械比对保持一致；layout 继续处理物理部署。没有新增转发层。

该阶段完整 Rust 检查和双进程 16 例通过；最终借用签名清理后另通过 workspace check、db Clippy 与 db 109 个本地测试。真实 catalogue Single 10／10、Cluster 11／11 和 Single 集成 44／44 通过，独立数据库清理成功。记录前缀 `/tmp/terrasync-db-refactor-real-read-tables-v1-` 与 `/tmp/terrasync-catalogue-read-tables-`。marker 完整快照回归验证窄投影未丢失原生事实。

## 纯任务模型与数据库身份策略

观察视图、事件和事实读取移到 task-model；其正常依赖仅为 data-mover-model、serde 和 thiserror。数据库不再依赖 storage-adapter；应用、传输与 Web 直接引用任务契约，adapter 不再公开重导出这些类型。原生观察和逐版本视图仍共享 Arc，不复制完整历史。

路径键保留 Cow 借用，删除无用的 owned 路径便捷接口。跨扫描身份的资格与编码集中到私有 db／schema／identity；比较阶段只判断资格，不再为布尔判断分配编码字符串。Local 身份排除规则和持久化前缀保持原样。模型错误通过透明类型包装保留 source，取消与 NotFound 的分类保持不变。

此阶段两项架构复审通过。fmt、workspace Clippy／测试与串行双进程 E2E 16 例通过；app 201、db 110、task-model 2 个本地测试通过，忽略项另计。真实 catalogue Single 10／10、Cluster 11／11、Single 集成 44／44，以及 Single／Cluster 切点各 3 例通过，独立数据库清理均成功。Rust 日志前缀 `/tmp/terrasync-task-model-`，真实报告前缀 `/tmp/terrasync-db-refactor-real-task-model-v1-`。依赖清单记录 `/tmp/terrasync-task-model-normal-dependencies.txt`。

## 传输契约与远端目录清单

TransferMode 移入纯任务模型，引擎策略映射保留在存储消费者。opaque wire 编码、NDX 目录页和 DataChunk 归 transport；transport 已解除 storage-adapter 依赖。底层流读写使用原生 PositionedChunk，应用将 wire 的 Bytes 移入原生块，不复制 payload。

目录排序、会话编号和容量 2 的目录页队列移到 app／remote_inventory。底层 DirectoryInventory 只交付原生事件，在编号前复用历史顺序和 Required tags 准入。错误文本、稳定排序、NDX 分配、结束判定和 ACK 流保持原样。无状态复制边界不再携带旧 AsyncReceiver 中未绑定的状态。

两项架构复审通过；完整 Rust 检查、workspace 测试和串行双进程 16 例通过。app 201、db 110、task-model 2、transport 33 个本地测试通过，另有 QUIC roundtrip 14、双进程流 21、Local roles 12、Direct patch 4 例通过；忽略项另计。日志前缀 `/tmp/terrasync-transport-extract-`，无 adapter 的传输依赖清单为 `/tmp/terrasync-transport-normal-dependencies.txt`。

对应 release SHA-256 为 `807904d0cab31d31aec687c4af139b69daf29a9956d6260b625c67f4f9d57d1b`，精确源码摘要在 `/tmp/terrasync-transport-extract-build-source.json`。以下报告均使用该候选，独立数据库、对象、HDFS 路径和认证配置清理成功：

- marker／tags 17 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-transport-marker-tags-20261105-205556/report.json`。
- 有序历史 14 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-transport-ordered-20261105-205703/report.json`。
- 零字节源替换 2 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-transport-zero-source-20261105-205816/report.json`。
- metadata-only 3 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-transport-metadata-only-v2-20261105-205955/report.json`，包含目标 tags PUT 被拒绝后没有成功 ACK；替换／清空 tags 均没有数据 PUT 或新增目标版本。
- 真实 Kerberos HDFS + ClickHouse Single／Cluster 各 11 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-transport-hdfs-single-20261105-205702/report.json` 与 `/work/terrasync-db-refactor-evidence/release-db-refactor-transport-hdfs-cluster-20261105-205708/report.json`。unordered／blocks 未变化第二轮均为 0→0；复制、QUIC、完整性、0640／01775 及未知数值主体正确。

metadata-only 首次夹具创建失败，没有执行业务用例，不作为产品回归或通过证据。重跑根据真实 LIST 修改时间的差值调整两侧 PUT 的创建时序，8 次后获得完全相同的实际时间；没有修改服务时钟、伪造响应或降低断言。

## 删除重复后端枚举

StorageEnum 的五个变体都包装同一个连接对象，现已删除枚举、Deref 分派和 storage_enum 转发模块。实际连接状态合并为 StorageContext，只持有原生 roles、块大小与可选重连配置；没有保留旧类型别名。StorageType 同时删除；连接前 URL 分类保持原判断顺序，返回原生 BackendKind，连接后的 HDFS／S3 规则读取实际连接身份。

工厂的块大小、ensure_dir、并发、角色认证与重连配置保持原样。HDFS 恢复身份、版本选择、目录元数据收尾和原始 typed errors 保留；观察和 payload 不增加复制。两项架构复审通过，workspace 全特性编译、Clippy、测试与双进程 E2E 16 例通过，记录前缀 `/tmp/terrasync-storage-context-`。

对应 release SHA-256：`906365304e32d05e9d86690fcc2a09cdfa47dc0c94bc317922702b5604ada2ec`，源码记录 `/tmp/terrasync-storage-context-build-source.json`。真实验证及资源清理均通过：

- Single／Cluster marker／tags 及 QUIC 17 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-context-marker-tags-20261105-210946/report.json`。
- Kerberos HDFS + ClickHouse Single 11 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-context-hdfs-single-20261105-211141/report.json`。
- Kerberos HDFS + ClickHouse Cluster 11 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-context-hdfs-cluster-20261105-211147/report.json`。两种部署的 unordered／blocks 未变化第二轮 metadata_changed 均为 0→0；Local／HDFS／QUIC 复制、完整性和真实权限正确。

## 删除失效的条目辅助类型

无运行时调用的 datetime_to_string 和 Tag 已删除；唯一使用 Tag 的测试改为直接构造同一原生夹具输入，标签应用与发布顺序断言保留。fmt 与对应 stream_s3_tags_are_applied_before_publication 测试通过，日志前缀 `/tmp/terrasync-unused-entry-dto-`。此清理不改变存储 I/O。

## 原生失败与清理错误

删除克隆式 StreamTransferDiagnostic、报告副本及产品层重复清理分支。失败按原传输策略由 data-mover 清理后直接透传 TransferFailure，保留阶段、目标可能已变化、原生元数据报告及独立清理失败。metadata 的 Error::source 采用原生契约（None），类型化原因由 metadata_failure 的 getter 获取；不能宣称旧 source 链完全不变。

依赖精确固定到 `4f42841f02d8fec3cb26d929dcbe245e274b7bd0`。原生完整 make ci 通过，日志 `/tmp/data-mover-expert-input-final-v2-ci.log`；TerraSync fmt、workspace check／Clippy、526 个 workspace 测试及串行双进程 E2E 16 例通过，85 个忽略项未计为通过。日志前缀 `/tmp/terrasync-native-error-final-`。两项架构复审通过。

真实 S3 代理只对本轮目标的数据 PUT 返回 403；旧候选缺少底层分类的复现与修复后复测形成对照。修复候选 release SHA-256 为 `45b48784df5db8b1ec4a4397eea80d5d6783bbfd968d08633259046add863afd`，源码记录 `/tmp/terrasync-cleanup-cause-fixed-build-source.json`。

- 有效失败证据：`/work/terrasync-db-refactor-evidence/release-db-refactor-cleanup-publish-denial-red-v2-20261105-214248/report.json`，拒绝写入的目标保护通过，但 commit 诊断只有 storage role failed，丢失 PermissionDenied。
- 修复后通过：`/work/terrasync-db-refactor-evidence/release-db-refactor-cleanup-publish-denial-fixed-20261105-221550/report.json`，准确显示 Publish／PermissionDenied／Permanent；1 次数据 PUT、0 次 tags PUT，目标内容、tags 和版本数不变，任务记录 1 个错误且无成功 ACK。独立对象与配置清理成功。

原生 Pending 输入取消回归也已修复：取消交付 typed Cancelled／Write／Transient，继续 await backend 写入以收束已有操作。它验证等待下一输入块的取消，不代表已停滞的所有协议请求都能被此 guard 中断；受影响真实链路的完整故障矩阵仍待最终候选执行。

## 原生有偏移输入与精确 basis 读取

write_positioned 集中连续偏移校验、完整摘要和恢复前缀跳过；TerraSync 只移动原生块并回填最终输入事实，删除重复 resume_offset。范围读取通过 read_current_range 绑定 fresh describe 的身份与已解析版本，保持精确 EOF 校验；旧 basis worker、channel、可选哈希和无人调用的 compute_hash／HashCalculator 已删除。重建单块直接移动 Bytes，多块才分配连续结果缓冲区。签名读取失败仍按原规则回退 full。两处读取仍使用独立 token；初始 describe 与已停滞请求仍依赖 backend 超时，不声明新增任务级取消保证。

原生依赖固定 `44deb026db29024d15f27d4173548a3192f28ecb`，完整 make ci 通过，记录 `/tmp/data-mover-basis-range-ci.log`。TerraSync 最终 fmt／workspace check／Clippy、527 个 workspace 测试与双进程 E2E 16 例通过；85 个忽略项未计为通过。记录前缀 `/tmp/terrasync-native-basis-final-`。Standards／Spec 均无阻断项。

对应 release SHA-256 为 `dc2231bf91c74ee9a331267e216e5751277955fafcf127b0acaab2d21034cee6`，源码记录 `/tmp/terrasync-native-basis-build-source.json`。以下 63 个真实场景通过，独立资源均清理成功：

- marker／tags 17 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-marker-tags-20261105-223123/report.json`。
- 有序版本历史 14 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-ordered-20261105-223233/report.json`。
- 零字节源替换 2 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-zero-source-20261105-223346/report.json`。
- metadata-only 3 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-metadata-only-v2-20261105-223358/report.json`。
- 真实 S3／QUIC delta 3 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-basis-delta-valid-v2-20261105-223531/report.json`。Atomic／Checkpoint 均实际读取 basis 范围、产生 delta token、校验完整目标并收到 1 个成功 ACK；额外拒绝一次 basis GET，保留 PermissionDenied 并正常回退 full。远端 sync 本身不使用数据库，此项不能算 ClickHouse 两种部署的持久化证据。
- 真实 Kerberos HDFS + ClickHouse Single／Cluster 各 11 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-hdfs-single-20261105-223400/report.json` 与 `/work/terrasync-db-refactor-evidence/release-db-refactor-native-basis-hdfs-cluster-20261105-223405/report.json`，包含 unordered／blocks 两轮、完整性、Local／HDFS／QUIC 和真实权限。
- 真实 S3 源断连及续传 2 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-basis-source-cut-checkpoint-20261105-223831/report.json`。80 MiB+17 字节源在原生 64 MiB checkpoint 写入后断连，8 个 part 保留，旧目标内容和 tags 不变，0 成功／1 错误；同一地址／任务重试只上传 part 9、10、11，前缀没有重写，完整内容和 tags 正确并成功 ACK。两次执行总耗时分别约 3.13／3.72 秒，包含正常传输，不能据此推导独立故障检测或恢复窗口耗时。最终 multipart 与对象清理成功。

最后两项的运行脚本和摘要已保存在对应证据目录。早期新夹具把 HEAD Content-Length 错写为零、查询远端 sync 并不创建的目录表，或在 64 MiB checkpoint 前断言复用，均不作为产品回归或通过证据；未降低最终内容、ACK 或前缀复用断言。

## 删除旧恢复上下文与共享归档清单

`ResumeContext`、忽略的恢复参数和伪造的 part 路径已删除；原生目标端仍是续传权威，实际使用的 CommitRecorder／HDFS 提交回调保留。应用直接构造 TarRequest，删除 adapter 的归档转发层；任务事件、transport 和数据库借用同一 Arc 清单，原生流游标使用索引，不克隆每个观察句柄。

结构提交为 `8cdf221`、`d5218a6`。随后三个原生行为修复独立提交：`1098432` 在每个文件读前／EOF 后共用观察校验，并按 Atomic 策略清理；`4edb721` 从独立 PermissionFacts 投影权限和已知数字主体；`8277b3c` 将 TAR mode 限定为权限位，类型由 typeflag 表达。未知权限 facet 不回退到旧数字字段，Named 主体不猜测 UID／GID；set-id 继续使用模型的共同安全规则。

检测到源变化会保留 typed Source／Read／Conflict，发生在计数、删源及成功 manifest 通知之前。清理保留原失败、发布状态及独立清理错误，不回滚已发布最终对象。这不提供可变目录的原子快照；删除归档源仍要求调用方独占控制。

原生最终 make ci 通过，记录 `/tmp/data-mover-tar-type-bits-final-v2-ci.log`，其中 7 个权限投影用例及 10 个真实 Local role 归档用例通过。TerraSync 最终 fmt、workspace check／Clippy、528 个 workspace 测试及串行双进程 E2E 16 例通过；85 个忽略项未计通过。日志前缀 `/tmp/terrasync-tar-guards-v2-`。前端 157 个测试与构建、MCP 7 个测试通过，记录前缀 `/tmp/terrasync-refactor-web-`、`/tmp/terrasync-refactor-mcp-`。

对应依赖精确固定 `8277b3cedc9a20e2b4cce449a03f03596b2318c7`；release SHA-256 为 `df5099bb961689d0b42ceaa84964c7ce7cba472b355e13c9e6bd87ac29bda151`，源码记录 `/tmp/terrasync-tar-guards-v2-build-source.json`。以下 45 个真实场景使用该同一候选，全部通过且独立资源清理成功；脚本与二进制源码摘要保存在报告同目录：

- Local→S3 + ClickHouse Single／Cluster 归档 2 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-header-20261106-001939/report.json`。核对完整内容、长路径、0640、PAX 时间、manifest 与已发布观察。
- S3 归档源变化 + Single／Cluster 8 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-stale-20261106-001939/report.json`。读前增长、等长修改、空文件变非空，以及真实 GET 后交付前修改源并要求删源；均保留变化后的源与旧目标，无完成行／manifest，有 typed Source Conflict，对应目标前缀无残留 multipart 或额外对象。
- Kerberos HDFS + ClickHouse Single／Cluster 各 15 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-hdfs-single-20261106-001951/report.json`、`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-hdfs-cluster-20261106-002004/report.json`。包含 unordered／blocks 两轮、Local／HDFS／QUIC、完整性、真实 0640 与目录 sticky 权限，以及 HDFS→Local／HDFS 归档和回读；header 和 manifest 保留真实 mode，manifest 的未知数字主体仍为 null。
- S3／QUIC delta 3 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-delta-20261106-001952/report.json`。Atomic／Checkpoint 的实际 basis 范围读取与 ACK，以及拒绝一次 GET 后保持类型并完整回退；远端 sync 不使用数据库，不计为两种 ClickHouse 部署证据。
- S3 源断连与续传 2 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-v2-source-cut-20261106-002009/report.json`。实际 64 MiB checkpoint 后断连，保留 8 个 part；同一任务重试只上传 9／10／11，完整内容、tags 与 ACK 正确。总执行耗时不代表独立的故障检测／恢复窗口。

本轮保留的有效失败对照包括：源变化后错误发布并删掉新源的 `/work/terrasync-db-refactor-evidence/release-db-refactor-tar-stale-red-20261105-233802/report.json`；旧 HDFS header 不保留独立权限的 `/work/terrasync-db-refactor-evidence/release-db-refactor-shared-tar-hdfs-single-valid-20261105-230640/report.json`；中间候选 Local mode 混入类型位的 `/work/terrasync-db-refactor-evidence/release-db-refactor-tar-guards-header-20261106-000517/report.json`。最后一项的 Local 最小复现为 0100640；原生真实 Local 回归修复前失败记录 `/tmp/data-mover-tar-type-bits-red.log`。这些失败报告不计为通过，未放宽最终断言。

## 合入前仍须完成

还需完成数据库目录会话与任务接口的整理，以及按设计删除 storage-adapter。任务契约与 transport 已分离，剩余存储机制和产品配置仍须分别归位。最终候选必须重跑对应 CI、真实 HDFS 和受影响故障链路，并完成全量／增量／续扫性能与峰值 RSS 对照。先前 HDFS 通过的二进制不能作为最终候选证据。
