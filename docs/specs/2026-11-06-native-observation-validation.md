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

## 任务显式持有目录作用域

删除按配置／job 查找的全局弱引用代次注册表。每次创建目录建立独立任务作用域；显式 Clone 共享代次状态与集群写锁。状态及锁集中到 catalogue／lifecycle，连接与 Clone 构造归 connection。clone_box 复用同一具体 Clone 契约。

全量／增量复制将已经启动代次的目录 clone 注入消费者，避免消费者再次连接并依赖隐式状态共享。独立消费者仍自行连接；纯扫描统计消费者继续不落库。Single／Cluster 的布局和写入差异保持在 db。删除无人调用的 get_database。

两项本地测试验证代次共享／任务隔离和写锁共享；四项真实共享测试的夹具改为显式 clone，写入、并发提交、失败传播及重新 begin 的原断言均保留，真正重开恢复仍使用 new。新增实际 Single／Cluster 用例验证 begin 前后的 clone 使用正确代次、独立 same-job 任务不能提交该代次，以及并发／重复提交。

最终 fmt、workspace check／Clippy、530 个 workspace 测试和双进程 E2E 16 例通过；86 个忽略项不计通过。匹配最终测试夹具的检查记录前缀 `/tmp/terrasync-catalogue-scope-final-`，E2E 为 `/tmp/terrasync-catalogue-scope-e2e.log`。

候选 release SHA-256 为 `4720949c51bffd8fc7f287b29741f513e229ed8e8387bf84295a5eb918676fe0`；源码记录 `/tmp/terrasync-catalogue-scope-build-source.json` 同时记录 tracked diff 与新增 lifecycle 文件摘要。以下 134 个真实用例通过，独立测试资源均清理成功：

- ClickHouse Single 目录 11、Cluster 目录 12、Single 集成 44、两种部署切点各 3 例，共 73 例。报告前缀 `/tmp/terrasync-db-refactor-real-catalogue-scope-v1-`，分别为 `single-catalogue.json`、`cluster-catalogue.json`、`single.json`、`single-cuts.json`、`cluster-cuts.json`。包含上述显式共享／隔离用例和保留的持久重开恢复断言；目录 fixture 的库按本轮日志逐一回查已删除。
- 同一 release 的 S3 marker／tags 17 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-scope-marker-tags-20261106-003246/report.json`。
- 有序版本、恢复及 null 历史 14 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-scope-ordered-20261106-003354/report.json`。
- Kerberos HDFS + Single／Cluster 各 15 例：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-scope-hdfs-single-20261106-003245/report.json`、`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-scope-hdfs-cluster-20261106-003259/report.json`。包含扫描两轮、复制／QUIC、真实权限、两类归档及回读。

## 具体目录接口与内聚拆分

删除 Database trait、Box<dyn Database>、clone_box 和 db 的 async-trait 依赖；应用显式持有 ClickHouseDatabase。Single／Cluster 差异仍私有于 db，未引入替代注册表。catalogue 根文件只保留模块和状态声明，读、写、代次、删除判定及表生命周期分别归位；移除仅调用同名实现的转发方法和无人使用的 prelude。目录绑定自身 job 检查复制基线，应用不再拼接物理表名。临时扫描表名及差异分类中间结果私有化。

排除行使用借用字符串的私有 SQL DTO，去掉序列化前路径／版本的重复分配；实际 RowBinary 解码覆盖 Unicode 和空版本。临时比较表的失败及清理验证通过真实 HTTP／RowBinary 客户端测试，包括插入失败后重试、分类失败、不发布部分结果，以及操作与清理双失败原因保留。应用删除大范围假数据库实现，改为从真实客户端错误入口断言状态和事件。

32 个移位方法体、4 个取消转发后的实际实现及删除查询 SQL 的等价检查记录在 `/tmp/terrasync-catalogue-concrete-move-verification.json`。最终 Standards／Spec 双轴审查通过。拆分期间发现 baseline_tests 注册遗漏，已恢复；原 db 137 个测试函数均保留。此前少计测试的候选与日志已作废，未用于真实验收。

最终 fmt、默认与 all-features 的 workspace all-targets check、Clippy、532 个 workspace 测试及双进程 E2E 16 例通过；86 个忽略项未计通过。记录前缀 `/tmp/terrasync-catalogue-concrete-v3-`。完整基线模块单独确认 3 个本地用例及 2 个环境用例注册。

匹配该源码的 release SHA-256 为 `5de332518685fc54e794da931f836a3c3f5fb4c042d0e4c68ae309aa6181f1d4`，源码记录 `/tmp/terrasync-catalogue-concrete-build-source.json`；构建前后校验 tracked diff 和全部未跟踪源码摘要。恢复测试入口不改变生产二进制，故 SHA 与已作废的中间产物相同；本轮采用完整检查后的新记录。以下 136 项真实用例全部通过，测试资源清理成功：

- DB 75 项：Single 目录 11、Cluster 目录 12、Single 集成 44、Single／Cluster 切点各 3、Single 首次基线与重开 2。报告前缀 `/tmp/terrasync-db-refactor-real-catalogue-concrete-v1-`，分别对应 `single-catalogue`、`cluster-catalogue`、`single`、`single-cuts`、`cluster-cuts`、`single-baseline`。目录 fixture 逐项回查已删除，首次基线 fixture 按本轮唯一时间范围回查无残留。
- S3 marker／tags 17 项：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-concrete-marker-tags-20261106-015928/report.json`。
- 有序版本、恢复及 null 历史 14 项：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-concrete-ordered-20261106-020042/report.json`。
- Kerberos HDFS + Single／Cluster 各 15 项：`/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-concrete-hdfs-single-20261106-015940/report.json` 与 `/work/terrasync-db-refactor-evidence/release-db-refactor-catalogue-concrete-hdfs-cluster-20261106-015956/report.json`。远端上传校验与报告 SHA 相符，覆盖两轮扫描、复制／QUIC、权限、归档及回读。

## 源读取预算由原生管理器持有

原生提交 `2752c8293fc5ffb1606a2fb17bc68ae94493f10d` 将配置快照到 SourceQosPolicy 的编译及共享预算归 QosManager；删除 adapter 中全局 Weak／Moka 注册表和重复编译，普通复制、checkpoint、HDFS、expert source 及归档直接共用管理器预算。缓存随 manager clone 生命周期释放，每个快照沿用首次默认读上限；热更新后的调用创建新预算，活动旧流继续持有旧预算。无新增后台任务或按 entry 累积的集合。

审查发现并修正锁外读取快照可能使旧调用覆盖新组的问题：现在持缓存锁后读取快照。4 项暂停时钟测试覆盖 clone 共用、热更新保留旧流、并发初始化及独立管理器／取消；8 路 admission 并发验证总预算，工作线程显式进入同一 runtime。原生完整 make ci 通过，日志 `/tmp/data-mover-source-group-final-v2-ci.log`；Standards／Spec 复审通过。

TerraSync 最终 fmt、默认与 all-features workspace all-targets check、Clippy、532 项 workspace 测试和 16 项双进程 E2E 通过；86 个忽略项不计通过，日志前缀 `/tmp/terrasync-source-group-`。release SHA-256 为 `e8ee185342ba88103754d29f12b302c3d101e7bb3268a5e9be0c6158ce280948`，源码记录 `/tmp/terrasync-source-group-build-source.json`，构建前后源码指纹一致。

该候选的 44 项真实回归通过，资源全部清理：

- S3／Local、跨 S3、QUIC、两个并发归档的不限速／限速对照共 8 项：`/work/terrasync-db-refactor-evidence/release-db-refactor-source-group-qos-20261106-022230/report.json`。每项核对最终内容，S3 普通与远端路径还核对 tags。1 MiB 总载荷共享 256 KiB/s，四条限速链路分别约 4.13／4.13／4.12／4.54 秒，不限速约 0.22～0.63 秒。这是预算功能回归，不代表最终吞吐／RSS 验收。
- Kerberos HDFS + ClickHouse Single／Cluster 各 18 项：`/work/terrasync-db-refactor-evidence/release-db-refactor-source-group-valid-hdfs-single-20261106-022419/report.json`、`/work/terrasync-db-refactor-evidence/release-db-refactor-source-group-valid-hdfs-cluster-20261106-022445/report.json`。每组保留 15 项扫描／复制／权限／归档／回读，再加 Local→HDFS 限速、HDFS→Local 不限速与限速 3 项。追加的限流用例关闭落库，不能将其计为集群落库验证；各 15 项原有场景分别验证对应部署。HDFS 源／目标的 1 MiB 载荷共享预算约 4.1～4.2 秒，回读内容一致。

首次新增 HDFS 限速夹具使用了不适用该 NameNode 的 64 KiB block 配置，Prepare 失败，记录 `/work/terrasync-db-refactor-evidence/release-db-refactor-source-group-hdfs-single-20261106-022233/report.json`；改为 1 MiB 后重跑上述完整两组，未改变产品代码或放宽内容、限速断言。该失败及旧构建的脚本对照 8 项不计入本轮 44 项通过。

## 合入前仍须完成

目录作用域、具体数据库接口及临时表协议已收敛；还需按设计删除 storage-adapter。任务契约与 transport 已分离，剩余存储机制和产品配置仍须分别归位。最终候选必须重跑对应 CI、真实 HDFS 和受影响故障链路，并完成全量／增量／续扫性能与峰值 RSS 对照。先前 HDFS 通过的二进制不能作为最终候选证据。


## storage-adapter 删除与实际职责迁移（2026-11-06）

运行时候选 `/work/terrasync-db-refactor-without-adapter`，SHA-256
`1dfde0c5d2e9f3d9720f0b7f3335db35c0fadea9d5c353b4a7148b749dcf16dc`。
精确构建来源为 `/tmp/terrasync-adapter-removal-build-source.json`：基于 `2ea8622` 的已记录工作区内容，包含每个新增文件的散列；检查、测试、构建期间保持源码冻结，构建后及追加本文前均核对指纹。原生依赖固定为
`0070c1599689e6c3a5cb3c8e7df602e03f00ca97`。

- 删除整个 storage-adapter crate、workspace 成员和所有依赖项，没有新增替代 facade crate。
  任务连接配置在 app/storage_factory，扫描策略／准入／恢复在 app/scan，复制策略及元数据损失策略在 app/file_copy，远端提交状态在 app/file_commit，报告映射在 app/integrity_check。
  普通／历史／当前版本／checkpoint／HDFS 的复制请求集中构造，IO、恢复、读回验证和发布仍由原生引擎承担。
- StorageContext 只持有真实连接角色、块大小及重连配置。删除静态复制／metadata／namespace 转发，单路径观察直接调用原生构造。删除重复 HDFS 恢复模式与单变体 MetadataApplication；生产调用使用已有 TransferMode，成功返回后才认定元数据已应用。
- ACL JSON 编码迁入 transport：编码借用原生字节，解码仅在接收边界取得所有权，保持现有 wire 字节；本机 ACL 复制直接消费原生事实。xattr 载荷直接移动，不再先 clone。
- 原生 SourceDescriptor／ObservedEntry 的 backend facts 使用 Bytes 共享；指针测试覆盖 model、traversal、expert 路径，v11 快照字节保持。不可用的历史／marker 描述仍被单路径观察拒绝；当前虚拟目录使用内联事实，避免访问不存在的对象。
- NFS 根目录特判归入 backend 的 Namespace：读取最多 16 条的一页后取消剩余枚举。
  原三个确定性断言迁入 native；完整 make ci 通过，真实 Linux NFS v3／v4.0／v4.1 只读 probe 通过。
  来源和日志为 `/tmp/data-mover-nfs-connectivity-final-real-report.json`、`/tmp/data-mover-native-nfs-connectivity-final-ci.log`；这些结果不包含 NFS 故障注入。
- 原 adapter 的测试按职责迁移并注册；删除一个仅表达无生产调用的 Require 模式的测试，三个 NFS probe 测试迁至 native。
  新增 ACL wire 两项、观察错误类型两项；普通复制直接校验实际 mtime 与 0640，而不再断言单变体返回值。
  原生聚合观察错误集中映射回已有 Role／Capability／ModelValue 分类，保留 NotFound／Cancelled 语义和 source。

本地 fmt、all-features/all-targets check 与 Clippy 均通过；workspace 测试 **532 通过、86 按原条件忽略**，真实双进程 E2E **16 通过**。
日志 `/tmp/terrasync-adapter-final-{fmt,check,clippy,tests,e2e}.log`。
Clippy 仍有仓库已有警告，以及待独立修复的删除进度路径未读取提示；不能写成零警告。

真实正常路径与协议契约共 **150 项通过**，所有本轮服务夹具已按报告清理：

| 范围 | 实际通过项 | 证据 |
| --- | ---: | --- |
| DB single-catalogue | 11 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-single-catalogue.json` |
| DB cluster-catalogue | 12 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-cluster-catalogue.json` |
| DB single | 44 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-single.json` |
| DB single-cuts | 3 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-single-cuts.json` |
| DB cluster-cuts | 3 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-cluster-cuts.json` |
| DB single-baseline | 2 | `/tmp/terrasync-db-refactor-real-adapter-removal-v1-single-baseline.json` |
| App point | 8 | `/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-point-20261106-041355/report.json` |
| App markers | 17 | `/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-markers-20261106-041419/report.json` |
| App ordered | 14 | `/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-ordered-20261106-041533/report.json` |
| App hdfs-single | 18 | `/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-hdfs-single-20261106-041701/report.json` |
| App hdfs-cluster | 18 | `/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-hdfs-cluster-20261106-041728/report.json` |

point 8 项覆盖 Single／Cluster、普通／QUIC 与标准／禁用 tags profile；验证没有 marker 对象的虚拟目录、子文件和普通文件。
禁用 tags profile 运行于真实 RustFS，不是 DXN 设备验证。
HDFS 每模式原 15 项使用真实 Single／Cluster；额外 3 项 QoS 的 Local→HDFS、HDFS→Local 路径关闭 DB，不能把这 6 项记成数据库部署模式验证。
HDFS 产物经 SHA 校验上传到本任务已有运行根；原始配置与 keytab 未修改。

整分支 Standards／Spec 复核结合阶段审查，未发现新增结构或观察语义阻断。以下仍须独立收尾，不能据上述正常路径宣布整体验收通过：

- rm 对失败事件的计数与返回、receiver 字符串化原始错误，按行为修复单独提交。
- 真实强退／重启／Pod 本地状态丢失及网络故障矩阵正在验证；最初的网络 harness 将未确认数据库写入误判为普通取消失败，报告 `without-adapter-scan-fault-initial` 整体未通过，不计入上述 150 项。
  日志保留了 RequestOutcomeUnconfirmed；修正 harness 后需分别核对取消耗时、未知结果、恢复后目录与累计统计，不能将未知写结果强行改标为已确认取消。
- 相同 unordered／blocks 的无 DB 基线性能、峰值 RSS 和持续增长尚未完成。
- 最终 callback 首次失败即退出、缺少文档目标中的三次重试及 ClickHouse 补偿，是基点 bedb632 已有流程，本次未改动；本轮不宣称该目标已达成。


## 删除失败的独立行为修复与强退恢复验证

storage-adapter 的结构迁移提交为 `da0dc0b`。随后独立修复删除结果：
只对真实删除事件计数，Entry／Terminal 原因和取消保持有类型；保留首个错误，排空事件直到 native finish 后再返回，避免错误后遗留在途删除。
成功日志仅在完整成功后产生，app 与 CLI 的根位置日志均复用 URL 脱敏。
新增 2 项测试覆盖原始错误分类及未完成事件不能计数；相关测试、app Clippy 与 workspace all-features/all-targets check 均通过。

必须纠正最初删除 harness 的证据：`without-adapter-rm-denial-red` 与 `without-adapter-rm-denial-green` 仅拦截 DELETE，没有拦截实际使用的 POST DeleteObjects，未命中拒绝分支，二者均不能证明拒绝行为。
补齐批量拦截后，`without-adapter-rm-batch-denial-red` 在旧候选上明确记录 **1 次拒绝、保护对象未变，但 CLI 仍退出 0**，才是有效红测。

修复候选 `/work/terrasync-db-refactor-rm-failure-v2`，SHA-256
`728464b14052a2f099da5a00b2fea91c3d37cb03449814e748ba2a269db4e342`。来源 `/tmp/terrasync-rm-failure-v2-build-source.json`，构建前后及追加本文前核对指纹。

真实 S3 两项绿测均通过：`/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-rm-batch-denial-green-20261106-044136/report.json`。

代理只拒绝本轮保护对象所在的删除批次，另一个已发出删除延迟 0.6 秒；CLI 失败后独立读回确认保护对象不变，允许删除的对象已完成，没有假成功日志或凭据泄漏。解除拒绝后再次删除成功；临时桶已清理。
最初无效运行的 3 份含旧原始 URL 的自有日志已脱敏，不保留凭据副本。

前一结构候选另有 **8 项真实 Single／Cluster 强退／重启回归通过**：
- `single`：`/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-resume-single-1793938701600005613/report.json`，4 项，4 个临时库均由测试 guard 清理。
- `cluster`：`/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-resume-cluster-1793938826074335154/report.json`，4 项，4 个临时库均由测试 guard 清理。

覆盖已封存全量的 Pod 本地状态丢失、checkpoint 后事实的新 metadata cut、完整／增量的强退与正常取消，以及 unordered 重启。回读核对实际源路径、唯一数据库事实、已确认 checkpoint 与累计统计，没有通过重扫再次累计。

网络矩阵 v2 的原报告整体未通过：单机 36 项及部分集群用例已通过；集群无注入的恢复执行遇到 1 秒 read_timeout，保留 RequestOutcomeUnconfirmed 并准确失败。
该失败不能标成整套通过，也没有因此改产品错误语义。将真实集群正常请求的读时限设为 5 秒后，独立的集群增量闪断及恢复对照已通过；完整矩阵仍需完成。
最终 callback 的基点既有限制与性能/RSS 门禁仍按上节标为未完成。


## Receiver 原始错误保留与最终运行时候选

删除修复已独立提交 `8a50c5c`。Receiver 的普通文件、源链接读取、目标链接建立原来把错误格式化为 CopyError(String)，丢失了原生类型。
新增 CopyStep／相对路径上下文及 boxed 原始 AppError，统一的 storage_error 递归保留分类入口；只在错误分支取得路径所有权，成功及重试流程不变。
三个真实 Local I/O 测试先以旧代码失败，再以修复代码通过：源文件消失、源软链消失、非空目标目录拒绝替换；同时断言旧目标、已有子文件和暂存清理。
红／绿日志为 `/tmp/terrasync-receiver-copy-failure-{red,green}-tests.log`。
两轴 review 均未发现此步新增问题。

最终运行时候选 `/work/terrasync-db-refactor-native-final`，SHA-256
`f5ba57d8264c2d81d44b028eff0db137c5d68ea3503eac608928eda8d1e5f882`，来源 `/tmp/terrasync-native-final-build-source.json`。

源码在完整检查、测试和构建期间冻结，构建后及追加本节前核对指纹。
fmt、workspace all-features/all-targets check／Clippy 通过；完整 workspace **537 通过、86 原条件忽略**，双进程 E2E **16 通过**。
日志 `/tmp/terrasync-native-final-{fmt,check,clippy,tests,e2e,release-build}.log`，不能把已有 Clippy 警告写成零警告。

最终候选的真实目标 S3 响应丢失验证 **4 项通过**：
`/work/terrasync-db-refactor-evidence/release-db-refactor-native-final-target-lost-ack-v2-20261106-045857/report.json`。

Single／Cluster × Atomic／Checkpoint：代理仅在真实最终写请求获得成功响应后切断响应。
首次和重跑后均独立读回内容及版本数；最终 1 份源事实，目标仅原始旧版本＋新版本，没有多出重传版本，完成后没有残留 multipart upload。
初次 harness 错把 Cluster 扫描表的共享命名用于 catalogue，前两项通过后在集群查询遇到 404；该整体失败报告不作为通过证据。
修正为两种部署均有的 catalogue 前端 base_<job> 后，以上完整四项通过。单机／集群的物理差异仍只由 db 决定。

另外，结构候选所用的实际 app 扫描 API 已在真实 Linux NFSv4.1 的 READDIR 链路通过 **6 项受控故障验证**：
`/work/terrasync-db-refactor-evidence/release-db-refactor-nfs-source-fault-20261106-045234/report.json`。

只通过本轮 TCP 代理识别和转发实际 RPC，不修改共享服务。覆盖正常、0.5 秒闪断、持续重置、静默不转发，以及重置／静默期间取消；每项解除代理故障后重新扫描成功。
成功的初次扫描和恢复扫描均核对 **68 条实际条目**，没有重放重复累计。
闪断从注入到成功约 0.71 秒；持续故障约 3.42 秒退出；静默故障约 4.02 秒退出（包含配置的 1 秒检测时限）；取消约 0.041 秒。
这些是从注入开始的实际观察值，不伪装为精确的识别后耗时。该 NFS 组关闭 DB，不能计成 NFS+Single／Cluster 故障组合验证。

最终候选的其余 S3／HA Kerberos HDFS 回归正在执行；网络矩阵采用 5 秒 ClickHouse 读时限的完整复测尚未收尾。
相同模式的性能／RSS 验收、最终 PR 状态及 worktree 清理尚未完成。


## 最终正常链路与完整扫描故障矩阵

Receiver 修复候选 f5ba57d8 的其余正常回归 **101 项通过**。报告目录前缀为
`/work/terrasync-db-refactor-evidence/release-db-refactor-native-final-`：
point 8、markers 17、ordered 14、tar-guards 8、tar-manifest 2、basis 3、checkpoint-source-cut 2、publish-denial 1、qos 8、rm-denial 2，以及 hdfs-single／hdfs-cluster 各 18。
与上述最终目标写 ACK 丢失 4 项分开计数；HDFS 额外 QoS 用例仍不计为 DB 部署模式验证。
成功报告均保留精确二进制来源和临时资源清理结果。

完整 **72 项扫描故障矩阵通过**，报告
`/work/terrasync-db-refactor-evidence/release-db-refactor-without-adapter-scan-fault-v3-20261106-044418/report.json`。
采用真实 S3、Single／Cluster 与真实 callback 接收器：full／incremental／resume／incremental_resume 四种执行，
分别注入源端闪断、持续重置、静默、故障取消，及数据库对应四项和成功写入后丢失 ACK。
每项恢复后回读 372 条实际路径、唯一事实及累计统计；成功终态载荷已独立核对。
静默检测与恢复分开配置：源端 operation 1 秒、DB read 5 秒，识别后 recovery 3 秒。
持续重置从注入到退出约 3 秒；源静默约 4 秒、DB 静默约 8 秒，包含各自检测时限。
报告是注入到完成的观测，不冒充精确识别后计时。未知写结果按有类型的 RequestOutcomeUnconfirmed 保留，不改成已确认取消。
本轮没有宣称 callback 交付重试／补偿的基点限制已修复，也不涵盖 HDFS 网络故障。

## 扫描 HTTP 合批性能修复

500500 条实际 NFS 事实的同配置对照发现新原生扫描的 1024 行请求上限引入约五倍单机全量回归：
主分支约 7.05 秒，未修复候选约 35.29 秒。
首轮 `release-db-refactor-native-performance-20261106-051339` 因该回归中止，不能计作性能验收通过。
原始主分支在同一热缓存 NFS 上也未达到相对无 DB 基线的 5%／10% 目标，不能将其 DB 成绩当作无 DB 基线。

将同一调用批次的原生行合并为最多 8 MiB 拥有载荷软预算／65536 行的请求；超大单行可越过软预算。
Single／Cluster 共用 NativeWriteBatches，调用批次成功边界、代次、checkpoint 与取消协议不变。
快照在重试外编码，重试仍由 SDK 生成 RowBinary；没有宣称完整 HTTP body 只编码一次。
2 项回归先红后绿，覆盖 5000 个小行的一次请求及大事实拆分后的全部快照字节。

候选 `/work/terrasync-db-refactor-request-packing`，SHA-256
`a99e37e1c46cafd81f42bdbffc6e7102986d31227c9bccbee322d50fe19fbcb3`，
来源 `/tmp/terrasync-request-packing-build-source.json`，基于 3791267 加本次两文件修改。
fmt、workspace all-features/all-targets check／Clippy、完整 workspace **539 通过、86 忽略**，串行双进程 E2E **16 通过**。
日志为 `/tmp/terrasync-request-packing-final-{fmt,clippy,tests,e2e}.log` 和 `/tmp/terrasync-request-packing-check.log`。
真实 DB 契约 **75 项通过**，独立库清理成功，报告前缀 `/tmp/terrasync-db-refactor-real-request-packing-v1-`。

实际大批次写入后丢失响应 **Single／Cluster 2 项通过**：
`/work/terrasync-db-refactor-evidence/release-db-refactor-request-packing-large-data-ack-loss-20261106-054953/report.json`。
5004 条事实均准确且无重复，首次与重试的实际数据请求 body hash 及非空去重 token 一致，累计统计无重复。
API 探针精确来源另存 `/tmp/terrasync-request-packing-scan-probe-source.json`。

同数据集初步性能对照报告
`/work/terrasync-db-refactor-evidence/release-db-refactor-request-packing-perf-control-20261106-054815/report.json`：
Single full 主分支 8.49 秒／候选 8.38 秒，incremental0 为 14.31／14.93 秒；五倍回归已消除。
Cluster full 为 23.77／28.70 秒，incremental0 为 28.86／27.77 秒；单轮差异需多轮同模式复核。
峰值候选 Single 约 179 MiB、Cluster 约 269 MiB；该报告 passed 仅表示功能和清理检查通过，不表示性能目标达标。
unordered／blocks、无变更／约 1% 变更、多轮 RSS 以及生产周期续扫成本仍在独立验收中。


## 非特权 CI 夹具修复与合批后恢复验证

e383ddb 的 GitHub Rust CI 在 dc_creates_symlink 失败：成功夹具把 UID／GID 固定为 0，普通 runner 无权 chown 到 root。
以 nobody、TMPDIR=/tmp 执行原测试二进制复现相同 ACK 断言失败（`/tmp/terrasync-unprivileged-symlink-red-v2.log`）。
最初未覆盖 TMPDIR 的尝试仅因 /work/tmp 不可写而失败，不计为该问题复现。
夹具改从真实临时文件取得执行身份，保留链接类型／目标／成功 ACK 断言并增加 UID／GID 检查；生产代码和权限失败语义不变。
同一 21 项 dual_process_streaming 套件在 root 和 nobody 下分别全部通过，fmt／对应测试 Clippy 通过。
证据 `/tmp/terrasync-unprivileged-fixture-{root,unprivileged}-green.log` 及 `...-clippy.log`。
Standards／Spec 复核未发现该测试修复掩盖权限错误。

合批二进制另有 **8 项真实应用强退／重启回归通过**：
Single `/work/terrasync-db-refactor-evidence/release-db-refactor-request-packing-resume-single-1793944386163925136/report.json`；
Cluster `/work/terrasync-db-refactor-evidence/release-db-refactor-request-packing-resume-cluster-1793944510654020615/report.json`。
各 4 项，均独立观察并核实 4 个临时数据库已清理；功能测试缩短周期，不能代替生产至少 60 秒 checkpoint 性能验证。

上游为满足 strict 分支保护合并 main 为 924c2c8；相对 0070c159 仅增加 Direct 中途取消测试及夹具支持，没有运行时代码变化。
新的 GitHub CI 仍在等待，未绕过保护规则；下游 native pin 的运行时代码与该合并头一致。


## 删除旧 schema 升级及完整当前建表契约

上游 PR #185 已于 2026-11-06 合入 main，merge commit 为 `b47b31d660c8e4ee27bf3c0a998d9836332b5d45`。
924c2c8 的 Linux 完整 CI、Windows HDFS compile contract 和 Windows native runtime 均通过。
原始 main 工作区已快进；只删除本任务的 native feature worktree、对应本地／远端分支。
下游固定的 0070c159 是合入历史中的祖先，运行时代码与上游最终树一致。

按照首次开发不保留旧版本兼容的约束，删除 metadata_hash、baseline_committed、statistics 与 snapshot 字段的探测和 ADD COLUMN。
这些升级分支在基点已存在，本轮将其作为 db 完整整理的一部分独立删除。
state 的完整建表定义现在直接包含 copy_in_progress／copy_request_key；默认 baseline_committed=false，首次成功提交才显式置 true。
当前复制 request 绑定、失败代次回标、checkpoint 捕获／恢复、Single 写去重窗口及 readiness 均保留。
准备方法按实际剩余职责改为 prepare_native_writes，Cluster 不保留空升级方法。
旧列升级 fixture 改为真实 fresh schema；复制首次 Fresh→重开 Recovering→提交→不同 request Fresh 的真实回归保留当前恢复要求。

候选 `/work/terrasync-db-refactor-current-schema`，SHA-256
`26169e5082494e54a085c2b80254e7236235ae98a5e3eec3fda73253173c272c`；
来源 `/tmp/terrasync-current-schema-build-source.json`，基于 a88945d 加当前 schema 清理。
最终冻结源指纹为 `/tmp/terrasync-current-schema-build-capture-v3.json`。
fmt、workspace all-features/all-targets check／Clippy、workspace **539 通过、86 忽略**、串行双进程 E2E **16 通过**。
日志 `/tmp/terrasync-current-schema-final-{fmt,check,clippy,tests,e2e}-v3.log`，release 构建日志同阶段 v3。
早期 check 因集成测试未同步方法名而失败；v2 构建期间补齐旧升级 fixture 后指纹检查拒绝采纳，未把它作为最终二进制来源。

真实 DB 契约 **76 项通过**：
- `/tmp/terrasync-db-refactor-real-current-schema-v1-{single,cluster}-catalogue.json`：11＋12。
- `/tmp/terrasync-db-refactor-real-current-schema-v2-single.json`：44。
- 同 v2 single-cuts／cluster-cuts：各 3；single-baseline：2；cluster-fresh：1（六副本的 fresh 列定义）。
临时库与 fixture 清理均通过。最初 v1 Single 的旧补列测试失败不计为通过；替换首次建表契约后完整 44 项通过。
两轴审查未发现本轮清理引入的职责、恢复或身份安全阻断。

## 三轮热缓存 NFS 诊断结果

报告 `/work/terrasync-db-refactor-evidence/release-db-refactor-packing-matrix-20261106-055848/report.json`。
实际 500000 文件＋500 目录，concurrency 8、batch 8192；unordered／blocks 分别三轮，incremental0 及约 1% 增删改均覆盖。
合批候选 a99e37e1 与 bedb632 原主分支对照，86 次运行的实际条目／去重检查全部通过，4 个临时数据库已清理。
**12 个相对无 DB 吞吐比较均未达标**，报告 passed=false、functional_passed=true；不能将功能通过改写为性能通过。

| 部署／模式 | full 主分支→候选秒 | incremental0 主分支→候选秒 | incremental1 主分支→候选秒 |
| --- | --- | --- | --- |
| Single unordered | 6.93→7.74 | 14.00→14.45 | 14.15→14.41 |
| Single blocks | 7.03→7.79 | 12.35→13.15 | 12.80→13.35 |
| Cluster unordered | 25.08→27.14 | 28.34→29.19 | 21.97→24.53 |
| Cluster blocks | 28.45→27.44 | 28.99→27.84 | 20.67→20.72 |

同候选无 DB 的两个模式中位数均约 1.017 秒。Single full 相对旧 DB 仍增加约 11%，Cluster unordered full 约 8%，unordered incremental1 约 12%；需要继续处理／解释，不作达标结论。
本轮峰值 RSS：原主分支 Single 240.4／Cluster 291.0 MiB，候选 Single 252.9／Cluster 333.0 MiB。
前／后段 p90 的运行内差值中位数均为负；这仅说明本轮采样未见持续单调增长，不证明任意规模无泄漏。
约 200 MiB 目标未达。当前 schema 清理候选的生产至少 60 秒 checkpoint 恢复成本另行验证，不与该热缓存诊断混为同一性能 profile。


## 去重能力归入建表契约

Single 的去重窗口通过完整建表定义一次确定：catalogue 两种 MergeTree 引擎、resume manifest 和 stage 共用 SINGLE_WRITE_SETTINGS。
删除重复 ALTER、公开 prepare 协议、app 两处准备调用和 readiness AtomicBool。
仅五处在重试外生成稳定 token 的 INSERT 使用 IdempotentWrite；Memory 操作、INSERT SELECT 和普通 SQL 的安全分类未放宽。
普通连接仍默认 deduplicate_insert=disable，新 token 表达新的写入意图，不依赖内容相同来抑制正常写入。

实际 owned state 建表回归先红后绿：旧 DDL 的同 token 重发产生 2 个物理行；新 DDL 为 1 个物理行，新 token 的相同内容产生第二行。
普通 0→1→0 写入仍完整可见。证据 `/tmp/terrasync-ddl-window-{red,green}.log`。
真实 fresh Single 五表均检查窗口；恢复 checkpoint 后检查 working 表仍有该设置。
恢复只 DROP／ATTACH working 分区，没有重建其 DDL；不把快照设置继承当作已测试的重试前提。

候选 `/work/terrasync-db-refactor-ddl-window`，SHA-256
`9f0c14a70d76ac98aa3c9634cca40cd78e851dc9b243dc491cd2cfcaa3b4f39f`；
来源 `/tmp/terrasync-ddl-window-build-source.json`，基于 9508426 加本轮修改。
fmt、workspace all-features/all-targets check／Clippy、**539 项 workspace＋16 项双进程 E2E** 均通过，86 忽略项和既有警告另计。
日志 `/tmp/terrasync-ddl-window-final-{fmt,check,clippy,tests,e2e}.log`。
真实 DB 契约 **76 项通过**，报告 `/tmp/terrasync-db-refactor-real-ddl-window-v1-{suite}.json`；所有临时库清理成功。
两轴审查通过；未把旧表的零窗口配置列为支持的恢复输入，开发旧表需清理重建。
本轮 API 故障探针 `/work/terrasync-db-refactor-scan-probe-ddl-window`，SHA-256
`3bc173026b2dad419495fe33e286b97c66f07ef4d874b4aef5a75eb9dce33d8c`；精确驱动／manifest／应用来源在 `/tmp/terrasync-ddl-window-scan-probe-source.json`。

## 生产周期恢复诊断与性能验收数据源调整

schema 清理候选 26169e50 在旧 Linux NFS 的受控延迟 profile 完成 **6 项生产周期强退恢复**，报告
`/work/terrasync-db-refactor-evidence/release-db-refactor-production-resume-20261106-062302/report.json`。
200000 文件＋200 目录，READDIR 额外 0.5 秒、concurrency 1；无 DB blocks 为 105.78 秒。
每项在至少 60 秒实际 checkpoint 后强退并从新工作目录恢复；最终事实、唯一条目和累计统计均为 200200，临时库已清理。
Single full／incremental0／incremental1 总活动时间 107.03／107.30／106.49 秒，吞吐损失 1.17%／1.41%／0.67%。
Cluster 对应 121.90／122.38／120.00 秒，损失 13.22%／13.56%／11.85%，均未达到其目标。
报告 functional_passed=true、passed=false，不能把整套写为性能通过。

用户随后明确性能数据源使用整个 `nfs://10.131.5.221/m1-source`，功能回归可使用小目录。
上述旧 share、受控延迟以及已取消的旧 share 新候选基准仅保留为诊断，不作为指定 NAS 的最终验收。
指定 NAS 的原生 NFSv3 只读连通探测通过；v4.1 返回 RPC service unavailable，不计通过。
首轮 whole-share 只读校准触及 harness 的 900 秒预算，未完成，不作为已完成的无 DB 基线。
已保留明确失败记录；源数据没有改动，后续长时基准按用户指定的四种 Cluster 场景执行，并分别匹配 unordered／blocks 无 DB 基线。
