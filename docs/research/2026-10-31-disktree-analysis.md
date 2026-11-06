> 更新：下文提及的 `project_pure_scan`／`ObservedEntryRecord` 独立原型已在原生观察重构中移除；历史分析不代表当前任务入口。当前方案见 [原生观察重构](../specs/2026-11-05-db-native-refactor-design.md)。

# disktree 调研与 TerraSync GUI 数据分析功能建议

调研日期：2026-10-31。外部源码固定在 [`tobi/disktree@9b18b272`](https://github.com/tobi/disktree/tree/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea)，工作区版本号为 0.11.0。以下外部项目结论来自源码静态检查；没有运行其桌面程序或做性能基准。TerraSync 对照和实现建议见后续章节。

**建议结论：值得实现。** 首版优先做可下钻的目录矩形树图、容量/数量切换、大小榜单、修改年龄分析、筛选和选择导出；随后把选择转为迁移任务草稿。沿用 Vue 与 ECharts，补后端目录聚合和执行快照身份。历史对比需先增加结果保留能力；物理占用、释放量与自动清理暂不作为首版承诺。

## 1. disktree 实际解决的问题

disktree 是围绕“发现占用 → 逐层定位 → 标记 → 复核 → 清理”的磁盘探索工具。它适合借鉴的是从数据可视化走到下一步操作的完整交互，而不只是矩形树图的外观。它的三个主要组成部分为：

| 模块 | 实际职责 | 对 TerraSync 的参考意义 |
| --- | --- | --- |
| `disktree-core` | 扫描、树模型、空间统计、布局、筛选、分类、清理规则，不依赖 UI | 分离统计口径、聚合查询与图表展示 |
| `disktree-app` | GPUI 桌面界面、选区、导航历史、标记与复核状态 | 借鉴交互和状态管理，不能直接嵌进 Vue GUI |
| `disktree-scan` | 无 GUI 扫描器，SSH 场景输出元数据流 | 说明远端扫描和本地展示可以解耦；不代表 TerraSync 必须增加 SSH |

来源：[工作区配置](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/Cargo.toml#L1-L14)、[core 边界](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/lib.rs#L1-L31)、[桌面依赖](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/Cargo.toml#L16-L23)。

## 2. 值得学习的图表与交互

1. **嵌套矩形树图和逐层导航。** 面积表达相对规模，目录保留标题带，帮助用户区分层级并选中父目录；底层使用 squarified treemap。默认绘制 3 层，每个目录保留 96 个主要子项，长尾合并成 `Others`。小于最小宽高的块不绘制，因此完整数字仍需由详情或列表承接。缩放通常复用布局坐标，仅树、视口或深度变化时重算。[布局实现](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/treemap.rs#L1-L11)、[布局限制与长尾](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/treemap.rs#L105-L137)、[分配与标题带](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/treemap.rs#L223-L278)。

2. **区分“面积指标”和“颜色指标”。** Size 按字节分配面积，Files 按文件数量分配面积；Age 依然按字节分配面积，只把颜色切换成修改时间年龄。年龄档位为 7、30、182、365 天及更早。它没有把“年龄”作为可以相加的面积指标。[模式切换](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/state.rs#L2528-L2549)、[年龄色阶](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/palette.rs#L66-L81)。

3. **筛选时保持统计含义。** 名称筛选忽略 ASCII 大小写；匹配目录时保留整棵子树，祖先只按匹配后代的字节和数量重新加权；不会把一个匹配父目录及其匹配后代重复计入命中总量。这一点适合用于 TerraSync 的目录前缀和条件筛选。[筛选模型](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/filter.rs#L1-L35)、[筛选计算](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/filter.rs#L75-L120)。

4. **标记与执行分离。** 先标记再进入复核页；选中父目录会吸收内部标记，父目录已选时不能再单独选内部路径，避免重复统计。标记以路径存储，绘制定位则使用树下标，重扫后重新解析。远程路径的实际身份还包含远程来源，不能只凭一个绝对路径认定是同一个对象。[标记语义](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/state.rs#L2101-L2157)、[标记存储](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/marks.rs#L1-L18)。

## 3. 数据口径：移植前必须明确的边界

- **逻辑大小和占用空间是不同指标。** 默认测量分配块；Unix 分支使用 `st_blocks × 512`，逻辑长度为另一选项。默认包含隐藏文件，不跟随符号链接，不穿越嵌套挂载点。文件大小不能直接当作对象存储的真实物理占用、账单大小或迁移后的目标占用。[扫描选项](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/scan.rs#L38-L80)、[分配块测量](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/scan.rs#L1244-L1274)。
- **硬链接去重是空间统计，不是内容去重。** 节点保存文件身份，聚合时把重复身份的字节置零；文件数量仍按文件条目累计。哪个目录承担硬链接的字节可能随并行遍历顺序变化。因此“文件数”“唯一文件数”“路径数”不能混用，也不能把该规则直接套到迁移数据量。[节点定义](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/tree.rs#L65-L98)、[去重聚合](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/tree.rs#L279-L284)、[叶子与目录处理](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/tree.rs#L358-L400)。
- **Age 表达最后修改，不能推导最后访问。** 目录的时间值取后代修改时间最大值。一个多年未修改但每天读取的数据库备份仍可能显示“旧”；因此只能称为“长期未修改”，不能直接称为“无用数据”或“可删除数据”。这是根据聚合实现作出的产品语义判断。[时间聚合](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/tree.rs#L377-L400)。
- **回收空间是受条件约束的估计。** disktree 的清理计划只向根所在卷计入可归属该卷的字节，其他卷与来源不明字节另列；README 也明确 APFS 克隆和本地快照会造成统计与实际释放不同。对 TerraSync，建议首先显示“选中逻辑字节数”，再根据协议能力决定是否展示占用和容量，不能承诺同等释放量。[卷归属统计](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/removal.rs#L70-L112)、[macOS 限制](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/README.md#L103-L113)。

## 4. “建议清理”并不是通用智能分析

分类器依靠目录名称、父类别和相邻文件推断。例如 `target` 旁有 `Cargo.toml` 才标为构建产物，`node_modules` 旁有 `package.json` 才标为可重装依赖。它还包含大量开发者个人目录约定，因此更适合个人开发机器，直接用于企业共享盘、NAS 或对象桶会误判。[分类规则](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/classify.rs#L129-L223)。

`Worth a look` 实际仅包含规则识别的可回收目录、agent worktrees、超过 30 天未修改的 experiments/tries；低于 64 MiB 不显示，按大小排序并截取指定数量。它会避开父子候选重复累加。这是可解释的启发式候选清单，没有证明数据已备份或业务不再需要。TerraSync 值得借用“候选 + 原因 + 数量/大小 + 可复核路径”，但规则应改为用户配置或明确业务条件。[候选实现](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/insights.rs#L1-L128)。

## 5. 扫描反馈和规模上的取舍

本地扫描采用 rayon 目录任务和完成计数，子目录完成后向父目录提交节点，最后自底向上聚合。进度只维护当前计数、取消状态和有上限的错误详情；没有已知总量时显示累计值，而不猜测完成百分比。扫描中的预览使用最多 128 个顶层桶和一个溢出桶，不维护另一份逐文件树；预览只是已观察贡献，不能当最终目录大小。GUI 对预览按约一秒节流。值得学习的是“有限状态、有限更新、临时结果明确标识”。[扫描结构](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/scan.rs#L1-L16)、[进度结构](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/scan.rs#L84-L124)、[预览桶](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/scan_preview.rs#L1-L64)、[GUI 节流](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/state.rs#L1382-L1400)。

**不要照搬完整内存树到浏览器。** 当前 `Node` 的结构体大小测试固定为 88 字节，尚未计入名字、子数组等堆分配；一千万节点仅结构体本体就约 0.88 GB。SSH Unix helper 通过深度优先流式输出使辅助端内存随打开祖先增长，但桌面接收端仍保留全树，整体不是常量内存方案。TerraSync 更适合后端持有明细或聚合结果、前端按目录按层获取；这是架构建议，不是 disktree 已实现的 Web 功能。[节点大小测试](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/tree.rs#L436-L446)、[远程流式遍历](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/remote_walk.rs#L1-L20)。

## 6. 不适合直接迁入 TerraSync 的部分

- GPUI/Omarchy 桌面栈、系统主题和本机文件管理器交互；现有 Web GUI 应保持自身组件体系。
- 把桌面内存树和布局代码整套搬进浏览器；优先借鉴数据语义和交互。
- 把路径名规则判定为企业数据可删除依据。
- 第一版便复制清理器。disktree 的本地清理有独立保护与复核机制；SSH 模式在执行入口直接禁用清理，只能导出选项。TerraSync 更适合先把选区交给既有迁移任务或导出清单，再单独设计清理流程。[本地清理机制](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-core/src/removal.rs#L1-L16)、[远程执行入口](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/crates/disktree-app/src/state.rs#L2554-L2567)。

以上为外部项目的已验证事实与借鉴边界。以下结合 TerraSync 实际实现给出建议。


## 7. TerraSync 当前实现与可复用基础

以下按本地提交 `65bcbbcc29a3ba414f3ef83a5295fefb13df5f17` 分析；这些是源码核查结论，尚未运行 GUI、扫描基准或真实存储测试。

| 能力 | 当前事实 | 对新功能的影响 |
| --- | --- | --- |
| GUI 与图表 | Vue 3、Naive UI、ECharts 6、vue-echarts；目前注册 Pie/Bar 与 CanvasRenderer | 增加 TreemapChart 即可复用图表基础，不需要引入 GPUI |
| 已有分析 | 文件类型、文件大小、时间分布，容量/数量切换；扫描期间显示文件数、目录数、字节、错误和耗时 | 这些应增强和联动，避免重复做一套概览 |
| 分析接口 | `GET /api/v1/tasks/{id}/analytics` 从 SQLite 的 `task_progress.report_json` 解析 FinalStats | 只有汇总桶，没有路径和父子关系，无法从这些数据还原目录树 |
| 明细数据 | ClickHouse 扫描表已有 relative_path/size 等列；同表也容纳 ObservedEntryRecord 查询投影 | 可作为分析明细基础，但需明确当前扫描入口实际写入的是哪种记录 |
| 任务流程 | 已有源端点、源路径、目标、过滤条件、任务配置和执行记录 | 很适合把分析选择变成迁移任务草稿 |
| 重扫与历史 | GUI 纯 scan 会先删除旧作业目录及扫描表，再调用 `app::scan::scan` | 当前不能承诺旧明细可回看、失败后保留上次完整结果或跨次目录对比 |

源码依据：
- [前端依赖](../../web-ui/package.json)、[图表注册](../../web-ui/src/composables/useEChartsLoader.ts)、[当前分析界面](../../web-ui/src/components/ScanAnalysis.vue)、[图表选项](../../web-ui/src/composables/useAnalyticsCharts.ts)。
- [AnalyticsService](../../crates/web/src/application/analytics_service.rs)、[分析接口](../../crates/web/src/api/analytics_handler.rs)、[FinalStats](../../crates/app/src/consumer/stats/report.rs)。
- [ClickHouse 表结构与发布机制](../../crates/db/src/clickhouse.rs)、[中立观测行](../../crates/db/src/observation_record.rs)、[GUI task runner](../../crates/web/src/infrastructure/task_runner.rs)。

### 7.1 应先解决的数据语义

1. **逻辑容量与物理占用分开。** 当前通用 Entry 的 size 及观测投影没有可直接用于跨后端统计的 allocated_bytes 列；NAS 投影目前也把 hard_links/ino 置为 None。因此第一版应标注“扫描到的逻辑数据量”，不能叫“实际磁盘占用”或“删除后可释放容量”。跨协议去重、压缩、稀疏、快照和存储侧计费无法从 size 推导。[Entry 定义](../../crates/storage-adapter/src/entry.rs)、[投影实现](../../crates/storage-adapter/src/projection.rs)。
2. **修改年龄不等于访问冷度。** 通用投影可用修改时间；访问时间的可得性和含义随协议变化。年龄图应叫“距上次修改”，用于发现归档候选。未知时间单独分桶，不把零值当作很久未访问；按扫描时刻固定年龄基准。
3. **观测范围不等于整个端点。** 每次分析必须标明端点身份、扫描根、深度限制、match/exclude、版本范围、时间和错误数。经过过滤或限制深度的结果不能标成全盘总量。
4. **S3 版本要有明确口径。** 当前记录有 version_id 等字段，但不能据此假设当前扫描保留了所有“是否最新/删除标记”信息。先核实写入路径，再决定是否能分别显示当前对象与历史版本；不要把多个版本相加却标成当前文件数。
5. **目录汇总必须守恒。** 父目录容量是后代文件容量的聚合，不能再重复加入父目录的自身 size；同级子目录、当前目录直属文件以及“其他”之和应等于当前范围的总量。S3 需要按 key 的前缀构造虚拟目录，考虑没有目录记录的情况。

### 7.2 现有分析中应顺带修正的口径问题

- 后端 TimeRangeStatEntry 已包含 total_size，但 Web TimeEntry 只输出 count。补出容量即可支持“旧数据占多少容量”，不需要重新扫描。[报告类型](../../crates/app/src/consumer/stats/report.rs)、[接口转换](../../crates/web/src/application/analytics_service.rs)。
- FullStats 当前时间桶更新包含目录和符号链接，而饼图 tooltip 写“文件数”。应统一统计对象，或明确写成条目数；年龄着色也应避免用一个最新修改的子文件代表整个大目录。[聚合实现](../../crates/app/src/consumer/stats/accumulator.rs)、[前端图表](../../web-ui/src/composables/useAnalyticsCharts.ts)。
- 无扩展名文件当前不会进入 extension_stats；不能用扩展名桶容量的合计作为所有文件的可靠总量。应补无扩展名桶，并明确分布图分母。[聚合实现](../../crates/app/src/consumer/stats/accumulator.rs)。
- 时间桶从 HashMap 转为 Vec 时未显式按年龄排序，前端却按数组位置配暖冷色。应为桶增加稳定 key/顺序。
- GUI 选择历史执行时摘要取自该 execution，分布图仍从 task 级接口获取最近的 FinalStats，存在串用不同执行数据的风险。新分析接口应绑定 execution_id/snapshot_id；现有图表也应一起调整。[任务详情逻辑](../../web-ui/src/composables/useTaskDetail.ts)、[进度存储](../../crates/web/src/infrastructure/progress_repo.rs)、[执行统计](../../crates/web/src/domain/execution.rs)。

## 8. 哪些功能值得实现

本节是针对 TerraSync 的产品与实现建议，不是 disktree 已具备的功能清单。

| 优先级 | 功能 | 在 TerraSync 中的实际价值 | 建议范围 |
| --- | --- | --- | --- |
| P0 | 目录矩形树图 + 面包屑下钻 | 快速回答“哪个业务目录占容量”，把概览落实到路径 | 单击选择显示详情，双击/明确按钮进入；返回上级；与列表保持同步 |
| P0 | 按逻辑容量 / 文件数量切换面积 | 同时找出大文件集中区和海量小文件目录，帮助评估迁移负载 | 文件数口径明确；零字节文件通过数量模式及列表可见 |
| P0 | 最大目录 / 最大文件榜单 | 数值比较、精确选择比仅看面积更方便 | Top N、容量、占当前范围比例、路径、修改时间；完整结果分页 |
| P0 | 名称、路径、类型、大小、修改年龄筛选 | 找出“某目录内超过阈值且多年未修改的数据” | 先筛选再聚合；所有图和列表用同一过滤范围，显示筛选后总量 |
| P0 | 修改年龄分布与着色 | 发现归档候选数据，补足现有年龄图只有数量的不足 | 容量/数量双口径；目录详情展示年龄分布；不直接称为冷数据 |
| P0 | 选中路径清单、去重、导出 | 把发现的范围交给后续处理和评估 | 父目录包含已选子项时避免重复计数；导出带端点/根/快照身份的 CSV/JSON |
| P1 | 从选择创建迁移 / 校验任务草稿 | 最能体现 TerraSync 自身价值的功能 | 单目录先复用路径配置；复杂筛选复用条件构建器；任意离散文件集合需新增 manifest 能力 |
| P1 | 小文件密度和迁移准备度 | 找出容量不大但文件数很多、可能拖慢迁移的目录 | 小文件数与占比、平均大小、路径深度；只给候选提示，不凭均值预测准确耗时 |
| P1 | 扫描中一级目录预览 | 长扫描时及早知道数据分布与扫描活动 | 有界桶、限频、显示“已观测”；不伪造百分比，不把部分结果作为完整快照 |
| P2 | 两次完整扫描的容量变化 / 增长榜单 | 长期容量治理和迁移效果回看 | 先保留不可变快照并绑定执行；同源、同根、同规则才适合直接比较 |
| P2 | 可配置业务分类、S3 版本分析 | 按业务目录/数据类型制定迁移与归档策略 | 根据真实使用需求扩展；规则可解释，版本数据先核实可得性 |

P0 表示首版值得覆盖的核心范围；其中“完成态、范围与执行身份”是所有图表共同的前置条件。P1/P2 的先后应按用户实际任务频率调整。

### 8.1 暂不建议照搬

- **GPUI 界面与桌面集成。** 当前 GUI 是 Web，直接沿用 Vue/Naive UI/ECharts；Linux 主题、终端打开、本地文件管理器和大量桌面快捷键的投入回报有限。浏览器可能运行在另一台机器上，不能把服务端路径当成浏览器本机路径。
- **disktree 的开发者目录分类规则。** target、node_modules、工具链或 agent 临时目录规则对个人开发机有用，对企业 NAS/S3 容量分析不足。先按类型、年龄、业务路径分析，再考虑用户自定义规则。
- **一键清理和可释放空间预测。** 首版优先生成迁移任务草稿或导出选择清单；缺少各后端物理容量和数据保护信息时不做释放量承诺。
- **复制其本地扫描器、SSH helper 与 NTFS 快速扫描。** TerraSync 已有存储适配和遍历层，增加第二套扫描链会使协议、过滤和元数据语义分叉。
- **把整棵文件树发到浏览器。** disktree 桌面端全树驻留内存；TerraSync 需按目录查询聚合和分页明细，单屏渲染量保持有界。
- **仅凭大小/时间判断重复文件。** 这不能证明内容相同；若以后实现内容去重分析，应独立设计哈希采集、读流量和一致性成本。

## 9. 推荐页面形态

入口建议放在“扫描任务 → 数据分析”，同一端点有多个根目录或过滤任务时，由任务确定分析范围；以后可在端点页增加跳转入口。先以 scan 任务为主，sync/integrity_check 的操作统计不能未经验证就当作完整源端数据清单。

```text
端点 / 扫描根 / 执行批次 / 扫描时间 / 完成与范围提示
[总逻辑容量] [文件数] [目录数] [未知元数据] [错误数]
[路径面包屑] [容量/数量] [按类型/修改年龄着色] [筛选]

┌─────────────────────────┬──────────────────────┐
│                         │ 当前选择详情         │
│       目录矩形树图       │ 路径、容量、占比     │
│                         │ 文件数、年龄分布     │
│                         │ [加入选择]           │
├─────────────────────────┴──────────────────────┤
│ 子目录/文件列表：排序、筛选、分页、选择           │
├────────────────────────────────────────────────┤
│ 已选：N 项，逻辑容量 X  [导出] [创建迁移草稿]   │
└────────────────────────────────────────────────┘
```

类型颜色与年龄颜色分成两种模式；选中状态用边框或明确标记，避免颜色同时承担类型、风险和选择三个含义。树图之外保留可键盘操作的列表，长路径提供完整值，零字节与未知大小对象也能在列表中发现。

## 10. 推荐技术方案

### 10.1 沿用现有 Web 架构，补分析查询层

```mermaid
flowchart LR
    A[现有扫描与存储适配] --> B[扫描明细与结果完整性]
    B --> C[按执行绑定的分析快照]
    C --> D[目录聚合与分页明细查询]
    D --> E[Vue + ECharts Treemap]
    E --> F[选择清单与任务草稿]
```

建议新增 AnalyticsRepository/AnalysisSnapshot 的领域接口，让 application service 描述“按快照查询目录、明细和分布”，ClickHouse 实现负责 SQL 与旧/新行格式兼容。前端不直接解析数据库表，也不反序列化 opaque entry_snapshot。已有 ObservationRecord 的 relative_path、entry_kind、size/size_observed、modified_unix_nanos 可作为中立基础；已有旧行则由查询层转换为同一 DTO，避免把两种行直接混算。

ECharts 6 已支持 treemap 的分层数据、面包屑、缩放、leafDepth、可见面积阈值等，可以复用当前 CanvasRenderer。其 treemap 源码默认关闭 progressive，因此不能指望开启通用渐进渲染选项解决全树数据量问题；后端裁剪与按需查询仍是关键。[ECharts 6 Treemap 源码](https://github.com/apache/echarts/blob/6.0.0/src/chart/treemap/TreemapSeries.ts)、[Canvas/SVG 官方说明](https://echarts.apache.org/handbook/en/best-practices/canvas-vs-svg/)。

### 10.2 查询粒度与聚合

- 每次只取当前目录的直接孩子，展示一层或少量预取层；首屏可从 100–200 个可见项开始验证，这是建议预算，不是已测得的性能上限。
- 超限项归入“其他”，保留完整容量/数量与独立明细入口；“其他”是聚合桶，不能当成真实路径创建任务。
- Top N 限制返回量并不等于限制数据库计算成本。先用实际规模测量按路径前缀聚合，再决定是否在成功扫描后构建目录汇总表；不要先为每个文件的每一级祖先复制一条数据。
- 大目录列表使用游标分页；索引或汇总表应围绕 snapshot_id + parent_path + 排序键设计。缓存键包含快照、目录、过滤条件和指标。
- 保留 UInt64 精确计量；API 需要约定大整数表示（例如十进制字符串）。图表使用近似数作几何布局，详情和导出使用精确值。

### 10.3 执行与快照身份

新增分析契约至少应包含：

```text
AnalysisSnapshot:
  snapshot_id, execution_id, task_id
  endpoint_identity, root, scan_started_at, scan_finished_at
  depth_limit, match_expr, exclude_expr, version_scope
  status, metadata_availability, error_count

DirectorySummary:
  path, name, kind
  logical_bytes, file_count, directory_count
  unknown_size_count, unknown_time_count
  modified_age_buckets, has_children
```

snapshot_id 应是不会重用的结果版本；现有 scan generation 是交替状态位，不能充当历史快照 ID。

新 API 的示意职责为 snapshot summary、directory children、entries、distributions 和 selection export，全部显式指定 snapshot_id。是否保留每次完整明细可分阶段：首版至少只发布已验证完成的结果；如果只保留最新明细，就明确旧执行仅有概要，不能用最新树图冒充旧树图。要支持历史下钻，再引入保留策略和空间预算。

当前 `project_pure_scan` 已有 staging、完整性核验与原子发布逻辑可借鉴，但 GUI runner 尚未通过该入口。设计文档标注 Implemented 不代表当前 GUI 已获得这套保证；实际接入与失败传播必须验证。[当前原生扫描实现](../../crates/db/src/scan/mod.rs)、[相关设计](../specs/2026-08-31-pure-observation-scan-design.md)、[GUI 入口](../../crates/web/src/infrastructure/task_runner.rs)。

### 10.4 选择如何进入后续任务

- 第一阶段先实现单目录创建草稿，以及带来源身份的路径清单导出。
- 路径前缀必须按目录边界匹配，避免选择 `a/b` 却包含 `a/b2`。S3 key 使用其自身路径语义；不能直接套宿主系统的 Path 规范化。
- “选中目录”与“目录内满足筛选的文件”语义不同。创建草稿时必须保留筛选条件、扫描根与来源说明，并预览实际执行范围。
- 任意多选集合不等于当前 TaskConfig 已支持文件清单执行。若用过滤表达式近似表达，必须验证语义一致；更完整的方案是新增 manifest 输入及任务执行前校验。
- “归档”可先体现为迁移到用户指定的目标存储；S3 存储类别切换、生命周期策略和迁移后删除源端属于另外的产品能力。

## 11. 分阶段交付与验收

| 阶段 | 交付物 | 验收重点 |
| --- | --- | --- |
| 第一阶段：数据基础与只读分析 | 固定执行/快照身份、完整性标记、目录聚合 API、Treemap、榜单、容量/数量/年龄、筛选与导出 | 所有组件同一范围；聚合守恒；失败或取消不显示为完整结果；大目录有界返回 |
| 第二阶段：分析进入任务 | 单目录与规则选择创建迁移草稿、小文件分析、预览与清单能力 | 父子选择不重复；路径边界正确；生成任务范围与分析选择一致 |
| 第三阶段：长期治理 | 历史快照保留、目录增长榜单、同范围对比、自定义分类、版本分析 | 比较口径一致；快照保留和删除可控；解释新增/减少量的来源 |

有价值的验证场景包括：空目录、零字节文件、无扩展名、直属大文件、百万孩子目录、深层路径、未知大小/时间、符号链接、硬链接、S3 虚拟前缀与多版本、筛选下父子目录、取消/错误/数据库写失败、旧执行与最新执行切换。性能应使用 TerraSync 的真实目标规模测试后确定，不沿用 disktree 桌面截图推断。

**首版建议：把“在哪些目录、什么数据、多少容量/文件、多久未修改、能否形成迁移范围”做完整。** 最具产品价值的后续闭环是“分析 → 选择 → 迁移或归档任务草稿”；历史治理和空间清理可分别评估后续需求。
