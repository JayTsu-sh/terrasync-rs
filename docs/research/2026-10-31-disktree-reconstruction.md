# disktree 数据分析 GUI 复刻记录

日期：2026-10-31。目标：复刻 disktree 的目录探索、面积指标、修改年龄着色和标记复核交互，同时遵循 TerraSync 当前 Vue / Naive UI / Tailwind 的蓝色、灰白卡片和表格体系。

## 证据与边界

| 证据 | 实际观测 | 范围 |
| --- | --- | --- |
| REA `inspect_artifact`：`ev_fae04f51dd232c89f51ba6553bb9c3517d230e7b06da9db8ef91ecd673852ee2` | disktree v0.11.0 macOS 发布 ZIP 内含原生 Mach-O 程序、应用图标与配置，未发现可直接复用的 Web 页面资源 | 静态发布包清单，不证明 GUI 运行行为 |
| REA `capture_process_scenario`：`ev_f40e8e258cc11099f1a7178062ebd36460d906e9a92966f8108659239933bfdf` | 实际运行 Linux 发布程序 `--help`，退出码 0；确认 treemap、文件数模式、目录层级，以及选择、进入和复核的操作说明 | 只执行 CLI 帮助，没有宣称运行过原程序的桌面交互 |
| [官方截图](https://github.com/tobi/disktree/blob/9b18b272ca3c5a145f6ff58eaf89dc4d311eeaea/assets/screenshot.png)与[此前源码调研](2026-10-31-disktree-analysis.md) | 左侧大面积树图、右侧选择详情和标记、上方导航与模式；Age 面积仍表示容量 | 截图与静态代码证据 |
| REA `inspect_web_page`：`ev_673d170a9829690bc6d3c32dd49f6e5224944eb9272a0563a32b28d089c62ba2` | 观测实现后的 TerraSync 分析页面、可访问性结构与页面状态 | Headless Chrome，回环地址，使用明确的测试 API fixture |
| REA `capture_web_screenshot`：`ev_26b12b7d32cebc1e3bda1c015f47421cc4b6f4f209162cd76fd0fd41ca770231` | 捕获下方界面截图 | 仅当前可见视口，不是生产扫描结果 |

macOS 发布 ZIP SHA-256：`16ddba6c3d983651cb6389d1c7fd6a070ba443aafcf03c2bc857e35e73b43f4d`。实际执行的 Linux 程序 SHA-256：`9abea1be419d923981d007ff2231a6f9b894ab63263054d223c9be219f9b120b`。下载后均核对发布方校验文件。

完整 REA 证据记录保存在本次工作环境的 `/work/terrasync-gui-validation/`。这次交付是依据上述证据进行的 Web 交互重建，不声明桌面程序与 Web 版本的全局行为等价。

## 实现结果

主入口为左侧“数据分析”菜单，独立页面 `/analysis` 自动选择最近完成的扫描结果，支持 `?task=<id>` 指定任务。已完成扫描的端点详情也直接展示目录树图；任务详情页仅保留“概览”和“运行记录”，端点的分析按钮跳转到独立页面 `/analysis?task=<id>`。

| 功能 | 行为 |
| --- | --- |
| 目录探索 | 面包屑、上一级、历史前进/返回、刷新；单击查看详情，双击目录或点“进入”按需加载 |
| 树图 | 面积按逻辑容量或文件数；修改年龄模式面积仍按容量，颜色按后代最近修改时间 |
| 当前详情 | 默认显示当前目录概览；选择条目后显示容量、文件数、目录数、占比和路径 |
| 条目列表 | 排序、每页 10 条、键盘选择，零字节条目仍可见 |
| 本层搜索 | 只筛选当前返回项，明确显示匹配范围，不改变顶部总量 |
| 标记 | 选中父目录吸收子项；避免 `a/b` 与 `a/b2` 前缀混淆；“其他”桶不能标记或进入 |
| 复核和导出 | 复核选择后导出 JSON，保留任务、扫描范围、时间、完整性与逻辑容量口径 |
| 状态 | 加载、失败、结果互斥；请求竞态保护；切换任务清空旧选择；重新扫描时不挂载旧目录分析 |

前端主要文件：[StorageExplorer.vue](../../web-ui/src/components/StorageExplorer.vue)、[StorageTreemap.vue](../../web-ui/src/components/StorageTreemap.vue)、[useStorageExplorer.ts](../../web-ui/src/composables/useStorageExplorer.ts)。

后端新增 `GET /api/v1/tasks/{id}/analysis/tree?path=&metric=size|count&limit=120`。由任务 ID 确定扫描表，路径单独绑定，ClickHouse 聚合当前目录直接孩子的后代数据；最多 500 个主要条目，超限部分在数据库内合并为“其他”。不把整棵文件树拉入浏览器或 Web 服务内存。

后端主要文件：[领域与接口契约](../../crates/web/src/domain/analysis.rs)、[分析服务](../../crates/web/src/application/storage_analysis_service.rs)、[ClickHouse 查询](../../crates/web/src/infrastructure/analysis_repo.rs)。

## 验证

- `cd web-ui && npm run test`：144 项通过。
- `cd web-ui && npm run build`：类型检查与生产构建通过。
- `cd web-ui && npm run test:e2e -- storage-analysis.spec.ts task-detail-flow.spec.ts navigation.spec.ts`：20 项通过。
- 浏览器验证实际 Canvas 绘制、目录进入与返回、数量模式请求、标记复核与 JSON 下载、端点入口、接口失败、运行中不查询未完成明细。
- 浏览器测试先发现 ECharts 会为没有业务条目的虚拟根请求标签，修复后完成实际树图渲染；没有仅用图表 stub 的单元测试代替浏览器验证。
- 补充对象存储同名对象与虚拟目录的回归测试：路径相同但类型不同的条目保持独立身份、独立标记，目录仍包含其后代路径。

- Rust 后端：6 项通过、0 失败、0 忽略，包括 SQLite 任务范围、路径预算、结果解码和真实 ClickHouse SQL。最后一轮直接运行本次源码构建出的 Rust 测试程序，耗时 70.42 秒。
- 真实 SQL 测试覆盖 13 组场景：旧/新列兼容、FINAL 去重、目录容量不重复、零字节文件、Unicode、Windows 分隔符、S3 虚拟目录、容量/数量 Top N 与“其他”守恒；另单独验证非空中立投影的条目类型、未知大小与纳秒修改时间。
- 本机真实 SQL 使用临时 QEMU 和 ClickHouse 26.10，全部数据为合成 fixture，不依赖生产数据库。可在具备 ClickHouse local 的环境中重现：`CLICKHOUSE_LOCAL=/path/to/clickhouse-local SKIP_FRONTEND_BUILD=1 cargo test -p web --lib analysis -- --include-ignored`。该测试默认忽略，普通单元测试不要求安装 ClickHouse。
- `cargo fmt --all -- --check` 与 `git diff --check` 通过。未执行真实 NAS/S3 扫描或大规模性能基准。

## 使用范围

- 当前只展示任务最新扫描明细，不提供历史快照下钻。
- 显示逻辑容量，不承诺物理占用、计费或释放空间；修改年龄不代表访问频率。
- 当前按目录逐层加载，没有一次性加载全盘多层树；“其他”是汇总而非真实路径。
- 产品代码没有演示数据或回退为假数据的逻辑。浏览器验证使用的 fixture 仅在 `e2e/fixtures/`。
- 已实现选择清单导出；迁移草稿、业务清理规则与删除操作仍是后续独立能力。

## 入口可见性修正

用户反馈普通浏览路径看不到变化后，使用真实运行服务上的浏览器脚本复现：主菜单没有分析入口，端点详情仍以原有统计组件为主。新接口与带查询参数的任务标签已经可用，因此问题在入口接入，而非扫描数据或 ClickHouse 配置。

修正后增加独立导航、扫描结果选择页和明确空态，并在完成扫描的端点详情直接挂载目录探索组件。保留用户已经完成的 ClickHouse 配置，不自动修改配置或重新扫描。

验证更新：137 项前端单元测试、19 项相关浏览器测试与生产构建通过。另在实际 `8080` 服务上运行 `/work/terrasync-gui-validation/visibility-check.mjs`，从默认页面点击全局菜单进入真实扫描树图，结果为 PASS；真实扫描为 374,053 个文件、182,348,305,308 逻辑字节。新增回归覆盖普通导航路径，避免仅验证已知深链接而遗漏功能可见性。

## 界面截图

截图来自 REA 对重建页面的真实浏览器捕获，数据为测试 fixture。SHA-256：`6515bb256b9ffd9ffdf2394d2895b14bab71f1a3c01e91523c2085a6374655b9`。

![TerraSync 存储数据分析界面](assets/terrasync-storage-analysis.png)

## 树图视觉改版

针对大面积纯蓝区块缺少层次的问题，改为从现有主题色派生的浅色填充、深色容量标签、目录分色图例和细边框。主要区块显示名称、容量、占当前视图比例与文件数；窄区块缩减文字，完整信息可在悬浮提示和详情面板读取。文本对浅色背景的对比度有回归检查，选中与标记仍以独立边框表达。

默认提供两层真实目录预览，并允许切换单层。最多额外读取 6 个占比至少 2% 的目录，每个目录最多 32 个主要条目及汇总桶；只在快照身份、路径、文件数和逻辑容量均一致时采用预览，失败或不一致时继续显示原父级。未合成虚构子区块，也未改变面积权重。

新增验证覆盖预览预算、失败回退、快照一致性、年龄未知值、文字对比度和层级切换保留标记。实际服务上的 `/work/terrasync-gui-validation/redesign-check.mjs` 验证了现有扫描的两层预览、年龄模式与浏览器无异常。新版实际截图保存在 `/work/terrasync-gui-validation/storage-analysis-redesign.png`；上方 REA 截图保留为初始实现证据。
