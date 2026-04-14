# web-ui CLAUDE.md

前端子项目的编码规范和架构约定。

## Build Commands

```bash
npm run dev          # 开发服务器
npm run build        # vue-tsc 类型检查 + vite 生产构建
```

## 技术栈

- Vue 3 + TypeScript (Composition API, `<script setup>`)
- Naive UI 组件库 + Tailwind CSS + ECharts (vue-echarts)
- Vue Router + 图标: `@vicons/ionicons5`

## 目录结构

```
src/
  api/          — API 调用层（fetch 封装）
  composables/  — 业务逻辑 composable（纯逻辑，无 UI 反馈）
  components/   — 可复用 UI 组件
  views/        — 页面组件（薄组装层 + UI 反馈）
  shared/       — 共享常量、工具函数、类型映射
```

## Vue Composable 架构约定（Pattern B）

Composable 是纯逻辑层，View 是 UI 反馈层。两者职责严格分离。

### 核心规则

- Composable **禁止**引入 `useMessage()` / `useDialog()` 等 UI 反馈 API
- 操作函数直接 `throw` 错误，由 View 层 `try/catch` 后调用 `message.error()` / `message.success()`
- 需要用户确认的操作（删除、冲突），composable 返回结构化结果（如 `{ needs_confirm, conflicting_tasks }`），View 层决定是否弹 dialog
- 验证错误使用 `ValidationError` 自定义类（定义在 composable 中，`export class ValidationError extends Error`），与 API 错误区分

### 函数命名约定

| 场景 | Composable 函数名 | View 包装函数名 |
|------|-------------------|----------------|
| 纯执行（无需确认） | `handleSave()`, `handleStart()` | `onSave()`, `onStart()` |
| 需 View 确认后执行 | `performDelete()` | `onDelete()` (含 dialog) |
| 返回结构化结果 | `handleCreate(): Promise<CreateResult>` | `onCreate()` (解析结果) |

### 示例模式

```typescript
// composable — 纯逻辑
async function handleSave(force = false): Promise<SaveResult> {
  if (!name) throw new ValidationError('名称不能为空')
  const resp = await updateEndpoint(id, { name, config, force })
  if (resp.needs_confirm) return { needs_confirm: true, conflicting_tasks: resp.conflicting_tasks }
  return { needs_confirm: false, conflicting_tasks: [] }
}

// view — UI 反馈
async function onSave(force = false) {
  try {
    const result = await handleSave(force)
    if (result.needs_confirm) {
      dialog.warning({ ..., onPositiveClick: () => onSave(true) })
      return
    }
    message.success('保存成功')
  } catch (e: any) {
    message.error(e instanceof ValidationError ? e.message : (e.message || '保存失败'))
  }
}
```

## 样式覆盖原则（强制）

**尽量依赖 Naive UI 组件默认样式，减少 Tailwind 手动覆盖。**

App.vue 的 `themeOverrides` 已全局统一字体、颜色、圆角等。手动覆盖会绕过这套体系，导致不一致。

- DataTable 列 render 函数中**不要**手动设 `text-xs`/`text-sm` 等字体大小，让 `tdFontSize` 主题覆盖生效
- 表单输入（NInput/NSelect/NInputNumber）**不指定** `size` prop，使用默认 medium（除非场景确需紧凑布局如 ConditionBuilder）
- 宽度限制用 Tailwind class（如 `class="w-[200px]"`），**禁止** inline style（如 `:style="{ width: '140px' }""`）
- 只在确实需要特殊效果时才覆盖默认样式（如标题级大输入框）
- 新增组件时先检查 App.vue 的 `themeOverrides` 是否已覆盖相关样式

### 设计体系：Tailwind v4 为唯一权威（强制）

App.vue 的 `themeOverrides` 已全面对齐 Tailwind v4 默认设计体系。所有视觉选择必须以 Tailwind 默认值为准。

#### 颜色对照表

| 语义 | themeOverrides token | Tailwind class | 色值 |
|------|---------------------|----------------|------|
| 主色 | primaryColor | blue-500 | #3b82f6 |
| 成功 | successColor | green-500 | #22c55e |
| 警告 | warningColor | amber-500 | #f59e0b |
| 错误 | errorColor | red-500 | #ef4444 |
| 主文字 | textColor1 | gray-900 | #111827 |
| 次文字 | textColor2 | gray-500 | #6b7280 |
| 辅助文字 | textColor3 | gray-400 | #9ca3af |
| 边框 | borderColor | gray-300 | #d1d5db |
| 分割线 | dividerColor | gray-100 | #f3f4f6 |

#### 尺寸对照表

| 维度 | themeOverrides | Tailwind class |
|------|---------------|----------------|
| 圆角 | borderRadius: 6px | rounded-md |
| 小圆角 | borderRadiusSmall: 4px | rounded |
| 按钮高度 | heightMedium: 36px | h-9 |
| 表格字号 | tdFontSize: 14px | text-sm |

#### 规则

- 非 Naive UI 元素直接用 Tailwind class（如 `text-gray-500`），自动与组件颜色一致
- **禁止**在代码中硬编码 hex 色值（如 `#1677ff`），必须通过 themeOverrides 或 Tailwind class 引用
- 新增语义颜色时，先在 Tailwind 默认色板中选色阶，再同步到 themeOverrides
- 字体栈以 themeOverrides 的 `fontFamily` 为准（`Inter, -apple-system, ...`），style.css body 保持一致

## UI 组件设计规范

### 表单弹窗（Modal）

- **分组标题**：所有弹窗表单必须使用分组小标题（`text-xs text-gray-400 font-semibold tracking-wider`），如"基础信息"、"连接配置"、"选项"
- **必填标记**：使用 NFormItem 的 `required` prop（Naive UI 原生），**禁止**手动添加 `<span>*</span>`
- **输入框高度统一**：所有表单输入框（NInput、NAutoComplete、NSelect、手工输入框）高度统一为 36px
- **密码字段**：AK/SK 等敏感字段使用 `type="password" show-password-on="click"` 显示眼睛图标
- **Placeholder 规则**：
  - 必填字段 placeholder 为空
  - 可选字段 placeholder 说明用途（如"可选，输入 / 后自动补全"）
  - 依赖发现的字段说明触发条件（如"输入 Host 后自动查询"）

### 详情页（Detail）

- **只读展示**：详情页配置信息为只读 key-value 网格展示，**不使用内联编辑**
- **编辑方式**：通过弹窗（EditEndpointModal）编辑，**禁止**在详情页内直接编辑
- **卡片化布局**：使用白色背景 + 圆角 8px + 微阴影（`shadow-sm`），配置区域使用 `bg-gray-50`
- **配置网格**：key-value 采用 2-3 列网格布局（label 灰色 `text-gray-500` + value 黑色 `text-gray-900`）

### 列表页（List）

- **搜索功能**：列表页 header 区域包含搜索框（按名称和地址模糊搜索）
- **分页**：使用 NDataTable 原生 `pagination` prop，默认 pageSize=10
- **交替行色**：启用 `striped` 属性
- **连接状态**：圆点 + 文字（"已连接"绿色 / "连接失败"红色 / "检测中"黄色），**不使用**纯圆点
- **列顺序**：名称 → 连接状态 → 类型 → 存储地址 → 创建时间 → 操作

### 编辑弹窗（EditModal）

- **警告 banner**：弹窗顶部显示蓝色提示"修改连接配置可能会影响关联的扫描/同步任务"
- **预填数据**：打开时自动填充当前 endpoint 数据
- **冲突确认**：保存时如有关联任务冲突，弹 dialog 确认后 force 保存

## 其他约定

- 表格列定义使用 Tailwind class，**禁止** inline style 的 `h()` 渲染函数中使用 `style` 属性
- 页面标题统一使用 `PageHeader` 组件
- 返回链接统一使用 `BackLink` 组件
- 扫描分析 UI 统一使用 `ScanAnalysis` 组件（EndpointDetail 和 TaskDetail 共享）
- 图标只使用 `@vicons/ionicons5`，**禁止**引入其他图标包

## 测试约定（测试驱动开发，强制）

**前端开发采用 TDD（测试驱动开发）方式：先写测试，再写实现。**

```bash
npm run test             # vitest run
npm run test:watch       # vitest watch 模式
```

### TDD 流程

1. **先写测试**：明确预期行为，编写失败的测试用例
2. **再写实现**：编写最少代码让测试通过
3. **重构**：在测试保护下优化代码

### 测试分类

| 类型 | 工具 | 放置位置 | 测试内容 |
|------|------|---------|---------|
| Composable 逻辑测试 | vitest | `composables/__tests__/` | mock API，测试 computed/ref 行为 |
| 组件渲染测试 | vitest + @vue/test-utils + happy-dom | `components/__tests__/` | DOM 结构、CSS class、props |
| UI 一致性测试 | vitest + @vue/test-utils | 同上 | 输入框高度、必填标记、placeholder、列顺序 |
| 纯函数测试 | vitest | 与源文件同级 `.test.ts` | 序列化、格式化、工具函数 |
| E2E 交互测试 | Playwright | `e2e/*.spec.ts` | 完整用户流程、跨页面导航、API 集成、操作按钮状态矩阵 |

### E2E 测试约定（Playwright）

```bash
npm run test:e2e           # 运行所有 E2E 测试
npm run test:e2e:ui        # 交互式 UI 模式
npm run test:e2e:headed    # 有头浏览器模式
```

- **API Mock**：通过 `page.route()` 拦截所有 `/api/v1/*` 请求，不依赖后端运行
- **Mock 数据**：集中在 `e2e/fixtures/mock-data.ts`，提供 `makeTask()` 等工厂函数
- **Mock 路由**：集中在 `e2e/fixtures/api-mock.ts`，`setupApiMocks(page, overrides?)` 一键安装
- **覆盖范围**：操作按钮状态矩阵（穷举 5 状态 × 3 操作）、表单类型联动、错误处理、导航流程
- **何时写 E2E**：涉及跨组件交互、路由跳转、API 调用链的场景用 E2E；纯逻辑/纯渲染用 vitest

### 每次修改必须包含的测试

- **新增功能**：对应的行为测试（搜索过滤、分页、表单验证等）
- **UI 排版变更**：验证 CSS class、props、DOM 结构的一致性测试
- **Bug 修复**：先写复现 bug 的回归测试，再修复
- **互斥性测试（强制）**：验证互斥状态不会同时存在
  - 类型互斥：NFS/Local/S3 字段只在对应类型下显示/生效
  - 状态互斥：编辑弹窗打开时不能同时打开创建弹窗
  - 表单互斥：只有对应类型的必填字段被验证
  - 操作互斥：保存中不能重复提交
