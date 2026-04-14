import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { mockEndpoints, mockTasksByStatus, mockAllTasks, mockExecutions } from './fixtures/mock-data'

// ============================================================
// EndpointDetail — 保存按钮 isDirty 逻辑
// ============================================================

// EndpointDetail 已改为只读展示 + 编辑弹窗模式，不再有页面内保存按钮
test.describe('EndpointDetail 操作按钮', () => {
  test('显示编辑按钮和测试连接按钮', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await expect(page.getByRole('button', { name: '编辑' })).toBeVisible()
    await expect(page.getByRole('button', { name: '测试连接' })).toBeVisible()
  })

  test('显示扫描启动按钮', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await expect(page.getByRole('button', { name: /扫描/ })).toBeVisible()
  })
})

// ============================================================
// EndpointDetail — 扫描按钮状态
// ============================================================

test.describe('EndpointDetail 扫描按钮状态', () => {
  test('正在扫描时显示取消扫描按钮和扫描中指示', async ({ page }) => {
    const runningTask = {
      ...mockTasksByStatus.running[0],
      source_endpoint_id: 'ep-nfs-1',
      task_type: 'scan',
    }

    await setupApiMocks(page, {
      tasksByEndpoint: { 'ep-nfs-1': [runningTask] },
      executions: [],
    })
    await page.goto('/endpoints/ep-nfs-1')

    // running 时显示"取消扫描"红色按钮 + "扫描中"加载指示
    await expect(page.getByRole('button', { name: '取消扫描' })).toBeVisible()
    await expect(page.getByText('扫描中')).toBeVisible()
  })

  test('无扫描任务时扫描按钮可点击', async ({ page }) => {
    await setupApiMocks(page, {
      tasks: [],
      tasksByEndpoint: {},
      executions: [],
    })
    await page.goto('/endpoints/ep-nfs-1')

    const scanBtn = page.getByRole('button', { name: /扫描/ })
    await expect(scanBtn).toBeEnabled()
  })
})

// ============================================================
// TaskList — 不同状态显示不同操作按钮
// ============================================================

test.describe('TaskList 状态驱动的操作按钮', () => {
  // TaskList 操作列渲染为 <span>（非 <button>），用 getByText 定位
  test('pending 任务显示启动和删除，不显示取消', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.pending })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-pending') })
    await expect(row.getByText('启动')).toBeVisible()
    await expect(row.getByText('删除')).toBeVisible()
    await expect(row.getByText('取消')).toBeHidden()
  })

  test('running 任务显示取消，不显示启动和删除', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.running })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-running') })
    await expect(row.getByText('取消')).toBeVisible()
    await expect(row.getByText('启动')).toBeHidden()
    await expect(row.getByText('删除')).toBeHidden()
  })

  // completed 任务可以重新启动（canStart('completed') === true）
  test('completed 任务显示启动和删除，不显示取消', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.completed })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-done') })
    await expect(row.getByText('启动')).toBeVisible()
    await expect(row.getByText('删除')).toBeVisible()
    await expect(row.getByText('取消', { exact: true })).toBeHidden()
  })

  test('failed 任务显示启动和删除', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.failed })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-fail') })
    await expect(row.getByText('启动')).toBeVisible()
    await expect(row.getByText('删除')).toBeVisible()
  })
})

// ============================================================
// TaskDetail — 状态驱动按钮
// ============================================================

test.describe('TaskDetail 状态驱动按钮', () => {
  test('pending 任务详情页显示启动按钮', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.pending })
    await page.goto('/tasks/t-pending')

    await expect(page.getByRole('button', { name: '启动' })).toBeVisible()
    await expect(page.getByRole('button', { name: '取消' })).toBeHidden()
  })

  test('running 任务详情页显示取消按钮', async ({ page }) => {
    await setupApiMocks(page, { tasks: mockTasksByStatus.running })
    await page.goto('/tasks/t-running')

    await expect(page.getByRole('button', { name: '取消' })).toBeVisible()
    await expect(page.getByRole('button', { name: '启动' })).toBeHidden()
  })
})

// ============================================================
// TaskCreate — 条件字段展示
// ============================================================

// TaskCreate 已改为 TaskList 页面内的弹窗（TaskCreateModal），默认类型为 sync
test.describe('TaskCreate 弹窗类型条件字段', () => {
  async function openCreateModal(page: import('@playwright/test').Page) {
    await setupApiMocks(page)
    await page.goto('/tasks')
    await page.getByRole('button', { name: '新建任务' }).click()
    await expect(page.locator('.n-modal')).toBeVisible()
  }

  test('默认 sync 类型显示目标端点和高级选项', async ({ page }) => {
    await openCreateModal(page)
    const modal = page.locator('.n-modal')

    // sync 是默认类型，高级选项应可见
    await expect(modal.getByText('带宽限速')).toBeVisible()
    await expect(modal.getByText('Block Size')).toBeVisible()
    await expect(modal.getByText('IOPS 限速')).toBeVisible()
    await expect(modal.locator('.n-form-item', { hasText: '目标迁移目标' })).toBeVisible()
  })

  test('切换 scan 类型隐藏目标端点和高级选项', async ({ page }) => {
    await openCreateModal(page)
    const modal = page.locator('.n-modal')

    // 切换到 scan
    await modal.locator('.n-select').first().click()
    await page.locator('.n-base-select-option', { hasText: '扫描' }).click()

    await expect(modal.getByText('带宽限速')).toBeHidden()
    await expect(modal.getByText('Block Size')).toBeHidden()
    await expect(modal.locator('.n-form-item', { hasText: '目标迁移目标' })).toBeHidden()
  })
})

// ============================================================
// TaskCreate — 表单验证
// ============================================================

test.describe('TaskCreate 弹窗表单验证', () => {
  test('未填必要字段时提交显示警告', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')
    await page.getByRole('button', { name: '新建任务' }).click()

    const modal = page.locator('.n-modal')
    // 直接点创建，不填任何字段
    await modal.getByRole('button', { name: '创建' }).click()

    // 应出现 naive-ui message 警告
    await expect(page.locator('.n-message')).toBeVisible()
  })
})

// ============================================================
// SystemSettings — 保存按钮 isDirty 逻辑
// ============================================================

test.describe('SystemSettings 保存按钮状态', () => {
  test('页面加载后保存按钮可见', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/settings')

    await expect(page.getByRole('button', { name: '保存' })).toBeVisible()
  })

  test('修改 DSN 后保存按钮仍可见', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/settings')

    // DSN 输入框（通过 placeholder 或 value 匹配）
    const dsnInput = page.getByRole('textbox').first()
    await dsnInput.fill('http://newhost:8123')

    await expect(page.getByRole('button', { name: '保存' })).toBeVisible()
  })
})
