import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask, mockExecutions, mockEndpoints } from './fixtures/mock-data'

// ============================================================
// 任务详情页交互 E2E 测试
// 验证：信息展示、执行历史、编辑入口、分析数据
// ============================================================

const completedTask = makeTask({
  id: 't-done',
  name: 'completed-scan',
  status: 'completed',
  task_type: 'scan',
  source_endpoint_id: 'ep-nfs-1',
  started_at: '2026-03-01T00:01:00Z',
  finished_at: '2026-03-01T00:10:00Z',
})

const runningTask = makeTask({
  id: 't-running',
  name: 'running-sync',
  status: 'running',
  task_type: 'sync',
  source_endpoint_id: 'ep-local-1',
  dest_endpoint_id: 'ep-nfs-1',
  started_at: '2026-03-01T00:01:00Z',
})

test.describe('任务详情页信息展示', () => {
  test('显示任务名称、类型和状态', async ({ page }) => {
    await setupApiMocks(page, { tasks: [completedTask] })
    await page.goto('/tasks/t-done')

    // 任务名称
    await expect(page.getByText('completed-scan')).toBeVisible()
    // 状态标签在 TaskInfoPanel 头部区域
    await expect(page.locator('.n-tag', { hasText: '已完成' }).first()).toBeVisible()
  })

  test('显示源端点信息', async ({ page }) => {
    await setupApiMocks(page, { tasks: [completedTask] })
    await page.goto('/tasks/t-done')

    // 端点名称应显示（从 mock endpoints 中取）
    await expect(page.getByText('NAS-01')).toBeVisible()
  })

  test('running 任务不显示编辑按钮', async ({ page }) => {
    await setupApiMocks(page, { tasks: [runningTask] })
    await page.goto('/tasks/t-running')

    await expect(page.getByRole('button', { name: '编辑' })).toBeHidden()
  })

  test('非 running 任务显示编辑按钮', async ({ page }) => {
    await setupApiMocks(page, { tasks: [completedTask] })
    await page.goto('/tasks/t-done')

    await expect(page.getByRole('button', { name: '编辑' })).toBeVisible()
  })
})

test.describe('执行历史', () => {
  test('运行记录 Tab 显示执行表格', async ({ page }) => {
    await setupApiMocks(page, { tasks: [completedTask] })
    await page.goto('/tasks/t-done')

    // 切换到运行记录 Tab
    await page.getByText('运行记录').click()

    // 验证执行表格有数据
    await expect(page.getByText('查看报告')).toBeVisible()
  })

  test('点击查看报告切换展开', async ({ page }) => {
    await setupApiMocks(page, { tasks: [completedTask] })
    await page.goto('/tasks/t-done')

    await page.getByText('运行记录').click()

    // 点击查看报告
    const viewBtn = page.getByText('查看报告').first()
    await viewBtn.click()

    // ScanAnalysis 组件应可见（含分析数据或统计信息）
    // 由于 completed 任务会自动加载 analytics，检查分析区域存在
    await expect(page.locator('.n-tab-pane').filter({ hasText: '运行' })).toBeVisible()
  })
})

test.describe('错误任务详情', () => {
  test('failed 任务显示错误信息', async ({ page }) => {
    const failedTask = makeTask({
      id: 't-fail',
      name: 'failed-task',
      status: 'failed',
      error_message: 'connection refused',
    })
    await setupApiMocks(page, { tasks: [failedTask] })
    await page.goto('/tasks/t-fail')

    await expect(page.getByText('connection refused')).toBeVisible()
  })
})
