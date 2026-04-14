import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask } from './fixtures/mock-data'
import { TASK_ACTIONS } from '../src/shared/task-rules'
import type { TaskStatus } from '../src/shared/task-rules'

// ============================================================
// 穷举矩阵：5 个状态 × 3 个操作 = 15 个测试
// 从 TASK_ACTIONS 共享规则自动派生预期值
// ============================================================

const statuses: TaskStatus[] = ['pending', 'running', 'completed', 'failed', 'cancelled']

test.describe('TaskList 操作按钮穷举矩阵', () => {
  for (const status of statuses) {
    const canStart = TASK_ACTIONS.canStart(status)
    const canCancel = TASK_ACTIONS.canCancel(status)
    const canDelete = TASK_ACTIONS.canDelete(status)

    test(`状态=${status}: 启动${canStart ? '可见' : '隐藏'}, 取消${canCancel ? '可见' : '隐藏'}, 删除${canDelete ? '可见' : '隐藏'}`, async ({ page }) => {
      const task = makeTask({ id: `t-${status}`, name: `task-${status}`, status })
      await setupApiMocks(page, { tasks: [task] })
      await page.goto('/tasks')

      // TaskList 操作列用 <span> 渲染（非 <button>），需用 getByText 在行内定位
      // 使用 exact 匹配避免"已取消"中的"取消"误匹配
      const row = page.locator('tr', { has: page.getByText(`task-${status}`) })

      if (canStart) {
        await expect(row.getByText('启动', { exact: true })).toBeVisible()
      } else {
        await expect(row.getByText('启动', { exact: true })).toBeHidden()
      }

      if (canCancel) {
        await expect(row.getByText('取消', { exact: true })).toBeVisible()
      } else {
        await expect(row.getByText('取消', { exact: true })).toBeHidden()
      }

      if (canDelete) {
        await expect(row.getByText('删除', { exact: true })).toBeVisible()
      } else {
        await expect(row.getByText('删除', { exact: true })).toBeHidden()
      }
    })
  }
})

// ============================================================
// TaskDetail 按钮穷举（这里用的是 <NButton>，所以用 getByRole）
// ============================================================

test.describe('TaskDetail 操作按钮穷举矩阵', () => {
  for (const status of statuses) {
    const canStart = TASK_ACTIONS.canStart(status)
    const canCancel = TASK_ACTIONS.canCancel(status)

    test(`状态=${status}: 启动${canStart ? '可见' : '隐藏'}, 取消${canCancel ? '可见' : '隐藏'}`, async ({ page }) => {
      const task = makeTask({ id: `t-${status}`, name: `task-${status}`, status })
      await setupApiMocks(page, { tasks: [task] })
      await page.goto(`/tasks/t-${status}`)

      if (canStart) {
        await expect(page.getByRole('button', { name: '启动' })).toBeVisible()
      } else {
        await expect(page.getByRole('button', { name: '启动' })).toBeHidden()
      }

      if (canCancel) {
        await expect(page.getByRole('button', { name: '取消' })).toBeVisible()
      } else {
        await expect(page.getByRole('button', { name: '取消' })).toBeHidden()
      }
    })
  }
})

// ============================================================
// EndpointDetail 扫描按钮穷举（按最新扫描任务状态）
// ============================================================

test.describe('EndpointDetail 扫描按钮穷举矩阵', () => {
  for (const status of statuses) {
    const isRunning = status === 'running'

    test(`最新扫描任务状态=${status}: ${isRunning ? '显示取消扫描按钮' : '显示扫描按钮'}`, async ({ page }) => {
      const scanTask = {
        ...makeTask({ id: `t-scan-${status}`, name: `scan-${status}`, status }),
        source_endpoint_id: 'ep-nfs-1',
        task_type: 'scan',
      }

      await setupApiMocks(page, {
        tasksByEndpoint: { 'ep-nfs-1': [scanTask] },
        executions: [],
      })
      await page.goto('/endpoints/ep-nfs-1')

      if (isRunning) {
        // running 时显示"取消扫描"红色按钮 + "扫描中"加载指示
        await expect(page.getByRole('button', { name: '取消扫描' })).toBeVisible()
        await expect(page.getByText('扫描中')).toBeVisible()
      } else {
        // 非 running 时显示正常扫描按钮
        await expect(page.getByRole('button', { name: '取消扫描' })).toBeHidden()
      }
    })
  }
})
