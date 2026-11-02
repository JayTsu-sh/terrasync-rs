import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask } from './fixtures/mock-data'
import { statusLabelMap } from '../src/shared/task-rules'

const cases = [
  ['endpoint', 'cancelled'], ['detail', 'cancelled'], ['list', 'cancelled'],
  ['list', 'completed'], ['list', 'failed'],
] as const

for (const [view, terminal] of cases) {
  test(`${view}/${terminal}: cancellation updates the visible status without waiting for regular polling`, async ({ page }) => {
    const task = makeTask({ id: 'cancel-feedback', task_type: 'scan', source_endpoint_id: 'ep-nfs-1', status: 'running' })
    await setupApiMocks(page, { tasks: [task], executions: [] })
    let stoppedAt = 0
    await page.route(/\/api\/v1\/tasks\/?(?:\?.*)?$/, route => route.fulfill({ json: [task] }))
    await page.route(/\/api\/v1\/tasks\/cancel-feedback$/, route => route.fulfill({ json: task }))
    await page.route(/\/api\/v1\/tasks\/cancel-feedback\/cancel$/, async route => {
      await route.fulfill({ json: null })
      // 请求接收先于真正停止；立即重读会得到 running，不能把它当作取消失败。
      setTimeout(() => { task.status = terminal; stoppedAt = Date.now() }, 300)
    })
    const path = view === 'endpoint' ? '/endpoints/ep-nfs-1' : view === 'detail' ? '/tasks/cancel-feedback' : '/tasks'
    await page.goto(path)
    const button = view === 'endpoint' ? page.getByRole('button', { name: '取消扫描', exact: true })
      : view === 'detail' ? page.getByRole('button', { name: '取消', exact: true })
        : page.getByText('取消', { exact: true })
    await button.click()
    await expect(page.getByText(statusLabelMap[terminal], { exact: true }).first()).toBeVisible({ timeout: 1500 })
    expect(stoppedAt).toBeGreaterThan(0)
    if (view === 'list') await expect(page.locator('.n-message')).toContainText('任务已停止')
  })
}
