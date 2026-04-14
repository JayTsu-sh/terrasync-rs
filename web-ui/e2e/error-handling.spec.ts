import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'

// ============================================================
// 后端拒绝时前端的错误处理
// 验证当 API 返回错误时，前端正确显示错误消息
// 操作列渲染为 <span>（非 <button>），用 getByText 定位
// ============================================================

test.describe('后端拒绝 — 错误消息展示', () => {
  test('删除端点被后端拒绝时显示错误消息', async ({ page }) => {
    await setupApiMocks(page)
    // 覆盖删除端点为返回 409
    await page.route(/\/api\/v1\/endpoints\/[^/]+$/, async route => {
      if (route.request().method() === 'DELETE') {
        await route.fulfill({
          status: 409,
          json: { error: '端点正在被运行中的任务使用' },
        })
      } else {
        await route.fallback()
      }
    })
    await page.goto('/endpoints')

    // 端点列表操作列是 <span>
    await page.getByText('删除').first().click()
    // 确认弹窗中的"删除"按钮（dialog.warning 的 positiveText）
    await page.locator('.n-dialog').getByRole('button', { name: '删除' }).click()

    // 验证错误消息显示
    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('启动任务失败时显示错误消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.route(/\/api\/v1\/tasks\/[^/]+\/start$/, async route => {
      await route.fulfill({
        status: 400,
        json: { error: 'Task is in running state, cannot start' },
      })
    })
    await page.goto('/tasks')

    // TaskList 操作列是 <span>
    const row = page.locator('tr', { has: page.getByText('scan-pending') })
    await row.getByText('启动').click()

    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('取消任务失败时显示错误消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.route(/\/api\/v1\/tasks\/[^/]+\/cancel$/, async route => {
      await route.fulfill({
        status: 400,
        json: { error: 'Task is not running, cannot cancel' },
      })
    })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-running') })
    await row.getByText('取消', { exact: true }).click()

    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('删除任务失败时显示错误消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.route(/\/api\/v1\/tasks\/[^/]+$/, async route => {
      if (route.request().method() === 'DELETE') {
        await route.fulfill({
          status: 400,
          json: { error: 'Task is in running state, cannot delete' },
        })
      } else {
        await route.fallback()
      }
    })
    await page.goto('/tasks')

    const row = page.locator('tr', { has: page.getByText('scan-pending') })
    await row.getByText('删除').click()

    // 确认弹窗
    await page.locator('.n-dialog').getByRole('button', { name: '删除' }).click()

    await expect(page.locator('.n-message')).toBeVisible()
  })
})
