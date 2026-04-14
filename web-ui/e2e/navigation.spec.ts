import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'

test.describe('路由导航', () => {
  test.beforeEach(async ({ page }) => {
    await setupApiMocks(page)
  })

  test('根路径重定向到 /endpoints', async ({ page }) => {
    await page.goto('/')
    await expect(page).toHaveURL(/\/endpoints/)
  })

  test('端点列表点击详情导航到详情页', async ({ page }) => {
    await page.goto('/endpoints')
    // 端点列表操作列用 <span> 渲染
    await page.getByText('详情').first().click()
    await expect(page).toHaveURL(/\/endpoints\//)
  })

  test('任务列表点击新建打开弹窗', async ({ page }) => {
    await page.goto('/tasks')
    await page.getByRole('button', { name: '新建任务' }).click()
    // 创建任务已改为弹窗，不再是独立页面
    await expect(page.locator('.n-modal')).toBeVisible()
  })

  test('任务详情页点击返回链接回到列表', async ({ page }) => {
    await page.goto('/tasks/t-pending')
    // BackLink 组件渲染为链接
    await page.getByText('返回列表').click()
    await expect(page).toHaveURL(/\/tasks$/)
  })

  test('端点详情页点击返回链接回到列表', async ({ page }) => {
    await page.goto('/endpoints/ep-nfs-1')
    await page.getByText('返回列表').click()
    await expect(page).toHaveURL(/\/endpoints$/)
  })
})
