import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'

test.describe('Endpoint 创建弹窗 — 类型切换字段', () => {
  test.beforeEach(async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints')
    // 打开创建弹窗
    await page.getByRole('button', { name: '新建目标' }).click()
    await expect(page.locator('.n-modal')).toBeVisible()
  })

  test('默认 local 类型显示路径配置', async ({ page }) => {
    const modal = page.locator('.n-modal')
    // 默认类型是本地文件系统
    await expect(modal.getByText('路径配置')).toBeVisible()
    // 不应显示 NFS/S3 字段
    await expect(modal.getByText('连接配置')).toBeHidden()
  })

  test('切换 NFS 类型显示连接配置字段', async ({ page }) => {
    const modal = page.locator('.n-modal')
    // 切换类型到 NFS
    await modal.locator('.n-select').first().click()
    await page.locator('.n-base-select-option', { hasText: 'NFS v3' }).click()

    await expect(modal.getByText('连接配置')).toBeVisible()
    // 应显示 NFS 特有字段
    await expect(modal.locator('.n-form-item', { hasText: '端口' })).toBeVisible()
  })

  test('切换 S3 类型显示 S3 配置字段', async ({ page }) => {
    const modal = page.locator('.n-modal')
    await modal.locator('.n-select').first().click()
    await page.locator('.n-base-select-option', { hasText: /^S3$/ }).click()

    // S3 分组标题为 "S3 连接配置"
    await expect(modal.getByText('S3 连接配置')).toBeVisible()
    await expect(modal.getByText('Access Key')).toBeVisible()
    await expect(modal.getByText('存储桶')).toBeVisible()
  })

  test('创建后自动扫描复选框存在', async ({ page }) => {
    await expect(page.getByText('创建后自动开始扫描')).toBeVisible()
  })
})

test.describe('Endpoint 创建弹窗 — 关闭与重置', () => {
  test('关闭弹窗后重新打开字段被重置', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints')

    // 打开弹窗，填写名称
    await page.getByRole('button', { name: '新建目标' }).click()
    const modal = page.locator('.n-modal')
    const nameInput = modal.locator('.n-form-item', { hasText: '名称' }).locator('input')
    await nameInput.fill('test-name')

    // 关闭弹窗
    await modal.getByRole('button', { name: '取消' }).click()

    // 重新打开，名称应为空
    await page.getByRole('button', { name: '新建目标' }).click()
    const nameInput2 = page.locator('.n-modal .n-form-item', { hasText: '名称' }).locator('input')
    await expect(nameInput2).toHaveValue('')
  })
})
