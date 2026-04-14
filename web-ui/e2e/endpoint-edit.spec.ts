import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'

// ============================================================
// 端点编辑弹窗 E2E 测试
// 验证：预填、isDirty、保存成功、冲突确认流程
// ============================================================

test.describe('端点编辑弹窗', () => {
  test('打开弹窗预填数据且保存按钮禁用', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')
    await expect(modal).toBeVisible()

    // 名称已预填
    const nameInput = modal.locator('.n-form-item', { hasText: '名称' }).locator('input')
    await expect(nameInput).toHaveValue('NAS-01')

    // 未修改时保存按钮禁用
    const saveBtn = modal.getByRole('button', { name: '保存' })
    await expect(saveBtn).toBeDisabled()
  })

  test('修改名称后保存按钮启用', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    const nameInput = modal.locator('.n-form-item', { hasText: '名称' }).locator('input')
    const saveBtn = modal.getByRole('button', { name: '保存' })

    // 修改名称
    await nameInput.fill('NAS-01-renamed')
    await expect(saveBtn).toBeEnabled()

    // 恢复原值 → 保存按钮再次禁用
    await nameInput.fill('NAS-01')
    await expect(saveBtn).toBeDisabled()
  })

  test('保存成功后弹窗关闭', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    // 修改名称并保存
    const nameInput = modal.locator('.n-form-item', { hasText: '名称' }).locator('input')
    await nameInput.fill('NAS-01-updated')
    await modal.getByRole('button', { name: '保存' }).click()

    // 弹窗关闭 + 成功消息
    await expect(modal).toBeHidden()
    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('保存触发冲突时弹出确认弹窗，确认后强制保存成功', async ({ page }) => {
    let callCount = 0
    // 需要在 setupApiMocks 之前注册冲突路由（后注册优先）
    await setupApiMocks(page)
    // 覆盖 PUT 路由：第一次返回冲突，第二次（force）返回成功
    await page.route(/\/api\/v1\/endpoints\/[^/]+$/, async route => {
      if (route.request().method() !== 'PUT') { await route.fallback(); return }
      callCount++
      const body = JSON.parse(route.request().postData() || '{}')
      if (!body.force) {
        // 非 force 保存：返回冲突
        await route.fulfill({ json: { endpoint: null, needs_confirm: true, conflicting_tasks: ['scan-prod', 'sync-backup'] } })
      } else {
        // force=true 保存：返回成功
        await route.fulfill({ json: { endpoint: { id: 'ep-nfs-1', name: body.name || 'NAS-01-updated', endpoint_type: 'nfs_v3', endpoint_type_display: 'NFS v3', config: { type: 'nfs' }, created_at: '2026-01-15T10:00:00Z', updated_at: '2026-03-22T00:00:00Z' }, needs_confirm: false, conflicting_tasks: [] } })
      }
    })

    await page.goto('/endpoints/ep-nfs-1')
    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    // 修改名称并保存
    const nameInput = modal.locator('.n-form-item', { hasText: '名称' }).locator('input')
    await nameInput.fill('NAS-01-updated')
    await modal.getByRole('button', { name: '保存' }).click()

    // 应弹出冲突确认 dialog
    const confirmDialog = page.locator('.n-dialog')
    await expect(confirmDialog).toBeVisible()
    await expect(confirmDialog).toContainText('2 个关联任务')

    // 点击确认保存
    await confirmDialog.getByRole('button', { name: '确认保存' }).click()

    // 编辑弹窗和确认 dialog 都关闭（两者都有 .n-modal class，用标题定位）
    await expect(page.getByText('编辑目标')).toBeHidden()
    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('显示修改警告 banner', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    await expect(modal.getByText('修改连接配置可能会影响关联的扫描/同步任务')).toBeVisible()
  })
})
