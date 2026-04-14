import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask, mockEndpoints } from './fixtures/mock-data'

// ============================================================
// 任务编辑弹窗 E2E 测试
// 验证：预填、类型联动、表单验证、保存流程
// ============================================================

const syncTask = makeTask({
  id: 't-sync-edit',
  name: 'sync-editable',
  status: 'pending',
  task_type: 'sync',
  source_endpoint_id: 'ep-local-1',
  dest_endpoint_id: 'ep-nfs-1',
  config: { qos: '100MiB/s', block_size: '4MiB', iops: 500, concurrency: 8, depth: 0 },
})

const scanTask = makeTask({
  id: 't-scan-edit',
  name: 'scan-editable',
  status: 'completed',
  task_type: 'scan',
  source_endpoint_id: 'ep-nfs-1',
  config: { depth: 0 },
})

test.describe('任务编辑弹窗', () => {
  test('sync 任务：编辑弹窗打开并预填数据', async ({ page }) => {
    await setupApiMocks(page, { tasks: [syncTask] })
    await page.goto('/tasks/t-sync-edit')

    // 点击编辑按钮
    await page.getByRole('button', { name: '编辑' }).click()

    // 验证弹窗打开
    const modal = page.locator('.n-modal')
    await expect(modal).toBeVisible()

    // 验证任务名称已预填
    const nameInput = modal.locator('.n-form-item').filter({ hasText: '任务名称' }).locator('input')
    await expect(nameInput).toHaveValue('sync-editable')

    // 验证任务类型标签显示（exact 避免匹配"源迁移目标"等包含"迁移"的文字）
    await expect(modal.getByText('迁移', { exact: true })).toBeVisible()
  })

  test('sync 任务：显示高级选项（QoS、IOPS、Block Size）', async ({ page }) => {
    await setupApiMocks(page, { tasks: [syncTask] })
    await page.goto('/tasks/t-sync-edit')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    // sync 任务应显示高级选项
    await expect(modal.getByText('带宽限速')).toBeVisible()
    await expect(modal.getByText('IOPS 限速')).toBeVisible()
    await expect(modal.getByText('Block Size')).toBeVisible()
    await expect(modal.getByText('完整性检查')).toBeVisible()

    // 应显示目标端点
    await expect(modal.getByText('目标端点')).toBeVisible()
  })

  test('scan 任务：隐藏目标端点和高级选项', async ({ page }) => {
    await setupApiMocks(page, { tasks: [scanTask] })
    await page.goto('/tasks/t-scan-edit')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')
    await expect(modal).toBeVisible()

    // scan 任务不显示高级选项
    await expect(modal.getByText('带宽限速')).toBeHidden()
    await expect(modal.getByText('IOPS 限速')).toBeHidden()
    await expect(modal.getByText('Block Size')).toBeHidden()

    // scan 任务不显示目标端点
    await expect(modal.getByText('目标端点')).toBeHidden()
  })

  test('表单验证：空名称提交显示警告', async ({ page }) => {
    await setupApiMocks(page, { tasks: [syncTask] })
    await page.goto('/tasks/t-sync-edit')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')

    // 清空名称
    const nameInput = modal.locator('.n-form-item').filter({ hasText: '任务名称' }).locator('input')
    await nameInput.fill('')

    // 点击保存
    await modal.getByRole('button', { name: '保存' }).click()

    // 应显示警告消息
    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('保存成功后弹窗关闭', async ({ page }) => {
    await setupApiMocks(page, { tasks: [syncTask] })
    await page.goto('/tasks/t-sync-edit')

    await page.getByRole('button', { name: '编辑' }).click()
    const modal = page.locator('.n-modal')
    await expect(modal).toBeVisible()

    // 点击保存
    await modal.getByRole('button', { name: '保存' }).click()

    // 弹窗应关闭
    await expect(modal).toBeHidden()

    // 应显示成功消息
    await expect(page.locator('.n-message')).toBeVisible()
  })
})
