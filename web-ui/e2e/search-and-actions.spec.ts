import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask, mockAllTasks } from './fixtures/mock-data'

// ============================================================
// 搜索过滤
// ============================================================

test.describe('TaskList 搜索过滤', () => {
  test('按名称搜索过滤任务', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 所有任务可见
    await expect(page.getByText('scan-pending')).toBeVisible()
    await expect(page.getByText('scan-running')).toBeVisible()

    // 输入搜索词
    await page.getByPlaceholder('搜索任务...').fill('pending')

    // 只有匹配的任务可见
    await expect(page.getByText('scan-pending')).toBeVisible()
    await expect(page.getByText('scan-running')).toBeHidden()
    await expect(page.getByText('scan-done')).toBeHidden()
  })

  test('搜索不区分大小写', async ({ page }) => {
    const task = makeTask({ id: 't-upper', name: 'MyTask-UPPER', status: 'pending' })
    await setupApiMocks(page, { tasks: [task] })
    await page.goto('/tasks')

    await page.getByPlaceholder('搜索任务...').fill('mytask')
    await expect(page.getByText('MyTask-UPPER')).toBeVisible()
  })

  test('清空搜索显示全部任务', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 搜索后清空
    const searchInput = page.getByPlaceholder('搜索任务...')
    await searchInput.fill('pending')
    await expect(page.getByText('scan-running')).toBeHidden()

    await searchInput.fill('')
    await expect(page.getByText('scan-running')).toBeVisible()
    await expect(page.getByText('scan-pending')).toBeVisible()
  })

  test('无匹配结果时表格为空', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    await page.getByPlaceholder('搜索任务...').fill('不存在的任务xyz')
    // NDataTable 空状态
    await expect(page.getByText('scan-pending')).toBeHidden()
    await expect(page.getByText('scan-running')).toBeHidden()
  })
})

test.describe('EndpointList 搜索过滤', () => {
  test('按名称搜索端点', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints')

    await page.getByPlaceholder('搜索目标...').fill('NAS')
    await expect(page.getByText('NAS-01')).toBeVisible()
    await expect(page.getByText('Local-Data')).toBeHidden()
    await expect(page.getByText('S3-Backup')).toBeHidden()
  })

  test('按地址搜索端点', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints')

    // 搜索 NFS 服务器地址
    await page.getByPlaceholder('搜索目标...').fill('10.0.0.1')
    await expect(page.getByText('NAS-01')).toBeVisible()
    await expect(page.getByText('Local-Data')).toBeHidden()
  })
})

// ============================================================
// 删除确认（成功路径）
// ============================================================

test.describe('删除操作成功路径', () => {
  test('删除端点：确认后显示成功消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints')

    // 点击第一个端点的删除
    await page.getByText('删除').first().click()

    // 确认弹窗
    const dialog = page.locator('.n-dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('确认删除')

    // 确认删除
    await dialog.getByRole('button', { name: '删除' }).click()

    // 成功消息
    await expect(page.locator('.n-message')).toBeVisible()
  })

  test('删除任务：确认后显示成功消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 找到 pending 任务行的删除操作
    const row = page.locator('tr', { has: page.getByText('scan-pending') })
    await row.getByText('删除').click()

    // 确认弹窗
    const dialog = page.locator('.n-dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('确认删除')

    // 确认删除
    await dialog.getByRole('button', { name: '删除' }).click()

    // 成功消息
    await expect(page.locator('.n-message')).toContainText('已删除')
  })
})

// ============================================================
// 任务操作反馈
// ============================================================

test.describe('任务操作反馈', () => {
  test('启动任务显示成功消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 点击 pending 任务的启动
    const row = page.locator('tr', { has: page.getByText('scan-pending') })
    await row.getByText('启动').click()

    await expect(page.locator('.n-message')).toContainText('已启动')
  })

  test('取消任务显示成功消息', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 点击 running 任务的取消
    const row = page.locator('tr', { has: page.getByText('scan-running') })
    await row.getByText('取消', { exact: true }).click()

    await expect(page.locator('.n-message')).toContainText('已取消')
  })
})

// ============================================================
// 端点连接测试
// ============================================================

test.describe('端点连接测试', () => {
  test('测试连接按钮显示连接结果', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/endpoints/ep-nfs-1')

    // 点击测试连接
    await page.getByRole('button', { name: '测试连接' }).click()

    // mock 返回成功，应显示成功状态
    await expect(page.getByText('已连接')).toBeVisible()
  })

  test('连接测试失败时显示错误', async ({ page }) => {
    await setupApiMocks(page)
    // 覆盖连接测试为失败
    await page.route(/\/api\/v1\/endpoints\/[^/]+\/test$/, async route => {
      await route.fulfill({ status: 500, json: { error: 'Connection refused' } })
    })
    await page.goto('/endpoints/ep-nfs-1')

    await page.getByRole('button', { name: '测试连接' }).click()

    // 应显示失败状态
    await expect(page.getByText('连接失败')).toBeVisible()
  })
})

// ============================================================
// 任务创建弹窗完整流程
// ============================================================

test.describe('任务创建流程', () => {
  test('填写表单并创建成功后弹窗关闭', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 打开创建弹窗
    await page.getByRole('button', { name: '新建任务' }).click()
    const modal = page.locator('.n-modal')
    await expect(modal).toBeVisible()

    // 填写任务名称
    const nameInput = modal.locator('.n-form-item', { hasText: '任务名称' }).locator('input')
    await nameInput.fill('new-sync-task')

    // 默认类型是 sync，选择源端点（Local-Data）
    const sourceSelect = modal.locator('.n-form-item', { hasText: '源迁移目标' }).locator('.n-select')
    await sourceSelect.click()
    await page.locator('.n-base-select-option', { hasText: 'Local-Data' }).click()
    // 等待下拉关闭
    await page.waitForTimeout(300)

    // 选择目标端点（NAS-01，不同于源）
    const destSelect = modal.locator('.n-form-item', { hasText: '目标迁移目标' }).locator('.n-select')
    await destSelect.click()
    // 多个下拉菜单共存于 DOM，用 visible filter 确保选到当前打开的
    await page.locator('.n-base-select-option:visible', { hasText: 'NAS-01' }).click()
    await page.waitForTimeout(300)

    // 点击创建
    await modal.getByRole('button', { name: '创建' }).click()

    // 弹窗关闭 + 成功消息
    await expect(modal).toBeHidden()
    await expect(page.locator('.n-message')).toContainText('创建成功')
  })

  test('未填名称提交显示验证警告', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    await page.getByRole('button', { name: '新建任务' }).click()
    const modal = page.locator('.n-modal')

    // 不填任何字段，直接点创建
    await modal.getByRole('button', { name: '创建' }).click()

    // 应显示 warning 消息（ValidationError）
    await expect(page.locator('.n-message')).toBeVisible()
    // 弹窗不应关闭
    await expect(modal).toBeVisible()
  })

  test('创建 scan 任务不需要目标端点', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    await page.getByRole('button', { name: '新建任务' }).click()
    const modal = page.locator('.n-modal')

    // 填写名称
    const nameInput = modal.locator('.n-form-item', { hasText: '任务名称' }).locator('input')
    await nameInput.fill('new-scan-task')

    // 切换到 scan 类型
    await modal.locator('.n-form-item', { hasText: '任务类型' }).locator('.n-select').click()
    await page.locator('.n-base-select-option', { hasText: '扫描' }).click()

    // 选择源端点
    const sourceSelect = modal.locator('.n-form-item', { hasText: '扫描目标' }).locator('.n-select')
    await sourceSelect.click()
    await page.locator('.n-base-select-option', { hasText: 'Local-Data' }).click()
    await page.waitForTimeout(300)

    // 点击创建（不需要目标端点）
    await modal.getByRole('button', { name: '创建' }).click()

    // 成功
    await expect(modal).toBeHidden()
    await expect(page.locator('.n-message')).toContainText('创建成功')
  })

  test('创建后弹窗重新打开时表单被重置', async ({ page }) => {
    await setupApiMocks(page)
    await page.goto('/tasks')

    // 第一次打开并填写
    await page.getByRole('button', { name: '新建任务' }).click()
    const modal = page.locator('.n-modal')
    const nameInput = modal.locator('.n-form-item', { hasText: '任务名称' }).locator('input')
    await nameInput.fill('temp-name')

    // 关闭弹窗
    await modal.getByRole('button', { name: '取消' }).click()

    // 重新打开，名称应为空
    await page.getByRole('button', { name: '新建任务' }).click()
    const nameInput2 = page.locator('.n-modal .n-form-item', { hasText: '任务名称' }).locator('input')
    await expect(nameInput2).toHaveValue('')
  })
})

// ============================================================
// TaskDetail 页面操作
// ============================================================

test.describe('TaskDetail 页面操作', () => {
  test('启动任务显示成功消息', async ({ page }) => {
    const task = makeTask({ id: 't-pending', name: 'task-pending', status: 'pending' })
    await setupApiMocks(page, { tasks: [task] })
    await page.goto('/tasks/t-pending')

    await page.getByRole('button', { name: '启动' }).click()
    await expect(page.locator('.n-message')).toContainText('已启动')
  })

  test('删除任务后跳转到列表', async ({ page }) => {
    const task = makeTask({ id: 't-pending', name: 'task-to-delete', status: 'pending' })
    await setupApiMocks(page, { tasks: [task] })
    await page.goto('/tasks/t-pending')

    // 点击删除
    await page.getByRole('button', { name: '删除' }).click()

    // 确认弹窗
    const dialog = page.locator('.n-dialog')
    await expect(dialog).toBeVisible()
    await dialog.getByRole('button', { name: '删除' }).click()

    // 跳转到任务列表
    await expect(page).toHaveURL(/\/tasks$/)
  })
})
