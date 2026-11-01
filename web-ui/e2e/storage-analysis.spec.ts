import { test, expect } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask } from './fixtures/mock-data'
import { analysisRoot, analysisProjects } from './fixtures/storage-analysis'

const task = makeTask({
  id: 't-done',
  name: '项目数据扫描',
  task_type: 'scan',
  status: 'completed',
  source_endpoint_id: 'ep-nfs-1',
})

test.beforeEach(async ({ page }) => {
  page.on('pageerror', (error) => console.error('Browser error:', error.message))
  await setupApiMocks(page, { tasks: [task] })
  await page.route('**/api/v1/tasks/*/analysis/tree?*', async (route) => {
    const path = new URL(route.request().url()).searchParams.get('path')
    await route.fulfill({ json: path === 'projects' ? analysisProjects : analysisRoot })
  })
})

test('独立数据分析显示真实接口树图，可下钻并通过面包屑返回', async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1600, height: 1100 })
  await page.goto('/analysis?task=t-done')
  const explorer = page.getByTestId('storage-explorer')
  await expect(explorer).toBeVisible()
  await expect(explorer.getByTestId('storage-treemap').locator('canvas')).toBeVisible()
  await expect(explorer.getByLabel('选择 empty.txt', { exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('storage-analysis.png'), fullPage: true })
  const request = page.waitForRequest(
    (r) => r.url().includes('/analysis/tree?') && new URL(r.url()).searchParams.get('path') === 'projects',
  )
  await explorer.getByRole('button', { name: '进入 projects', exact: true }).click()
  await request
  await expect(explorer.getByLabel('选择 research', { exact: true })).toBeVisible()
  await explorer.getByRole('button', { name: '返回上一目录', exact: true }).click()
  await expect(explorer.getByLabel('选择 empty.txt', { exact: true })).toBeVisible()
})

test('直接链接可以打开数据分析，接口失败显示可重试错误', async ({ page }) => {
  await page.route('**/api/v1/tasks/*/analysis/tree?*', (route) =>
    route.fulfill({ status: 409, json: { error: '扫描明细不可用，请重新扫描' } }),
  )
  await page.goto('/analysis?task=t-done')
  await expect(page.getByRole('heading', { name: '存储空间分析', exact: true })).toBeVisible()
  await expect(
    page.getByTestId('storage-explorer').getByText('扫描明细不可用，请重新扫描', { exact: false }),
  ).toBeVisible()
  await expect(page.getByTestId('storage-explorer').locator('canvas')).toHaveCount(0)
})

test('切换数量模式请求相应榜单，标记后可复核并导出来源清单', async ({ page }) => {
  await page.goto('/analysis?task=t-done')
  const explorer = page.getByTestId('storage-explorer')
  await expect(explorer.locator('canvas')).toBeVisible()
  const request = page.waitForRequest(
    (r) => r.url().includes('/analysis/tree?') && new URL(r.url()).searchParams.get('metric') === 'count',
  )
  await explorer.getByRole('button', { name: '文件数量', exact: true }).click()
  await request
  await explorer.locator('table').getByRole('button', { name: '标记 archive', exact: true }).click()
  await explorer.getByRole('button', { name: '复核已标记清单', exact: true }).click()
  await expect(page.getByText('archive', { exact: true }).last()).toBeVisible()
  const download = page.waitForEvent('download')
  await page.getByRole('button', { name: '导出 JSON 清单', exact: true }).click()
  const file = await download
  const exported = JSON.parse(await readFile((await file.path())!, 'utf8'))
  expect(exported.task_id).toBe('t-done')
  expect(exported.source_path).toBe('/data/project')
  expect(exported.paths.map((node: { path: string }) => node.path)).toEqual(['archive'])
})

test('扫描运行时不查询尚未完成的目录明细', async ({ page }) => {
  const running = { ...task, status: 'running' }
  await setupApiMocks(page, { tasks: [running] })
  let requested = false
  page.on('request', (request) => {
    if (request.url().includes('/analysis/tree')) requested = true
  })
  await page.goto('/analysis?task=t-done')
  await expect(page.getByText('扫描完成后可浏览目录分布')).toBeVisible()
  expect(requested).toBe(false)
})

test('端点详情提供进入最新扫描分析的入口', async ({ page }) => {
  await setupApiMocks(page, { tasks: [task], tasksByEndpoint: { 'ep-nfs-1': [task] } })
  await page.goto('/endpoints/ep-nfs-1')
  await expect(page.getByTestId('storage-explorer').locator('canvas')).toBeVisible()
  await page.getByRole('button', { name: '数据分析', exact: true }).click()
  await expect(page).toHaveURL('/analysis?task=t-done')
  await expect(page.getByTestId('storage-explorer')).toBeVisible()
})

test('主菜单直接进入数据分析并自动打开已完成扫描', async ({ page }) => {
  await page.goto('/endpoints')
  await page.getByRole('menuitem', { name: '数据分析', exact: true }).click()
  await expect(page).toHaveURL(/\/analysis(?:\?|$)/)
  await expect(page.getByRole('heading', { name: '数据分析', exact: true })).toBeVisible()
  await expect(page.getByTestId('storage-explorer').locator('canvas')).toBeVisible()
  await expect(page.getByText('项目数据扫描 · 已完成', { exact: true })).toBeVisible()
})

test('没有扫描结果时数据分析提供创建扫描的明确入口', async ({ page }) => {
  await setupApiMocks(page, { tasks: [] })
  await page.goto('/analysis')
  await expect(page.getByRole('heading', { name: '数据分析', exact: true })).toBeVisible()
  await expect(page.getByText('还没有扫描任务', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: '选择扫描目标', exact: true }).click()
  await expect(page).toHaveURL(/\/endpoints$/)
})

test('真实子目录预览可以切换层级且不会清除已标记条目', async ({ page }) => {
  const previewRequest = page.waitForRequest(
    (request) =>
      request.url().includes('/analysis/tree?') &&
      new URL(request.url()).searchParams.get('path') === 'projects' &&
      new URL(request.url()).searchParams.get('limit') === '32',
  )
  await page.goto('/analysis')
  await previewRequest
  const explorer = page.getByTestId('storage-explorer')
  await expect(explorer.locator('canvas')).toBeVisible()
  await explorer.locator('table').getByRole('button', { name: '标记 archive', exact: true }).click()
  await explorer.getByRole('button', { name: '显示一层目录', exact: true }).click()
  await expect(explorer.getByRole('button', { name: '复核已标记清单', exact: true })).toBeEnabled()
  await explorer.getByRole('button', { name: '显示两层目录', exact: true }).click()
  await expect(explorer.locator('canvas')).toBeVisible()
  await expect(explorer.getByRole('button', { name: '复核已标记清单', exact: true })).toBeEnabled()
})


test('任务详情仅显示概览和运行记录，旧查询参数也不会加载目录分析', async ({ page }) => {
  let requested = false
  page.on('request', request => { if (request.url().includes('/analysis/tree')) requested = true })
  await page.goto('/tasks/t-done?tab=analysis')
  await expect(page.getByRole('button', { name: '编辑', exact: true })).toBeVisible()
  await expect(page.locator('.n-tabs-tab')).toHaveCount(2)
  await expect(page.locator('.n-tabs-tab').filter({ hasText: '数据分析' })).toHaveCount(0)
  await expect(page.getByTestId('storage-explorer')).toHaveCount(0)
  expect(requested).toBe(false)
})
