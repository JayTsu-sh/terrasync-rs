import { Page, Route } from '@playwright/test'
import {
  mockEndpoints, mockPaths, mockAllTasks, mockExecutions, mockConfig,
  mockFilterFields, mockTaskLogs,
  type Endpoint, type MigrationTask, type MigrationPath, type TaskExecution,
} from './mock-data'

export interface MockOverrides {
  endpoints?: Endpoint[]
  tasks?: MigrationTask[]
  paths?: MigrationPath[]
  executions?: TaskExecution[]
  /** 按 endpoint id 返回对应 tasks（用于 EndpointDetail 扫描场景） */
  tasksByEndpoint?: Record<string, MigrationTask[]>
}

/**
 * 为页面安装全量 API Mock。
 * 所有 /api/v1/* 请求都会被拦截，返回 mock 数据。
 */
export async function setupApiMocks(page: Page, overrides?: MockOverrides) {
  const endpoints = overrides?.endpoints ?? mockEndpoints
  const tasks = overrides?.tasks ?? mockAllTasks
  const paths = overrides?.paths ?? mockPaths
  const executions = overrides?.executions ?? mockExecutions

  // ---- Endpoints ----
  await page.route('**/api/v1/endpoints', async (route: Route) => {
    const method = route.request().method()
    if (method === 'GET') {
      await route.fulfill({ json: endpoints })
    } else if (method === 'POST') {
      const body = JSON.parse(route.request().postData() || '{}')
      await route.fulfill({
        json: {
          id: 'new-ep-' + Date.now(),
          endpoint_type_display: body.endpoint_type,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          ...body,
        },
      })
    } else {
      await route.continue()
    }
  })

  // 单个 endpoint GET / PUT / DELETE
  await page.route(/\/api\/v1\/endpoints\/[^/]+$/, async (route: Route) => {
    const method = route.request().method()
    const url = route.request().url()
    const id = url.split('/endpoints/')[1]?.split('?')[0]
    const ep = endpoints.find(e => e.id === id)

    if (method === 'GET') {
      await route.fulfill({ json: ep ?? endpoints[0] })
    } else if (method === 'PUT') {
      await route.fulfill({ json: { endpoint: { ...ep, ...JSON.parse(route.request().postData() || '{}') }, needs_confirm: false, conflicting_tasks: [] } })
    } else if (method === 'DELETE') {
      await route.fulfill({ status: 200, json: {} })
    } else {
      await route.continue()
    }
  })

  // Endpoint test connection
  await page.route(/\/api\/v1\/endpoints\/[^/]+\/test$/, async (route: Route) => {
    await route.fulfill({ json: 'ok' })
  })

  // ---- Paths ----
  await page.route(/\/api\/v1\/endpoints\/[^/]+\/paths/, async (route: Route) => {
    const method = route.request().method()
    const url = route.request().url()
    const epId = url.match(/endpoints\/([^/]+)\/paths/)?.[1]

    if (method === 'GET') {
      await route.fulfill({ json: paths.filter(p => p.endpoint_id === epId) })
    } else if (method === 'POST') {
      const body = JSON.parse(route.request().postData() || '{}')
      await route.fulfill({
        json: { id: 'new-path-' + Date.now(), endpoint_id: epId, created_at: new Date().toISOString(), ...body },
      })
    } else {
      await route.continue()
    }
  })

  // ---- Tasks ----
  await page.route(/\/api\/v1\/tasks(\?|$)/, async (route: Route) => {
    const method = route.request().method()
    if (method === 'GET') {
      const url = new URL(route.request().url())
      const statusFilter = url.searchParams.get('status')
      const typeFilter = url.searchParams.get('task_type')
      let filtered = tasks
      if (statusFilter) filtered = filtered.filter(t => t.status === statusFilter)
      if (typeFilter) filtered = filtered.filter(t => t.task_type === typeFilter)

      // 支持按 endpoint 过滤（EndpointDetail 场景）
      if (overrides?.tasksByEndpoint) {
        // 如果有 endpoint 维度的 override，按 source_endpoint_id 匹配
        const allByEp = Object.values(overrides.tasksByEndpoint).flat()
        if (typeFilter) {
          await route.fulfill({ json: allByEp.filter(t => t.task_type === typeFilter) })
          return
        }
        await route.fulfill({ json: allByEp })
        return
      }

      await route.fulfill({ json: filtered })
    } else if (method === 'POST') {
      const body = JSON.parse(route.request().postData() || '{}')
      await route.fulfill({
        json: {
          id: 'new-task-' + Date.now(),
          status: 'pending',
          created_at: new Date().toISOString(),
          config: {},
          ...body,
        },
      })
    } else {
      await route.continue()
    }
  })

  // 单个 task
  await page.route(/\/api\/v1\/tasks\/[^/]+$/, async (route: Route) => {
    const method = route.request().method()
    const url = route.request().url()
    const id = url.split('/tasks/')[1]?.split('?')[0]
    const task = tasks.find(t => t.id === id)

    if (method === 'GET') {
      await route.fulfill({ json: task ?? tasks[0] })
    } else if (method === 'PUT') {
      const body = JSON.parse(route.request().postData() || '{}')
      await route.fulfill({ json: { ...task, ...body, config: { ...task?.config, ...body.config } } })
    } else if (method === 'DELETE') {
      await route.fulfill({ status: 200, json: {} })
    } else {
      await route.continue()
    }
  })

  // Task start / cancel
  await page.route(/\/api\/v1\/tasks\/[^/]+\/(start|cancel)$/, async (route: Route) => {
    await route.fulfill({ json: 'ok' })
  })

  // Task progress (real-time polling)
  await page.route(/\/api\/v1\/tasks\/[^/]+\/progress$/, async (route: Route) => {
    await route.fulfill({
      json: { scanned_files: 1200, scanned_dirs: 50, total_bytes: 536870912, error_count: 0, elapsed_secs: 30, speed_bytes_per_sec: 17895697 },
    })
  })

  // Task executions
  await page.route(/\/api\/v1\/tasks\/[^/]+\/executions$/, async (route: Route) => {
    await route.fulfill({ json: executions })
  })

  // Execution logs
  await page.route(/\/api\/v1\/executions\/[^/]+\/logs/, async (route: Route) => {
    const url = new URL(route.request().url())
    const level = url.searchParams.get('level')
    let filtered = mockTaskLogs
    if (level) filtered = filtered.filter(l => l.level === level)
    await route.fulfill({ json: filtered })
  })

  // Single execution DELETE
  await page.route(/\/api\/v1\/executions\/[^/]+$/, async (route: Route) => {
    const method = route.request().method()
    if (method === 'DELETE') {
      await route.fulfill({ status: 200, json: {} })
    } else {
      await route.continue()
    }
  })

  // Task analytics
  await page.route(/\/api\/v1\/tasks\/[^/]+\/analytics$/, async (route: Route) => {
    await route.fulfill({
      json: {
        file_type_distribution: [
          { ext: '.jpg', count: 5000, total_size: 524288000 },
          { ext: '.txt', count: 8000, total_size: 104857600 },
        ],
        size_distribution: [
          { range: '0-1KB', count: 3000, total_size: 1048576 },
          { range: '1KB-1MB', count: 7000, total_size: 209715200 },
        ],
        time_distribution: [
          { bucket: '最近7天', count: 2000 },
          { bucket: '7-30天', count: 5000 },
        ],
      },
    })
  })

  // ---- Filter Fields ----
  await page.route('**/api/v1/filter-fields', async (route: Route) => {
    await route.fulfill({ json: { fields: mockFilterFields } })
  })

  // ---- FS / NFS ----
  await page.route('**/api/v1/fs/list-dirs', async (route: Route) => {
    await route.fulfill({
      json: { parent: '/', entries: [{ name: 'data', full_path: '/data' }], sep: '/' },
    })
  })

  await page.route('**/api/v1/nfs/exports', async (route: Route) => {
    await route.fulfill({
      json: [{ path: '/data', groups: ['*'] }, { path: '/backup', groups: ['10.0.0.0/8'] }],
    })
  })

  // ---- Config ----
  await page.route('**/api/v1/config', async (route: Route) => {
    const method = route.request().method()
    if (method === 'GET') {
      await route.fulfill({ json: mockConfig })
    } else if (method === 'PUT') {
      await route.fulfill({ status: 200, json: {} })
    } else {
      await route.continue()
    }
  })

  await page.route('**/api/v1/config/test-clickhouse', async (route: Route) => {
    await route.fulfill({ json: { success: true, message: '连接成功' } })
  })

  // ---- WebSocket (静默忽略) ----
  // 注意：不 abort，直接 fulfill 空响应，避免干扰页面加载
  await page.route('**/api/v1/ws', async (route: Route) => {
    await route.fulfill({ status: 200, body: '' })
  })
}
