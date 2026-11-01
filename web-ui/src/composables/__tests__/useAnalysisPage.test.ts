import { describe, it, expect, vi, afterEach } from 'vitest'
import { effectScope, nextTick, ref } from 'vue'
import type { MigrationTask } from '../../api/tasks'

vi.mock('../../api/tasks', () => ({ listTasks: vi.fn() }))
import { listTasks } from '../../api/tasks'
import { useAnalysisPage } from '../useAnalysisPage'

const completed: MigrationTask = {
  id: 'completed',
  name: '完成扫描',
  task_type: 'scan',
  status: 'completed',
  source_endpoint_id: 'source',
  config: {},
  created_at: '2026-10-30T00:00:00Z',
}
const running: MigrationTask = {
  ...completed,
  id: 'running',
  name: '正在扫描',
  status: 'running',
  created_at: '2026-10-31T00:00:00Z',
}
const scopes: ReturnType<typeof effectScope>[] = []
function setup(requested = ref('')) {
  const scope = effectScope()
  scopes.push(scope)
  return scope.run(() => useAnalysisPage(requested))!
}
afterEach(() => {
  scopes.splice(0).forEach((scope) => scope.stop())
  vi.resetAllMocks()
})

describe('useAnalysisPage', () => {
  it('defaults to a completed scan and excludes migration tasks', async () => {
    vi.mocked(listTasks).mockResolvedValue([running, { ...completed, id: 'sync', task_type: 'sync' }, completed])
    const page = setup()
    await page.fetchTasks()
    expect(listTasks).toHaveBeenCalledWith({ task_type: 'scan' })
    expect(page.tasks.value.map((task) => task.id)).toEqual(['running', 'completed'])
    expect(page.selectedId.value).toBe('completed')
  })

  it('honors a requested scan and follows browser query changes', async () => {
    vi.mocked(listTasks).mockResolvedValue([running, completed])
    const requested = ref('running')
    const page = setup(requested)
    await page.fetchTasks()
    expect(page.selectedTask.value?.id).toBe('running')
    requested.value = 'completed'
    await nextTick()
    expect(page.selectedTask.value?.id).toBe('completed')
  })

  it('retains a selection on refresh and falls back when it disappears', async () => {
    vi.mocked(listTasks).mockResolvedValueOnce([completed, running]).mockResolvedValueOnce([running])
    const page = setup()
    await page.fetchTasks()
    await page.fetchTasks()
    expect(page.selectedId.value).toBe('running')
  })

  it('clears loading on failure and lets the view display the error', async () => {
    vi.mocked(listTasks).mockRejectedValue(new Error('无法读取任务'))
    const page = setup()
    await expect(page.fetchTasks()).rejects.toThrow('无法读取任务')
    expect(page.loading.value).toBe(false)
  })
})
