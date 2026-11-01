import { afterEach, describe, expect, it, vi } from 'vitest'
import { effectScope } from 'vue'
import type { MigrationTask } from '../../api/tasks'

vi.mock('../../api/endpoints', () => ({ getEndpoint: vi.fn(), testConnection: vi.fn() }))
vi.mock('../../api/tasks', () => ({
  createTask: vi.fn(), startTask: vi.fn(), deleteTask: vi.fn(), cancelTask: vi.fn(),
  listTasks: vi.fn(), listExecutions: vi.fn(), getTaskProgress: vi.fn(),
}))
vi.mock('../../api/paths', () => ({ createPath: vi.fn(), listPaths: vi.fn() }))
vi.mock('../../api/analytics', () => ({ getTaskAnalytics: vi.fn() }))
import { getEndpoint } from '../../api/endpoints'
import { startTask, createTask, deleteTask, listTasks, listExecutions } from '../../api/tasks'
import { listPaths } from '../../api/paths'
import { useEndpointDetail } from '../useEndpointDetail'

const scopes: ReturnType<typeof effectScope>[] = []
afterEach(() => { scopes.splice(0).forEach(s => s.stop()); vi.resetAllMocks() })

describe('endpoint resumable scan', () => {
  it.each(['cancelled', 'failed', 'completed'] as const)('retains scan identity and stored results for %s', async status => {
    const task: MigrationTask = {
      id: 'scan-keep', name: 'scan', task_type: 'scan', status, source_endpoint_id: 'source',
      config: {}, created_at: '2026-11-01T00:00:00Z',
    }
    vi.mocked(getEndpoint).mockResolvedValue({ id: 'source', name: 'source', config: { type: 'local', path: '/data' } } as any)
    vi.mocked(listTasks).mockResolvedValue([task])
    vi.mocked(listExecutions).mockResolvedValue([])
    vi.mocked(listPaths).mockResolvedValue([{ id: 'path', endpoint_id: 'source', sub_path: '/', created_at: '' }])
    const scope = effectScope(); scopes.push(scope)
    const detail = scope.run(() => useEndpointDetail('source'))!
    await detail.fetchData()
    await detail.handleScan()
    expect(startTask).toHaveBeenCalledWith('scan-keep')
    expect(deleteTask).not.toHaveBeenCalled()
    expect(createTask).not.toHaveBeenCalled()
  })
})
