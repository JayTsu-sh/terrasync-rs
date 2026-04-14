import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('../../api/tasks', () => ({
  listTasks: vi.fn().mockResolvedValue([]),
  deleteTask: vi.fn().mockResolvedValue(undefined),
}))

import { useTaskList } from '../useTaskList'
import { listTasks } from '../../api/tasks'
import type { MigrationTask } from '../../api/tasks'

function makeTask(overrides: Partial<MigrationTask> & { id: string; name: string }): MigrationTask {
  return {
    task_type: 'scan',
    source_endpoint_id: 'ep-1',
    config: {},
    status: 'pending',
    created_at: '2024-01-01T00:00:00Z',
    ...overrides,
  } as MigrationTask
}

describe('useTaskList - 搜索过滤', () => {
  let list: ReturnType<typeof useTaskList>

  const sampleTasks = [
    makeTask({ id: '1', name: '生产数据扫描', task_type: 'scan', status: 'completed' }),
    makeTask({ id: '2', name: '备份同步任务', task_type: 'sync', status: 'running' }),
    makeTask({ id: '3', name: 'NFS一致性检查', task_type: 'ace', status: 'pending' }),
  ]

  beforeEach(() => {
    list = useTaskList()
    list.tasks.value = sampleTasks
  })

  it('searchQuery 为空时返回全部任务', () => {
    list.searchQuery.value = ''
    expect(list.filteredTasks.value).toHaveLength(3)
  })

  it('按名称搜索能正确过滤', () => {
    list.searchQuery.value = '扫描'
    expect(list.filteredTasks.value).toHaveLength(1)
    expect(list.filteredTasks.value[0].name).toBe('生产数据扫描')
  })

  it('搜索大小写不敏感', () => {
    list.searchQuery.value = 'nfs'
    expect(list.filteredTasks.value).toHaveLength(1)
    expect(list.filteredTasks.value[0].name).toBe('NFS一致性检查')
  })

  it('搜索无结果时返回空数组', () => {
    list.searchQuery.value = '不存在的任务'
    expect(list.filteredTasks.value).toHaveLength(0)
  })

  it('搜索空白字符串等同于空搜索', () => {
    list.searchQuery.value = '   '
    expect(list.filteredTasks.value).toHaveLength(3)
  })

  it('清空搜索后恢复全部任务', () => {
    list.searchQuery.value = '扫描'
    expect(list.filteredTasks.value).toHaveLength(1)
    list.searchQuery.value = ''
    expect(list.filteredTasks.value).toHaveLength(3)
  })
})

describe('useTaskList - fetchTasks', () => {
  it('fetchTasks 正确设置 loading 状态并获取数据', async () => {
    const mockTasks = [makeTask({ id: '1', name: '测试任务' })]
    vi.mocked(listTasks).mockResolvedValueOnce(mockTasks)

    const list = useTaskList()
    expect(list.loading.value).toBe(false)

    const promise = list.fetchTasks()
    expect(list.loading.value).toBe(true)

    await promise
    expect(list.loading.value).toBe(false)
    expect(list.tasks.value).toEqual(mockTasks)
  })

  it('fetchTasks 失败时 loading 重置为 false 并抛出错误', async () => {
    vi.mocked(listTasks).mockRejectedValueOnce(new Error('网络错误'))

    const list = useTaskList()
    await expect(list.fetchTasks()).rejects.toThrow('网络错误')
    expect(list.loading.value).toBe(false)
  })
})
