import { beforeEach, describe, expect, it, vi } from 'vitest'
import { effectScope, nextTick, ref } from 'vue'
import type { AnalysisNode, StorageAnalysis } from '../../api/storage-analysis'

vi.mock('../../api/storage-analysis', () => ({ getStorageAnalysis: vi.fn() }))
import { getStorageAnalysis } from '../../api/storage-analysis'
import { useStorageExplorer } from '../useStorageExplorer'

const directory: AnalysisNode = {
  path: 'archive',
  name: 'archive',
  kind: 'directory',
  bytes: 2048,
  file_count: 2,
  directory_count: 1,
  modified_at: null,
}
const file: AnalysisNode = {
  path: 'archive/a.txt',
  name: 'a.txt',
  kind: 'file',
  bytes: 1024,
  file_count: 1,
  directory_count: 0,
  modified_at: null,
}
const result = (path = '', children = [directory]): StorageAnalysis => ({
  task_id: 'task-1',
  path,
  source_path: '/data',
  scanned_at: '2026-10-31T00:00:00Z',
  is_partial: false,
  total_bytes: 2048,
  file_count: 2,
  directory_count: 1,
  children,
})

describe('useStorageExplorer', () => {
  beforeEach(() => vi.resetAllMocks())

  it('only requests an active task and resets navigation and marks on task switch', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue(result())
    const taskId = ref('task-1')
    const active = ref(false)
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(taskId, active))!
    await nextTick()
    expect(getStorageAnalysis).not.toHaveBeenCalled()
    active.value = true
    await vi.waitFor(() => expect(explorer.data.value).not.toBeNull())
    explorer.toggleMark(directory)
    await explorer.navigate('archive')
    taskId.value = 'task-2'
    await nextTick()
    expect(explorer.marks.value).toEqual([])
    expect(explorer.path.value).toBe('')
    scope.stop()
  })

  it('absorbs child marks into a parent without confusing sibling prefixes', () => {
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref(''), ref(false)))!
    explorer.toggleMark(file)
    explorer.toggleMark({ ...file, path: 'archive2/a.txt' })
    explorer.toggleMark(directory)
    expect(explorer.marks.value.map((n) => n.path)).toEqual(['archive2/a.txt', 'archive'])
    expect(explorer.markedBytes.value).toBe(3072)
    expect(explorer.coveringMark(file)?.path).toBe('archive')
    explorer.toggleMark(file)
    expect(explorer.marks.value).toHaveLength(2)
    explorer.toggleMark(directory)
    expect(explorer.marks.value).toHaveLength(1)
    scope.stop()
  })

  it('never selects or opens an aggregate bucket and keeps zero-byte files in the list', async () => {
    const other: AnalysisNode = { ...directory, path: '', name: '其他', kind: 'other' }
    vi.mocked(getStorageAnalysis).mockResolvedValue(result('', [other, { ...file, bytes: 0 }]))
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref('task-1'), ref(true)))!
    await vi.waitFor(() => expect(explorer.data.value).not.toBeNull())
    explorer.toggleMark(other)
    await explorer.openNode(other)
    expect(explorer.marks.value).toEqual([])
    expect(explorer.visibleNodes.value).toHaveLength(2)
    expect(getStorageAnalysis).toHaveBeenCalledTimes(1)
    scope.stop()
  })

  it('keeps an S3 object distinct from a virtual directory with the same path', () => {
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref(''), ref(false)))!
    const object = { ...file, path: directory.path, name: directory.name }
    explorer.toggleMark(directory)
    explorer.toggleMark(object)
    expect(explorer.marks.value).toHaveLength(2)
    expect(explorer.markedBytes.value).toBe(directory.bytes + object.bytes)
    explorer.toggleMark(directory)
    expect(explorer.marks.value).toEqual([object])
    scope.stop()
  })

  it('navigates back and forward without adding duplicate history and requests count Top N', async () => {
    vi.mocked(getStorageAnalysis).mockImplementation(async (_id, query) => result(query.path))
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref('task-1'), ref(true)))!
    await vi.waitFor(() => expect(explorer.data.value).not.toBeNull())
    await explorer.navigate('archive')
    await explorer.navigate('archive/sub')
    await explorer.back()
    expect(explorer.path.value).toBe('archive')
    await explorer.forward()
    expect(explorer.path.value).toBe('archive/sub')
    explorer.mode.value = 'count'
    await nextTick()
    await vi.waitFor(() =>
      expect(getStorageAnalysis).toHaveBeenLastCalledWith(
        'task-1',
        expect.objectContaining({ metric: 'count', path: 'archive/sub' }),
        expect.anything(),
      ),
    )
    scope.stop()
  })

  it('ignores stale responses and keeps error/loading/data states exclusive', async () => {
    let finishOld: (value: StorageAnalysis) => void = () => {}
    vi.mocked(getStorageAnalysis)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishOld = resolve
          }),
      )
      .mockResolvedValueOnce(result('archive'))
      .mockRejectedValueOnce(new Error('扫描明细不存在'))
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref('task-1'), ref(true)))!
    await explorer.navigate('archive')
    finishOld(result())
    await nextTick()
    expect(explorer.data.value?.path).toBe('archive')
    await explorer.reload()
    expect(explorer.error.value).toBe('扫描明细不存在')
    expect(explorer.loading.value).toBe(false)
    expect(explorer.data.value).toBeNull()
    scope.stop()
  })

  it('filters only loaded items while retaining unfiltered totals and exports source identity', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue(result('', [directory, file]))
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref('task-1'), ref(true)))!
    await vi.waitFor(() => expect(explorer.data.value).not.toBeNull())
    explorer.search.value = 'A.TXT'
    expect(explorer.visibleNodes.value.map((n) => n.name)).toEqual(['a.txt'])
    expect(explorer.data.value?.total_bytes).toBe(2048)
    explorer.toggleMark(directory)
    const exported = JSON.parse(explorer.exportSelection())
    expect(exported.source_path).toBe('/data')
    expect(exported.task_id).toBe('task-1')
    expect(exported.paths[0].path).toBe('archive')
    scope.stop()
  })

  it('retains selection provenance while a refresh is loading', async () => {
    vi.mocked(getStorageAnalysis)
      .mockResolvedValueOnce(result())
      .mockReturnValueOnce(new Promise(() => {}))
    const scope = effectScope()
    const explorer = scope.run(() => useStorageExplorer(ref('task-1'), ref(true)))!
    await vi.waitFor(() => expect(explorer.data.value).not.toBeNull())
    explorer.toggleMark(directory)
    void explorer.reload()
    const exported = JSON.parse(explorer.exportSelection())
    expect(exported.source_path).toBe('/data')
    expect(exported.scanned_at).toBe('2026-10-31T00:00:00Z')
    expect(exported.is_partial).toBe(false)
    scope.stop()
  })
})
