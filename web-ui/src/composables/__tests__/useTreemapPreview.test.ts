import { afterEach, describe, expect, it, vi } from 'vitest'
import { effectScope, nextTick, ref } from 'vue'
import type { AnalysisNode, StorageAnalysis } from '../../api/storage-analysis'

vi.mock('../../api/storage-analysis', () => ({ getStorageAnalysis: vi.fn() }))
import { getStorageAnalysis } from '../../api/storage-analysis'
import { useTreemapPreview } from '../useTreemapPreview'

const directory: AnalysisNode = {
  path: 'alpha',
  name: 'alpha',
  kind: 'directory',
  bytes: 100,
  file_count: 4,
  directory_count: 2,
  modified_at: null,
}
const nested: AnalysisNode = { ...directory, path: 'alpha/data', name: 'data', bytes: 100, directory_count: 1 }
const result = (path = '', children = [directory]): StorageAnalysis => ({
  task_id: 'scan',
  path,
  source_path: '/data',
  scanned_at: '2026-10-31T00:00:00Z',
  is_partial: false,
  total_bytes: 100,
  file_count: 4,
  directory_count: 2,
  children,
})
const scopes: ReturnType<typeof effectScope>[] = []
function setup(data = ref<StorageAnalysis | null>(result()), enabled = ref(true)) {
  const scope = effectScope()
  scopes.push(scope)
  return {
    data,
    enabled,
    preview: scope.run(() => useTreemapPreview(ref('scan'), data, ref<'size' | 'count'>('size'), enabled))!,
  }
}
afterEach(() => {
  scopes.splice(0).forEach((scope) => scope.stop())
  vi.resetAllMocks()
})

describe('useTreemapPreview', () => {
  it('enriches a directory with real children while preserving its measured totals', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue(result('alpha', [nested]))
    const { preview } = setup()
    await vi.waitFor(() => expect(preview.nodes.value[0]?.children).toEqual([nested]))
    expect(preview.nodes.value[0]?.bytes).toBe(100)
    expect(getStorageAnalysis).toHaveBeenCalledWith(
      'scan',
      { path: 'alpha', metric: 'size', limit: 32 },
      expect.anything(),
    )
  })

  it('retains the current layer when previews are disabled or cannot be loaded', async () => {
    vi.mocked(getStorageAnalysis).mockRejectedValue(new Error('unavailable'))
    const { enabled, preview } = setup(undefined, ref(false))
    await nextTick()
    expect(getStorageAnalysis).not.toHaveBeenCalled()
    enabled.value = true
    await vi.waitFor(() => expect(preview.loading.value).toBe(false))
    expect(preview.nodes.value).toEqual([directory])
  })

  it('discards previews from another snapshot or with different measured totals', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue({
      ...result('alpha', [{ ...nested, bytes: 90 }]),
      scanned_at: '2026-10-30T00:00:00Z',
    })
    const { preview } = setup()
    await vi.waitFor(() => expect(preview.loading.value).toBe(false))
    expect(preview.nodes.value[0]?.children).toBeUndefined()
  })

  it('limits prefetch to six significant directories and ignores files and summary buckets', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue(result('no-match', []))
    const directories = Array.from({ length: 20 }, (_, i) => ({ ...directory, path: `dir${i}`, name: `dir${i}` }))
    setup(
      ref(
        result('', [
          ...directories,
          { ...directory, kind: 'other', path: '' },
          { ...directory, kind: 'file', path: 'file' },
        ]),
      ),
    )
    await vi.waitFor(() => expect(getStorageAnalysis).toHaveBeenCalledTimes(6))
  })
})
