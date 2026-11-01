import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import StorageExplorer from '../StorageExplorer.vue'
import { getStorageAnalysis, type StorageAnalysis } from '../../api/storage-analysis'

vi.mock('../../api/storage-analysis', () => ({ getStorageAnalysis: vi.fn() }))

const fixture: StorageAnalysis = {
  task_id: 'scan-1',
  path: '',
  source_path: '/data',
  scanned_at: '2026-10-31T00:00:00Z',
  is_partial: false,
  total_bytes: 2048,
  file_count: 3,
  directory_count: 1,
  children: [
    {
      path: 'archive',
      name: 'archive',
      kind: 'directory',
      bytes: 2048,
      file_count: 2,
      directory_count: 1,
      modified_at: null,
    },
    {
      path: 'empty.txt',
      name: 'empty.txt',
      kind: 'file',
      bytes: 0,
      file_count: 1,
      directory_count: 0,
      modified_at: null,
    },
  ],
}

const wrappers: ReturnType<typeof mount>[] = []
function render() {
  const wrapper = mount(StorageExplorer, {
    props: { taskId: 'scan-1' },
    global: { stubs: { StorageTreemap: { template: '<div data-testid="storage-treemap">treemap</div>' } } },
  })
  wrappers.push(wrapper)
  return wrapper
}

describe('StorageExplorer', () => {
  beforeEach(() => vi.resetAllMocks())
  afterEach(() => {
    wrappers.forEach((wrapper) => wrapper.unmount())
    wrappers.length = 0
  })

  it('shows loading without the result or error interface', () => {
    vi.mocked(getStorageAnalysis).mockReturnValue(new Promise(() => {}))
    const wrapper = render()
    expect(wrapper.text()).toContain('正在读取目录分析')
    expect(wrapper.find('[role="alert"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="storage-treemap"]').exists()).toBe(false)
  })

  it('shows the backend error with retry, without misleading empty results', async () => {
    vi.mocked(getStorageAnalysis).mockRejectedValue(new Error('任务扫描明细不存在，请重新扫描'))
    const wrapper = render()
    await flushPromises()
    expect(wrapper.text()).toContain('任务扫描明细不存在，请重新扫描')
    expect(wrapper.text()).toContain('重新加载')
    expect(wrapper.find('[data-testid="storage-treemap"]').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('正在读取目录分析')
  })

  it('renders theme-aligned cards, explicit scope and a keyboard-selectable zero-byte file', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue(fixture)
    const wrapper = render()
    await flushPromises()
    await nextTick()
    expect(wrapper.classes()).toEqual(expect.arrayContaining(['bg-white', 'rounded-lg', 'border-gray-100']))
    expect(wrapper.text()).toContain('最新扫描结果')
    expect(wrapper.text()).toContain('不代表物理磁盘占用或可释放空间')
    const row = wrapper.find('[aria-label="选择 empty.txt"]')
    expect(row.exists()).toBe(true)
    await row.trigger('keydown', { key: 'Enter' })
    expect(wrapper.text()).toContain('当前选择')
    expect(wrapper.find('[aria-label="标记 empty.txt"]').exists()).toBe(true)
    expect(wrapper.find('[aria-label="进入 empty.txt"]').exists()).toBe(false)
    expect(wrapper.find('[aria-label="进入 archive"]').exists()).toBe(true)
  })

  it('labels the current-item search and marks partial results explicitly', async () => {
    vi.mocked(getStorageAnalysis).mockResolvedValue({ ...fixture, is_partial: true })
    const wrapper = render()
    await flushPromises()
    expect(wrapper.text()).toContain('仅表示已观测数据')
    const input = wrapper.find('input[placeholder="搜索当前显示项"]')
    await input.setValue('empty')
    expect(wrapper.text()).toContain('仅筛选当前已加载的 2 项，匹配 1 项')
  })
})
