import { computed, onScopeDispose, ref, watch, type Ref } from 'vue'
import { getStorageAnalysis, type AnalysisNode, type StorageAnalysis } from '../api/storage-analysis'

export type ExplorerMode = 'size' | 'count' | 'age'

export function containsAnalysisNode(parent: AnalysisNode, child: AnalysisNode) {
  return (
    (parent.path === child.path && parent.kind === child.kind) ||
    (parent.kind === 'directory' && child.path.startsWith(`${parent.path}/`))
  )
}

export function useStorageExplorer(taskId: Ref<string>, active: Ref<boolean>) {
  const data = ref<StorageAnalysis | null>(null)
  const loading = ref(false)
  const error = ref('')
  const mode = ref<ExplorerMode>('size')
  const path = ref('')
  const search = ref('')
  const selected = ref<AnalysisNode | null>(null)
  const marks = ref<AnalysisNode[]>([])
  const provenance = ref<Pick<StorageAnalysis, 'source_path' | 'scanned_at' | 'is_partial'> | null>(null)
  const history = ref([''])
  const historyIndex = ref(0)
  let requestVersion = 0
  let controller: AbortController | null = null
  let sourceIdentity = ''

  const metric = computed(() => (mode.value === 'count' ? 'count' : 'size'))
  const canBack = computed(() => historyIndex.value > 0)
  const canForward = computed(() => historyIndex.value < history.value.length - 1)
  const visibleNodes = computed(() => {
    const query = search.value.trim().toLocaleLowerCase()
    return (data.value?.children ?? []).filter((node) => !query || node.name.toLocaleLowerCase().includes(query))
  })
  const markedBytes = computed(() => marks.value.reduce((sum, node) => sum + node.bytes, 0))
  const markedFiles = computed(() => marks.value.reduce((sum, node) => sum + node.file_count, 0))
  const breadcrumbs = computed(() => {
    const parts = path.value.split('/').filter(Boolean)
    return [
      { name: '扫描根目录', path: '' },
      ...parts.map((name, index) => ({ name, path: parts.slice(0, index + 1).join('/') })),
    ]
  })

  async function reload() {
    controller?.abort()
    const version = ++requestVersion
    if (!taskId.value || !active.value) {
      loading.value = false
      return
    }
    controller = new AbortController()
    loading.value = true
    error.value = ''
    data.value = null
    selected.value = null
    try {
      const result = await getStorageAnalysis(
        taskId.value,
        { path: path.value, metric: metric.value, limit: 120 },
        controller.signal,
      )
      if (version !== requestVersion) return
      const identity = `${result.source_path}\n${result.scanned_at ?? ''}`
      if (sourceIdentity && sourceIdentity !== identity) marks.value = []
      sourceIdentity = identity
      provenance.value = {
        source_path: result.source_path,
        scanned_at: result.scanned_at,
        is_partial: result.is_partial,
      }
      data.value = result
    } catch (reason) {
      if (version !== requestVersion) return
      error.value = reason instanceof Error ? reason.message : '加载目录分析失败'
    } finally {
      if (version === requestVersion) loading.value = false
    }
  }

  async function navigate(nextPath: string) {
    if (nextPath !== path.value) {
      history.value = [...history.value.slice(0, historyIndex.value + 1), nextPath]
      historyIndex.value = history.value.length - 1
      path.value = nextPath
    }
    search.value = ''
    await reload()
  }

  async function openNode(node: AnalysisNode) {
    if (node.kind === 'directory') await navigate(node.path)
  }

  async function back() {
    if (!canBack.value) return
    historyIndex.value -= 1
    path.value = history.value[historyIndex.value] ?? ''
    search.value = ''
    await reload()
  }

  async function forward() {
    if (!canForward.value) return
    historyIndex.value += 1
    path.value = history.value[historyIndex.value] ?? ''
    search.value = ''
    await reload()
  }

  function coveringMark(node: AnalysisNode) {
    return marks.value.find((mark) => containsAnalysisNode(mark, node))
  }

  function toggleMark(node: AnalysisNode) {
    if (node.kind === 'other') return
    const covering = coveringMark(node)
    if (covering) {
      if (covering.path === node.path)
        marks.value = marks.value.filter((mark) => mark.path !== node.path || mark.kind !== node.kind)
      return
    }
    marks.value = [...marks.value.filter((mark) => !containsAnalysisNode(node, mark)), node]
  }

  function exportSelection() {
    return JSON.stringify(
      {
        task_id: taskId.value,
        source_path: provenance.value?.source_path ?? '',
        scanned_at: provenance.value?.scanned_at ?? null,
        is_partial: provenance.value?.is_partial ?? true,
        exported_at: new Date().toISOString(),
        size_basis: 'logical_bytes',
        total_bytes: markedBytes.value,
        file_count: markedFiles.value,
        paths: marks.value,
      },
      null,
      2,
    )
  }

  watch(
    [taskId, active],
    ([id], [oldId] = ['', false]) => {
      if (id !== oldId) {
        path.value = ''
        history.value = ['']
        historyIndex.value = 0
        marks.value = []
        selected.value = null
        search.value = ''
        data.value = null
        provenance.value = null
        sourceIdentity = ''
      }
      void reload()
    },
    { immediate: true },
  )
  watch(metric, () => {
    void reload()
  })
  onScopeDispose(() => {
    requestVersion += 1
    controller?.abort()
  })

  return {
    data,
    loading,
    error,
    mode,
    path,
    search,
    selected,
    marks,
    canBack,
    canForward,
    visibleNodes,
    markedBytes,
    markedFiles,
    breadcrumbs,
    reload,
    navigate,
    openNode,
    back,
    forward,
    coveringMark,
    toggleMark,
    exportSelection,
  }
}
