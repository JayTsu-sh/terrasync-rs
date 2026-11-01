import { computed, onScopeDispose, ref, watch, type Ref } from 'vue'
import { getStorageAnalysis, type AnalysisNode, type StorageAnalysis } from '../api/storage-analysis'

/** 只为最多六个主要目录加载一层预览；失败时保留已测量的父级。 */
export function useTreemapPreview(
  taskId: Ref<string>,
  data: Ref<StorageAnalysis | null>,
  metric: Ref<'size' | 'count'>,
  enabled: Ref<boolean>,
) {
  const children = ref(new Map<string, AnalysisNode[]>())
  const loading = ref(false)
  let version = 0
  let controller: AbortController | null = null
  const nodes = computed(() =>
    (data.value?.children ?? []).map((node) => ({
      ...node,
      ...(children.value.has(node.path) && node.kind === 'directory'
        ? { children: children.value.get(node.path) }
        : {}),
    })),
  )

  watch(
    [taskId, data, metric, enabled],
    async () => {
      controller?.abort()
      const request = ++version
      children.value = new Map()
      loading.value = false
      const snapshot = data.value
      if (!enabled.value || !snapshot || !taskId.value) return
      const currentMetric = metric.value
      const weight = (node: AnalysisNode) => (currentMetric === 'count' ? node.file_count : node.bytes)
      const total = currentMetric === 'count' ? snapshot.file_count : snapshot.total_bytes
      const candidates = snapshot.children
        .filter((node) => node.kind === 'directory' && weight(node) > 0 && weight(node) >= total * 0.02)
        .sort((a, b) => weight(b) - weight(a))
        .slice(0, 6)
      if (!candidates.length) return
      controller = new AbortController()
      const signal = controller.signal
      loading.value = true
      const results = await Promise.all(
        candidates.map(async (node) => {
          try {
            const result = await getStorageAnalysis(
              taskId.value,
              { path: node.path, metric: currentMetric, limit: 32 },
              signal,
            )
            const sameSnapshot =
              result.task_id === snapshot.task_id &&
              result.path === node.path &&
              result.source_path === snapshot.source_path &&
              result.scanned_at === snapshot.scanned_at &&
              result.is_partial === snapshot.is_partial
            const sameTotals =
              result.children.reduce((sum, item) => sum + item.bytes, 0) === node.bytes &&
              result.children.reduce((sum, item) => sum + item.file_count, 0) === node.file_count
            if (!sameSnapshot || !sameTotals || !result.children.length) return null
            return [node.path, result.children] as const
          } catch {
            return null
          }
        }),
      )
      if (request !== version) return
      children.value = new Map(results.filter((value): value is NonNullable<typeof value> => value !== null))
      loading.value = false
    },
    { immediate: true },
  )

  onScopeDispose(() => {
    version += 1
    controller?.abort()
  })
  return { nodes, loading }
}
