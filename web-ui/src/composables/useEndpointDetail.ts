import { ref, computed, watch } from 'vue'
import type { Endpoint } from '../api/endpoints'
import { getEndpoint, testConnection } from '../api/endpoints'
import { createTask, startTask, deleteTask, cancelTask, listTasks, listExecutions, getTaskProgress } from '../api/tasks'
import type { MigrationTask, TaskExecution, ExecutionStats, TaskProgressSnapshot } from '../api/tasks'
import { createPath, listPaths } from '../api/paths'
import { getTaskAnalytics } from '../api/analytics'
import type { AnalyticsData } from '../api/analytics'
import { statusLabelMap as defaultStatusLabelMap } from '../shared/task-rules'
import { getEndpointTypeInfo, getEndpointAddress } from '../shared/endpoint-display'
import { usePolling } from './usePolling'

export function useEndpointDetail(id: string) {
  const endpoint = ref<Endpoint | null>(null)
  const testing = ref(false)
  const connectivityStatus = ref<'testing' | 'ok' | 'fail'>('testing')
  const showEditModal = ref(false)

  // Computed
  const isLocal = computed(() => endpoint.value?.config.type === 'local')
  const isNfs = computed(() => endpoint.value?.config.type === 'nfs')
  const isS3 = computed(() => endpoint.value?.config.type === 's3')

  const typeInfo = computed(() => {
    if (!endpoint.value) return getEndpointTypeInfo('')
    return getEndpointTypeInfo(endpoint.value.endpoint_type)
  })

  const addressDisplay = computed(() => {
    if (!endpoint.value) return ''
    return getEndpointAddress(endpoint.value, { includePrefix: true })
  })

  // Scan state
  const scanning = ref(false)
  const cancelling = ref(false)
  const latestScanTask = ref<MigrationTask | null>(null)
  const latestExecution = ref<TaskExecution | null>(null)
  const analyticsData = ref<AnalyticsData | null>(null)
  const loadingAnalytics = ref(false)
  const liveProgress = ref<TaskProgressSnapshot | null>(null)

  const isScanning = computed(() => latestScanTask.value?.status === 'running')
  const scanStats = computed<ExecutionStats | null>(() => latestExecution.value?.stats || null)

  const scanStatusLabelMap: Record<string, string> = {
    ...defaultStatusLabelMap,
    running: '扫描中',
    cancelled: '已取消',
  }

  const polling = usePolling(fetchScanInfo, 3000)

  watch(latestScanTask, (task) => {
    if (task?.status === 'running') polling.start()
    else polling.stop()
  })

  async function fetchScanInfo() {
    try {
      const allTasks = await listTasks()
      const endpointTasks = allTasks
        .filter((t) => t.source_endpoint_id === id)
        .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())

      latestScanTask.value = endpointTasks[0] || null

      if (latestScanTask.value) {
        const execs = await listExecutions(latestScanTask.value.id)
        latestExecution.value = execs[0] || null

        if (latestScanTask.value.status === 'running') {
          try {
            liveProgress.value = await getTaskProgress(latestScanTask.value.id)
          } catch (e) {
            console.warn('获取实时进度失败:', e)
          }
        } else {
          liveProgress.value = null
        }

        if (latestScanTask.value.status === 'completed' && !analyticsData.value) {
          loadingAnalytics.value = true
          try {
            analyticsData.value = await getTaskAnalytics(latestScanTask.value.id)
          } catch (e) {
            console.warn('加载分析数据失败:', e)
          }
          loadingAnalytics.value = false
        }
      }
    } catch (e) {
      console.warn('刷新扫描信息失败:', e)
    }
  }

  async function fetchData() {
    endpoint.value = await getEndpoint(id)
    checkConnectivity()
    await fetchScanInfo()
  }

  async function onEditSuccess() {
    await fetchData()
  }

  async function checkConnectivity() {
    connectivityStatus.value = 'testing'
    try {
      await testConnection(id)
      connectivityStatus.value = 'ok'
    } catch {
      connectivityStatus.value = 'fail'
    }
  }

  async function handleTest() {
    testing.value = true
    try {
      await testConnection(id)
      connectivityStatus.value = 'ok'
    } catch (e) {
      connectivityStatus.value = 'fail'
      throw e
    } finally {
      testing.value = false
    }
  }

  async function handleScan() {
    if (!endpoint.value || scanning.value) return
    scanning.value = true
    try {
      const paths = await listPaths(id)
      let path = paths.find((p) => p.sub_path === '/')
      if (!path) path = await createPath(id, { sub_path: '/' })

      if (latestScanTask.value && !['pending', 'running'].includes(latestScanTask.value.status)) {
        await deleteTask(latestScanTask.value.id)
        latestScanTask.value = null
      }

      if (latestScanTask.value && latestScanTask.value.status === 'pending') {
        await startTask(latestScanTask.value.id)
      } else {
        const task = await createTask({
          name: `扫描-${endpoint.value.name}`,
          task_type: 'scan',
          source_endpoint_id: id,
          source_path_id: path.id,
        })
        await startTask(task.id)
      }
      analyticsData.value = null
      await fetchScanInfo()
    } finally {
      scanning.value = false
    }
  }

  async function handleCancelScan() {
    if (!latestScanTask.value || cancelling.value) return
    cancelling.value = true
    try {
      await cancelTask(latestScanTask.value.id)
      await fetchScanInfo()
    } finally {
      cancelling.value = false
    }
  }

  return {
    endpoint,
    testing,
    connectivityStatus,
    showEditModal,
    isLocal,
    isNfs,
    isS3,
    typeInfo,
    addressDisplay,
    latestScanTask,
    isScanning,
    scanning,
    cancelling,
    liveProgress,
    analyticsData,
    loadingAnalytics,
    scanStats,
    scanStatusLabelMap,
    fetchData,
    onEditSuccess,
    handleTest,
    handleScan,
    handleCancelScan,
  }
}
