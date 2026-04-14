import { ref, computed, watch, nextTick, h } from 'vue'
import { NTag } from 'naive-ui'
import {
  getTask,
  startTask,
  cancelTask,
  deleteTask,
  deleteExecution,
  listExecutions,
  getTaskProgress,
  getExecutionLogs,
} from '../api/tasks'
import type { MigrationTask, TaskExecution, ExecutionStats, TaskProgressSnapshot, TaskLog } from '../api/tasks'
import { getEndpoint } from '../api/endpoints'
import type { Endpoint } from '../api/endpoints'
import { getFilterFields } from '../api/filters'
import type { FilterField } from '../api/filters'
import { getTaskAnalytics } from '../api/analytics'
import type { AnalyticsData } from '../api/analytics'
import { statusColorMap, statusLabelMap, taskTypeLabelMap } from '../shared/task-rules'
import type { TaskStatus } from '../shared/task-rules'
import { formatNumber, formatBytes, formatDuration } from '../shared/formatters'
import { usePolling } from './usePolling'

interface TaskDetailOptions {
  onDeleteExecution?: (id: string) => void
}

export function useTaskDetail(id: string, options?: TaskDetailOptions) {
  const task = ref<MigrationTask | null>(null)
  const sourceEndpoint = ref<Endpoint | null>(null)
  const destEndpoint = ref<Endpoint | null>(null)
  const executions = ref<TaskExecution[]>([])
  const liveProgress = ref<TaskProgressSnapshot | null>(null)
  const analyticsData = ref<AnalyticsData | null>(null)
  const loadingAnalytics = ref(false)
  const filterFields = ref<FilterField[]>([])

  // Tab state
  const activeTab = ref<string>('runs')
  const expandedExecutionId = ref<string | null>(null)

  const isRunning = computed(() => task.value?.status === 'running')
  const scanStats = computed<ExecutionStats | null>(() => {
    // If there is an expanded execution, use its stats
    if (expandedExecutionId.value) {
      const exec = executions.value.find((e) => e.id === expandedExecutionId.value)
      return exec?.stats || null
    }
    const completed = executions.value.findLast((e) => e.status === 'completed' && e.stats)
    return completed?.stats || null
  })
  const taskTypeLabel = computed(() => {
    if (!task.value) return ''
    return taskTypeLabelMap[task.value.task_type] || task.value.task_type
  })

  // Polling
  const polling = usePolling(pollProgress, 3000)

  async function pollProgress() {
    try {
      const prevStatus = task.value?.status
      task.value = await getTask(id)
      if (task.value.status === 'running') {
        try {
          liveProgress.value = await getTaskProgress(id)
        } catch (e) {
          console.warn('获取实时进度失败:', e)
        }
      } else {
        liveProgress.value = null
        if (prevStatus === 'running') {
          executions.value = await listExecutions(id)
          // 任务刚完成，自动跳转到最新执行的报告
          const latestWithStats = executions.value.findLast((e) => e.stats)
          if (latestWithStats) expandedExecutionId.value = latestWithStats.id
          if (task.value.status === 'completed') loadAnalytics()
        }
      }
    } catch (e) {
      console.warn('轮询任务状态失败:', e)
    }
  }

  watch(
    () => task.value?.status,
    (status) => {
      if (status === 'running') polling.start()
      else polling.stop()
    },
  )

  // Logs
  const selectedExecutionId = ref<string | null>(null)
  const logLevelFilter = ref<string | null>(null)
  const logs = ref<TaskLog[]>([])
  const logsLoading = ref(false)
  const logOffset = ref(0)
  const logHasMore = ref(true)
  const LOG_PAGE_SIZE = 100

  watch([selectedExecutionId, logLevelFilter], async ([execId, level]) => {
    if (!execId) {
      logs.value = []
      return
    }
    logsLoading.value = true
    logOffset.value = 0
    logHasMore.value = true
    try {
      const result = await getExecutionLogs(execId, { level: level || undefined, offset: 0, limit: LOG_PAGE_SIZE })
      logs.value = result
      logHasMore.value = result.length >= LOG_PAGE_SIZE
    } catch {
      logs.value = []
      throw new Error('加载日志失败')
    } finally {
      logsLoading.value = false
    }
    await nextTick()
    document.getElementById('log-section')?.scrollIntoView({ behavior: 'smooth' })
  })

  async function loadMoreLogs() {
    if (!selectedExecutionId.value) return
    logOffset.value += LOG_PAGE_SIZE
    try {
      const more = await getExecutionLogs(selectedExecutionId.value, {
        level: logLevelFilter.value || undefined,
        offset: logOffset.value,
        limit: LOG_PAGE_SIZE,
      })
      logs.value.push(...more)
      logHasMore.value = more.length >= LOG_PAGE_SIZE
    } catch (e) {
      console.warn('加载更多日志失败:', e)
    }
  }

  function closeLog() {
    selectedExecutionId.value = null
    logs.value = []
  }

  // Execution columns — matches design: ID, 状态, 开始时间, 持续时间, 迁移容量, 迁移速率, 操作
  const executionColumns = computed(() => {
    const isSync = task.value?.task_type === 'sync'
    return [
      {
        title: 'ID',
        key: 'id',
        width: 50,
        render: (_row: TaskExecution, index: number) => String(executions.value.length - index),
      },
      {
        title: '状态',
        key: 'status',
        width: 80,
        render: (row: TaskExecution) =>
          h(
            NTag,
            { type: statusColorMap[row.status as TaskStatus] as any, size: 'small' },
            { default: () => statusLabelMap[row.status as TaskStatus] || row.status },
          ),
      },
      {
        title: '开始时间',
        key: 'started_at',
        width: 160,
        render: (row: TaskExecution) => new Date(row.started_at).toLocaleString(),
      },
      {
        title: '持续时间',
        key: 'duration',
        width: 80,
        render: (row: TaskExecution) => (row.stats ? formatDuration(row.stats.duration_secs) : '--'),
      },
      {
        title: isSync ? '迁移容量' : '扫描容量',
        key: 'volume',
        width: 100,
        render: (row: TaskExecution) => (row.stats ? formatBytes(row.stats.total_bytes) : '--'),
      },
      {
        title: isSync ? '迁移速率' : '扫描速度',
        key: 'speed',
        render: (row: TaskExecution) => {
          if (!row.stats) return '--'
          if (isSync) return `${formatBytes(row.stats.avg_speed_bytes_per_sec)}/s`
          return row.stats.duration_secs > 0
            ? `${formatNumber(Math.round(row.stats.scanned_files / row.stats.duration_secs))} 个/秒`
            : '--'
        },
      },
      {
        title: '操作',
        key: 'actions',
        width: 140,
        render: (row: TaskExecution) => {
          const isRunning = row.status === 'running'
          return h('span', { class: 'space-x-3' }, [
            h(
              'span',
              {
                class: 'text-blue-500 cursor-pointer',
                onClick: () => {
                  expandedExecutionId.value = expandedExecutionId.value === row.id ? null : row.id
                },
              },
              '查看报告',
            ),
            h(
              'span',
              {
                class: isRunning ? 'text-gray-300 cursor-not-allowed' : 'text-red-500 cursor-pointer',
                onClick: () => {
                  if (!isRunning) options?.onDeleteExecution?.(row.id)
                },
              },
              '删除',
            ),
          ])
        },
      },
    ]
  })

  // Data loading
  async function loadAnalytics() {
    if (analyticsData.value || loadingAnalytics.value) return
    loadingAnalytics.value = true
    try {
      analyticsData.value = await getTaskAnalytics(id)
    } catch (e) {
      console.warn('加载分析数据失败:', e)
    }
    loadingAnalytics.value = false
  }

  // Throws on failure
  async function fetchData() {
    task.value = await getTask(id)
    const [srcEp, destEp, execs, fields] = await Promise.all([
      getEndpoint(task.value.source_endpoint_id).catch(() => null),
      task.value.dest_endpoint_id ? getEndpoint(task.value.dest_endpoint_id).catch(() => null) : Promise.resolve(null),
      listExecutions(id),
      getFilterFields().catch(() => [] as FilterField[]),
    ])
    sourceEndpoint.value = srcEp
    destEndpoint.value = destEp
    executions.value = execs
    filterFields.value = fields

    // 页面加载时自动展开最新有 stats 的执行
    if (!expandedExecutionId.value) {
      const latestWithStats = execs.findLast((e) => e.stats)
      if (latestWithStats) expandedExecutionId.value = latestWithStats.id
    }

    if (task.value.status === 'running') {
      try {
        liveProgress.value = await getTaskProgress(id)
      } catch (e) {
        console.warn('获取实时进度失败:', e)
      }
    }
    if (task.value.status === 'completed') loadAnalytics()
  }

  // Throws on failure
  async function handleStart() {
    await startTask(id)
    analyticsData.value = null
    await fetchData()
  }

  // Throws on failure
  async function handleCancel() {
    await cancelTask(id)
    await fetchData()
  }

  // Throws on failure — view layer handles confirmation dialog
  async function performDelete() {
    await deleteTask(id)
  }

  // Throws on failure — view layer handles confirmation dialog
  async function performDeleteExecution(executionId: string) {
    await deleteExecution(executionId)
    executions.value = executions.value.filter((e) => e.id !== executionId)
    if (expandedExecutionId.value === executionId) expandedExecutionId.value = null
    if (selectedExecutionId.value === executionId) closeLog()
  }

  return {
    task,
    sourceEndpoint,
    destEndpoint,
    executions,
    liveProgress,
    isRunning,
    analyticsData,
    loadingAnalytics,
    scanStats,
    taskTypeLabel,
    activeTab,
    expandedExecutionId,
    filterFields,
    selectedExecutionId,
    logLevelFilter,
    logs,
    logsLoading,
    logHasMore,
    executionColumns,
    loadMoreLogs,
    closeLog,
    fetchData,
    handleStart,
    handleCancel,
    performDelete,
    performDeleteExecution,
  }
}
