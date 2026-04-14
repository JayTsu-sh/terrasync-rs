import { computed, ref } from 'vue'
import type { Ref } from 'vue'
import type { AnalyticsData } from '../api/analytics'
import type { ExecutionStats } from '../api/tasks'
import { formatBytes } from '../shared/formatters'

const WARM_TO_COLD = [
  '#e74c3c',
  '#e67e22',
  '#f39c12',
  '#f1c40f',
  '#2ecc71',
  '#1abc9c',
  '#3498db',
  '#2980b9',
  '#8e44ad',
  '#2c3e50',
]

export function useAnalyticsCharts(analyticsData: Ref<AnalyticsData | null>, scanStats: Ref<ExecutionStats | null>) {
  const viewMode = ref<'size' | 'count'>('size')
  const activeTab = ref('file-type')

  const totalSize = computed(() => {
    if (!analyticsData.value) return 0
    return analyticsData.value.file_type_distribution.reduce((sum, d) => sum + d.total_size, 0)
  })

  const analyticsDbError = computed(() => analyticsData.value?.db_error || null)

  const donutOption = computed(() => {
    if (!analyticsData.value) return null
    const dist = analyticsData.value.time_distribution
    if (dist.length === 0) return null

    const totalBytes = scanStats.value?.total_bytes || totalSize.value
    const centerText = formatBytes(totalBytes)

    return {
      tooltip: {
        trigger: 'item',
        formatter: (p: any) => `${p.name}<br/>文件数: ${p.value.toLocaleString()} (${p.percent}%)`,
      },
      graphic: [
        {
          type: 'group',
          left: 'center',
          top: 'center',
          children: [
            {
              type: 'text',
              style: { text: centerText, textAlign: 'center', fill: '#333', fontSize: 20, fontWeight: 'bold' },
              left: 'center',
              top: -12,
            },
            {
              type: 'text',
              style: { text: '总数据量', textAlign: 'center', fill: '#999', fontSize: 12 },
              left: 'center',
              top: 14,
            },
          ],
        },
      ],
      series: [
        {
          type: 'pie',
          radius: ['45%', '75%'],
          center: ['50%', '50%'],
          avoidLabelOverlap: true,
          itemStyle: { borderRadius: 4, borderColor: '#fff', borderWidth: 2 },
          label: { show: false },
          emphasis: {
            label: { show: true, fontSize: 13, fontWeight: 'bold' },
            itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.2)' },
          },
          data: dist.map((d, i) => ({
            name: d.bucket,
            value: d.count,
            itemStyle: { color: WARM_TO_COLD[i % WARM_TO_COLD.length] },
          })),
        },
      ],
    }
  })

  const usageBarOption = computed(() => {
    if (!analyticsData.value) return null
    const isFileType = activeTab.value === 'file-type'
    const rawData = isFileType ? analyticsData.value.file_type_distribution : analyticsData.value.size_distribution

    if (!rawData || rawData.length === 0) return null

    const isSizeMode = viewMode.value === 'size'

    const items = rawData.map((d) => ({
      label: 'ext' in d ? (d.ext ? (d.ext.startsWith('.') ? d.ext : '.' + d.ext) : '(无扩展名)') : d.range,
      size: d.total_size,
      count: d.count,
    }))

    items.sort((a, b) => (isSizeMode ? b.size - a.size : b.count - a.count))

    const total = items.reduce((s, d) => s + (isSizeMode ? d.size : d.count), 0)
    const labels = items.map((d) => d.label)
    const values = items.map((d) => (isSizeMode ? d.size : d.count))

    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        formatter: (params: any) => {
          const p = params[0]
          const idx = items.length - 1 - p.dataIndex
          const item = items[idx]
          return `${item.label}<br/>文件数: ${item.count.toLocaleString()}<br/>容量: ${formatBytes(item.size)}`
        },
      },
      xAxis: { type: 'value', show: false },
      yAxis: {
        type: 'category',
        data: labels.slice().reverse(),
        axisLabel: { width: 90, overflow: 'truncate', fontSize: 13 },
        axisTick: { show: false },
        axisLine: { show: false },
      },
      series: [
        {
          type: 'bar',
          data: values.slice().reverse(),
          itemStyle: { color: '#5470c6', borderRadius: [0, 4, 4, 0] },
          barMaxWidth: 28,
          label: {
            show: true,
            position: 'right',
            fontSize: 12,
            color: '#666',
            formatter: (p: any) => {
              const idx = items.length - 1 - p.dataIndex
              const item = items[idx]
              const val = isSizeMode ? formatBytes(item.size) : item.count.toLocaleString()
              const pct = total > 0 ? (((isSizeMode ? item.size : item.count) / total) * 100).toFixed(1) : '0'
              return `${val} (${pct}%)`
            },
          },
        },
      ],
      grid: { left: 100, right: 140, top: 8, bottom: 8 },
    }
  })

  const usageBarHeight = computed(() => {
    if (!analyticsData.value) return '200px'
    const isFileType = activeTab.value === 'file-type'
    const count = isFileType
      ? analyticsData.value.file_type_distribution.length
      : analyticsData.value.size_distribution.length
    return `${Math.max(160, count * 28 + 32)}px`
  })

  return {
    viewMode,
    activeTab,
    totalSize,
    analyticsDbError,
    donutOption,
    usageBarOption,
    usageBarHeight,
  }
}
