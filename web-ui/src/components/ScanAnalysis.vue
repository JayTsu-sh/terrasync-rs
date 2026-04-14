<script setup lang="ts">
import { computed } from 'vue'
import { NTag, NSpin, NTabs, NTabPane, NButtonGroup, NButton } from 'naive-ui'
import type { ExecutionStats, TaskProgressSnapshot } from '../api/tasks'
import type { AnalyticsData } from '../api/analytics'
import { statusColorMap } from '../shared/task-rules'
import { formatNumber, formatBytes, formatDuration } from '../shared/formatters'
import { VChart } from '../composables/useEChartsLoader'
import { useAnalyticsCharts } from '../composables/useAnalyticsCharts'

const props = withDefaults(
  defineProps<{
    status: string
    statusLabel?: string
    errorMessage?: string
    isRunning: boolean
    liveProgress: TaskProgressSnapshot | null
    scanStats: ExecutionStats | null
    analyticsData: AnalyticsData | null
    loadingAnalytics: boolean
    pendingMessage?: string
    taskType?: string
  }>(),
  {
    pendingMessage: '任务尚未执行，启动后可查看扫描分析',
    taskType: 'scan',
  },
)

const analyticsRef = computed(() => props.analyticsData)
const scanStatsRef = computed(() => props.scanStats)

const { viewMode, activeTab, analyticsDbError, donutOption, usageBarOption, usageBarHeight } = useAnalyticsCharts(
  analyticsRef,
  scanStatsRef,
)

const displayLabel = computed(() => {
  if (props.statusLabel) return props.statusLabel
  if (props.status === 'running') {
    if (props.taskType === 'sync') return '迁移中'
    if (props.taskType === 'integrity_check') return '检查中'
    return '扫描中'
  }
  return props.status
})

// 根据任务类型返回不同的统计标签
const isSyncType = computed(() => props.taskType === 'sync')
const isIntegrityType = computed(() => props.taskType === 'integrity_check')

const analysisTitle = computed(() => {
  if (isSyncType.value) return '迁移分析'
  if (isIntegrityType.value) return '检查分析'
  return '扫描分析'
})

const scanSpeed = (progress: TaskProgressSnapshot) =>
  progress.elapsed_secs > 0 ? formatNumber(Math.round(progress.scanned_files / progress.elapsed_secs)) : '0'
</script>

<template>
  <div class="bg-white border border-gray-100 rounded-lg p-6 mb-6">
    <div class="flex items-center justify-between mb-4">
      <h2 class="text-xl font-semibold m-0">{{ analysisTitle }}</h2>
      <NTag :type="statusColorMap[status as keyof typeof statusColorMap] as any" size="medium">
        <template v-if="status === 'running'">
          <NSpin :size="12" class="mr-1" />
        </template>
        {{ displayLabel }}
      </NTag>
    </div>

    <div
      v-if="status === 'failed' && errorMessage"
      class="mb-4 p-3 rounded bg-red-50 text-red-600 text-sm border border-red-200"
    >
      {{ errorMessage }}
    </div>

    <!-- 实时进度 -->
    <div v-if="isRunning && liveProgress" class="bg-blue-50 border border-blue-200 rounded-[10px] p-5 mb-6">
      <div class="grid grid-cols-3 gap-4">
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">已扫描文件</span>
          <span class="text-xl font-semibold text-gray-700 tabular-nums">{{
            formatNumber(liveProgress.scanned_files)
          }}</span>
        </div>
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">已扫描目录</span>
          <span class="text-xl font-semibold text-gray-700 tabular-nums">{{
            formatNumber(liveProgress.scanned_dirs)
          }}</span>
        </div>
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">数据量</span>
          <span class="text-xl font-semibold text-gray-700 tabular-nums">{{
            formatBytes(liveProgress.total_bytes)
          }}</span>
        </div>
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">扫描速度</span>
          <span class="text-xl font-semibold text-gray-700 tabular-nums">{{ scanSpeed(liveProgress) }} 个/秒</span>
        </div>
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">已耗时</span>
          <span class="text-xl font-semibold text-gray-700 tabular-nums">{{
            formatDuration(liveProgress.elapsed_secs)
          }}</span>
        </div>
        <div class="flex flex-col items-center gap-1">
          <span class="text-xs text-gray-400 font-medium">错误数</span>
          <span
            class="text-xl font-semibold text-gray-700 tabular-nums"
            :class="{ 'text-red-500': liveProgress.error_count > 0 }"
          >
            {{ formatNumber(liveProgress.error_count) }}
          </span>
        </div>
      </div>
    </div>
    <div v-else-if="isRunning && !liveProgress" class="bg-blue-50 border border-blue-200 rounded-[10px] p-5 mb-6">
      <div class="flex items-center justify-center py-6 text-gray-400">
        <NSpin :size="16" class="mr-2" /> 正在初始化扫描...
      </div>
    </div>

    <!-- 概览统计 -->
    <div v-if="scanStats" class="bg-gray-50 border border-gray-200 rounded-[10px] p-5 mb-6">
      <div class="grid grid-cols-2 gap-6 items-start">
        <div class="flex flex-col items-center">
          <div class="text-[13px] text-gray-400 font-medium mb-3 uppercase tracking-wide">数据时间分布</div>
          <div v-if="donutOption" class="w-full">
            <VChart :option="donutOption" style="height: 250px" autoresize />
          </div>
          <div v-else class="h-[250px] flex items-center justify-center">
            <div class="text-gray-400 text-sm">
              {{ analyticsDbError ? '请先在设置中检查 ClickHouse 数据库配置' : '暂无时间分布数据' }}
            </div>
          </div>
          <div v-if="donutOption" class="flex items-center gap-2 mt-1">
            <span class="text-[11px] text-gray-400">最近</span>
            <div class="legend-bar"></div>
            <span class="text-[11px] text-gray-400">更早</span>
          </div>
        </div>

        <div class="py-2">
          <div class="text-[13px] text-gray-400 font-medium mb-3 uppercase tracking-wide">摘要统计</div>
          <!-- sync 类型使用迁移专用标签 -->
          <template v-if="isSyncType">
            <div class="flex flex-col">
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">已迁移</span>
                <span class="text-base font-semibold text-blue-600 tabular-nums"
                  >{{ formatNumber(scanStats.scanned_files) }} 个</span
                >
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">跳过</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums"
                  >{{ formatNumber(scanStats.scanned_dirs) }} 个</span
                >
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">失败</span>
                <span
                  class="text-base font-semibold text-gray-700 tabular-nums"
                  :class="{ 'text-red-500': scanStats.error_count > 0 }"
                  >{{ formatNumber(scanStats.error_count) }}</span
                >
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">迁移速率</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums"
                  >{{ formatBytes(scanStats.avg_speed_bytes_per_sec) }}/s</span
                >
              </div>
              <div class="flex justify-between items-center px-4 py-2.5 transition-colors hover:bg-gray-50">
                <span class="text-sm text-gray-500">耗时</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums">{{
                  formatDuration(scanStats.duration_secs)
                }}</span>
              </div>
            </div>
          </template>
          <!-- scan / integrity_check 类型使用原始标签 -->
          <template v-else>
            <div class="flex flex-col">
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">总文件数</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums">{{
                  formatNumber(scanStats.scanned_files)
                }}</span>
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">总目录数</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums">{{
                  formatNumber(scanStats.scanned_dirs)
                }}</span>
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">总数据量</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums">{{
                  formatBytes(scanStats.total_bytes)
                }}</span>
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">错误数</span>
                <span
                  class="text-base font-semibold text-gray-700 tabular-nums"
                  :class="{ 'text-red-500': scanStats.error_count > 0 }"
                >
                  {{ formatNumber(scanStats.error_count) }}
                </span>
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">扫描速度</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums"
                  >{{
                    scanStats.duration_secs > 0
                      ? formatNumber(Math.round(scanStats.scanned_files / scanStats.duration_secs))
                      : '0'
                  }}
                  个/秒</span
                >
              </div>
              <div
                class="flex justify-between items-center px-4 py-2.5 border-b border-gray-100 last:border-b-0 transition-colors hover:bg-gray-50"
              >
                <span class="text-sm text-gray-500">扫描耗时</span>
                <span class="text-base font-semibold text-gray-700 tabular-nums">{{
                  formatDuration(scanStats.duration_secs)
                }}</span>
              </div>
            </div>
          </template>
        </div>
      </div>
    </div>

    <!-- 分布图表 -->
    <div
      v-if="analyticsData && scanStats && scanStats.scanned_files > 0"
      class="bg-gray-50 border border-gray-200 rounded-[10px] p-5"
    >
      <div class="flex justify-between items-end mb-4">
        <NTabs v-model:value="activeTab" type="line" size="medium" class="flex-1">
          <NTabPane name="file-type" tab="文件类型" />
          <NTabPane name="file-size" tab="文件大小" />
        </NTabs>
        <NButtonGroup size="small" class="shrink-0 mb-2">
          <NButton :type="viewMode === 'size' ? 'primary' : 'default'" @click="viewMode = 'size'">按容量</NButton>
          <NButton :type="viewMode === 'count' ? 'primary' : 'default'" @click="viewMode = 'count'">按数量</NButton>
        </NButtonGroup>
      </div>
      <div v-if="usageBarOption" class="bg-white rounded-lg py-3">
        <VChart :option="usageBarOption" :style="{ height: usageBarHeight }" autoresize />
      </div>
      <div v-else class="text-center py-8 text-gray-400 text-sm">暂无分布数据</div>
    </div>

    <div v-else-if="loadingAnalytics" class="flex items-center justify-center py-8 text-gray-400">
      <NSpin :size="20" class="mr-2" /> 加载分析数据...
    </div>

    <div v-else-if="status === 'pending'" class="text-center py-8 text-gray-400 text-sm">
      {{ pendingMessage }}
    </div>
  </div>
</template>

<style scoped>
.legend-bar {
  width: 120px;
  height: 6px;
  border-radius: 3px;
  background: linear-gradient(to right, #e74c3c, #f39c12, #2ecc71, #3498db, #2c3e50);
}
</style>
