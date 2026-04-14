<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { NButton, NTag, useMessage } from 'naive-ui'
import BackLink from '../components/BackLink.vue'
import ScanAnalysis from '../components/ScanAnalysis.vue'
import EditEndpointModal from '../components/EditEndpointModal.vue'
import { useEndpointDetail } from '../composables/useEndpointDetail'

const route = useRoute()
const id = route.params.id as string
const message = useMessage()

const {
  endpoint,
  connectivityStatus,
  showEditModal,
  isNfs,
  isS3,
  typeInfo,
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
} = useEndpointDetail(id)

const timeDisplay = computed(() => {
  if (!endpoint.value) return ''
  const created = new Date(endpoint.value.created_at).toLocaleString()
  const updated = new Date(endpoint.value.updated_at).toLocaleString()
  if (endpoint.value.updated_at && updated !== created) {
    return `创建于 ${created} · 更新于 ${updated}`
  }
  return `创建于 ${created}`
})

const connInfo = computed(() => {
  const map = {
    testing: { dotClass: 'bg-yellow-400', text: '检测中', textClass: 'text-yellow-500' },
    ok: { dotClass: 'bg-green-500', text: '已连接', textClass: 'text-green-600' },
    fail: { dotClass: 'bg-red-500', text: '连接失败', textClass: 'text-red-500' },
  }
  return map[connectivityStatus.value]
})

// Config fields for display grid
type ConfigField = { label: string; value: string; muted?: boolean }
type ConfigRow = ConfigField[]

const configRows = computed<ConfigRow[]>(() => {
  if (!endpoint.value) return []
  const c = endpoint.value.config
  if (c.type === 'local') {
    return [[{ label: '本地路径', value: c.path || '' }]]
  }
  if (c.type === 'nfs') {
    return [
      [
        { label: '服务器地址', value: c.server || '' },
        { label: '端口', value: String(c.port || '') },
      ],
      [{ label: '共享路径', value: c.export_path || '' }],
      [{ label: '前缀路径', value: c.prefix || '(未设置)', muted: !c.prefix }],
    ]
  }
  if (c.type === 's3') {
    return [
      [
        { label: '服务器地址', value: c.host || '' },
        { label: '端口', value: String(c.port || '') },
      ],
      [
        { label: 'Access Key', value: (c.access_key || '').replace(/.(?=.{4})/g, '*') },
        { label: 'Secret Key', value: (c.secret_key || '').replace(/.(?=.{4})/g, '*') },
      ],
      [{ label: '存储桶', value: c.bucket || '' }],
      [{ label: '对象前缀', value: c.prefix || '(未设置)', muted: !c.prefix }],
    ]
  }
  return []
})

const configTitle = computed(() => {
  if (isNfs.value) return '连接配置'
  if (isS3.value) return 'S3 连接配置'
  return '存储配置'
})

async function loadData() {
  try {
    await fetchData()
  } catch (e: any) {
    message.error(e.message || '获取迁移目标详情失败')
  }
}

async function onTest() {
  try {
    await handleTest()
    message.success('连接成功')
  } catch (e: any) {
    message.error(e.message)
  }
}

async function onScan() {
  try {
    await handleScan()
    message.success('扫描任务已启动')
  } catch (e: any) {
    message.error(e.message || '启动扫描失败')
  }
}

async function onCancelScan() {
  try {
    await handleCancelScan()
    message.success('已发送取消请求')
  } catch (e: any) {
    message.error(e.message || '取消扫描失败')
  }
}

onMounted(loadData)
</script>

<template>
  <div v-if="endpoint">
    <BackLink to="/endpoints" />

    <!-- Detail Card -->
    <div class="bg-white border border-gray-200 rounded-lg shadow-sm mb-5 overflow-hidden">
      <!-- Header -->
      <div class="flex items-center justify-between px-6 py-5 border-b border-gray-100">
        <div>
          <div class="flex items-center gap-2.5">
            <span class="text-lg font-semibold text-gray-900">{{ endpoint.name }}</span>
            <NTag :color="{ color: typeInfo.bg, textColor: typeInfo.color, borderColor: typeInfo.border }" size="small">
              {{ typeInfo.label }}
            </NTag>
            <div class="flex items-center gap-1.5">
              <span :class="['inline-block w-2.5 h-2.5 rounded-full', connInfo.dotClass]"></span>
              <span :class="['text-xs', connInfo.textClass]">{{ connInfo.text }}</span>
            </div>
          </div>
          <div class="text-xs text-gray-400 mt-1">{{ timeDisplay }}</div>
        </div>
        <div class="flex items-center gap-2">
          <NButton v-if="isScanning" type="error" size="small" :loading="cancelling" @click="onCancelScan"
            >取消扫描</NButton
          >
          <NButton v-else type="primary" size="small" :loading="scanning" @click="onScan">扫描启动</NButton>
          <NButton size="small" @click="showEditModal = true">编辑</NButton>
          <NButton size="small" @click="onTest">测试连接</NButton>
        </div>
      </div>

      <!-- Config grid (read-only) -->
      <div class="px-6 py-5 bg-gray-50">
        <div class="text-base font-semibold text-gray-900 mb-4">{{ configTitle }}</div>
        <div class="space-y-3">
          <div v-for="(row, i) in configRows" :key="i" class="flex gap-8">
            <div v-for="field in row" :key="field.label" class="flex items-baseline gap-3 flex-1 min-w-0">
              <span class="text-xs text-gray-500 whitespace-nowrap shrink-0">{{ field.label }}:</span>
              <span :class="['text-sm truncate', field.muted ? 'text-gray-300 italic' : 'text-gray-900']">{{
                field.value
              }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Scan Analysis -->
    <div v-if="latestScanTask" class="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden">
      <ScanAnalysis
        :status="latestScanTask.status"
        :status-label="scanStatusLabelMap[latestScanTask.status]"
        :error-message="latestScanTask.error_message"
        :is-running="isScanning"
        :live-progress="liveProgress"
        :scan-stats="scanStats"
        :analytics-data="analyticsData"
        :loading-analytics="loadingAnalytics"
      />
    </div>

    <!-- Edit Modal -->
    <EditEndpointModal v-model:show="showEditModal" :endpoint="endpoint" @saved="onEditSuccess" />
  </div>
</template>
