<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NTabs, NTabPane, NDataTable, NCard, NSpin, useMessage, useDialog } from 'naive-ui'
import BackLink from '../components/BackLink.vue'
import TaskInfoPanel from '../components/TaskInfoPanel.vue'
import TaskConfigPanel from '../components/TaskConfigPanel.vue'
import TaskEditModal from '../components/TaskEditModal.vue'
import ScanAnalysis from '../components/ScanAnalysis.vue'
import ExecutionLogViewer from '../components/ExecutionLogViewer.vue'
import { useTaskDetail } from '../composables/useTaskDetail'

const route = useRoute()
const router = useRouter()
const id = route.params.id as string
const message = useMessage()
const dialog = useDialog()

const showEditModal = ref(false)
const pageLoading = ref(true)

const detail = useTaskDetail(id, {
  onDeleteExecution(executionId: string) {
    dialog.warning({
      title: '确认删除',
      content: '确定要删除此执行记录吗？关联的日志也将被删除，此操作不可恢复。',
      positiveText: '删除',
      negativeText: '取消',
      onPositiveClick: async () => {
        try {
          await detail.performDeleteExecution(executionId)
          message.success('执行记录已删除')
        } catch (e: any) {
          message.error(e.message || '删除失败')
        }
      },
    })
  },
})

const {
  task,
  sourceEndpoint,
  destEndpoint,
  executions,
  liveProgress,
  isRunning,
  analyticsData,
  loadingAnalytics,
  scanStats,
  activeTab,
  expandedExecutionId,
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
} = detail

async function loadData() {
  try {
    await fetchData()
  } catch (e: any) {
    message.error(e.message || '获取任务详情失败')
  } finally {
    pageLoading.value = false
  }
}

async function onStart() {
  try {
    await handleStart()
    message.success('任务已启动')
  } catch (e: any) {
    message.error(e.message)
  }
}

async function onCancel() {
  try {
    await handleCancel()
    message.success('任务已取消')
  } catch (e: any) {
    message.error(e.message)
  }
}

function onDelete() {
  dialog.warning({
    title: '确认删除',
    content: `确定要删除任务「${task.value?.name}」吗？此操作不可恢复。`,
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await performDelete()
        message.success('任务已删除')
        router.push('/tasks')
      } catch (e: any) {
        message.error(e.message)
      }
    },
  })
}

function onConfigSaved() {
  loadData()
}

onMounted(loadData)
</script>

<template>
  <NSpin :show="pageLoading" class="min-h-[200px]">
    <div v-if="task">
      <BackLink to="/tasks" />

      <TaskInfoPanel
        :task="task"
        :source-endpoint="sourceEndpoint"
        :dest-endpoint="destEndpoint"
        @start="onStart"
        @cancel="onCancel"
        @delete="onDelete"
        @edit="showEditModal = true"
      />

      <NTabs v-model:value="activeTab" type="line">
        <NTabPane name="overview" tab="概览">
          <TaskConfigPanel :task="task" />
        </NTabPane>

        <NTabPane name="runs" tab="运行记录">
          <!-- 运行表 -->
          <NCard class="mb-4">
            <template #header>运行</template>
            <NDataTable :columns="executionColumns" :data="executions" :bordered="false" />
          </NCard>

          <!-- 展开报告 -->
          <ScanAnalysis
            v-if="expandedExecutionId || isRunning"
            :task-type="task.task_type"
            :status="task.status"
            :is-running="isRunning"
            :live-progress="liveProgress"
            :scan-stats="scanStats"
            :analytics-data="analyticsData"
            :loading-analytics="loadingAnalytics"
            pending-message="任务尚未执行，启动后可查看分析"
          />
        </NTabPane>
      </NTabs>

      <TaskEditModal v-model:show="showEditModal" :task="task" @saved="onConfigSaved" />

      <ExecutionLogViewer
        :execution-id="selectedExecutionId"
        :logs="logs"
        :loading="logsLoading"
        :has-more="logHasMore"
        :level-filter="logLevelFilter"
        @update:level-filter="logLevelFilter = $event"
        @load-more="loadMoreLogs"
        @close="closeLog"
      />
    </div>
  </NSpin>
</template>
