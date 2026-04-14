<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { NButton, NDataTable, NInput, NIcon, useMessage, useDialog } from 'naive-ui'
import { SearchOutline } from '@vicons/ionicons5'
import { useRouter } from 'vue-router'
import { startTask, cancelTask } from '../api/tasks'
import PageHeader from '../components/PageHeader.vue'
import TaskCreateModal from '../components/TaskCreateModal.vue'
import { useTaskList } from '../composables/useTaskList'
import { useTaskColumns } from '../composables/useTaskColumns'

const router = useRouter()
const message = useMessage()
const dialog = useDialog()
const showCreateModal = ref(false)

const { filteredTasks, loading, searchQuery, fetchTasks, removeTask } = useTaskList()

const pagination = computed(() =>
  filteredTasks.value.length > 10 ? { pageSize: 10, showSizePicker: true, pageSizes: [10, 20, 50] } : false,
)

const { columns } = useTaskColumns({
  onDetail: (id) => router.push(`/tasks/${id}`),
  onStart: async (id) => {
    try {
      await startTask(id)
      message.success('任务已启动')
      await fetchTasks()
    } catch (e: any) {
      message.error(e.message)
    }
  },
  onCancel: async (id) => {
    try {
      await cancelTask(id)
      message.success('任务已取消')
      await fetchTasks()
    } catch (e: any) {
      message.error(e.message)
    }
  },
  onDelete: (task) => {
    dialog.warning({
      title: '确认删除',
      content: `确定要删除任务「${task.name}」吗？此操作不可恢复。`,
      positiveText: '删除',
      negativeText: '取消',
      onPositiveClick: async () => {
        try {
          await removeTask(task.id)
          message.success('任务已删除')
        } catch (e: any) {
          message.error(e.message)
        }
      },
    })
  },
})

onMounted(async () => {
  try {
    await fetchTasks()
  } catch (e: any) {
    message.error(e.message || '加载任务列表失败')
  }
})
</script>

<template>
  <div>
    <PageHeader title="任务管理">
      <template #right>
        <div class="flex items-center gap-3">
          <NInput v-model:value="searchQuery" placeholder="搜索任务..." clearable class="w-60">
            <template #prefix>
              <NIcon :component="SearchOutline" class="text-gray-400" />
            </template>
          </NInput>
          <NButton type="primary" @click="showCreateModal = true">新建任务</NButton>
        </div>
      </template>
    </PageHeader>

    <div class="bg-white border border-gray-200 rounded-lg overflow-hidden shadow-sm">
      <NDataTable
        :columns="columns"
        :data="filteredTasks"
        :loading="loading"
        :bordered="false"
        :striped="true"
        :pagination="pagination"
      />
    </div>

    <TaskCreateModal v-model:show="showCreateModal" @created="fetchTasks" />
  </div>
</template>
