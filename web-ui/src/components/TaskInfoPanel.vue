<script setup lang="ts">
import { computed } from 'vue'
import { NTag, NButton, NIcon } from 'naive-ui'
import { ArrowForward } from '@vicons/ionicons5'
import type { MigrationTask } from '../api/tasks'
import type { Endpoint } from '../api/endpoints'
import { TASK_ACTIONS, statusColorMap, statusLabelMap, taskTypeLabelMap } from '../shared/task-rules'
import type { TaskStatus } from '../shared/task-rules'
import EndpointCard from './EndpointCard.vue'

const props = defineProps<{
  task: MigrationTask
  sourceEndpoint: Endpoint | null
  destEndpoint: Endpoint | null
}>()

const emit = defineEmits<{
  start: []
  cancel: []
  delete: []
  edit: []
}>()

const taskTypeLabel = computed(() => taskTypeLabelMap[props.task.task_type] || props.task.task_type)
const status = computed(() => props.task.status as TaskStatus)
</script>

<template>
  <div class="bg-white border border-gray-200 rounded-lg overflow-hidden mb-6">
    <!-- Header -->
    <div class="flex items-center justify-between gap-3 px-6 py-5 border-b border-gray-100">
      <div class="flex items-center gap-3">
        <span class="text-lg font-semibold m-0 leading-snug">{{ task.name }}</span>
        <NTag :type="statusColorMap[status] as any" size="small">
          {{ statusLabelMap[status] || status }}
        </NTag>
        <!-- ⚠️ 非默认: monospace 短码 badge, NTag 不适配 -->
        <span class="bg-gray-100 px-1.5 py-0.5 rounded text-[11px] font-medium text-gray-500 font-mono">{{
          task.id.slice(0, 8)
        }}</span>
      </div>
      <div class="flex items-center gap-2">
        <span class="text-xs text-gray-300">创建 {{ new Date(task.created_at).toLocaleString() }}</span>
        <NButton v-if="TASK_ACTIONS.canEdit(status)" size="small" @click="emit('edit')">编辑</NButton>
        <NButton v-if="TASK_ACTIONS.canStart(status)" type="success" size="small" @click="emit('start')">启动</NButton>
        <NButton v-if="TASK_ACTIONS.canCancel(status)" size="small" @click="emit('cancel')">取消</NButton>
        <NButton v-if="TASK_ACTIONS.canDelete(status)" type="error" size="small" @click="emit('delete')">删除</NButton>
      </div>
    </div>

    <!-- Flow: source → dest -->
    <div v-if="sourceEndpoint" class="px-6 py-5 border-b border-gray-100">
      <div class="flex items-center justify-center gap-4">
        <EndpointCard :endpoint="sourceEndpoint" />
        <!-- ⚠️ 非默认: text-gray-300 装饰性箭头 -->
        <NIcon :size="24" class="text-gray-300 shrink-0"><ArrowForward /></NIcon>
        <EndpointCard
          :endpoint="destEndpoint"
          :placeholder="destEndpoint ? undefined : { label: '无需目标', description: `${taskTypeLabel} 任务` }"
        />
      </div>
    </div>

    <!-- Error -->
    <div v-if="task.error_message" class="px-6 py-3 bg-red-50 border-b border-red-200 text-red-600 text-[13px]">
      {{ task.error_message }}
    </div>
  </div>
</template>
