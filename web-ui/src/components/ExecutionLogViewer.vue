<script setup lang="ts">
import { NButton, NButtonGroup, NTag, NSpin } from 'naive-ui'
import type { TaskLog } from '../api/tasks'

defineProps<{
  executionId: string | null
  logs: TaskLog[]
  loading: boolean
  hasMore: boolean
  levelFilter: string | null
}>()

const emit = defineEmits<{
  'update:levelFilter': [value: string | null]
  loadMore: []
  close: []
}>()

const logLevelTagType: Record<string, string> = {
  info: 'default',
  warn: 'warning',
  error: 'error',
}
</script>

<template>
  <div v-if="executionId" id="log-section" class="bg-white border border-gray-100 rounded-lg mb-6 overflow-hidden">
    <div class="flex items-center justify-between px-6 py-5 border-b border-gray-100">
      <h3 class="text-base font-semibold text-gray-900 m-0">
        执行日志
        <span class="text-[13px] font-normal text-gray-400 ml-2 font-mono">{{ executionId.slice(0, 8) }}...</span>
      </h3>
      <NButton size="small" @click="emit('close')">关闭</NButton>
    </div>

    <div class="px-6 py-3 border-b border-gray-100 bg-gray-50">
      <NButtonGroup size="small">
        <NButton :type="levelFilter === null ? 'primary' : 'default'" @click="emit('update:levelFilter', null)"
          >全部</NButton
        >
        <NButton :type="levelFilter === 'info' ? 'primary' : 'default'" @click="emit('update:levelFilter', 'info')"
          >Info</NButton
        >
        <NButton :type="levelFilter === 'warn' ? 'primary' : 'default'" @click="emit('update:levelFilter', 'warn')"
          >Warn</NButton
        >
        <NButton :type="levelFilter === 'error' ? 'primary' : 'default'" @click="emit('update:levelFilter', 'error')"
          >Error</NButton
        >
      </NButtonGroup>
    </div>

    <div class="max-h-[500px] overflow-y-auto">
      <div v-if="loading" class="flex items-center justify-center py-8 text-gray-400">
        <NSpin :size="16" class="mr-2" /> 加载日志...
      </div>
      <template v-else-if="logs.length > 0">
        <div
          v-for="log in logs"
          :key="log.id"
          class="flex items-start gap-2.5 px-6 py-2 border-b border-gray-50 text-[13px] leading-relaxed hover:bg-gray-50"
        >
          <span class="shrink-0 text-gray-400 text-xs font-mono min-w-[150px]">{{
            new Date(log.created_at).toLocaleString()
          }}</span>
          <NTag :type="(logLevelTagType[log.level] || 'default') as any" size="small" class="shrink-0">
            {{ log.level.toUpperCase() }}
          </NTag>
          <span class="flex-1 text-gray-700 break-all">{{ log.message }}</span>
          <span
            v-if="log.file_path"
            class="shrink-0 max-w-[300px] overflow-hidden text-ellipsis whitespace-nowrap text-gray-400 text-xs font-mono"
            :title="log.file_path"
            >{{ log.file_path }}</span
          >
        </div>
        <div v-if="hasMore" class="text-center py-4">
          <NButton size="small" @click="emit('loadMore')">加载更多</NButton>
        </div>
      </template>
      <div v-else class="text-center py-8 text-gray-400 text-sm">暂无日志</div>
    </div>
  </div>
</template>
