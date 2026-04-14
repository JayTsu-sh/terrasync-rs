<script setup lang="ts">
import { computed } from 'vue'
import { NCard } from 'naive-ui'
import type { MigrationTask } from '../api/tasks'
import { needsAdvancedOptions } from '../shared/task-rules'

const props = defineProps<{
  task: MigrationTask
}>()

const showAdvanced = computed(() => needsAdvancedOptions(props.task.task_type))
const hasFilter = computed(
  () => props.task.task_type !== 'integrity_check' && (props.task.config.match_expr || props.task.config.exclude_expr),
)

interface ConfigItem {
  label: string
  value: string
}

const configItems = computed<ConfigItem[]>(() => {
  const items: ConfigItem[] = []
  if (showAdvanced.value) {
    items.push({ label: '带宽限速', value: props.task.config.qos || '不限制' })
    items.push({ label: 'IOPS 限速', value: props.task.config.iops ? String(props.task.config.iops) : '不限制' })
    items.push({ label: 'Block Size', value: props.task.config.block_size || '自适应' })
  }
  items.push({ label: '并发度', value: String(props.task.config.concurrency ?? 8) })
  if (showAdvanced.value) {
    items.push({ label: '完整性检查', value: props.task.config.enable_integrity_check ? '是' : '否' })
  }
  return items
})
</script>

<template>
  <NCard>
    <template #header>任务配置</template>

    <!-- ⚠️ 非默认: bg-gray-50 只读区域视觉区分 -->
    <div class="bg-gray-50 rounded-lg p-5">
      <div class="grid grid-cols-2 gap-4">
        <div v-for="item in configItems" :key="item.label">
          <!-- ⚠️ 非默认: label text-gray-500 + value text-gray-900 项目标准配置网格 -->
          <div class="text-xs text-gray-500 mb-1">{{ item.label }}</div>
          <div class="text-sm text-gray-900">{{ item.value }}</div>
        </div>
      </div>
    </div>

    <!-- 筛选条件（只读，显示原始表达式） -->
    <div v-if="hasFilter" class="mt-4">
      <div class="text-xs text-gray-400 font-semibold tracking-wider mb-3">筛选条件</div>
      <div v-if="task.config.match_expr" class="mb-3">
        <div class="text-xs text-gray-500 mb-1">包含条件</div>
        <div class="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2 font-mono break-all">
          {{ task.config.match_expr }}
        </div>
      </div>
      <div v-if="task.config.exclude_expr">
        <div class="text-xs text-gray-500 mb-1">排除条件</div>
        <div class="text-sm text-gray-900 bg-gray-50 rounded px-3 py-2 font-mono break-all">
          {{ task.config.exclude_expr }}
        </div>
      </div>
    </div>
  </NCard>
</template>
