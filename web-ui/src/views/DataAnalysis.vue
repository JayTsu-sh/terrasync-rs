<script setup lang="ts">
import { computed, defineAsyncComponent, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NAlert, NButton, NEmpty, NIcon, NResult, NSelect, NSpin, NTag } from 'naive-ui'
import { RefreshOutline } from '@vicons/ionicons5'
import PageHeader from '../components/PageHeader.vue'
import { useAnalysisPage } from '../composables/useAnalysisPage'
import { statusLabelMap } from '../shared/task-rules'

const StorageExplorer = defineAsyncComponent(() => import('../components/StorageExplorer.vue'))
const route = useRoute()
const router = useRouter()
const requestedId = computed(() => (typeof route.query.task === 'string' ? route.query.task : ''))
const { tasks, selectedId, selectedTask, loading, fetchTasks } = useAnalysisPage(requestedId)
const error = ref('')
const options = computed(() =>
  tasks.value.map((task) => ({ value: task.id, label: `${task.name} · ${statusLabelMap[task.status]}` })),
)

async function load() {
  error.value = ''
  try {
    await fetchTasks()
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : '无法加载扫描任务'
  }
}

function chooseTask(id: string) {
  selectedId.value = id
  void router.replace({ path: '/analysis', query: { task: id } })
}

onMounted(load)
</script>

<template>
  <PageHeader title="数据分析">
    <template #right>
      <NButton @click="router.push('/endpoints')">选择扫描目标</NButton>
    </template>
  </PageHeader>
  <p class="text-sm text-gray-500 mt-0 mb-5">查看扫描数据的目录分布、容量和修改年龄，逐层定位并导出需要处理的数据。</p>

  <div v-if="loading" class="bg-white border border-gray-100 rounded-lg p-12 flex justify-center" role="status">
    <NSpin />
  </div>
  <NResult v-else-if="error" status="warning" title="无法加载扫描任务" :description="error">
    <template #footer><NButton @click="load">重新加载</NButton></template>
  </NResult>
  <div v-else-if="!tasks.length" class="bg-white border border-gray-100 rounded-lg p-12">
    <NEmpty description="还没有扫描任务"
      ><template #extra>请先选择一个存储目标并启动扫描，完成后即可查看目录树图。</template></NEmpty
    >
  </div>
  <template v-else>
    <div
      class="bg-white border border-gray-100 rounded-lg shadow-sm p-4 mb-5 flex flex-wrap items-center justify-between gap-3"
    >
      <div class="flex flex-wrap items-center gap-3 min-w-0">
        <span class="text-sm text-gray-500">扫描结果</span>
        <div class="w-full sm:w-[360px]">
          <NSelect
            :value="selectedId"
            :options="options"
            filterable
            placeholder="选择扫描任务"
            aria-label="选择扫描任务"
            @update:value="chooseTask"
          />
        </div>
        <NTag size="small" :bordered="false" type="info">最新扫描明细</NTag>
      </div>
      <div class="flex gap-2">
        <NButton :disabled="!selectedId" @click="router.push(`/tasks/${selectedId}`)">查看扫描任务</NButton>
        <NButton aria-label="刷新扫描任务" @click="load"
          ><template #icon
            ><NIcon><RefreshOutline /></NIcon></template
        ></NButton>
      </div>
    </div>
    <template v-if="selectedTask">
      <NAlert
        v-if="selectedTask.config.depth || selectedTask.config.match_expr || selectedTask.config.exclude_expr"
        :show-icon="false"
        class="mb-4"
      >
        扫描范围受以下配置限制
        <span v-if="selectedTask.config.depth"> · 扫描深度 {{ selectedTask.config.depth }}</span>
        <div v-if="selectedTask.config.match_expr" class="mt-1 break-all">
          匹配条件：{{ selectedTask.config.match_expr }}
        </div>
        <div v-if="selectedTask.config.exclude_expr" class="mt-1 break-all">
          排除条件：{{ selectedTask.config.exclude_expr }}
        </div>
      </NAlert>
      <div
        v-if="['pending', 'running'].includes(selectedTask.status)"
        class="bg-white border border-gray-100 rounded-lg p-12"
      >
        <NEmpty description="扫描完成后可浏览目录分布" />
      </div>
      <StorageExplorer v-else :key="`${selectedTask.id}:${selectedTask.finished_at}`" :task-id="selectedTask.id" />
    </template>
  </template>
</template>
