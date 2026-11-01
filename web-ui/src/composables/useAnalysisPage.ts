import { computed, ref, watch, type Ref } from 'vue'
import { listTasks, type MigrationTask } from '../api/tasks'

/** 独立分析入口只使用扫描任务，优先展示已完成的数据。 */
export function useAnalysisPage(requestedId: Ref<string>) {
  const tasks = ref<MigrationTask[]>([])
  const selectedId = ref<string | null>(null)
  const loading = ref(false)
  const selectedTask = computed(() => tasks.value.find((task) => task.id === selectedId.value) ?? null)

  function chooseTask() {
    const requested = tasks.value.find((task) => task.id === requestedId.value)
    const current = tasks.value.find((task) => task.id === selectedId.value)
    selectedId.value =
      (requested ?? current ?? tasks.value.find((task) => task.status === 'completed') ?? tasks.value[0])?.id ?? null
  }

  async function fetchTasks() {
    loading.value = true
    try {
      const result = await listTasks({ task_type: 'scan' })
      tasks.value = result
        .filter((task) => task.task_type === 'scan')
        .sort((a, b) => Date.parse(b.finished_at ?? b.created_at) - Date.parse(a.finished_at ?? a.created_at))
      chooseTask()
    } finally {
      loading.value = false
    }
  }

  watch(requestedId, chooseTask)
  return { tasks, selectedId, selectedTask, loading, fetchTasks }
}
