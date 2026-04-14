import { ref, computed } from 'vue'
import type { MigrationTask } from '../api/tasks'
import { listTasks, deleteTask } from '../api/tasks'

export function useTaskList() {
  const tasks = ref<MigrationTask[]>([])
  const loading = ref(false)
  const searchQuery = ref('')

  const filteredTasks = computed(() => {
    const q = searchQuery.value.trim().toLowerCase()
    if (!q) return tasks.value
    return tasks.value.filter((t) => t.name.toLowerCase().includes(q))
  })

  async function fetchTasks() {
    loading.value = true
    try {
      tasks.value = await listTasks()
    } finally {
      loading.value = false
    }
  }

  async function removeTask(id: string) {
    await deleteTask(id)
    await fetchTasks()
  }

  return { tasks, loading, searchQuery, filteredTasks, fetchTasks, removeTask }
}
