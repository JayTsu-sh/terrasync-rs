import { ref, onUnmounted } from 'vue'
import type { Ref } from 'vue'
import { listDirs } from '../api/endpoints'

interface PathOption {
  label: string
  value: string
}

/**
 * 提供路径输入的防抖自动补全功能。
 * 可选传入 storageUrl 参数用于 NFS 远程目录浏览。
 */
export function usePathAutocomplete(modelValue: Ref<string>, storageUrlFn?: () => string | undefined, delay = 400) {
  const options = ref<PathOption[]>([])
  let timer: ReturnType<typeof setTimeout> | null = null

  function clearTimer() {
    if (timer) {
      clearTimeout(timer)
      timer = null
    }
  }

  function handleInput(value: string) {
    modelValue.value = value
    clearTimer()

    if (!value.trim()) {
      options.value = []
      return
    }

    timer = setTimeout(async () => {
      try {
        const storageUrl = storageUrlFn?.()
        const resp = await listDirs(value, storageUrl)
        options.value = resp.entries.map((e) => {
          const path = storageUrl && !e.full_path.startsWith('/') ? '/' + e.full_path : e.full_path
          return { label: path, value: path + resp.sep }
        })
      } catch {
        options.value = []
      }
    }, delay)
  }

  function reset() {
    clearTimer()
    options.value = []
  }

  onUnmounted(clearTimer)

  return { options, handleInput, reset }
}
