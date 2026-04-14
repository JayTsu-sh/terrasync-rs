import { onUnmounted } from 'vue'

/**
 * 简单的 setInterval 轮询封装，自动在组件卸载时停止。
 */
export function usePolling(callback: () => void, intervalMs: number) {
  let timer: ReturnType<typeof setInterval> | null = null

  function start() {
    stop()
    timer = setInterval(callback, intervalMs)
  }

  function stop() {
    if (timer) {
      clearInterval(timer)
      timer = null
    }
  }

  onUnmounted(stop)

  return { start, stop }
}
