import { describe, expect, it } from 'vitest'
import { shallowMount } from '@vue/test-utils'
import ScanAnalysis from '../ScanAnalysis.vue'
import type { ExecutionStats, TaskProgressSnapshot } from '../../api/tasks'

const stats: ExecutionStats = {
  skipped_count: 2, scanned_files: 1, scanned_dirs: 9, total_bytes: 5,
  transferred_bytes: 5, error_count: 0, duration_secs: 1, avg_speed_bytes_per_sec: 5,
}
const progress: TaskProgressSnapshot = {
  ...stats, elapsed_secs: 1, speed_bytes_per_sec: 5, skipped_count: 3, in_flight_bytes: 12,
}

describe('迁移跳过统计', () => {
  it('使用累计跳过数，并保留目录数和错误数的独立含义', async () => {
    const wrapper = shallowMount(ScanAnalysis, { props: {
      status: 'running', isRunning: true, taskType: 'sync', liveProgress: progress,
      scanStats: null, analyticsData: null, loadingAnalytics: false,
    } })
    expect(wrapper.get('[data-testid="live-copy-skips"]').text()).toContain('已跳过 3 个')
    expect(wrapper.get('[data-testid="live-copy-skips"]').text()).toContain('源端已保留')
    expect(wrapper.get('[data-testid="copy-in-flight-bytes"]').text()).toBe('12 B')
    expect(wrapper.get('[data-testid="copy-published-bytes"]').text()).toBe('5 B')
    await wrapper.setProps({ isRunning: false, status: 'completed', liveProgress: null, scanStats: stats })
    expect(wrapper.get('[data-testid="copy-skipped-count"]').text()).toBe('2 个')
    await wrapper.setProps({ scanStats: { ...stats, skipped_count: 0 } })
    expect(wrapper.get('[data-testid="copy-skipped-count"]').text()).toBe('0 个')
  })
})
